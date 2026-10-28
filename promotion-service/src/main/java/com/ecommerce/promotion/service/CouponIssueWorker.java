package com.ecommerce.promotion.service;

import com.ecommerce.promotion.config.PromotionIssueProperties;
import com.ecommerce.promotion.model.PromotionIssue;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumer;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * redis-async: writes the winners queued on {@link CouponStockStore#ISSUE_STREAM} to MySQL.
 *
 * <p>Every pod runs one worker in the consumer group {@value #GROUP}, so Redis hands each entry
 * to one of them. An entry is written, its pending mark dropped, then acknowledged and deleted,
 * in that order: an entry handed out again after a crash only meets its own row (the unique
 * key) and is acknowledged, so a winner is never written twice.</p>
 *
 * <p>Reads do not block: a blocking read would hold a Redis connection longer than the command
 * timeout. An empty read waits {@code worker-poll-interval} instead.</p>
 *
 * <p>Every {@code worker-reclaim-interval} the worker also takes over entries left
 * unacknowledged for {@code worker-claim-idle} (a pod died, or the write failed) and retries
 * them. An entry handed out more than {@code worker-max-deliveries} times whose row MySQL still
 * refuses is moved to {@link CouponStockStore#ISSUE_DEAD_LETTER}, the user is marked failed and
 * the coupon is given back. Consumers of old pods with nothing pending are removed.</p>
 */
@Slf4j
@Component
public class CouponIssueWorker implements SmartLifecycle {

    static final String GROUP = "coupon-writer";

    private static final Duration RETRY_AFTER_FAILURE = Duration.ofSeconds(1);
    private static final Duration STOP_WAIT = Duration.ofSeconds(10);

    private final StringRedisTemplate redis;
    private final CouponStockStore couponStockStore;
    private final PromotionRepository promotionRepository;
    private final PromotionIssueRepository promotionIssueRepository;
    private final PromotionIssueProperties properties;
    private final CouponIssueQueueMonitor monitor;
    private final TransactionTemplate transactionTemplate;
    private final String consumerName = consumerName();

    private volatile boolean running;
    private volatile boolean groupReady;
    private Thread thread;
    private long nextMaintenance;

    public CouponIssueWorker(StringRedisTemplate redis,
                             CouponStockStore couponStockStore,
                             PromotionRepository promotionRepository,
                             PromotionIssueRepository promotionIssueRepository,
                             PromotionIssueProperties properties,
                             CouponIssueQueueMonitor monitor,
                             PlatformTransactionManager transactionManager) {
        this.redis = redis;
        this.couponStockStore = couponStockStore;
        this.promotionRepository = promotionRepository;
        this.promotionIssueRepository = promotionIssueRepository;
        this.properties = properties;
        this.monitor = monitor;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    @Override
    public boolean isAutoStartup() {
        return properties.getStore() == PromotionIssueProperties.Store.REDIS_ASYNC;
    }

    @Override
    public void start() {
        running = true;
        thread = Thread.ofVirtual().name("coupon-issue-worker").start(this::run);
        log.info("Coupon issue worker started as {} in group {}", consumerName, GROUP);
    }

    /** Lets the current batch finish; anything not acknowledged is handed out again later. */
    @Override
    public void stop() {
        running = false;
        if (thread != null) {
            try {
                thread.join(STOP_WAIT);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public boolean isRunning() {
        return running;
    }

    private void run() {
        while (running) {
            try {
                if (pollOnce() == 0) {
                    pause(properties.getWorkerPollInterval());
                }
                maintainIfDue();
            } catch (RuntimeException e) {
                if (mentions(e, "NOGROUP")) {
                    // The stream was removed (e.g. Redis lost its data): create the group again.
                    groupReady = false;
                }
                log.warn("Coupon issue worker failed, retrying: {}", e.getMessage());
                pause(RETRY_AFTER_FAILURE);
            }
        }
    }

    /**
     * Reads the next batch for this consumer and writes it.
     *
     * @return the number of entries read
     */
    int pollOnce() {
        ensureGroup();
        List<MapRecord<String, Object, Object>> records = redis.opsForStream().read(
                Consumer.from(GROUP, consumerName),
                StreamReadOptions.empty().count(properties.getWorkerBatchSize()),
                StreamOffset.create(CouponStockStore.ISSUE_STREAM, ReadOffset.lastConsumed()));
        if (records == null || records.isEmpty()) {
            return 0;
        }
        write(records);
        return records.size();
    }

    private void maintainIfDue() {
        long now = System.nanoTime();
        if (now - nextMaintenance < 0) {
            return;
        }
        nextMaintenance = now + properties.getWorkerReclaimInterval().toNanos();
        reclaimStuck();
        removeIdleConsumers();
    }

    /**
     * Takes over entries no consumer acknowledged within {@code worker-claim-idle} and writes
     * them again. One handed out more than {@code worker-max-deliveries} times gets one last try
     * and is given up on only if MySQL refuses its row; if MySQL cannot be reached at all, the
     * entry is kept, so an outage does not give back the coupons of everyone queued meanwhile.
     *
     * @return the number of entries taken over
     */
    int reclaimStuck() {
        ensureGroup();
        Duration idle = properties.getWorkerClaimIdle();
        Map<RecordId, Long> deliveries = new LinkedHashMap<>();
        for (CouponStockStore.StuckEntry entry
                : couponStockStore.stuckQueued(GROUP, idle, properties.getWorkerBatchSize())) {
            deliveries.put(entry.id(), entry.deliveries());
        }
        if (deliveries.isEmpty()) {
            return 0;
        }
        // min-idle again here, so two pods reclaiming at once do not both take the same entry.
        List<MapRecord<String, Object, Object>> taken = redis.opsForStream().claim(CouponStockStore.ISSUE_STREAM,
                GROUP, consumerName, idle, deliveries.keySet().toArray(RecordId[]::new));
        monitor.recordReclaimed(taken.size());
        List<MapRecord<String, Object, Object>> retry = new ArrayList<>();
        for (MapRecord<String, Object, Object> record : taken) {
            long handedOut = deliveries.getOrDefault(record.getId(), 0L) + 1;
            if (handedOut > properties.getWorkerMaxDeliveries()) {
                lastTry(record, handedOut);
            } else {
                retry.add(record);
            }
        }
        if (!retry.isEmpty()) {
            log.info("Retrying {} coupon issue entries left unacknowledged", retry.size());
            write(retry);
        }
        return taken.size();
    }

    /** Writes the entry once more; gives up only when MySQL refuses the row (or it is unreadable). */
    private void lastTry(MapRecord<String, Object, Object> record, long handedOut) {
        Optional<QueuedClaim> claim = QueuedClaim.of(record);
        // writeAlone throws when MySQL cannot be reached: the entry stays for the next round.
        if (claim.isPresent() && writeAlone(claim.get())) {
            finish(List.of(claim.get()));
            return;
        }
        claim.ifPresent(c -> {
            // Failed first, then given back: the user's status never reads as never-claimed.
            couponStockStore.markFailed(c.promotionId(), c.userId());
            couponStockStore.undoClaim(c.promotionId(), c.userId(), true);
        });
        Map<String, String> entry = new HashMap<>();
        record.getValue().forEach((k, v) -> entry.put(String.valueOf(k), String.valueOf(v)));
        entry.put("sourceId", record.getId().getValue());
        entry.put("deliveries", String.valueOf(handedOut));
        entry.put("deadAt", String.valueOf(Instant.now().toEpochMilli()));
        couponStockStore.deadLetterQueued(GROUP, record.getId(), entry);
        monitor.recordDeadLettered();
        log.error("Gave up on coupon issue entry {} after {} deliveries: {}", record.getId(), handedOut,
                record.getValue());
    }

    /** Pods get a new name on every start; their consumers stay in the group until removed. */
    private void removeIdleConsumers() {
        Duration expiry = properties.getWorkerConsumerExpiry();
        for (XInfoConsumer consumer : redis.opsForStream().consumers(CouponStockStore.ISSUE_STREAM, GROUP)) {
            if (!consumer.consumerName().equals(consumerName) && consumer.pendingCount() == 0
                    && consumer.idleTime().compareTo(expiry) >= 0) {
                redis.opsForStream().deleteConsumer(CouponStockStore.ISSUE_STREAM,
                        Consumer.from(GROUP, consumer.consumerName()));
                log.info("Removed idle coupon issue consumer {}", consumer.consumerName());
            }
        }
    }

    private void write(List<MapRecord<String, Object, Object>> records) {
        List<QueuedClaim> claims = new ArrayList<>();
        for (MapRecord<String, Object, Object> record : records) {
            QueuedClaim.of(record).ifPresentOrElse(claims::add,
                    () -> log.error("Unreadable coupon issue entry {}: {}", record.getId(), record.getValue()));
        }
        List<QueuedClaim> written;
        try {
            transactionTemplate.executeWithoutResult(status -> claims.forEach(this::insert));
            written = claims;
        } catch (DataIntegrityViolationException e) {
            // A duplicate (an entry handed out again) or a bad entry: one at a time, so the rest
            // of the batch still goes through.
            written = claims.stream().filter(this::writeAlone).toList();
        }
        finish(written);
    }

    private boolean writeAlone(QueuedClaim claim) {
        try {
            transactionTemplate.executeWithoutResult(status -> insert(claim));
            return true;
        } catch (DataIntegrityViolationException e) {
            if (PromotionIssueService.isDuplicateIssue(e)) {
                return true;
            }
            log.warn("Could not write coupon {} for {}: {}", claim.promotionId(), claim.userId(),
                    e.getMostSpecificCause().getMessage());
            return false;
        }
    }

    private void insert(QueuedClaim claim) {
        promotionIssueRepository.saveAndFlush(PromotionIssue.builder()
                .promotion(promotionRepository.getReferenceById(claim.promotionId()))
                .userId(claim.userId())
                .issuedAt(claim.claimedAt())
                .build());
    }

    /** Rows are committed: drop the pending marks, acknowledge and delete the entries, in one step. */
    private void finish(List<QueuedClaim> written) {
        if (written.isEmpty()) {
            return;
        }
        couponStockStore.completeQueued(GROUP,
                written.stream().map(QueuedClaim::id).toList(),
                written.stream().map(QueuedClaim::promotionId).toList(),
                written.stream().map(QueuedClaim::userId).toList());
        monitor.recordWritten(written.size());
    }

    private void ensureGroup() {
        if (groupReady) {
            return;
        }
        byte[] key = CouponStockStore.ISSUE_STREAM.getBytes(StandardCharsets.UTF_8);
        try {
            // MKSTREAM: the group can exist before the first claim.
            redis.execute((RedisCallback<String>) connection ->
                    connection.streamCommands().xGroupCreate(key, GROUP, ReadOffset.from("0-0"), true));
        } catch (DataAccessException e) {
            if (!mentions(e, "BUSYGROUP")) {
                throw e;
            }
        }
        groupReady = true;
    }

    private static boolean mentions(Throwable e, String text) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t.getMessage() != null && t.getMessage().contains(text)) {
                return true;
            }
        }
        return false;
    }

    private void pause(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }

    /** The pod name in Kubernetes, so each pod is its own consumer. */
    private static String consumerName() {
        String host = System.getenv("HOSTNAME");
        if (host != null && !host.isBlank()) {
            return host;
        }
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "worker-" + UUID.randomUUID();
        }
    }

    record QueuedClaim(RecordId id, Long promotionId, String userId, Instant claimedAt) {

        static Optional<QueuedClaim> of(MapRecord<String, Object, Object> record) {
            Map<Object, Object> value = record.getValue();
            try {
                return Optional.of(new QueuedClaim(record.getId(),
                        Long.valueOf((String) value.get("promotionId")),
                        Objects.requireNonNull((String) value.get("userId")),
                        Instant.ofEpochMilli(Long.parseLong((String) value.get("claimedAt")))));
            } catch (RuntimeException e) {
                return Optional.empty();
            }
        }
    }
}
