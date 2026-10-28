package com.ecommerce.promotion.service;

import com.ecommerce.promotion.config.PromotionIssueProperties;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.ToDoubleFunction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.Limit;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessagesSummary;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

/**
 * How far behind the redis-async queue is, for Prometheus and for the claim status poll interval.
 *
 * <ul>
 *   <li>{@code coupon_issue_queue_length}: entries not written yet (the worker deletes written
 *       ones), {@code coupon_issue_queue_pending} of them handed out and not acknowledged</li>
 *   <li>{@code coupon_issue_queue_oldest_age_seconds}: how long the oldest entry has waited</li>
 *   <li>{@code coupon_issue_dead_letters}: entries given up on, for a person to look at</li>
 *   <li>{@code coupon_issue_written_total}, {@code _reclaimed_total}, {@code _dead_lettered_total}</li>
 * </ul>
 *
 * <p>Redis is read at most once a {@link #REFRESH}, whether by a scrape or by polling callers;
 * anything else gets the last reading. Nothing is read unless the store is redis-async.</p>
 */
@Slf4j
@Component
public class CouponIssueQueueMonitor {

    static final Duration REFRESH = Duration.ofSeconds(1);
    // The poll interval grows with the queue age, up to this many times the configured one.
    private static final long MAX_POLL_FACTOR = 3;
    private static final long AGE_PER_POLL_STEP_MS = 5_000;

    private final StringRedisTemplate redis;
    private final PromotionIssueProperties properties;
    private final Counter written;
    private final Counter reclaimed;
    private final Counter deadLettered;
    private final AtomicLong nextRefresh = new AtomicLong(System.nanoTime());
    private volatile Snapshot snapshot = Snapshot.EMPTY;

    record Snapshot(long length, long pending, long deadLetters, long oldestAgeMs) {
        static final Snapshot EMPTY = new Snapshot(0, 0, 0, 0);
    }

    public CouponIssueQueueMonitor(StringRedisTemplate redis, PromotionIssueProperties properties,
                                   MeterRegistry registry) {
        this.redis = redis;
        this.properties = properties;
        gauge(registry, "coupon.issue.queue.length", "Queued winners not written to MySQL yet", Snapshot::length);
        gauge(registry, "coupon.issue.queue.pending", "Queued winners handed to a worker, not acknowledged",
                Snapshot::pending);
        gauge(registry, "coupon.issue.dead.letters", "Queue entries given up on", Snapshot::deadLetters);
        Gauge.builder("coupon.issue.queue.oldest.age", this, m -> m.current().oldestAgeMs() / 1000.0)
                .description("How long the oldest queued winner has waited")
                .baseUnit("seconds")
                .register(registry);
        this.written = Counter.builder("coupon.issue.written")
                .description("Queued winners written to MySQL").register(registry);
        this.reclaimed = Counter.builder("coupon.issue.reclaimed")
                .description("Unacknowledged entries taken over from another consumer").register(registry);
        this.deadLettered = Counter.builder("coupon.issue.dead.lettered")
                .description("Entries given up on, coupon given back").register(registry);
    }

    private void gauge(MeterRegistry registry, String name, String description, ToDoubleFunction<Snapshot> value) {
        Gauge.builder(name, this, m -> value.applyAsDouble(m.current())).description(description).register(registry);
    }

    /** The configured status poll interval, longer while the queue is behind. */
    public long pollAfterMs() {
        return pollAfterMs(properties.getStatusPollAfter().toMillis(), current().oldestAgeMs());
    }

    static long pollAfterMs(long baseMs, long oldestAgeMs) {
        long steps = Math.min(MAX_POLL_FACTOR, 1 + oldestAgeMs / AGE_PER_POLL_STEP_MS);
        return baseMs * steps;
    }

    void recordWritten(int count) {
        written.increment(count);
    }

    void recordReclaimed(int count) {
        reclaimed.increment(count);
    }

    void recordDeadLettered() {
        deadLettered.increment();
    }

    Snapshot current() {
        if (properties.getStore() != PromotionIssueProperties.Store.REDIS_ASYNC) {
            return Snapshot.EMPTY;
        }
        long now = System.nanoTime();
        long due = nextRefresh.get();
        // One caller refreshes; the rest use the last reading meanwhile.
        if (now - due >= 0 && nextRefresh.compareAndSet(due, now + REFRESH.toNanos())) {
            try {
                snapshot = read();
            } catch (DataAccessException e) {
                log.debug("Could not read the coupon issue queue: {}", e.getMessage());
            }
        }
        return snapshot;
    }

    private Snapshot read() {
        Long length = redis.opsForStream().size(CouponStockStore.ISSUE_STREAM);
        Long deadLetters = redis.opsForStream().size(CouponStockStore.ISSUE_DEAD_LETTER);
        long pending = 0;
        long oldestAgeMs = 0;
        if (length != null && length > 0) {
            List<MapRecord<String, Object, Object>> oldest = redis.opsForStream()
                    .range(CouponStockStore.ISSUE_STREAM, Range.unbounded(), Limit.limit().count(1));
            if (oldest != null && !oldest.isEmpty()) {
                oldestAgeMs = Math.max(0, System.currentTimeMillis() - oldest.get(0).getId().getTimestamp());
            }
            PendingMessagesSummary summary =
                    redis.opsForStream().pending(CouponStockStore.ISSUE_STREAM, CouponIssueWorker.GROUP);
            pending = summary == null ? 0 : summary.getTotalPendingMessages();
        }
        return new Snapshot(orZero(length), pending, orZero(deadLetters), oldestAgeMs);
    }

    private static long orZero(Long value) {
        return value == null ? 0 : value;
    }
}
