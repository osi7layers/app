package com.ecommerce.promotion.service;

import com.ecommerce.promotion.repository.PromotionIssueRepository;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiPredicate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * First-come coupon state in Redis, per promotion:
 * <ul>
 *   <li>{@code coupon:{id}:stock} — coupons still left</li>
 *   <li>{@code coupon:{id}:users} — ids of users who already claimed one</li>
 *   <li>{@code coupon:{id}:pending} — claims whose MySQL row is not confirmed yet, with the
 *       claim time</li>
 * </ul>
 * The {@code {id}} hash tag keeps both keys in one slot, so a script can touch them together
 * on a Redis Cluster too. With redis-async, winners are also queued on {@link #ISSUE_STREAM}
 * for {@code CouponIssueWorker} to write to MySQL.
 *
 * <p>MySQL ({@code promotion_issue}) stays the source of truth. When the keys are missing
 * (first claim, or Redis lost its data) they are rebuilt from it; one pod rebuilds while the
 * others wait, so a burst of first claims does not read the issue table a thousand times.</p>
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class CouponStockStore {

    private static final int LOAD_BATCH = 1000;
    private static final Duration LOAD_LOCK_TTL = Duration.ofSeconds(10);
    private static final Duration LOAD_WAIT = Duration.ofSeconds(3);
    private static final long LOAD_POLL_MILLIS = 50;

    /** Winners waiting for their MySQL row (redis-async). One stream for all coupons. */
    public static final String ISSUE_STREAM = "coupon:issue-stream";
    /** Queue entries the worker gave up on, kept for a person to look at. */
    public static final String ISSUE_DEAD_LETTER = "coupon:issue-dlq";

    private static final RedisScript<Long> CLAIM =
            RedisScript.of(new ClassPathResource("redis/coupon-claim.lua"), Long.class);
    private static final RedisScript<Long> CLAIM_AND_ENQUEUE =
            RedisScript.of(new ClassPathResource("redis/coupon-claim-async.lua"), Long.class);
    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> QUEUE_STUCK =
            RedisScript.of(new ClassPathResource("redis/coupon-queue-stuck.lua"), List.class);
    private static final RedisScript<Long> QUEUE_COMPLETE =
            RedisScript.of(new ClassPathResource("redis/coupon-queue-complete.lua"), Long.class);
    private static final RedisScript<Long> QUEUE_DEAD_LETTER =
            RedisScript.of(new ClassPathResource("redis/coupon-queue-dead-letter.lua"), Long.class);
    private static final RedisScript<Long> UNCLAIM =
            RedisScript.of(new ClassPathResource("redis/coupon-unclaim.lua"), Long.class);

    private final StringRedisTemplate redis;
    private final PromotionIssueRepository promotionIssueRepository;
    // ReentrantLock rather than synchronized: virtual threads would pin their carrier on a monitor.
    private final ConcurrentHashMap<Long, ReentrantLock> loadLocks = new ConcurrentHashMap<>();

    static String stockKey(Long promotionId) {
        return "coupon:{" + promotionId + "}:stock";
    }

    static String usersKey(Long promotionId) {
        return "coupon:{" + promotionId + "}:users";
    }

    static String pendingKey(Long promotionId) {
        return "coupon:{" + promotionId + "}:pending";
    }

    /** Users whose queued claim was given up on (redis-async), with the time. */
    static String failedKey(Long promotionId) {
        return "coupon:{" + promotionId + "}:failed";
    }

    private static List<String> claimKeys(Long promotionId) {
        return List.of(stockKey(promotionId), usersKey(promotionId), pendingKey(promotionId));
    }

    private static String loadLockKey(Long promotionId) {
        return "coupon:{" + promotionId + "}:loading";
    }

    public enum ClaimResult {
        CLAIMED,
        SOLD_OUT,
        ALREADY_CLAIMED,
        /** Redis has no state for this coupon yet; call {@link #loadIfAbsent} and try again. */
        NOT_LOADED
    }

    /** Takes one coupon for the user if any are left and the user has none yet. */
    public ClaimResult tryClaim(Long promotionId, String userId) {
        Long result = redis.execute(CLAIM, claimKeys(promotionId), userId,
                String.valueOf(Instant.now().toEpochMilli()));
        return toClaimResult(result);
    }

    /**
     * Like {@link #tryClaim}, and a winner is queued on {@link #ISSUE_STREAM} in the same step,
     * so the stock never drops without a message for the worker.
     */
    public ClaimResult tryClaimAndEnqueue(Long promotionId, String userId) {
        List<String> keys = List.of(stockKey(promotionId), usersKey(promotionId), pendingKey(promotionId),
                ISSUE_STREAM);
        Long result = redis.execute(CLAIM_AND_ENQUEUE, keys, userId,
                String.valueOf(Instant.now().toEpochMilli()), String.valueOf(promotionId));
        return toClaimResult(result);
    }

    private static ClaimResult toClaimResult(Long result) {
        return switch (result == null ? -1 : result.intValue()) {
            case 0 -> ClaimResult.CLAIMED;
            case 1 -> ClaimResult.SOLD_OUT;
            case 2 -> ClaimResult.ALREADY_CLAIMED;
            default -> ClaimResult.NOT_LOADED;
        };
    }

    /** The claim's MySQL row is written: it no longer needs to be checked. */
    public void confirmClaim(Long promotionId, String userId) {
        redis.opsForHash().delete(pendingKey(promotionId), userId);
    }

    /**
     * Undoes {@link #tryClaim} after the MySQL write failed.
     *
     * @param forgetUser false when MySQL already holds this user's coupon, so they stay a claimer
     */
    public void undoClaim(Long promotionId, String userId, boolean forgetUser) {
        redis.execute(UNCLAIM, claimKeys(promotionId), userId, forgetUser ? "1" : "0");
    }

    /**
     * Settles claims still pending after {@code olderThan}: confirmed if MySQL has the row,
     * given back if not. Such a claim is left behind when the caller stopped waiting for
     * Redis (timeout) after the script had already run, or the pod died before writing.
     *
     * @param issuedInMysql (promotion id, user id) → whether the issue row exists
     * @return number of claims given back
     */
    public int settleStaleClaims(Duration olderThan, BiPredicate<Long, String> issuedInMysql) {
        long cutoff = Instant.now().minus(olderThan).toEpochMilli();
        int givenBack = 0;
        ScanOptions pattern = ScanOptions.scanOptions().match("coupon:{*}:pending").count(100).build();
        try (Cursor<String> keys = redis.scan(pattern)) {
            while (keys.hasNext()) {
                String key = keys.next();
                Long promotionId = Long.valueOf(key.substring(key.indexOf('{') + 1, key.indexOf('}')));
                for (Map.Entry<Object, Object> claim : redis.opsForHash().entries(key).entrySet()) {
                    String userId = (String) claim.getKey();
                    if (Long.parseLong((String) claim.getValue()) > cutoff) {
                        continue;
                    }
                    if (issuedInMysql.test(promotionId, userId)) {
                        confirmClaim(promotionId, userId);
                    } else {
                        undoClaim(promotionId, userId, true);
                        givenBack++;
                        log.warn("Gave back coupon {} claimed by {}: no issue row after {}", promotionId, userId, olderThan);
                    }
                }
            }
        }
        return givenBack;
    }

    /**
     * How many of each coupon are gone, read from the live stock in one round trip.
     * Coupons Redis has no state for are left out.
     *
     * @param issueLimits promotion id to its total quantity
     */
    public Map<Long, Integer> issuedCounts(Map<Long, Integer> issueLimits) {
        List<Long> ids = new ArrayList<>(issueLimits.keySet());
        List<String> stocks = redis.opsForValue().multiGet(ids.stream().map(CouponStockStore::stockKey).toList());
        Map<Long, Integer> issued = new HashMap<>();
        for (int i = 0; i < ids.size(); i++) {
            String stock = stocks == null ? null : stocks.get(i);
            if (stock != null) {
                int limit = issueLimits.get(ids.get(i));
                issued.put(ids.get(i), Math.min(limit, Math.max(0, limit - Integer.parseInt(stock))));
            }
        }
        return issued;
    }

    /** Seeds a coupon that has no claims yet. Does nothing if its stock is already there. */
    public void prime(Long promotionId, int issueLimit) {
        redis.opsForValue().setIfAbsent(stockKey(promotionId), String.valueOf(Math.max(0, issueLimit)));
    }

    /** The user's claim is taken but its MySQL row is not written yet. */
    public boolean isPending(Long promotionId, String userId) {
        return redis.opsForHash().hasKey(pendingKey(promotionId), userId);
    }

    /** A queue entry handed out and not acknowledged, and how many times it was handed out. */
    public record StuckEntry(RecordId id, long deliveries) {
    }

    /** Entries of {@link #ISSUE_STREAM} unacknowledged for at least {@code idle}, oldest first. */
    public List<StuckEntry> stuckQueued(String group, Duration idle, int count) {
        List<?> flat = redis.execute(QUEUE_STUCK, List.of(ISSUE_STREAM), group,
                String.valueOf(idle.toMillis()), String.valueOf(count));
        List<StuckEntry> stuck = new ArrayList<>();
        for (int i = 0; flat != null && i + 1 < flat.size(); i += 2) {
            stuck.add(new StuckEntry(RecordId.of((String) flat.get(i)), Long.parseLong((String) flat.get(i + 1))));
        }
        return stuck;
    }

    /**
     * Queue entries whose rows are written: their pending marks are dropped and they are
     * acknowledged and deleted, in one step. The three lists are entry by entry.
     */
    public void completeQueued(String group, List<RecordId> ids, List<Long> promotionIds, List<String> userIds) {
        List<String> keys = new ArrayList<>(ids.size() + 1);
        keys.add(ISSUE_STREAM);
        promotionIds.forEach(id -> keys.add(pendingKey(id)));
        List<String> args = new ArrayList<>(2 * ids.size() + 1);
        args.add(group);
        ids.forEach(id -> args.add(id.getValue()));
        args.addAll(userIds);
        redis.execute(QUEUE_COMPLETE, keys, args.toArray());
    }

    /** Moves a queue entry, with these fields, to {@link #ISSUE_DEAD_LETTER} in one step. */
    public void deadLetterQueued(String group, RecordId id, Map<String, String> fields) {
        List<String> args = new ArrayList<>(2 * fields.size() + 2);
        args.add(group);
        args.add(id.getValue());
        fields.forEach((field, value) -> {
            args.add(field);
            args.add(value);
        });
        redis.execute(QUEUE_DEAD_LETTER, List.of(ISSUE_STREAM, ISSUE_DEAD_LETTER), args.toArray());
    }

    /** Coupons on which the user's claim is pending, and those whose queued claim failed. */
    public record ClaimMarks(Set<Long> pending, Set<Long> failed) {
    }

    /** {@link #isPending} and {@link #hasFailed} for many coupons in one round trip. */
    public ClaimMarks claimMarks(List<Long> promotionIds, String userId) {
        byte[] user = userId.getBytes(StandardCharsets.UTF_8);
        List<Object> answers = redis.executePipelined((RedisCallback<Object>) connection -> {
            for (Long id : promotionIds) {
                connection.hashCommands().hExists(pendingKey(id).getBytes(StandardCharsets.UTF_8), user);
                connection.hashCommands().hExists(failedKey(id).getBytes(StandardCharsets.UTF_8), user);
            }
            return null;
        });
        Set<Long> pending = new HashSet<>();
        Set<Long> failed = new HashSet<>();
        for (int i = 0; i < promotionIds.size(); i++) {
            if (Boolean.TRUE.equals(answers.get(2 * i))) {
                pending.add(promotionIds.get(i));
            }
            if (Boolean.TRUE.equals(answers.get(2 * i + 1))) {
                failed.add(promotionIds.get(i));
            }
        }
        return new ClaimMarks(pending, failed);
    }

    /** The user's queued claim was given up on and the coupon given back. */
    public boolean hasFailed(Long promotionId, String userId) {
        return redis.opsForHash().hasKey(failedKey(promotionId), userId);
    }

    /** Remembers that the user's queued claim was given up on, so their status can say so. */
    public void markFailed(Long promotionId, String userId) {
        redis.opsForHash().put(failedKey(promotionId), userId, String.valueOf(Instant.now().toEpochMilli()));
    }

    /** Removes a coupon's keys, e.g. after the promotion is deleted. */
    public void evict(Long promotionId) {
        List<String> keys = new ArrayList<>(claimKeys(promotionId));
        keys.add(failedKey(promotionId));
        redis.delete(keys);
    }

    /**
     * Rebuilds stock and claimers from MySQL unless Redis already has them.
     *
     * @return false if another pod was rebuilding and did not finish in time
     */
    public boolean loadIfAbsent(Long promotionId, int issueLimit) {
        ReentrantLock local = loadLocks.computeIfAbsent(promotionId, id -> new ReentrantLock());
        local.lock();
        try {
            if (isLoaded(promotionId)) {
                return true;
            }
            Boolean acquired = redis.opsForValue().setIfAbsent(loadLockKey(promotionId), "1", LOAD_LOCK_TTL);
            if (!Boolean.TRUE.equals(acquired)) {
                return waitForLoad(promotionId);
            }
            try {
                if (!isLoaded(promotionId)) {
                    load(promotionId, issueLimit);
                }
                return true;
            } finally {
                redis.delete(loadLockKey(promotionId));
            }
        } finally {
            local.unlock();
        }
    }

    private void load(Long promotionId, int issueLimit) {
        List<String> userIds = promotionIssueRepository.findUserIdsByPromotionId(promotionId);
        // Claimers first, stock last: until stock exists every claim asks for a reload,
        // so nobody is admitted against a half-built claimer set.
        redis.delete(List.of(usersKey(promotionId), pendingKey(promotionId)));
        for (int from = 0; from < userIds.size(); from += LOAD_BATCH) {
            List<String> batch = userIds.subList(from, Math.min(from + LOAD_BATCH, userIds.size()));
            redis.opsForSet().add(usersKey(promotionId), batch.toArray(String[]::new));
        }
        int remaining = Math.max(0, issueLimit - userIds.size());
        redis.opsForValue().set(stockKey(promotionId), String.valueOf(remaining));
        log.info("Loaded coupon {} into Redis: {} left, {} claimed", promotionId, remaining, userIds.size());
    }

    private boolean isLoaded(Long promotionId) {
        return Boolean.TRUE.equals(redis.hasKey(stockKey(promotionId)));
    }

    private boolean waitForLoad(Long promotionId) {
        long deadline = System.nanoTime() + LOAD_WAIT.toNanos();
        while (System.nanoTime() < deadline) {
            if (isLoaded(promotionId)) {
                return true;
            }
            try {
                Thread.sleep(LOAD_POLL_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return isLoaded(promotionId);
    }
}
