package com.ecommerce.promotion.service;

import com.ecommerce.promotion.repository.PromotionIssueRepository;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.stereotype.Component;

/**
 * First-come coupon state in Redis, per promotion:
 * <ul>
 *   <li>{@code coupon:{id}:stock} — coupons still left</li>
 *   <li>{@code coupon:{id}:users} — ids of users who already claimed one</li>
 * </ul>
 * The {@code {id}} hash tag keeps both keys in one slot, so a script can touch them together
 * on a Redis Cluster too.
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

    private static final RedisScript<Long> CLAIM =
            RedisScript.of(new ClassPathResource("redis/coupon-claim.lua"), Long.class);
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
        Long result = redis.execute(CLAIM, List.of(stockKey(promotionId), usersKey(promotionId)), userId);
        return switch (result == null ? -1 : result.intValue()) {
            case 0 -> ClaimResult.CLAIMED;
            case 1 -> ClaimResult.SOLD_OUT;
            case 2 -> ClaimResult.ALREADY_CLAIMED;
            default -> ClaimResult.NOT_LOADED;
        };
    }

    /**
     * Undoes {@link #tryClaim} after the MySQL write failed.
     *
     * @param forgetUser false when MySQL already holds this user's coupon, so they stay a claimer
     */
    public void undoClaim(Long promotionId, String userId, boolean forgetUser) {
        redis.execute(UNCLAIM, List.of(stockKey(promotionId), usersKey(promotionId)),
                userId, forgetUser ? "1" : "0");
    }

    /** Seeds a coupon that has no claims yet. Does nothing if its stock is already there. */
    public void prime(Long promotionId, int issueLimit) {
        redis.opsForValue().setIfAbsent(stockKey(promotionId), String.valueOf(Math.max(0, issueLimit)));
    }

    /** Removes a coupon's keys, e.g. after the promotion is deleted. */
    public void evict(Long promotionId) {
        redis.delete(List.of(stockKey(promotionId), usersKey(promotionId)));
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
        redis.delete(usersKey(promotionId));
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
