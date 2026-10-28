package com.ecommerce.promotion.service;

import com.ecommerce.commonlib.exception.BusinessException;
import com.ecommerce.commonlib.exception.ErrorCode;
import com.ecommerce.commonlib.security.AuthenticationUtils;
import com.ecommerce.promotion.config.PromotionIssueProperties;
import com.ecommerce.promotion.model.Promotion;
import com.ecommerce.promotion.model.PromotionIssue;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import com.ecommerce.promotion.service.CouponStockStore.ClaimResult;
import com.ecommerce.promotion.service.CouponWindowCache.CouponWindow;
import com.ecommerce.promotion.viewmodel.CouponClaimStatusVm;
import com.ecommerce.promotion.viewmodel.PromotionIssueVm;
import java.time.Instant;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Hands out first-come coupons, one per user, never more than {@code issueLimit}.
 *
 * <ul>
 *   <li><b>redis</b> (default): a Lua script checks the claimer set and decrements the stock in
 *       one step, so claims do not queue on a row lock; only winners insert into
 *       {@code promotion_issue}. If that insert fails the claim is given back in Redis.</li>
 *   <li><b>redis-async</b>: the same script also queues the winner on a Redis Stream and the
 *       claim answers PENDING at once; {@link CouponIssueWorker} writes the row. See
 *       {@link #enqueue}.</li>
 *   <li><b>db</b>: a conditional UPDATE on the promotion row, then the insert, in one
 *       transaction. Kept for comparing the stores under load.</li>
 * </ul>
 */
@Slf4j
@Service
public class PromotionIssueService {

    private static final String DUPLICATE_ISSUE_CONSTRAINT = "uk_promotion_issue_promotion_user";

    private final PromotionRepository promotionRepository;
    private final PromotionIssueRepository promotionIssueRepository;
    private final CouponStockStore couponStockStore;
    private final CouponWindowCache couponWindowCache;
    private final PromotionIssueProperties properties;
    private final CouponIssueQueueMonitor queueMonitor;
    private final TransactionTemplate transactionTemplate;

    public PromotionIssueService(PromotionRepository promotionRepository,
                                 PromotionIssueRepository promotionIssueRepository,
                                 CouponStockStore couponStockStore,
                                 CouponWindowCache couponWindowCache,
                                 PromotionIssueProperties properties,
                                 CouponIssueQueueMonitor queueMonitor,
                                 PlatformTransactionManager transactionManager) {
        this.promotionRepository = promotionRepository;
        this.promotionIssueRepository = promotionIssueRepository;
        this.couponStockStore = couponStockStore;
        this.couponWindowCache = couponWindowCache;
        this.properties = properties;
        this.queueMonitor = queueMonitor;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
    }

    /** Whether claims are queued (redis-async) and answered with {@link #enqueue} instead of {@link #issue}. */
    public boolean queuesIssues() {
        return properties.getStore() == PromotionIssueProperties.Store.REDIS_ASYNC;
    }

    /** Claims and writes the coupon before answering (redis and db stores). */
    public PromotionIssueVm issue(Long promotionId) {
        String userId = AuthenticationUtils.requireUserId();
        Instant now = Instant.now();
        return properties.usesRedis()
                ? issueWithRedis(promotionId, userId, now)
                : transactionTemplate.execute(status -> issueWithDatabase(promotionId, userId, now));
    }

    // ------------------------------------------------------------------
    // redis
    // ------------------------------------------------------------------

    /**
     * redis-async: claims in Redis and queues the winner in one step, then answers PENDING
     * without waiting for MySQL. Losers get the same errors as {@link #issue}.
     */
    public CouponClaimStatusVm enqueue(Long promotionId) {
        String userId = AuthenticationUtils.requireUserId();
        claimInRedis(openWindow(promotionId, Instant.now()), userId, couponStockStore::tryClaimAndEnqueue);
        return CouponClaimStatusVm.pending(promotionId, queueMonitor.pollAfterMs());
    }

    /**
     * The caller's claim on a coupon, for the coupon page to poll after a 202.
     *
     * <p>Redis pending is read before MySQL: the worker writes the row and only then drops the
     * pending mark, so a claim is always found in one of the two. Read the other way round, a
     * claim written in between would be found in neither and read as NONE.</p>
     *
     * <p>With Redis down, a row in MySQL still answers ISSUED. Without one, a queued claim
     * cannot be told from no claim, so the answer is 503 (PRM-6004) and the page polls again
     * later, rather than NONE, which it would show as a failure.</p>
     */
    public CouponClaimStatusVm claimStatus(Long promotionId) {
        String userId = AuthenticationUtils.requireUserId();
        boolean redis = properties.usesRedis();
        Boolean pending = redis ? redisAnswer(() -> couponStockStore.isPending(promotionId, userId)) : Boolean.FALSE;
        if (Boolean.TRUE.equals(pending)) {
            return CouponClaimStatusVm.pending(promotionId, queueMonitor.pollAfterMs());
        }
        Optional<PromotionIssue> issued = promotionIssueRepository.findByPromotion_IdAndUserId(promotionId, userId);
        if (issued.isPresent()) {
            return CouponClaimStatusVm.issued(promotionId, issued.get().getId(), issued.get().getIssuedAt());
        }
        Boolean failed = redis ? redisAnswer(() -> couponStockStore.hasFailed(promotionId, userId)) : Boolean.FALSE;
        if (pending == null || failed == null) {
            throw BusinessException.of(ErrorCode.COUPON_STORE_UNAVAILABLE);
        }
        return CouponClaimStatusVm.of(promotionId,
                failed ? CouponClaimStatusVm.Status.FAILED : CouponClaimStatusVm.Status.NONE);
    }

    /** A Redis check for a status read; null when Redis cannot answer. */
    private static Boolean redisAnswer(Supplier<Boolean> check) {
        try {
            return Boolean.TRUE.equals(check.get());
        } catch (DataAccessException e) {
            log.warn("Coupon store unavailable while reading a claim status: {}", e.getMessage());
            return null;
        }
    }

    private PromotionIssueVm issueWithRedis(Long promotionId, String userId, Instant now) {
        claimInRedis(openWindow(promotionId, now), userId, couponStockStore::tryClaim);
        PromotionIssueVm issued;
        try {
            issued = transactionTemplate.execute(status -> save(promotionId, userId, now));
        } catch (DataIntegrityViolationException ex) {
            if (isDuplicateIssue(ex)) {
                // MySQL already has this user's coupon: keep them a claimer, give the stock back.
                undoClaim(promotionId, userId, false);
                throw BusinessException.of(ErrorCode.COUPON_ALREADY_CLAIMED);
            }
            undoClaim(promotionId, userId, true);
            throw ex;
        } catch (RuntimeException ex) {
            undoClaim(promotionId, userId, true);
            throw ex;
        }
        try {
            couponStockStore.confirmClaim(promotionId, userId);
        } catch (DataAccessException e) {
            // Harmless: StaleClaimSettler finds the row in MySQL and confirms it later.
            log.debug("Could not confirm coupon {} for {} in Redis: {}", promotionId, userId, e.getMessage());
        }
        return issued;
    }

    private CouponWindow openWindow(Long promotionId, Instant now) {
        CouponWindow window = couponWindowCache.get(promotionId)
                .orElseThrow(() -> BusinessException.notFound("promotion.coupon.not.found", promotionId));
        if (!window.isOpen(now)) {
            throw BusinessException.of(ErrorCode.COUPON_NOT_OPEN);
        }
        return window;
    }

    /** Runs the claim script, rebuilding the keys from MySQL first if needed; throws unless claimed. */
    private void claimInRedis(CouponWindow window, String userId, BiFunction<Long, String, ClaimResult> claim) {
        Long promotionId = window.promotionId();
        ClaimResult result = redisCall(() -> claim.apply(promotionId, userId));
        if (result == ClaimResult.NOT_LOADED) {
            boolean loaded = redisCall(() -> couponStockStore.loadIfAbsent(promotionId, window.issueLimit()));
            result = loaded ? redisCall(() -> claim.apply(promotionId, userId)) : ClaimResult.NOT_LOADED;
        }
        switch (result) {
            case SOLD_OUT -> throw BusinessException.of(ErrorCode.COUPON_SOLD_OUT);
            case ALREADY_CLAIMED -> throw BusinessException.of(ErrorCode.COUPON_ALREADY_CLAIMED);
            case NOT_LOADED -> throw BusinessException.of(ErrorCode.COUPON_STORE_UNAVAILABLE);
            case CLAIMED -> {
                // the caller writes or has queued the row
            }
        }
    }

    private void undoClaim(Long promotionId, String userId, boolean forgetUser) {
        try {
            couponStockStore.undoClaim(promotionId, userId, forgetUser);
        } catch (DataAccessException e) {
            // Stock stays one short until the keys are rebuilt; never over-issues.
            log.error("Could not give back coupon {} claimed by {} in Redis", promotionId, userId, e);
        }
    }

    /** A Redis failure stops issuing (503) instead of falling back to MySQL with stale stock. */
    private static <T> T redisCall(Supplier<T> call) {
        try {
            return call.get();
        } catch (DataAccessException e) {
            log.warn("Coupon store unavailable: {}", e.getMessage());
            throw BusinessException.of(ErrorCode.COUPON_STORE_UNAVAILABLE);
        }
    }

    // ------------------------------------------------------------------
    // db
    // ------------------------------------------------------------------

    private PromotionIssueVm issueWithDatabase(Long promotionId, String userId, Instant now) {
        if (promotionRepository.claimIssueSlot(promotionId, now) == 0) {
            throw explainClosedClaim(promotionId, now);
        }
        try {
            return save(promotionId, userId, now);
        } catch (DataIntegrityViolationException ex) {
            if (isDuplicateIssue(ex)) {
                throw BusinessException.of(ErrorCode.COUPON_ALREADY_CLAIMED);
            }
            throw ex;
        }
    }

    private BusinessException explainClosedClaim(Long promotionId, Instant now) {
        Promotion promotion = promotionRepository.findById(promotionId)
                .orElseThrow(() -> BusinessException.notFound("promotion.coupon.not.found", promotionId));
        if (!CouponWindow.of(promotion).isOpen(now)) {
            return BusinessException.of(ErrorCode.COUPON_NOT_OPEN);
        }
        return BusinessException.of(ErrorCode.COUPON_SOLD_OUT);
    }

    // ------------------------------------------------------------------
    // shared
    // ------------------------------------------------------------------

    private PromotionIssueVm save(Long promotionId, String userId, Instant now) {
        PromotionIssue saved = promotionIssueRepository.saveAndFlush(PromotionIssue.builder()
                .promotion(promotionRepository.getReferenceById(promotionId))
                .userId(userId)
                .issuedAt(now)
                .build());
        return new PromotionIssueVm(saved.getId(), promotionId, userId, saved.getIssuedAt());
    }

    /** The user already has this coupon in MySQL (the unique key on promotion and user). */
    static boolean isDuplicateIssue(DataIntegrityViolationException ex) {
        String message = ex.getMostSpecificCause().getMessage();
        return message != null && message.contains(DUPLICATE_ISSUE_CONSTRAINT);
    }
}
