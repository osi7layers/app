package com.ecommerce.promotion.service;

import com.ecommerce.commonlib.security.AuthenticationUtils;
import com.ecommerce.promotion.config.PromotionIssueProperties;
import com.ecommerce.promotion.model.Promotion;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import com.ecommerce.promotion.service.CouponStockStore.ClaimMarks;
import com.ecommerce.promotion.viewmodel.CouponClaimStatusVm.Status;
import com.ecommerce.promotion.viewmodel.StorefrontCouponVm;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/** The storefront coupon page: active first-come coupons, upcoming, open, or just ended. */
@Slf4j
@Service
@RequiredArgsConstructor
public class StorefrontCouponService {

    // Ended coupons stay listed this long so the page can show them as ended.
    private static final Duration ENDED_VISIBLE = Duration.ofDays(1);
    private static final int MAX_COUPONS = 100;
    private static final ClaimMarks NO_MARKS = new ClaimMarks(Set.of(), Set.of());

    private final PromotionRepository promotionRepository;
    private final PromotionIssueRepository promotionIssueRepository;
    private final CouponStockStore couponStockStore;
    private final PromotionIssueProperties properties;

    @Transactional(readOnly = true)
    public List<StorefrontCouponVm> listCoupons() {
        List<Promotion> coupons = promotionRepository.findFirstComeCoupons(
                Instant.now().minus(ENDED_VISIBLE), PageRequest.of(0, MAX_COUPONS));
        if (coupons.isEmpty()) {
            return List.of();
        }
        List<Long> ids = coupons.stream().map(Promotion::getId).toList();
        Map<Long, Integer> liveCounts = liveIssuedCounts(coupons);
        Optional<String> caller = AuthenticationUtils.currentUserId();
        Set<Long> claimed = caller
                .<Set<Long>>map(userId -> new HashSet<>(promotionIssueRepository.findClaimedPromotionIds(userId, ids)))
                .orElse(Set.of());
        ClaimMarks marks = caller.map(userId -> claimMarks(ids, userId)).orElse(NO_MARKS);
        return coupons.stream()
                .map(p -> StorefrontCouponVm.of(p,
                        liveCounts.getOrDefault(p.getId(), p.getIssuedCount()),
                        statusOf(p.getId(), claimed, marks)))
                .toList();
    }

    /** Same order as the status endpoint: pending, then MySQL, then failed. */
    private static Status statusOf(Long promotionId, Set<Long> claimed, ClaimMarks marks) {
        if (marks.pending().contains(promotionId)) {
            return Status.PENDING;
        }
        if (claimed.contains(promotionId)) {
            return Status.ISSUED;
        }
        return marks.failed().contains(promotionId) ? Status.FAILED : Status.NONE;
    }

    /** The caller's pending and failed claims; none when stock is not in Redis or Redis is down. */
    private ClaimMarks claimMarks(List<Long> ids, String userId) {
        if (!properties.usesRedis()) {
            return NO_MARKS;
        }
        try {
            return couponStockStore.claimMarks(ids, userId);
        } catch (DataAccessException e) {
            log.warn("Coupon store unavailable, listing claims from MySQL only: {}", e.getMessage());
            return NO_MARKS;
        }
    }

    /**
     * With stock in Redis, issued counts come from the live stock rather than issued_count,
     * which trails by a few seconds. Falls back to issued_count when Redis is unavailable:
     * the list is only a display, unlike claiming.
     */
    private Map<Long, Integer> liveIssuedCounts(List<Promotion> coupons) {
        if (!properties.usesRedis()) {
            return Map.of();
        }
        try {
            return couponStockStore.issuedCounts(coupons.stream()
                    .collect(Collectors.toMap(Promotion::getId, Promotion::getIssueLimit)));
        } catch (DataAccessException e) {
            log.warn("Coupon store unavailable, listing stored issued counts: {}", e.getMessage());
            return Map.of();
        }
    }
}
