package com.ecommerce.promotion.viewmodel;

import com.ecommerce.promotion.model.Promotion;
import com.ecommerce.promotion.model.enumeration.DiscountType;
import java.time.Instant;

/**
 * One card on the storefront first-come coupon page. Field names match the frontend's
 * {@code CouponEvent}; {@code claimed} is false for anonymous callers.
 *
 * <p>{@code claimed} is true once the coupon is the caller's in MySQL (ISSUED), as before.
 * {@code issueStatus} also tells a queued claim (PENDING, redis-async) and one given back
 * (FAILED), so a reloaded page can pick up polling; NONE for anonymous callers.</p>
 */
public record StorefrontCouponVm(Long promotionId,
                                 String name,
                                 String description,
                                 DiscountType discountType,
                                 Long discountAmount,
                                 Long discountPercentage,
                                 Long minimumOrderPurchaseAmount,
                                 int issueLimit,
                                 int issuedCount,
                                 Instant startDate,
                                 Instant endDate,
                                 boolean claimed,
                                 CouponClaimStatusVm.Status issueStatus) {

    public static StorefrontCouponVm of(Promotion promotion, int issuedCount, CouponClaimStatusVm.Status issueStatus) {
        return new StorefrontCouponVm(promotion.getId(), promotion.getName(), promotion.getDescription(),
                promotion.getDiscountType(), promotion.getDiscountAmount(), promotion.getDiscountPercentage(),
                promotion.getMinimumOrderPurchaseAmount(), promotion.getIssueLimit(), issuedCount,
                promotion.getStartDate(), promotion.getEndDate(),
                issueStatus == CouponClaimStatusVm.Status.ISSUED, issueStatus);
    }
}
