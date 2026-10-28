package com.ecommerce.promotion.viewmodel;

import java.time.Instant;

/**
 * Where the caller's claim on a first-come coupon stands (redis-async). Answered by the claim
 * itself (202, PENDING) and by the status endpoint the coupon page polls.
 *
 * @param issueId     the promotion_issue row, once ISSUED
 * @param issuedAt    when the coupon was claimed, once ISSUED
 * @param pollAfterMs how long to wait before asking again while PENDING
 */
public record CouponClaimStatusVm(Long promotionId, Status status, Long issueId, Instant issuedAt, long pollAfterMs) {

    public enum Status {
        /** Claimed and queued; the MySQL row is not written yet. */
        PENDING,
        /** The coupon is the caller's. */
        ISSUED,
        /** The queued claim could not be written and the coupon was given back. */
        FAILED,
        /** No claim by the caller. */
        NONE
    }

    public static CouponClaimStatusVm pending(Long promotionId, long pollAfterMs) {
        return new CouponClaimStatusVm(promotionId, Status.PENDING, null, null, pollAfterMs);
    }
}
