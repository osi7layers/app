package com.ecommerce.promotion.controller;

import com.ecommerce.commonlib.viewmodel.ApiResponse;
import com.ecommerce.promotion.service.PromotionIssueService;
import com.ecommerce.promotion.service.StorefrontCouponService;
import com.ecommerce.promotion.viewmodel.CouponClaimStatusVm;
import com.ecommerce.promotion.viewmodel.StorefrontCouponVm;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
public class PromotionIssueController {

    private final PromotionIssueService promotionIssueService;
    private final StorefrontCouponService storefrontCouponService;

    /** Coupon page cards. Works without a token; with one, {@code claimed} is filled in. */
    @GetMapping("/storefront/promotions")
    public ApiResponse<List<StorefrontCouponVm>> listCoupons() {
        return ApiResponse.ok(storefrontCouponService.listCoupons());
    }

    /**
     * 201 with the issued coupon (redis, db), or 202 PENDING when the claim is queued
     * (redis-async): the caller then polls the claim status.
     */
    @PostMapping("/storefront/promotions/{promotionId}/issue")
    public ResponseEntity<?> issue(@PathVariable Long promotionId) {
        if (promotionIssueService.queuesIssues()) {
            return ResponseEntity.status(HttpStatus.ACCEPTED).body(promotionIssueService.enqueue(promotionId));
        }
        return ResponseEntity.status(HttpStatus.CREATED).body(promotionIssueService.issue(promotionId));
    }

    /**
     * The caller's claim: PENDING (poll again after {@code pollAfterMs}), ISSUED, FAILED or NONE.
     * Same shape as the 202 body, so the page can poll this after claiming.
     */
    @GetMapping("/storefront/promotions/{promotionId}/issue")
    public CouponClaimStatusVm claimStatus(@PathVariable Long promotionId) {
        return promotionIssueService.claimStatus(promotionId);
    }
}
