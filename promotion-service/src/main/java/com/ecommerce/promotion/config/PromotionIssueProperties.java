package com.ecommerce.promotion.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "promotion.issue")
public class PromotionIssueProperties {

    /** Where first-come coupon stock is kept and decremented. */
    private Store store = Store.REDIS;

    public enum Store {
        REDIS,
        DB
    }
}
