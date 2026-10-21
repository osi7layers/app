package com.ecommerce.promotion.config;

import java.time.Duration;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Getter
@Setter
@ConfigurationProperties(prefix = "promotion.issue")
public class PromotionIssueProperties {

    /** Where first-come coupon stock is kept and decremented. */
    private Store store = Store.REDIS;

    /** How long a pod trusts its cached copy of a coupon's open window and total quantity. */
    private Duration windowCacheTtl = Duration.ofSeconds(3);

    public enum Store {
        REDIS,
        DB
    }
}
