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

    /** How often issued_count is caught up with promotion_issue when stock is kept in Redis. */
    private Duration countSyncInterval = Duration.ofSeconds(5);

    /**
     * Redis store only: a claim with no MySQL row after this long is given back. Must be longer
     * than a claim can take, including waiting for a database connection (Hikari: 30s).
     */
    private Duration staleClaimAfter = Duration.ofSeconds(60);

    /** redis-async only: how many queued winners the worker writes to MySQL in one transaction. */
    private int workerBatchSize = 100;

    /** redis-async only: how long the worker waits before reading again when the queue was empty. */
    private Duration workerPollInterval = Duration.ofMillis(200);

    /** redis-async only: how often the worker looks for stuck entries and gone consumers. */
    private Duration workerReclaimInterval = Duration.ofSeconds(10);

    /**
     * redis-async only: an entry handed out this long ago and not acknowledged is taken over
     * (its pod died, or its write failed). Longer than a batch can take.
     */
    private Duration workerClaimIdle = Duration.ofSeconds(30);

    /**
     * redis-async only: after this many hand-outs an entry is dead-lettered and the coupon given
     * back. With a 30s claim idle, 10 tolerates about five minutes of MySQL outage.
     */
    private int workerMaxDeliveries = 10;

    /** redis-async only: a consumer with nothing pending and idle this long (an old pod) is removed. */
    private Duration workerConsumerExpiry = Duration.ofHours(1);

    /** Whether stock is kept in Redis (both Redis stores), so the Redis keys must be kept in step. */
    public boolean usesRedis() {
        return store == Store.REDIS || store == Store.REDIS_ASYNC;
    }

    public enum Store {
        REDIS,
        /** Like REDIS, but winners are queued on a Redis Stream and written to MySQL by a worker. */
        REDIS_ASYNC,
        DB
    }
}
