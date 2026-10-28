package com.ecommerce.promotion.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.promotion.model.Promotion;
import com.ecommerce.promotion.model.PromotionIssue;
import com.ecommerce.promotion.model.enumeration.ApplyTo;
import com.ecommerce.promotion.model.enumeration.DiscountType;
import com.ecommerce.promotion.model.enumeration.UsageType;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import com.ecommerce.promotion.service.CouponStockStore.ClaimResult;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** redis-async against real MySQL and Redis, with the worker running. Skipped without Docker. */
@SpringBootTest(properties = {
        "promotion.issue.store=redis-async",
        "promotion.issue.count-sync-interval=1h",
        "promotion.issue.worker-poll-interval=50ms"
})
@Testcontainers(disabledWithoutDocker = true)
class RedisAsyncCouponIssueIntegrationTest {

    private static final Duration WAIT = Duration.ofSeconds(10);

    @Container
    @ServiceConnection
    static MySQLContainer<?> mysql = new MySQLContainer<>("mysql:8.0");

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @Autowired
    private CouponStockStore couponStockStore;
    @Autowired
    private PromotionRepository promotionRepository;
    @Autowired
    private PromotionIssueRepository promotionIssueRepository;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @Test
    void workerWritesQueuedWinnersToMysql() {
        Promotion coupon = openCoupon(3);
        assertTrue(couponStockStore.loadIfAbsent(coupon.getId(), 3));

        assertEquals(ClaimResult.CLAIMED, couponStockStore.tryClaimAndEnqueue(coupon.getId(), "w1"));
        assertEquals(ClaimResult.CLAIMED, couponStockStore.tryClaimAndEnqueue(coupon.getId(), "w2"));

        await().atMost(WAIT).until(() -> issuedUsers(coupon).size() == 2);
        assertEquals(List.of("w1", "w2"), issuedUsers(coupon).stream().sorted().toList());
        await().atMost(WAIT).until(() -> queuedUsers(coupon).isEmpty() && pendingCount(coupon) == 0);
        assertEquals("1", stock(coupon));
    }

    @Test
    void entryHandedOutAgainIsAcknowledgedWithoutASecondRow() {
        Promotion coupon = openCoupon(3);
        // What a crash between the MySQL commit and the acknowledgement leaves behind.
        insertIssue(coupon, "again");
        enqueueRaw(coupon, "again");

        await().atMost(WAIT).until(() -> queuedUsers(coupon).isEmpty());
        assertEquals(List.of("again"), issuedUsers(coupon));
    }

    @Test
    void aDuplicateDoesNotHoldBackTheRestOfTheBatch() {
        Promotion coupon = openCoupon(5);
        insertIssue(coupon, "d1");
        enqueueRaw(coupon, "d1");
        enqueueRaw(coupon, "d2");
        enqueueRaw(coupon, "d3");

        await().atMost(WAIT).until(() -> issuedUsers(coupon).size() == 3 && queuedUsers(coupon).isEmpty());
        assertEquals(List.of("d1", "d2", "d3"), issuedUsers(coupon).stream().sorted().toList());
    }

    // ------------------------------------------------------------------

    private Promotion openCoupon(int issueLimit) {
        Instant now = Instant.now();
        return promotionRepository.save(Promotion.builder()
                .name("async coupon")
                .slug("async-" + UUID.randomUUID())
                .discountType(DiscountType.FIXED)
                .discountAmount(1000L)
                .usageType(UsageType.UNLIMITED)
                .applyTo(ApplyTo.PRODUCT)
                .isActive(true)
                .startDate(now.minus(1, ChronoUnit.HOURS))
                .endDate(now.plus(1, ChronoUnit.HOURS))
                .issueLimit(issueLimit)
                .build());
    }

    private void insertIssue(Promotion coupon, String userId) {
        promotionIssueRepository.saveAndFlush(PromotionIssue.builder()
                .promotion(coupon).userId(userId).issuedAt(Instant.now()).build());
    }

    /** A queue entry as the claim script writes it, without touching stock. */
    private void enqueueRaw(Promotion coupon, String userId) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .in(CouponStockStore.ISSUE_STREAM)
                .ofMap(Map.of("promotionId", String.valueOf(coupon.getId()),
                        "userId", userId,
                        "claimedAt", String.valueOf(Instant.now().toEpochMilli()))));
    }

    private List<String> issuedUsers(Promotion coupon) {
        return promotionIssueRepository.findUserIdsByPromotionId(coupon.getId());
    }

    private List<String> queuedUsers(Promotion coupon) {
        List<MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream().range(CouponStockStore.ISSUE_STREAM, Range.unbounded());
        return records == null ? List.of() : records.stream()
                .filter(r -> String.valueOf(coupon.getId()).equals(r.getValue().get("promotionId")))
                .map(r -> (String) r.getValue().get("userId"))
                .toList();
    }

    private long pendingCount(Promotion coupon) {
        return redisTemplate.opsForHash().size(CouponStockStore.pendingKey(coupon.getId()));
    }

    private String stock(Promotion coupon) {
        return redisTemplate.opsForValue().get(CouponStockStore.stockKey(coupon.getId()));
    }
}
