package com.ecommerce.promotion.service;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.ecommerce.commonlib.exception.BusinessException;
import com.ecommerce.promotion.model.Promotion;
import com.ecommerce.promotion.model.PromotionIssue;
import com.ecommerce.promotion.model.enumeration.ApplyTo;
import com.ecommerce.promotion.model.enumeration.DiscountType;
import com.ecommerce.promotion.model.enumeration.UsageType;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import com.ecommerce.promotion.service.CouponStockStore.ClaimResult;
import com.ecommerce.promotion.viewmodel.CouponClaimStatusVm;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.StreamInfo.XInfoConsumer;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

/** redis-async against real MySQL and Redis, with the worker running. Skipped without Docker. */
@SpringBootTest(properties = {
        "promotion.issue.store=redis-async",
        "promotion.issue.count-sync-interval=1h",
        "promotion.issue.worker-poll-interval=50ms",
        "promotion.issue.worker-reclaim-interval=100ms",
        "promotion.issue.worker-claim-idle=300ms",
        "promotion.issue.worker-max-deliveries=2",
        "promotion.issue.worker-consumer-expiry=500ms"
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
    private PromotionIssueService issueService;
    @Autowired
    private CouponStockStore couponStockStore;
    @Autowired
    private PromotionRepository promotionRepository;
    @Autowired
    private PromotionIssueRepository promotionIssueRepository;
    @Autowired
    private StringRedisTemplate redisTemplate;

    @AfterEach
    void clearSecurity() {
        SecurityContextHolder.clearContext();
    }

    @Test
    void queuedClaimsNeverIssueMoreThanTheLimitUnderConcurrency() throws Exception {
        Promotion coupon = openCoupon(50);

        List<String> outcomes = enqueueConcurrently(coupon.getId(), 300);

        Map<String, Long> counts = outcomes.stream().collect(Collectors.groupingBy(o -> o, Collectors.counting()));
        assertEquals(50, counts.get("PENDING"));
        assertEquals(250, counts.get("PRM-6002"));
        await().atMost(WAIT).until(() -> issuedUsers(coupon).size() == 50
                && queuedUsers(coupon).isEmpty() && pendingCount(coupon) == 0);
        assertEquals("0", stock(coupon));
    }

    @Test
    void queuedClaimByTheSameUserIsRefused() {
        Promotion coupon = openCoupon(5);

        assertEquals(CouponClaimStatusVm.Status.PENDING, asUser("twice", () -> issueService.enqueue(coupon.getId())).status());
        BusinessException again = assertThrows(BusinessException.class,
                () -> asUser("twice", () -> issueService.enqueue(coupon.getId())));

        assertEquals("PRM-6003", again.getErrorCode());
        await().atMost(WAIT).until(() -> issuedUsers(coupon).equals(List.of("twice")));
    }

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

    @Test
    void entryLeftByADeadPodIsTakenOverAndWritten() {
        Promotion coupon = openCoupon(3);
        enqueueTakenBy("dead-pod", coupon, "orphan");

        await().atMost(WAIT).until(() -> issuedUsers(coupon).contains("orphan") && queuedUsers(coupon).isEmpty());
    }

    @Test
    void entryThatKeepsFailingIsDeadLetteredAndTheCouponGivenBack() {
        // No such promotion: every write fails on the foreign key.
        Long missing = 987_654_321L;
        redisTemplate.opsForValue().set(CouponStockStore.stockKey(missing), "0");
        redisTemplate.opsForSet().add(CouponStockStore.usersKey(missing), "ghost");
        redisTemplate.opsForHash().put(CouponStockStore.pendingKey(missing), "ghost", "1");
        enqueueRaw(missing, "ghost");

        await().atMost(WAIT).until(() -> deadLettered(missing).contains("ghost"));
        assertEquals("1", redisTemplate.opsForValue().get(CouponStockStore.stockKey(missing)));
        assertFalse(Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(CouponStockStore.usersKey(missing), "ghost")));
        assertFalse(redisTemplate.opsForHash().hasKey(CouponStockStore.pendingKey(missing), "ghost"));
        assertTrue(redisTemplate.opsForHash().hasKey(CouponStockStore.failedKey(missing), "ghost"));
    }

    @Test
    void idleConsumerOfAnOldPodIsRemoved() {
        await().atMost(WAIT).until(() -> Boolean.TRUE.equals(redisTemplate.hasKey(CouponStockStore.ISSUE_STREAM)));
        redisTemplate.execute(RedisScript.of("return redis.call('XGROUP', 'CREATECONSUMER', KEYS[1], ARGV[1], ARGV[2])",
                Long.class), List.of(CouponStockStore.ISSUE_STREAM), CouponIssueWorker.GROUP, "old-pod");
        assertTrue(consumerNames().contains("old-pod"));

        await().atMost(WAIT).until(() -> !consumerNames().contains("old-pod"));
    }

    // ------------------------------------------------------------------

    private List<String> enqueueConcurrently(Long promotionId, int requests) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(64);
        try {
            List<Callable<String>> tasks = new ArrayList<>();
            for (int i = 0; i < requests; i++) {
                String userId = "async-user-" + i;
                tasks.add(() -> {
                    try {
                        return asUser(userId, () -> issueService.enqueue(promotionId)).status().name();
                    } catch (BusinessException e) {
                        return e.getErrorCode();
                    }
                });
            }
            List<String> outcomes = new ArrayList<>();
            for (Future<String> f : pool.invokeAll(tasks)) {
                outcomes.add(f.get());
            }
            return outcomes;
        } finally {
            pool.shutdown();
        }
    }

    private static <T> T asUser(String userId, Supplier<T> action) {
        Jwt jwt = Jwt.withTokenValue("t").header("alg", "none").subject(userId).build();
        SecurityContextHolder.getContext().setAuthentication(new JwtAuthenticationToken(jwt));
        try {
            return action.get();
        } finally {
            SecurityContextHolder.clearContext();
        }
    }

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
        enqueueRaw(coupon.getId(), userId);
    }

    private void enqueueRaw(Long promotionId, String userId) {
        redisTemplate.opsForStream().add(StreamRecords.newRecord()
                .in(CouponStockStore.ISSUE_STREAM)
                .ofMap(Map.of("promotionId", String.valueOf(promotionId),
                        "userId", userId,
                        "claimedAt", String.valueOf(Instant.now().toEpochMilli()))));
    }

    /**
     * Queues an entry and hands it to another consumer in one step, before this pod's worker can
     * read it: what a pod that died before acknowledging leaves behind.
     */
    private void enqueueTakenBy(String consumer, Promotion coupon, String userId) {
        await().atMost(WAIT).until(() -> Boolean.TRUE.equals(redisTemplate.hasKey(CouponStockStore.ISSUE_STREAM)));
        RedisScript<String> script = RedisScript.of("""
                local id = redis.call('XADD', KEYS[1], '*', 'promotionId', ARGV[1], 'userId', ARGV[2], 'claimedAt', ARGV[3])
                redis.call('XREADGROUP', 'GROUP', ARGV[4], ARGV[5], 'COUNT', '1000', 'STREAMS', KEYS[1], '>')
                return id
                """, String.class);
        redisTemplate.execute(script, List.of(CouponStockStore.ISSUE_STREAM), String.valueOf(coupon.getId()),
                userId, String.valueOf(Instant.now().toEpochMilli()), CouponIssueWorker.GROUP, consumer);
    }

    private List<String> deadLettered(Long promotionId) {
        List<MapRecord<String, Object, Object>> records =
                redisTemplate.opsForStream().range(CouponStockStore.ISSUE_DEAD_LETTER, Range.unbounded());
        return records == null ? List.of() : records.stream()
                .filter(r -> String.valueOf(promotionId).equals(r.getValue().get("promotionId")))
                .map(r -> (String) r.getValue().get("userId"))
                .toList();
    }

    private List<String> consumerNames() {
        return redisTemplate.opsForStream().consumers(CouponStockStore.ISSUE_STREAM, CouponIssueWorker.GROUP)
                .stream().map(XInfoConsumer::consumerName).toList();
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
