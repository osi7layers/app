package com.ecommerce.promotion.service;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ecommerce.promotion.config.PromotionIssueProperties;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.RecordId;
import org.springframework.data.redis.connection.stream.StreamRecords;
import org.springframework.data.redis.core.StreamOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.PlatformTransactionManager;

/** The last try for an entry handed out too often: written, given up on, or kept. */
class CouponIssueWorkerTest {

    private static final RecordId ID = RecordId.of("1-0");
    private static final Long PROMOTION_ID = 7L;
    private static final String USER_ID = "late";

    private final StringRedisTemplate redis = mock(StringRedisTemplate.class);
    @SuppressWarnings("unchecked")
    private final StreamOperations<String, Object, Object> streams = mock(StreamOperations.class);
    private final CouponStockStore store = mock(CouponStockStore.class);
    private final PromotionIssueRepository issues = mock(PromotionIssueRepository.class);
    private final PlatformTransactionManager transactions = mock(PlatformTransactionManager.class);
    private CouponIssueWorker worker;

    @BeforeEach
    void setUp() {
        PromotionIssueProperties properties = new PromotionIssueProperties();
        properties.setStore(PromotionIssueProperties.Store.REDIS_ASYNC);
        properties.setWorkerMaxDeliveries(2);
        worker = new CouponIssueWorker(redis, store, mock(PromotionRepository.class), issues, properties,
                new CouponIssueQueueMonitor(redis, properties, new SimpleMeterRegistry()), transactions);

        when(redis.opsForStream()).thenReturn(streams);
        when(store.stuckQueued(eq(CouponIssueWorker.GROUP), any(Duration.class), any(Integer.class)))
                .thenReturn(List.of(new CouponStockStore.StuckEntry(ID, 2)));
        MapRecord<String, Object, Object> record = StreamRecords.newRecord()
                .in(CouponStockStore.ISSUE_STREAM).withId(ID)
                .ofMap(Map.<Object, Object>of("promotionId", "7", "userId", USER_ID, "claimedAt", "1"));
        when(streams.claim(eq(CouponStockStore.ISSUE_STREAM), eq(CouponIssueWorker.GROUP), anyString(),
                any(Duration.class), eq(ID))).thenReturn(List.of(record));
    }

    @Test
    void writtenOnTheLastTryIsCompletedNotGivenUp() {
        worker.reclaimStuck();

        verify(store).completeQueued(CouponIssueWorker.GROUP, List.of(ID), List.of(PROMOTION_ID), List.of(USER_ID));
        verify(store, never()).deadLetterQueued(any(), any(), anyMap());
        verify(store, never()).undoClaim(any(), any(), any(Boolean.class));
    }

    @Test
    void refusedByMysqlIsGivenUpAndTheCouponGivenBack() {
        when(issues.saveAndFlush(any())).thenThrow(new DataIntegrityViolationException("foreign key"));

        worker.reclaimStuck();

        verify(store).markFailed(PROMOTION_ID, USER_ID);
        verify(store).undoClaim(PROMOTION_ID, USER_ID, true);
        verify(store).deadLetterQueued(eq(CouponIssueWorker.GROUP), eq(ID), anyMap());
    }

    @Test
    void keptWhileMysqlCannotBeReached() {
        when(transactions.getTransaction(any())).thenThrow(new CannotCreateTransactionException("db down"));

        assertThrows(CannotCreateTransactionException.class, () -> worker.reclaimStuck());

        verify(store, never()).markFailed(any(), any());
        verify(store, never()).undoClaim(any(), any(), any(Boolean.class));
        verify(store, never()).deadLetterQueued(any(), any(), anyMap());
    }
}
