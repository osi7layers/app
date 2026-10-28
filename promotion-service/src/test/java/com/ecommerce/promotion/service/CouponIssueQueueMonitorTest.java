package com.ecommerce.promotion.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.ecommerce.promotion.config.PromotionIssueProperties;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.StringRedisTemplate;

class CouponIssueQueueMonitorTest {

    @Test
    void pollIntervalGrowsWithTheQueueAgeUpToThreeTimes() {
        assertEquals(1000, CouponIssueQueueMonitor.pollAfterMs(1000, 0));
        assertEquals(1000, CouponIssueQueueMonitor.pollAfterMs(1000, 4_999));
        assertEquals(2000, CouponIssueQueueMonitor.pollAfterMs(1000, 5_000));
        assertEquals(3000, CouponIssueQueueMonitor.pollAfterMs(1000, 10_000));
        assertEquals(3000, CouponIssueQueueMonitor.pollAfterMs(1000, 600_000));
    }

    @Test
    void synchronousStoresDoNotReadTheQueue() {
        StringRedisTemplate redis = mock(StringRedisTemplate.class);
        PromotionIssueProperties properties = new PromotionIssueProperties();
        properties.setStore(PromotionIssueProperties.Store.REDIS);
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        CouponIssueQueueMonitor monitor = new CouponIssueQueueMonitor(redis, properties, registry);

        assertEquals(1000, monitor.pollAfterMs());
        assertEquals(0.0, registry.get("coupon.issue.queue.length").gauge().value());
        verifyNoInteractions(redis);
    }
}
