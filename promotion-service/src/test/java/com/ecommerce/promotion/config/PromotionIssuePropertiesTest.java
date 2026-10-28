package com.ecommerce.promotion.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.ecommerce.promotion.config.PromotionIssueProperties.Store;
import com.ecommerce.promotion.repository.PromotionIssueRepository;
import com.ecommerce.promotion.repository.PromotionRepository;
import com.ecommerce.promotion.service.CouponStockStore;
import com.ecommerce.promotion.service.IssuedCountSynchronizer;
import com.ecommerce.promotion.service.StaleClaimSettler;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.context.properties.source.MapConfigurationPropertySource;
import org.springframework.scheduling.config.ScheduledTaskRegistrar;
import org.springframework.transaction.PlatformTransactionManager;

class PromotionIssuePropertiesTest {

    @ParameterizedTest
    @CsvSource({"redis,REDIS", "redis-async,REDIS_ASYNC", "REDIS_ASYNC,REDIS_ASYNC", "db,DB"})
    void bindsTheStoreFromTheEnvironmentValue(String value, Store expected) {
        Binder binder = new Binder(new MapConfigurationPropertySource(Map.of("promotion.issue.store", value)));

        PromotionIssueProperties properties = binder.bind("promotion.issue", PromotionIssueProperties.class).get();

        assertEquals(expected, properties.getStore());
    }

    @Test
    void bothRedisStoresUseRedis() {
        assertTrue(with(Store.REDIS).usesRedis());
        assertTrue(with(Store.REDIS_ASYNC).usesRedis());
        assertFalse(with(Store.DB).usesRedis());
    }

    @Test
    void issuedCountIsSyncedForBothRedisStores() {
        assertEquals(1, scheduledSyncs(Store.REDIS));
        assertEquals(1, scheduledSyncs(Store.REDIS_ASYNC));
        assertEquals(0, scheduledSyncs(Store.DB));
    }

    @Test
    void staleClaimsAreSettledOnlyForTheSynchronousRedisStore() {
        assertEquals(1, scheduledSettles(Store.REDIS));
        // Pending claims there are winners still queued for the worker.
        assertEquals(0, scheduledSettles(Store.REDIS_ASYNC));
        assertEquals(0, scheduledSettles(Store.DB));
    }

    private static PromotionIssueProperties with(Store store) {
        PromotionIssueProperties properties = new PromotionIssueProperties();
        properties.setStore(store);
        return properties;
    }

    private static int scheduledSyncs(Store store) {
        ScheduledTaskRegistrar registrar = new ScheduledTaskRegistrar();
        new IssuedCountSynchronizer(mock(PromotionRepository.class), with(store),
                mock(PlatformTransactionManager.class)).configureTasks(registrar);
        return registrar.getFixedDelayTaskList().size();
    }

    private static int scheduledSettles(Store store) {
        ScheduledTaskRegistrar registrar = new ScheduledTaskRegistrar();
        new StaleClaimSettler(mock(CouponStockStore.class), mock(PromotionIssueRepository.class), with(store))
                .configureTasks(registrar);
        return registrar.getFixedDelayTaskList().size();
    }
}
