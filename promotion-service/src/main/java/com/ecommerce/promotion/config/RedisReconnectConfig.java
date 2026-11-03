package com.ecommerce.promotion.config;

import io.lettuce.core.resource.Delay;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.autoconfigure.data.redis.ClientResourcesBuilderCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Caps Lettuce's reconnect backoff at 1s (default: up to 30s).
 *
 * <p>With the default, a redis-coupon pod restart left coupon issuing stalled for 9~17s even
 * though the new pod was ready in about 7~8s. Retrying at least every second brings the stall
 * close to the pod start time.</p>
 */
@Configuration(proxyBeanMethods = false)
public class RedisReconnectConfig {

    private static final Duration MIN_RECONNECT_DELAY = Duration.ofMillis(100);
    private static final Duration MAX_RECONNECT_DELAY = Duration.ofSeconds(1);

    @Bean
    ClientResourcesBuilderCustomizer redisReconnectDelayCustomizer() {
        return builder -> builder.reconnectDelay(
                Delay.exponential(MIN_RECONNECT_DELAY, MAX_RECONNECT_DELAY, 2, TimeUnit.MILLISECONDS));
    }
}
