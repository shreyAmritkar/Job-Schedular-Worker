package com.example.scheduler.common.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Project convention: Redis mechanics use raw Lettuce commands, not
 * Spring Data Redis's RedisTemplate. spring-boot-starter-data-redis stays
 * on the classpath (it's what powers /actuator/health's Redis check via
 * its auto-configured connection factory) — these beans are a separate,
 * deliberately lower-level path used only by leader election (and, later,
 * the job queue).
 */
@Configuration
public class RedisClientConfig {

    @Value("${spring.data.redis.host}")
    private String host;

    @Value("${spring.data.redis.port}")
    private int port;

    @Bean(destroyMethod = "shutdown")
    public RedisClient redisClient() {
        return RedisClient.create(RedisURI.create(host, port));
    }

    @Bean(destroyMethod = "close")
    public StatefulRedisConnection<String, String> redisConnection(RedisClient redisClient) {
        return redisClient.connect();
    }

    @Bean
    public RedisCommands<String, String> redisCommands(StatefulRedisConnection<String, String> connection) {
        return connection.sync();
    }
}
