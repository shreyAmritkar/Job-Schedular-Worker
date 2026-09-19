package com.example.scheduler.execution;

import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.stereotype.Component;

/**
 * Deduplicates side effects across possibly-duplicate deliveries of the
 * same logical job (the entire reason at-least-once delivery needs this
 * layer — see the Stage 4 design writeup's race analysis). Does NOT
 * prevent two workers from both being told to run the same job; it only
 * ensures at most one of them actually performs the real side effect.
 */
@Component
public class IdempotencyGuard {

    static final String KEY_PREFIX = "jobs:idempotency:";

    private final RedisCommands<String, String> redis;
    private final ExecutionProperties properties;

    public IdempotencyGuard(RedisCommands<String, String> redis, ExecutionProperties properties) {
        this.redis = redis;
        this.properties = properties;
    }

    /**
     * Returns true if this call is the first to claim the key (the caller
     * should proceed with the real side effect), false if it was already
     * claimed (the caller should skip it — this is a duplicate).
     */
    public boolean tryClaim(String idempotencyKey) {
        String result = redis.set(KEY_PREFIX + idempotencyKey, "1",
                SetArgs.Builder.nx().ex(properties.getIdempotencyKeyTtl().toSeconds()));
        return "OK".equals(result);
    }
}
