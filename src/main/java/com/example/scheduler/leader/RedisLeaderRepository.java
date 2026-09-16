package com.example.scheduler.leader;

import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.SetArgs;
import io.lettuce.core.api.sync.RedisCommands;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.util.Optional;

/**
 * All ownership-sensitive operations (renew, release) are single Lua
 * scripts, not GET-then-write round trips, so the check and the write are
 * atomic from Redis's point of view. See the design writeup for why: a
 * separate GET followed by a separate EXPIRE/DEL reopens exactly the race
 * this class exists to close.
 */
@Repository
public class RedisLeaderRepository {

    static final String LEADER_KEY = "scheduler:leader";

    // Renew only if the caller still owns the key; returns 1 if renewed, 0 otherwise.
    private static final String RENEW_SCRIPT =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('EXPIRE', KEYS[1], ARGV[2]) "
                    + "else "
                    + "return 0 "
                    + "end";

    // Delete only if the caller still owns the key ("compare-and-delete");
    // returns 1 if deleted, 0 otherwise. Never a blind DEL.
    private static final String RELEASE_SCRIPT =
            "if redis.call('GET', KEYS[1]) == ARGV[1] then "
                    + "return redis.call('DEL', KEYS[1]) "
                    + "else "
                    + "return 0 "
                    + "end";

    private final RedisCommands<String, String> redis;

    public RedisLeaderRepository(RedisCommands<String, String> redis) {
        this.redis = redis;
    }

    /**
     * SET scheduler:leader {instanceId} NX EX {ttl}. Atomic at the Redis
     * command level: exactly one concurrent caller can ever see the key
     * absent and win.
     */
    public boolean tryAcquire(String instanceId, Duration ttl) {
        String result = redis.set(LEADER_KEY, instanceId, SetArgs.Builder.nx().ex(ttl.toSeconds()));
        return "OK".equals(result);
    }

    /**
     * Extends the TTL only if the key's current value still equals
     * instanceId. Returns false if the lease expired, was never held by
     * this instance, or is now held by someone else — all of which mean
     * "you are not the leader" from this instance's point of view.
     */
    public boolean renew(String instanceId, Duration ttl) {
        Long result = redis.eval(RENEW_SCRIPT, ScriptOutputType.INTEGER,
                new String[]{LEADER_KEY}, instanceId, String.valueOf(ttl.toSeconds()));
        return result != null && result == 1L;
    }

    /**
     * Compare-and-delete. Returns false (and deletes nothing) if this
     * instance doesn't currently own the key — e.g. its lease already
     * expired and someone else acquired it. Deleting unconditionally here
     * would delete that other instance's active lease.
     */
    public boolean release(String instanceId) {
        Long result = redis.eval(RELEASE_SCRIPT, ScriptOutputType.INTEGER,
                new String[]{LEADER_KEY}, instanceId);
        return result != null && result == 1L;
    }

    /** Read-only, for diagnostics/tests — not used in the acquire/renew/release hot path. */
    public Optional<String> currentLeader() {
        return Optional.ofNullable(redis.get(LEADER_KEY));
    }
}
