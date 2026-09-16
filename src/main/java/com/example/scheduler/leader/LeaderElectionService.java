package com.example.scheduler.leader;

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Drives the leader-election state machine on a single dedicated background
 * thread (java.util.concurrent.ScheduledExecutorService — deliberately not
 * Spring's @Scheduled, per project convention: that's reserved for keeping
 * the actual job-scheduling feature off Spring's scheduling abstractions,
 * not for infrastructure like this heartbeat loop).
 *
 * Uses self-rescheduling (schedule the next tick only after the current one
 * finishes) rather than scheduleAtFixedRate, for two reasons: the delay
 * differs depending on state (fixed 10s renewal interval for the leader vs.
 * jittered retry for a standby), and fixed-rate scheduling can let ticks
 * overlap if one runs long — self-rescheduling never does.
 */
@Service
public class LeaderElectionService {

    private static final Logger log = LoggerFactory.getLogger(LeaderElectionService.class);

    private final RedisLeaderRepository repository;
    private final LeaderElectionProperties properties;
    private final LeaderState state;
    private final String instanceId;

    private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(runnable -> {
        Thread thread = new Thread(runnable, "leader-election");
        thread.setDaemon(true);
        return thread;
    });

    public LeaderElectionService(RedisLeaderRepository repository,
                                  LeaderElectionProperties properties,
                                  LeaderState state,
                                  InstanceIdProvider instanceIdProvider) {
        this.repository = repository;
        this.properties = properties;
        this.state = state;
        this.instanceId = instanceIdProvider.getInstanceId();
    }

    /**
     * Wait for full application startup (so the Redis connection beans are
     * definitely ready) before the first acquisition attempt, then start
     * the self-rescheduling tick loop.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void onApplicationReady() {
        log.info("Instance {} starting leader election", instanceId);
        tick();
    }

    private void tick() {
        try {
            if (state.isLeader()) {
                renew();
            } else {
                tryAcquire();
            }
        } catch (RuntimeException e) {
            log.error("Instance {} hit an unexpected error during leader election", instanceId, e);
            // Fail safe: if we're not certain we still hold the lease, assume we don't.
            if (state.isLeader()) {
                state.markStandby();
                log.warn("Instance {} LOST leadership due to an unexpected error: {}", instanceId, e.getMessage());
            }
        } finally {
            scheduleNextTick();
        }
    }

    private void tryAcquire() {
        boolean acquired = repository.tryAcquire(instanceId, properties.getLeaseTtl());
        if (acquired) {
            state.markLeader(Instant.now());
            log.info("Instance {} ACQUIRED leadership", instanceId);
        }
        // Failing to acquire is the expected steady state for standbys — no per-attempt log spam.
    }

    private void renew() {
        boolean renewed = repository.renew(instanceId, properties.getLeaseTtl());
        if (renewed) {
            state.markRenewed(Instant.now());
            log.debug("Instance {} renewed leadership lease", instanceId);
        } else {
            state.markStandby();
            log.warn("Instance {} LOST leadership: renewal failed "
                    + "(lease expired or taken over by another instance)", instanceId);
        }
    }

    private void scheduleNextTick() {
        long delayMillis = state.isLeader()
                ? properties.getRenewalInterval().toMillis()
                : properties.getRetryInterval().toMillis() + jitterMillis();

        executor.schedule(this::tick, delayMillis, TimeUnit.MILLISECONDS);
    }

    private long jitterMillis() {
        long maxJitter = properties.getRetryJitter().toMillis();
        return maxJitter <= 0 ? 0 : ThreadLocalRandom.current().nextLong(0, maxJitter + 1);
    }

    /**
     * Graceful shutdown: if we currently believe we're leader, actively
     * release the lease (compare-and-delete) so a standby doesn't have to
     * wait out the remaining TTL before taking over.
     */
    @PreDestroy
    public void shutdown() {
        executor.shutdownNow();
        if (state.isLeader()) {
            boolean released = repository.release(instanceId);
            state.markStandby();
            log.info("Instance {} STEPPED DOWN as leader (lease released={})", instanceId, released);
        }
    }
}
