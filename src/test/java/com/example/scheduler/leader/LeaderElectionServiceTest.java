package com.example.scheduler.leader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Exercises the state-machine logic (acquire / renew / step-down / release)
 * against a mocked RedisLeaderRepository — no real Redis needed here, since
 * the atomicity guarantees themselves are proven in RedisLeaderRepositoryIT.
 * This class is about "does the service react correctly to what Redis
 * tells it", not "is Redis's SET NX actually atomic".
 */
@ExtendWith(MockitoExtension.class)
class LeaderElectionServiceTest {

    private static final String INSTANCE_ID = "test-instance-1";

    @Mock
    private RedisLeaderRepository repository;

    private LeaderState state;
    private LeaderElectionService service;

    @BeforeEach
    void setUp() {
        LeaderElectionProperties properties = new LeaderElectionProperties();
        properties.setLeaseTtl(Duration.ofSeconds(30));
        properties.setRenewalInterval(Duration.ofSeconds(10));

        InstanceIdProvider fixedIdProvider = fixedInstanceIdProvider();
        state = new LeaderState(fixedIdProvider);
        service = new LeaderElectionService(repository, properties, state, fixedIdProvider);
    }

    @AfterEach
    void tearDown() {
        // Stops the background executor started by each test's service instance.
        // Safe to call even if a test already called shutdown() itself.
        service.shutdown();
    }

    private InstanceIdProvider fixedInstanceIdProvider() {
        // InstanceIdProvider generates a random id in its constructor; for
        // deterministic tests we need a fixed one. A tiny anonymous
        // subclass avoids adding test-only setters to production code.
        return new InstanceIdProvider() {
            @Override
            public String getInstanceId() {
                return INSTANCE_ID;
            }
        };
    }

    @Test
    void becomesLeaderWhenAcquisitionSucceeds() {
        when(repository.tryAcquire(eq(INSTANCE_ID), any(Duration.class))).thenReturn(true);

        service.onApplicationReady();

        assertThat(state.isLeader()).isTrue();
        assertThat(state.getLastRenewalAt()).isNotNull();
    }

    @Test
    void remainsStandbyWhenAcquisitionFails() {
        when(repository.tryAcquire(eq(INSTANCE_ID), any(Duration.class))).thenReturn(false);

        service.onApplicationReady();

        assertThat(state.isLeader()).isFalse();
    }

    @Test
    void releasesLeaseOnShutdownWhenCurrentlyLeader() {
        when(repository.tryAcquire(eq(INSTANCE_ID), any(Duration.class))).thenReturn(true);
        service.onApplicationReady();
        assertThat(state.isLeader()).isTrue();

        service.shutdown();

        verify(repository).release(INSTANCE_ID);
        assertThat(state.isLeader()).isFalse();
    }

    @Test
    void doesNotReleaseOnShutdownWhenNotLeader() {
        when(repository.tryAcquire(eq(INSTANCE_ID), any(Duration.class))).thenReturn(false);
        service.onApplicationReady();
        assertThat(state.isLeader()).isFalse();

        service.shutdown();

        verify(repository, never()).release(anyString());
    }

    @Test
    void renewalSuccessKeepsLeadershipAndUpdatesLastRenewalAt() throws Exception {
        when(repository.tryAcquire(eq(INSTANCE_ID), any(Duration.class))).thenReturn(true);
        service.onApplicationReady();
        Instant firstRenewal = state.getLastRenewalAt();

        Thread.sleep(5); // ensure a measurable clock difference
        when(repository.renew(eq(INSTANCE_ID), any(Duration.class))).thenReturn(true);
        invokeTick();

        assertThat(state.isLeader()).isTrue();
        assertThat(state.getLastRenewalAt()).isAfter(firstRenewal);
    }

    @Test
    void renewalFailureStepsDownImmediately() throws Exception {
        when(repository.tryAcquire(eq(INSTANCE_ID), any(Duration.class))).thenReturn(true);
        service.onApplicationReady();
        assertThat(state.isLeader()).isTrue();

        when(repository.renew(eq(INSTANCE_ID), any(Duration.class))).thenReturn(false);
        invokeTick();

        assertThat(state.isLeader()).isFalse();
    }

    /**
     * tick() is private (it's an internal implementation detail driven by
     * the scheduler, not part of the public contract), but the "renewal
     * failure steps down" behavior it implements is exactly what this test
     * needs to exercise deterministically without waiting on real timers.
     * Reflection here is a deliberate, narrow trade-off for that determinism.
     */
    private void invokeTick() throws Exception {
        var tickMethod = LeaderElectionService.class.getDeclaredMethod("tick");
        tickMethod.setAccessible(true);
        tickMethod.invoke(service);
    }
}
