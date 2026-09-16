package com.example.scheduler.leader;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the atomicity guarantees directly against real Redis — this is
 * where "two instances never both believe they hold the lease" actually
 * lives, since it's a property of Redis's SET NX and Lua EVAL, not of
 * anything above the repository layer. A full two-Spring-context test
 * would exercise this same guarantee through far more moving parts without
 * proving anything additional about the atomicity itself.
 *
 * Deliberately not extending AbstractIntegrationTest: no Postgres, no web
 * server, no JPA needed here, so a separate lightweight Redis-only
 * container keeps this fast and focused.
 */
@Testcontainers
class RedisLeaderRepositoryIT {

    private static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7")).withExposedPorts(6379);

    private static RedisClient redisClient;
    private static StatefulRedisConnection<String, String> connection;
    private static RedisCommands<String, String> commands;

    private RedisLeaderRepository repository;

    @BeforeAll
    static void startRedis() {
        REDIS.start();
        redisClient = RedisClient.create(RedisURI.create(REDIS.getHost(), REDIS.getMappedPort(6379)));
        connection = redisClient.connect();
        commands = connection.sync();
    }

    @AfterAll
    static void stopRedis() {
        connection.close();
        redisClient.shutdown();
        REDIS.stop();
    }

    @BeforeEach
    void setUp() {
        repository = new RedisLeaderRepository(commands);
    }

    @AfterEach
    void cleanUp() {
        commands.del(RedisLeaderRepository.LEADER_KEY);
    }

    @Test
    void tryAcquire_succeedsWhenKeyAbsent() {
        boolean acquired = repository.tryAcquire("instance-a", Duration.ofSeconds(30));

        assertThat(acquired).isTrue();
        assertThat(repository.currentLeader()).contains("instance-a");
    }

    @Test
    void tryAcquire_failsWhenAlreadyHeldByAnotherInstance() {
        repository.tryAcquire("instance-a", Duration.ofSeconds(30));

        boolean secondAcquire = repository.tryAcquire("instance-b", Duration.ofSeconds(30));

        assertThat(secondAcquire).isFalse();
        assertThat(repository.currentLeader()).contains("instance-a"); // unchanged
    }

    @Test
    void renew_succeedsWhenOwnedByCaller() {
        repository.tryAcquire("instance-a", Duration.ofSeconds(2));

        boolean renewed = repository.renew("instance-a", Duration.ofSeconds(30));

        assertThat(renewed).isTrue();
        Long ttl = commands.ttl(RedisLeaderRepository.LEADER_KEY);
        assertThat(ttl).isGreaterThan(2); // TTL was actually extended, not just left alone
    }

    @Test
    void renew_failsWhenOwnedByAnotherInstance() {
        repository.tryAcquire("instance-a", Duration.ofSeconds(30));

        // instance-b never legitimately held the key, but tries to renew as if it did.
        boolean renewed = repository.renew("instance-b", Duration.ofSeconds(30));

        assertThat(renewed).isFalse();
        assertThat(repository.currentLeader()).contains("instance-a"); // instance-a's lease untouched
    }

    @Test
    void renew_failsAfterLeaseExpires() throws InterruptedException {
        repository.tryAcquire("instance-a", Duration.ofSeconds(1));

        Thread.sleep(1500); // let the 1s TTL genuinely expire

        boolean renewed = repository.renew("instance-a", Duration.ofSeconds(30));

        assertThat(renewed).isFalse();
        assertThat(repository.currentLeader()).isEmpty();
    }

    @Test
    void release_deletesWhenOwnedByCaller() {
        repository.tryAcquire("instance-a", Duration.ofSeconds(30));

        boolean released = repository.release("instance-a");

        assertThat(released).isTrue();
        assertThat(repository.currentLeader()).isEmpty();
    }

    @Test
    void release_doesNotDeleteWhenOwnedByAnotherInstance() {
        // instance-a holds the lease, then (simulating a resumed zombie process)
        // instance-b acquires it after instance-a's lease would have expired.
        repository.tryAcquire("instance-a", Duration.ofSeconds(30));
        commands.del(RedisLeaderRepository.LEADER_KEY); // simulate instance-a's lease having expired
        repository.tryAcquire("instance-b", Duration.ofSeconds(30));

        // instance-a, unaware it lost the lease, tries to release "its" lock.
        boolean released = repository.release("instance-a");

        assertThat(released).isFalse();
        assertThat(repository.currentLeader()).contains("instance-b"); // instance-b's lease survives untouched
    }

    @Test
    void concurrentAcquisition_exactlyOneInstanceWins() throws Exception {
        int contenderCount = 20;
        ExecutorService pool = Executors.newFixedThreadPool(contenderCount);
        CountDownLatch startGate = new CountDownLatch(1);
        AtomicInteger successCount = new AtomicInteger();

        List<Callable<Boolean>> attempts = IntStream.range(0, contenderCount)
                .mapToObj(i -> (Callable<Boolean>) () -> {
                    startGate.await(); // release all threads at once to maximize race pressure
                    boolean acquired = repository.tryAcquire("contender-" + i, Duration.ofSeconds(30));
                    if (acquired) {
                        successCount.incrementAndGet();
                    }
                    return acquired;
                })
                .collect(Collectors.toList());

        List<Future<Boolean>> futures = attempts.stream().map(pool::submit).collect(Collectors.toList());
        startGate.countDown();
        for (Future<Boolean> future : futures) {
            future.get(5, TimeUnit.SECONDS);
        }
        pool.shutdown();

        assertThat(successCount.get()).isEqualTo(1);
    }

    @Test
    void keyUsesTheSpecifiedRedisKeyName() {
        assertThat(RedisLeaderRepository.LEADER_KEY).isEqualTo("scheduler:leader");
    }
}
