# Distributed Job Scheduler — Stage 2

## Redis Leader Election

Stage 2 introduces **Redis-based leader election** for the distributed job scheduler.

Multiple Spring Boot scheduler instances can run simultaneously, but only one instance should act as the **scheduler leader** at a time.

> **Stage 2 does not implement the job queue, workers, job execution, or retries.**

---

## 1. Objective

The objective of Stage 2 is to ensure that when multiple scheduler instances are running:

```text
Instance A ─┐
Instance B ─┼──> Redis ──> One Leader
Instance C ─┘
```

Only one instance acquires the scheduler leadership lease.

If the current leader fails or loses its lease, another instance can acquire leadership.

---

## 2. Technology Used

* Java 21
* Spring Boot 3.3
* Maven
* Redis 7
* Lettuce Redis Client
* JUnit 5
* Mockito
* Testcontainers
* Docker / Docker Compose

Redis is used only for **leader election** in this stage.

---

# 3. Redis Leader Key

The leader is stored using:

```text
scheduler:leader
```

The value of the key is the unique `instanceId` of the current leader.

Example:

```text
Key:
scheduler:leader

Value:
scheduler-01-a83f12bc
```

The key has a **30-second TTL**.

---

# 4. Leader Acquisition

A scheduler attempts to acquire leadership using:

```redis
SET scheduler:leader {instanceId} NX EX 30
```

### Meaning

`NX`:

```text
Only create the key if it does not already exist.
```

`EX 30`:

```text
Set the key's expiration time to 30 seconds.
```

Because Redis executes this command atomically, two instances cannot both successfully acquire the same existing lease.

### Example

Suppose instances A and B start at approximately the same time.

```text
Instance A:
SET scheduler:leader A NX EX 30
        ↓
       OK
        ↓
     LEADER
```

```text
Instance B:
SET scheduler:leader B NX EX 30
        ↓
      NIL
        ↓
    STANDBY
```

Only A successfully acquires the lease.

---

# 5. Instance ID

Every application instance generates a unique `instanceId`.

Example:

```text
scheduler-host-a83f12bc
```

The ID is generated once when the application starts and remains constant for that JVM lifetime.

The instance ID is stored in:

```text
InstanceIdProvider.java
```

It is used as the Redis value:

```text
scheduler:leader → instanceId
```

This allows an instance to verify whether it still owns the leader lease.

---

# 6. Leader Election Lifecycle

The leader election state machine is:

```text
                 Application Start
                        |
                        v
                 Try to acquire
                   /         \
              Success        Failure
                |               |
                v               v
             LEADER          STANDBY
                |               |
                |               |
          Renew every 10s   Retry periodically
                |               |
             Success         Acquisition
                |               |
                v               v
             LEADER          LEADER
                |
             Failure
                |
                v
          Step down
                |
                v
             STANDBY
```

---

# 7. Leader Renewal

The leader renews its lease every **10 seconds**.

The lease itself is 30 seconds.

Therefore:

```text
Lease TTL       = 30 seconds
Renewal period  = 10 seconds
```

Renewal does **not** blindly call:

```redis
EXPIRE scheduler:leader 30
```

Instead, the current Redis value is first compared with the instance's own ID.

Conceptually:

```text
IF scheduler:leader == my instanceId
    renew TTL
ELSE
    renewal fails
```

This operation is implemented using a Redis Lua script.

---

# 8. Why Ownership Verification Is Required

Consider this situation:

```text
Instance A is leader
        |
        v
A stops/pause for > 30 seconds
        |
        v
Redis lease expires
        |
        v
Instance B becomes leader
```

Redis now contains:

```text
scheduler:leader → B
```

If A wakes up and blindly executes:

```redis
EXPIRE scheduler:leader 30
```

A could accidentally renew B's lease.

Therefore renewal must verify:

```text
Redis value == my instanceId
```

If the comparison fails, A immediately steps down.

---

# 9. Step Down on Lease Loss

If the leader cannot successfully renew its lease, it immediately stops considering itself the leader.

The state changes:

```text
LEADER
   |
   | renewal failure
   v
STANDBY
```

The transition is logged as:

```text
LOST leadership
STEPPED DOWN
```

This provides fail-closed behavior when ownership can no longer be verified.

---

# 10. Standby Retry and Jitter

Instances that are not leaders periodically attempt to acquire leadership.

A fixed retry interval is used with random jitter.

Example:

```text
Retry interval = 5 seconds
Jitter         = 0–2 seconds
```

Therefore different instances may retry at:

```text
Instance A → 5.1 seconds
Instance B → 6.3 seconds
Instance C → 5.7 seconds
Instance D → 6.8 seconds
```

Instead of:

```text
A → 5 sec
B → 5 sec
C → 5 sec
D → 5 sec
```

Random jitter reduces the **thundering herd problem**, where many standby instances simultaneously contact Redis after a lease becomes available.

---

# 11. Compare-and-Delete

When an instance releases leadership, it must not blindly execute:

```redis
DEL scheduler:leader
```

### Why?

Suppose:

```text
A = old leader

A pauses
    ↓
Lease expires
    ↓
B becomes leader
    ↓
scheduler:leader = B

A wakes up
    ↓
DEL scheduler:leader
```

A would accidentally delete B's valid leadership lease.

Therefore release uses a Lua compare-and-delete operation:

```text
IF scheduler:leader == my instanceId
    DELETE scheduler:leader
ELSE
    DO NOTHING
```

---

# 12. Why Lua Is Used

A normal Java implementation might do:

```text
GET scheduler:leader
        |
        v
Compare value
        |
        v
DEL / EXPIRE
```

This creates a race between the `GET` and the mutation.

Another instance could acquire the lease between those operations.

Lua allows the comparison and mutation to happen atomically inside Redis.

The two important Lua operations are:

```text
Compare-and-Expire
```

and:

```text
Compare-and-Delete
```

---

# 13. Project Structure

The Stage 2 code is mainly located under:

```text
src/main/java/com/example/scheduler/

├── common/
│   └── redis/
│       └── RedisClientConfig.java
│
└── leader/
    ├── InstanceIdProvider.java
    ├── LeaderElectionProperties.java
    ├── LeaderState.java
    ├── RedisLeaderRepository.java
    ├── LeaderElectionService.java
    ├── SchedulerStatusController.java
    │
    └── dto/
        └── SchedulerStatusResponse.java
```

### Important classes

| Class                       | Responsibility                                  |
| --------------------------- | ----------------------------------------------- |
| `RedisClientConfig`         | Creates Redis/Lettuce connection                |
| `InstanceIdProvider`        | Generates unique instance ID                    |
| `LeaderElectionProperties`  | Stores lease/retry/renewal configuration        |
| `RedisLeaderRepository`     | Performs Redis acquisition, renewal and release |
| `LeaderState`               | Stores current local leader state               |
| `LeaderElectionService`     | Controls the leader-election state machine      |
| `SchedulerStatusController` | Exposes scheduler status API                    |
| `SchedulerStatusResponse`   | Response DTO                                    |

---

# 14. Redis Configuration

Redis configuration is defined through Spring configuration.

Example:

```yaml
spring:
  data:
    redis:
      host: localhost
      port: 6379

scheduler:
  leader-election:
    lease-ttl: 30s
    renewal-interval: 10s
    retry-interval: 5s
    retry-jitter: 2s
```

---

# 15. API

Stage 2 exposes:

```http
GET /api/scheduler/status
```

Example response:

```json
{
  "instanceId": "scheduler-01-a83f12bc",
  "isLeader": true,
  "lastRenewalAt": "2026-09-16T10:30:15Z"
}
```

### Fields

| Field           | Description                                             |
| --------------- | ------------------------------------------------------- |
| `instanceId`    | Unique ID of this application instance                  |
| `isLeader`      | Whether this instance currently considers itself leader |
| `lastRenewalAt` | Timestamp of the last successful lease renewal          |

---

# 16. Leadership Logs

Leadership transitions are logged.

### Acquisition

```text
ACQUIRED leadership
```

### Lease loss

```text
LOST leadership
```

### Step down

```text
STEPPED DOWN
```

These logs make it possible to observe leadership changes across multiple scheduler instances.

---

# 17. Testing

Stage 2 includes tests for:

### 1. Leader acquisition

Verify that an instance can acquire an empty leader key.

### 2. Failed acquisition

Verify that an instance becomes standby when another instance owns the key.

### 3. Two-instance election

Run two logical scheduler instances and verify that only one acquires the lease.

### 4. Renewal

Verify that the leader's lease is renewed when it still owns the key.

### 5. Lease loss

Verify that the instance steps down when renewal fails or ownership is lost.

### 6. Compare-and-delete

Verify that:

```text
Owner A → can delete A's key
Owner B → cannot delete A's key
```

### 7. Concurrent acquisition

Multiple instances attempt acquisition concurrently.

Redis must allow only one successful acquisition.

### 8. Testcontainers

Redis integration tests use Testcontainers so tests can run against a real Redis instance rather than a mocked Redis implementation.

Run:

```bash
mvn clean verify
```

A Docker daemon must be available for Testcontainers.

---

# 18. Failure Scenarios

## Process crashes

If the leader process crashes:

```text
Leader
  ↓
Process stops
  ↓
No renewal
  ↓
30-second TTL expires
  ↓
Another instance can acquire leadership
```

No explicit unlock is required for crash recovery.

---

## Process pauses longer than 30 seconds

Suppose:

```text
A = leader
```

A then pauses for more than 30 seconds.

Redis expires:

```text
scheduler:leader
```

Another instance B can acquire it.

When A resumes, it must not continue acting as leader merely because its local state says it was previously leader.

The renewal/ownership check causes A to step down when it cannot prove ownership.

---

## Redis unavailable

If the application cannot communicate with Redis, it cannot safely confirm leadership.

The implementation therefore fails closed and steps down rather than continuing as an unverified leader.

---

# 19. Limitations of Redis Leader Election

This implementation provides a **Redis lease**, not a full consensus protocol.

Important limitations include:

* Redis availability directly affects leader election.
* Network partitions can prevent an instance from confirming leadership.
* A process can pause longer than the lease and resume with stale local state.
* TTL provides lease expiration but is not a fencing mechanism.
* Redis failover configuration can affect the guarantees of the system.
* There are no fencing tokens or leader epochs in Stage 2.

Therefore, later stages that perform important external side effects may require **fencing tokens / leader epochs** to prevent a stale process from performing work after losing leadership.

---

# 20. Why TTL Alone Is Not Enough

A TTL answers:

```text
"When should this Redis key expire?"
```

It does not answer:

```text
"Does this particular process still own the key?"
```

For example:

```text
A owns lease
    ↓
A pauses
    ↓
Lease expires
    ↓
B acquires lease
    ↓
A resumes
```

A TTL alone cannot prevent A from believing it is still leader.

Therefore Stage 2 combines:

```text
Unique instance ID
        +
SET NX EX
        +
Ownership verification
        +
Lua compare-and-expire
        +
Lua compare-and-delete
        +
Immediate step-down
```

---

# 21. Stage 2 Scope

### Included

* Redis integration
* Unique scheduler instance IDs
* Leader acquisition
* 30-second lease
* Lease renewal every 10 seconds
* Standby retry
* Random retry jitter
* Ownership verification
* Lua compare-and-expire
* Lua compare-and-delete
* Leader state tracking
* Leadership transition logging
* Scheduler status endpoint
* Redis integration tests

### Not Included

The following are intentionally **not implemented in Stage 2**:

* Job queue
* Redis queue
* Worker processes
* Job execution
* Cron execution
* Retry execution
* Job claiming
* Fencing tokens
* Leader epoch numbers
* Execution workers

These belong to later stages.

---

# 22. Summary

Stage 2 establishes a Redis-backed leader election mechanism:

```text
Multiple Spring Boot Instances
             |
             v
     Redis Leader Key
   scheduler:leader
             |
       ┌─────┴─────┐
       |           |
    Leader       Standby
       |           |
    Renew       Retry
       |           |
       └─────┬─────┘
             |
      Leadership Changes
```

The key safety properties are:

```text
SET NX EX
    ↓
Atomic acquisition

Lua compare-and-expire
    ↓
Only current owner can renew

Lua compare-and-delete
    ↓
Only current owner can release

Failed renewal
    ↓
Immediate step-down
```

This completes the Redis Leader Election stage without implementing Stage 3 job queue or worker functionality.
