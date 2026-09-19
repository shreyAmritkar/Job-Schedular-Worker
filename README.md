# Distributed Job Scheduler

Stages 0–4 complete: project skeleton, job CRUD, Redis-based leader
election, a Redis-backed priority job queue with a worker pool, and
visibility-timeout-based crash recovery with retry/backoff, a dead-letter
queue, and idempotency-key-based duplicate-execution protection. No cron
scheduling or per-job distributed locking/fencing yet — that starts in
later stages.

## Stack

- Java 21, Spring Boot 3.3
- Maven
- PostgreSQL 16
- Redis 7 (via Lettuce, through `spring-boot-starter-data-redis`) — connected
  since Stage 0, not yet used for queueing
- Flyway
- JUnit 5, Testcontainers, Mockito
- Docker Compose

## Project layout

```
distributed-job-scheduler/
├── pom.xml
├── Dockerfile
├── docker-compose.yml
├── src/main/java/com/example/scheduler/
│   ├── SchedulerApplication.java
│   ├── job/                              # Stage 1: job CRUD
│   │   ├── Job.java                      # entity
│   │   ├── JobStatus.java                # ACTIVE, PAUSED, DISABLED, CANCELLED
│   │   ├── ScheduleType.java             # CRON, ONE_TIME
│   │   ├── JobRepository.java            # atomic cancel/trigger queries
│   │   ├── JobService.java               # business logic, transactions
│   │   ├── JobValidator.java             # scheduling domain rules
│   │   ├── JobMapper.java
│   │   ├── JobController.java
│   │   ├── dto/
│   │   │   ├── CreateJobRequest.java
│   │   │   └── JobResponse.java
│   │   └── exception/
│   │       ├── JobNotFoundException.java
│   │       ├── InvalidJobScheduleException.java
│   │       └── JobConflictException.java
│   ├── leader/                           # Stage 2: Redis leader election
│   │   ├── InstanceIdProvider.java       # one stable id per JVM
│   │   ├── LeaderElectionProperties.java # lease TTL, renewal/retry intervals, jitter
│   │   ├── LeaderState.java              # thread-safe current-state holder
│   │   ├── RedisLeaderRepository.java    # SET NX EX + Lua compare-and-swap
│   │   ├── LeaderElectionService.java    # acquire/renew/step-down state machine
│   │   ├── SchedulerStatusController.java
│   │   └── dto/SchedulerStatusResponse.java
│   ├── queue/                            # Stage 3: Redis priority queue + workers
│   │   ├── JobQueueService.java          # ZADD/ZPOPMIN/ZRANGEBYSCORE, backpressure, Lua claim/aging/promote
│   │   ├── JobQueueProperties.java       # backpressure threshold, aging timing
│   │   ├── QueuedJob.java                # jobId + score, returned by claim()
│   │   ├── WorkerPool.java               # configurable worker threads (claim -> ClaimedJobHandler)
│   │   ├── WorkerPoolProperties.java     # pool size, poll backoff
│   │   ├── JobExecutor.java              # the business-logic-only execution interface
│   │   ├── LoggingJobExecutor.java       # Stage 3's placeholder implementation
│   │   └── exception/QueueBackpressureException.java
│   ├── execution/                        # Stage 4: visibility timeout, retry, dead-letter, idempotency
│   │   ├── ExecutionProperties.java      # visibility timeout, recovery interval, backoff, idempotency TTL
│   │   ├── ClaimedJobHandler.java        # the seam WorkerPool calls after claim()
│   │   ├── JobExecutionCoordinator.java  # idempotency check -> execute -> record -> complete/retry
│   │   ├── RetryCoordinator.java         # shared retry/backoff/dead-letter decision
│   │   ├── RetryPolicy.java              # pure exponential-backoff formula
│   │   ├── IdempotencyGuard.java         # Redis SET NX-based duplicate-effect prevention
│   │   ├── JobRecoveryService.java       # leader-only, every 10s: reclaim expired claims, sweep backoff
│   │   ├── DeadLetterService.java        # list / manually requeue dead-lettered jobs
│   │   └── DeadLetterController.java
│   └── common/
│       ├── PageResponse.java             # stable pagination shape
│       ├── redis/RedisClientConfig.java  # raw Lettuce beans (not RedisTemplate)
│       └── exception/
│           ├── ApiError.java
│           └── GlobalExceptionHandler.java
├── src/main/resources/
│   ├── application.yml                   # shared config (JPA, Flyway, actuator)
│   ├── application-local.yml             # run from IDE, localhost DB/Redis
│   ├── application-docker.yml            # run inside docker compose
│   ├── application-test.yml              # Testcontainers overrides coordinates at runtime
│   └── db/migration/
│       ├── V1__init_schema.sql           # jobs, job_executions tables
│       └── V2__job_schedule_type.sql     # cron/one-time scheduling, CANCELLED status
└── src/test/java/com/example/scheduler/
    ├── AbstractIntegrationTest.java      # shared Postgres+Redis Testcontainers base
    ├── SchedulerApplicationIT.java       # context load + health check
    └── job/
        ├── JobValidatorTest.java         # Mockito/plain unit tests
        ├── JobServiceTest.java           # Mockito unit tests
        ├── JobRepositoryIT.java          # Testcontainers Postgres integration
        └── JobControllerIT.java          # full HTTP stack integration
```

## Running with Docker Compose

```bash
docker compose up --build
```

This starts Postgres 16, Redis 7, and the app (`docker` profile). The app
waits for both dependencies to report healthy before starting.

Verify:

```bash
curl localhost:8080/actuator/health
# {"status":"UP"}
```

## Running locally (IDE / CLI)

Start Postgres and Redis yourself (e.g. `docker compose up postgres redis`),
matching the credentials in `application-local.yml`
(`job_scheduler` / `scheduler` / `scheduler` on `localhost:5432` and
`localhost:6379`), then run the app with the `local` profile:

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

## Running tests

```bash
mvn clean verify
```

Tests use Testcontainers to spin up disposable Postgres and Redis containers
per JVM run — no docker-compose stack needs to be running first. Requires a
working Docker daemon on the machine running the tests.

## API (Stage 4)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/jobs/dead-letter` | List jobs currently in the dead-letter queue. |
| `POST` | `/api/jobs/dead-letter/{id}/requeue` | Reset a dead-lettered job back to ACTIVE with a fresh retry budget and push it back into `jobs:queue`. `404` unknown job, `409` job isn't currently dead-lettered. |

**Job model change:** every job now has an `idempotencyKey` (UUID), generated
server-side unless the caller supplies one at creation. Executors use it via
`IdempotencyGuard` to deduplicate side effects across possibly-duplicate
deliveries — see the design writeup for exactly why duplicates are possible
even with visibility timeout in place, and what this does and doesn't
protect against.

**New job status:** `FAILED` — set when a job exhausts its retry budget and
is moved to the dead-letter queue. The only way back to `ACTIVE` is the
manual requeue endpoint above.

Redis structures added:
- `jobs:processing` — sorted set, score = claim deadline (`now + visibilityTimeout`). A worker's claim on a job, not proof the work is happening.
- `jobs:delayed` — sorted set, score = `readyAt`. Holding area for a job between "recovery decided to retry it" and "its backoff has elapsed." Not named in the original spec — added because "re-enqueue after backoff" needs somewhere to wait without blocking the recovery loop. Swept back into `jobs:queue` on the same 10s cadence as recovery.
- `jobs:dead_letter` — list, job IDs that exhausted their retry budget.
- `jobs:idempotency:{key}` — individual `SET NX EX` keys, one per idempotency key claimed, TTL-bound.

Metrics: `jobs.retried`, `jobs.dead_lettered`, `recovery.runs`, `recovery.recovered`.

Backoff: `min(1000ms * 2^retryCount, 5 minutes)`, using the retry count
*after* incrementing.

Recovery: leader-only (checked via Stage 2's `LeaderState`), every 10
seconds — `ZRANGEBYSCORE jobs:processing 0 {now}` for expired claims, then
a sweep of `jobs:delayed` for anything whose backoff has elapsed.

**A residual risk, stated plainly:** visibility timeout does not, by
itself, prevent a job from being executed twice when a worker is slow but
not actually crashed — recovery can't tell the difference between "abandoned"
and "still running, just slow." This stage mitigates the *consequence*
(duplicate side effects) via idempotency keys, but does not eliminate the
*race* itself. Fixing that structurally needs fencing tokens / per-job
distributed locking, which is still out of scope. Full analysis in the
Stage 4 design writeup.

## API (Stage 3)

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs/{id}/enqueue` | Push an ACTIVE job into the Redis execution queue. `202` on success, `404` unknown job, `409` job not ACTIVE, `429` queue at backpressure threshold. |

**Priority direction, clarified:** as of Stage 3, `priority` is a P1-style
scale — **1 is the most urgent, 10 is the least urgent.** Stage 1 never
pinned this down explicitly; it's fixed now because the queue's scoring
formula requires a direction. See the Stage 3 design writeup (or ask) for
why.

Redis structures:
- `jobs:queue` — sorted set, score = `priority * 1_000_000_000 + enqueuedAtMillis`. Corrects the originally-specified formula, which used `-` instead of `+` on the timestamp and produced LIFO instead of the required FIFO within a priority tier.
- `jobs:queue:enqueued-at` — companion hash (`jobId -> true original enqueue timestamp`), needed because aging repeatedly modifies scores in a way that can't be reliably decoded back into "how long has this waited," so the true timestamp is tracked separately.

Backpressure: queue depth ≥ 10,000 rejects new submissions with `429` (soft
limit — see design writeup for the check-then-act race this accepts).
Aging: every 60s, jobs waiting over 5 minutes get their score reduced by
one full priority level's worth, repeatedly, until they reach the front.

Metrics (via Micrometer/Actuator, `/actuator/metrics/...`):
`scheduler.queue.depth`, `scheduler.worker.active`.

## API (Stage 2)

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/scheduler/status` | `{ instanceId, isLeader, lastRenewalAt }` for this instance. |

Redis key `scheduler:leader` holds the current leader's `instanceId` with a
30s TTL. Acquisition is `SET scheduler:leader {instanceId} NX EX 30`.
Renewal and release are Lua scripts that verify the caller still owns the
key before extending/deleting it — never a blind `EXPIRE`/`DEL`. See the
design writeup in-repo (or ask) for why that verification is required.

## Correction from Stage 1

`spring-boot-starter-validation` was missing from the POM. `@Valid` /
`@NotBlank` etc. on `CreateJobRequest` compile fine without it but throw
`NoProviderFoundException` at runtime the first time validation actually
runs — meaning `POST /api/jobs` would likely have failed on every request.
Added now; if you already had Stage 1 running, pull this dependency in.

## API (Stage 1)

All endpoints under `/api/jobs`.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/jobs` | Create a job. `201` + `Location` header on success. |
| `GET` | `/api/jobs?page=&size=` | Paginated list, `size` clamped to 100. |
| `GET` | `/api/jobs/{id}` | Fetch one job, `404` if missing. |
| `DELETE` | `/api/jobs/{id}` | Soft delete — sets `status = CANCELLED`. Idempotent (`204` even if already cancelled). |
| `POST` | `/api/jobs/{id}/trigger` | Marks the job ready for later scheduling by setting `next_run_at = now()`. Does **not** execute anything. `409` if the job isn't `ACTIVE`. |

**Create request body:**

```json
{
  "name": "nightly-backup",
  "cronExpression": "0 0 3 * * *",
  "runAt": null,
  "payload": { "any": "json" },
  "priority": 8,
  "maxRetries": 3
}
```

Exactly one of `cronExpression` / `runAt` must be set. `priority` is 1–10,
`maxRetries` is ≥ 0. `cronExpression` is validated with Spring's
`CronExpression.isValidExpression()` (six-field syntax, with seconds) — used
purely as a syntax validator, not to schedule anything.

## Schema

**`jobs`** (V1 + V2) — `id` (UUID), `name`, `schedule_type`
(`CRON`/`ONE_TIME`), `cron_expression` (nullable), `run_at` (nullable,
`TIMESTAMPTZ`), `payload` (JSONB), `priority` (1–10), `max_retries` (≥0),
`retry_count`, `status` (`ACTIVE`/`PAUSED`/`DISABLED`/`CANCELLED`),
`next_run_at`, `created_at`, `updated_at`. A `CHECK` constraint enforces
that exactly one of `cron_expression`/`run_at` is set, matching
`schedule_type` — enforced at the DB level, not just in application code.

**`job_executions`** — unchanged since Stage 0, still unused. No entity
exists for it yet; it's schema-only until a later stage needs to write
execution records.

## Troubleshooting

**`Remote host terminated the handshake` / `SSL peer shut down incorrectly`
during `docker compose build`** — this is a Maven HTTP connection pooling
issue against `repo.maven.apache.org`, not a project misconfiguration. The
Dockerfile already sets `MAVEN_OPTS` to disable connection pooling as a fix.
If it persists:

1. `docker compose build --no-cache app` (rule out a bad cached layer)
2. `docker run --rm curlimages/curl -sI https://repo.maven.apache.org/maven2/`
   to check whether Docker's build network can reach Maven Central at all
3. If you're on a VPN or corporate proxy with SSL inspection, that's the
   likely root cause — try without it, or add your org's CA to the build
   image's truststore

## Explicitly out of scope so far

- Any cron *execution* — cron expressions are validated, never evaluated/run
- Real job execution logic beyond LoggingJobExecutor's placeholder log line
- Per-job distributed locking / fencing tokens — the one gap visibility
  timeout + idempotency keys don't close; see the Stage 4 design writeup
- The leader automatically discovering due jobs from Postgres and calling
  the enqueue endpoint — still a Stage 5+ concern once cron evaluation exists
- Dashboards/alerting on the new metrics — they're registered via
  Micrometer and queryable at `/actuator/metrics/...`, nothing more yet
