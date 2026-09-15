# Distributed Job Scheduler

Stage 0 + Stage 1 complete: project skeleton with verified PostgreSQL/Redis
connectivity, Flyway-managed schema, a health endpoint, and job CRUD (create,
list, retrieve, soft-delete/cancel, manual trigger). No Redis queue mechanics,
leader election, worker pools, cron execution, retries, or distributed locks
yet — that starts in later stages.

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
│   └── common/
│       ├── PageResponse.java             # stable pagination shape
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

- Redis queue mechanics (raw Lettuce commands come later)
- Any cron *execution* — cron expressions are validated, never evaluated/run
- Leader election, worker pools, visibility timeout, distributed locks
- Retry execution logic (the `max_retries`/`retry_count` columns exist but
  nothing acts on them yet)
- A `JobExecution` entity/repository — `job_executions` is schema-only