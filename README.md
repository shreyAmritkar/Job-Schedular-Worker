# Distributed Job Scheduler

Stage 0: project skeleton with verified connectivity to PostgreSQL and Redis,
Flyway-managed schema, and a health endpoint. No scheduling, queueing, or
execution logic yet — that starts in later stages.

## Stack

- Java 21, Spring Boot 3.3
- Maven
- PostgreSQL 16
- Redis 7 (via Lettuce, through `spring-boot-starter-data-redis`)
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
│   └── SchedulerApplication.java
├── src/main/resources/
│   ├── application.yml            # shared config (JPA, Flyway, actuator)
│   ├── application-local.yml      # run from IDE, localhost DB/Redis
│   ├── application-docker.yml     # run inside docker compose
│   ├── application-test.yml       # Testcontainers overrides coordinates at runtime
│   └── db/migration/
│       └── V1__init_schema.sql    # jobs, job_executions tables
└── src/test/java/com/example/scheduler/
    ├── AbstractIntegrationTest.java  # shared Postgres+Redis Testcontainers base
    └── SchedulerApplicationIT.java   # context load + health check
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

## Schema (V1)

**`jobs`** — `id` (UUID), `name`, `cron_expression`, `payload` (JSONB),
`priority`, `max_retries`, `retry_count`, `status`
(`ACTIVE`/`PAUSED`/`DISABLED`), `next_run_at`, `created_at`, `updated_at`.

**`job_executions`** — `id` (UUID), `job_id` (FK → `jobs.id`, `ON DELETE
CASCADE`), `worker_id`, `started_at`, `finished_at`, `status`
(`PENDING`/`RUNNING`/`SUCCESS`/`FAILED`/`TIMEOUT`), `error_message`.

No JPA entities exist yet — Stage 0 only proves Flyway can create the schema
and Hibernate can validate against it (`ddl-auto: validate`). Entities and
repositories arrive when a later stage actually needs to read/write rows.

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

## Explicitly out of scope for Stage 0

- JPA entities, repositories, or any CRUD
- Redis queue mechanics (raw Lettuce commands come later)
- Any scheduling logic or cron evaluation
- Worker registration or execution logic
