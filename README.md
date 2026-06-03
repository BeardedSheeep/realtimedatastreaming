<!-- Copyright (c) 2026 BeardedSheeep -->

# Realtime User Profile Quality Streaming

`realtimedatastreaming` is a deliberately small real-time user profile ingestion and quality monitoring service.

The product scope is intentionally limited: fetch demo user profiles, normalize them into explicit events, validate data quality, publish Kafka events, and expose enough operational signals to run the service professionally.

The engineering goal is broader than the product goal: this repository should become a compact but production-grade example of CI/CD, deployment automation, observability, SRE practices, and continuous security.

## Goal

The functional goal is to answer one narrow question:

> Are user profiles being ingested correctly, and which profiles are valid, invalid, enriched, or failing quality checks?

The target MVP pipeline follows this flow:

```text
Random User API
    |
    v
Python ingestion job
    |
    v
Kafka topic: users_created
Kafka topic: users_created_invalid
```

The platform should make it possible to:

- fetch user profiles from `https://randomuser.me/api/` as a reproducible demo source;
- normalize useful user fields into a stable `UserCreated` event;
- publish valid ingestion events to Kafka;
- validate required fields and basic data quality rules;
- separate invalid profiles into a dead-letter or invalid-events topic;
- run deterministic tests and a real Kafka/Schema Registry integration test;
- keep a tested, typed, linted, secure, deployable Python foundation.

The project keeps the distributed data platform direction: Airflow, Spark, Cassandra, dashboards, and long-term analytical storage remain part of the target architecture. They are sequenced after the first production-shaped ingestion slice so the system grows with operational discipline instead of becoming a pile of services.

The target operational goal is to make this small service deployable like a serious production workload: automated releases, repeatable environments, safe rollout patterns, actionable SLOs, runbooks, and security checks in the delivery path.

## Current Repository State

The repository already contains the Python application foundation:

- importable `realtimedatastreaming` package;
- `realtimedatastreaming` smoke-test CLI;
- typed application settings with Pydantic Settings;
- Random User ingestion boundary with typed normalized profiles;
- HTTP timeout, retry, backoff, rate limiting, and failure classification for ingestion;
- `UserCreated` and `UserProfileInvalid` profile contracts;
- versioned Kafka JSON Schema artifacts for Schema Registry;
- Confluent Schema Registry registration helpers with topic-value subject naming;
- Schema Registry-aware Kafka publisher with async, single-message sync, and batch sync publishing paths;
- optional Docker Compose integration test for Kafka and Schema Registry;
- user profile quality rules with explicit rejection reasons;
- privacy-safe invalid profile events with allowlisted payloads and salted source id pseudonymization;
- JSON/text logging with correlation context;
- dependency management with `uv`;
- developer checks orchestrated with `nox`;
- Ruff, mypy, pytest, coverage, and pip-audit;
- minimal application Dockerfile;
- GitHub Actions workflows for quality, CI, and CI/CD.

## Product Scope

### In Scope

- Random User ingestion boundary;
- normalized user profile event contracts;
- semantic quality validation;
- privacy-reduced invalid events;
- Kafka publication with Schema Registry JSON Schema contracts;
- Airflow DAG orchestration;
- Spark Streaming processing;
- Cassandra persistence for queryable profiles and quality views;
- local and CI quality gates;
- optional local integration test against real Kafka and Schema Registry;
- deployment and SRE documentation.

### Out Of Scope

- multi-source ingestion;
- complex enrichment beyond simple quality metadata.

The first deployable slice is intentionally narrow, but Airflow orchestration, Spark Streaming processing, and Cassandra persistence remain part of the project scope and must be implemented as staged platform capabilities.

## Planned Architecture

### Functional Domain

The core domain is user profile ingestion quality:

- `UserCreated`: normalized user profile accepted by the ingestion boundary;
- `UserProfileInvalid`: rejected profile with explicit rejection reasons;
- quality counters: counts by validation status, source, and rejection reason.

### Execution Model

The first execution model is a small Python ingestion job. It will call application code responsible for fetching, normalizing, validating, and publishing user profile events.
The current Airflow DAG is an orchestration scaffold: it validates the runtime path and reserves the ingestion task, but the Random User API ingestion is not plugged into the DAG yet.

Scheduling starts with Kubernetes CronJob for a production-shaped MVP. Airflow is still part of the target platform and should take over when the pipeline has multiple tasks, backfills, retries, dataset dependencies, and operational ownership needs.

### Ingestion

The initial source is the public Random User API. Raw data is filtered into a stable business payload:

- identity;
- gender;
- address;
- country;
- ISO-3166 alpha-2 country code when it can be derived;
- email;
- username;
- date of birth;
- registration date;
- phone number;
- profile picture.

The implemented ingestion boundary is `realtimedatastreaming.ingestion.random_user`.

It provides:

- `RandomUserClient` for fetching one or more profiles from the Random User API;
- `NormalizedUserProfile`, a typed normalized profile contract;
- strict validation of required identity fields before returning normalized data;
- derivation of `country_code` for supported Random User countries;
- optional handling for non-critical nested source objects such as street, coordinates, timezone, registration, and pictures;
- HTTP timeout enforcement, including when a shared `httpx.Client` is injected;
- explicit failure classification through `RandomUserError.reason`;
- bounded retries for transient failures: timeouts, HTTP `429`, HTTP `5xx`, and empty `results`;
- backoff between retries, reusing the last configured delay if the retry count exceeds the schedule;
- a local thread-safe client-side rate limiter, defaulting to approximately 2 requests per second;
- deterministic unit tests with `httpx.MockTransport`, without real network calls.

The current normalized profile requires source identity, name, country, email, username, and date of birth. Address details, coordinates, timezone, registration date, phone numbers, pictures, nationality, and country code are preserved or derived when present and returned as `None` when optional source data is missing.

### Messaging

Kafka is the event bus. The initial target topic is:

```text
users_created
```

Invalid events are published to:

```text
users_created_invalid
```

The Confluent stack includes Schema Registry and Control Center to provide explicit message schemas and better topic observability.

Kafka value contracts are stored as versioned JSON Schema artifacts in
`realtimedatastreaming/ingestion/schema_registry_schemas/` and use the standard topic-value subject naming strategy:

- `users_created-value`
- `users_created_invalid-value`

Subjects can also be derived from configured topic names. For example, `KAFKA_USERS_CREATED_TOPIC=events.users.created` maps to the Schema Registry subject `events.users.created-value`.

Pydantic models remain Python DTOs for local validation, but the JSON Schema artifacts are the Kafka-facing contracts to register with Schema Registry. Registration is handled through the official Confluent Schema Registry client in `realtimedatastreaming.ingestion.schema_registry`.

Publication is implemented by `realtimedatastreaming.messaging.kafka_producer.UserProfileEventProducer`.
It provides three publishing modes:

- `publish`: asynchronous publication that queues the message locally and requires the caller to flush or close the producer before process exit;
- `publish_sync`: single-message publication that waits for a delivery acknowledgement and raises `KafkaPublicationError` on delivery failure or missing acknowledgement;
- `publish_batch_sync`: batch publication for short-lived jobs, producing multiple `KafkaProducerRecord` values, flushing once, and raising if any message fails, times out, or is not acknowledged.

Short-lived batch or CronJob-style workloads should prefer `publish_batch_sync` for multiple messages and `publish_sync` for one-off messages. Long-running services may use `publish` when they own producer lifecycle management and call `flush` or `close` during shutdown.

### Profile Contracts And Quality

The implemented profile contract module is `realtimedatastreaming.ingestion.schemas`.

`UserCreated` validates the structured event shape before quality rules run:

- required source identity, name, country, email, username, and date of birth;
- typed email through Pydantic `EmailStr`;
- typed `date_of_birth` and `registered_at` datetimes;
- bounded numeric latitude and longitude;
- typed picture URLs;
- optional ISO-3166 alpha-2 `country_code`.

The implemented quality module is `realtimedatastreaming.ingestion.quality`.

It provides:

- `validate_user_profile_quality` for semantic quality checks;
- `is_valid_user_profile` as a boolean convenience wrapper;
- `build_invalid_user_profile_event` for dead-letter or invalid-events topics;
- source-specific Random User country-name validation;
- ISO country code and nationality validation;
- plausible age checks;
- registration date checks;
- timezone offset checks;
- granular picture URL rejection reasons such as `invalid_picture_url:picture_large`.

Invalid events are intentionally privacy-reduced:

- `source_user_id` is pseudonymized with salted HMAC-SHA256;
- `PII_PSEUDONYMIZATION_SALT` is required outside `development`;
- invalid payloads use a strict allowlist: `schema_version`, `event_type`, `source`, and `country_code`.

### Processing

Processing is intentionally minimal in the first deployable slice. The ingestion job validates profiles before publishing and routes invalid profiles to the invalid-events topic.
The Airflow DAG does not execute this ingestion slice yet; it will be connected after the ingestion job entrypoint is ready. Spark, Cassandra persistence, enrichment, and dashboards remain later stages.

Implemented quality rules currently cover:

- required identity fields are present;
- email has a valid shape;
- date of birth produces a plausible age;
- username is present;
- country is present;
- source-specific country validation where applicable;
- ISO country code and nationality validation;
- registration date consistency;
- timezone offset sanity;
- HTTPS picture URL checks.

Spark Streaming remains the target distributed processing layer. It should be introduced once Kafka publication is stable and there is a concrete need for stream validation, enrichment, repartitioning, or stateful quality counters.

### Storage

Cassandra remains the target storage layer for queryable profiles and quality monitoring views. It is not required for the first deployable slice, but it is part of the intended distributed system.

Its schema must be designed from query patterns rather than copied from source JSON. Candidate query patterns:

- latest valid profiles by ingestion time;
- valid profiles by country;
- invalid profile events by reason and processing time;
- quality counters by time window.

## Planned Local Services

| Service | Role | Local port |
|---|---|---:|
| Kafka broker | Event bus | `19092` locally / platform-managed in envs |
| Schema Registry | Kafka value contract registry | `18081` locally / platform-managed in envs |
| Airflow | Pipeline orchestration and backfills | `8080` locally / platform-managed in envs |
| Spark | Distributed stream processing | future local stack |
| Cassandra | Queryable profile and quality views | future local stack |

Local services should be added in phases: Kafka and Schema Registry first, then Airflow, then Spark, then Cassandra, with healthchecks and runbooks at each step.

Start the local Kafka, Schema Registry, and Airflow integration stack with:

```bash
docker compose -f realtimedatastreaming/docker-compose.integration.yml --profile airflow up --build
```

Then open `http://localhost:8080` and sign in with `airflow` / `airflow`.
Kafka remains available on `localhost:19092`, and Schema Registry remains available on `localhost:18081`.

## Delivery And SRE Target

This repository should evolve toward a small service delivered with professional platform practices.

### Deployment Strategy

- Build immutable container images from the application `Dockerfile`.
- Keep all deployment manifests versioned in Git.
- Use Kubernetes CronJob as the first production-shaped deployment target.
- Keep the workload stateless and environment-configured.
- Use `concurrencyPolicy: Forbid` for the MVP so scheduled runs do not overlap.
- Promote immutable SHA-tagged images between environments.
- Roll back by redeploying the previous known-good image tag and pausing the CronJob if publication is unsafe.
- Promote orchestration from CronJob to Airflow when the pipeline has multiple tasks, backfills, retries, and dataset dependencies.

The MVP deployment target is:

| Concern | Decision |
|---|---|
| Runtime | Kubernetes CronJob |
| State | Stateless application process |
| Schedule | Platform-owned CronJob schedule |
| Image | Immutable SHA-tagged container |
| Config | Environment variables from ConfigMap and Secret |
| Secrets | Kubernetes Secret or external secret manager |
| Rollback | Previous known-good image tag |
| Scaling | One job at a time for the MVP |
| Persistence | None in the MVP |

Target platform progression:

| Phase | Runtime | Purpose |
|---|---|---|
| 1 | Kubernetes CronJob + Kafka + Schema Registry | production-shaped ingestion and publication |
| 2 | Airflow + Kafka + Schema Registry | scheduled orchestration, retries, backfills, ownership |
| 3 | Airflow + Kafka + Spark | distributed validation and enrichment |
| 4 | Airflow + Kafka + Spark + Cassandra | queryable profiles and quality views |

### Release Process

The intended release path is:

1. merge to `main`;
2. run format, lint, typing, unit tests, dependency audit, and security scans;
3. build and scan the container image;
4. deploy automatically to a non-production environment;
5. run smoke and integration tests;
6. promote to production with an explicit approval gate;
7. monitor SLO and rollback indicators after release.

Manual production changes should be exceptional, documented, and traceable.

Release gates are:

| Stage | Trigger | Required | Checks |
|---|---|---|---|
| PR quality | `pull_request` | Yes | format, lint, typing, unit tests, dependency audit |
| Mainline security | push to `main` | Yes | image build, image smoke test, Trivy, OSV, Gitleaks |
| Integration | manual or scheduled | Before release | real Kafka and Schema Registry publish/consume test |
| Promotion | release approval | Yes | deploy immutable image, run smoke checks, watch SLO indicators |

Base-image patching policy:

- Docker base images are pinned by digest and refreshed weekly through Dependabot.
- Trivy image scans are blocking for HIGH and CRITICAL vulnerabilities.
- Default remediation is to refresh the pinned base-image digest, rebuild, smoke test, and rescan.
- Fallback remediation is allowed for urgent HIGH or CRITICAL CVEs when a fixed Debian package exists but the upstream base image has not been refreshed yet.
- Fallback OS package upgrades must pin exact package versions, stay limited to the vulnerable packages, reference the CVE or Trivy finding, and include a removal condition.
- Generic `apt-get upgrade` and unpinned `apt-get install --only-upgrade` are not allowed because they make builds depend on repository state at build time.
- Vulnerabilities without a published Debian Bookworm fixed package, such as the temporary `perl-base` exception for `CVE-2026-48962`, belong in `.trivyignore.yaml` with owner, expiry, impact-specific justification, and a clear removal condition.

Fallback package pinning template:

```dockerfile
# Temporary CVE remediation for CVE-XXXX.
# Remove after the refreshed pinned base-image digest includes these fixed versions.
RUN apt-get update \
    && apt-get install -y --no-install-recommends --only-upgrade \
        openssl=3.0.20-1~deb12u1 \
        libssl3=3.0.20-1~deb12u1 \
    && rm -rf /var/lib/apt/lists/*
```

### Operations

Day-2 operations should be documented before the system grows:

- startup and shutdown commands;
- Kafka and Schema Registry diagnostics;
- replay and duplicate-handling notes;
- rollback procedure;
- dependency and base-image patching cadence;
- Cassandra backup and restore procedures before Cassandra is promoted beyond local/demo use.

Rollback policy:

1. stop or suspend the CronJob if the current image is actively producing bad events;
2. redeploy the previous known-good image SHA;
3. run the post-deploy smoke check;
4. verify Kafka publication success rate and invalid-event rate;
5. document whether replay is required.

Data loss and duplicate policy:

- Kafka producer idempotence is enabled, but end-to-end exactly-once processing is not claimed.
- The event key is the source user id when available.
- Retries may produce duplicates if the caller retries after an ambiguous delivery outcome.
- Short-lived jobs should use `publish_sync` or `publish_batch_sync` so delivery callbacks are observed before process exit.
- Consumers must be designed to tolerate duplicate `source_user_id` events.
- Replay is manual until a dedicated replay command exists.
- Once Cassandra is introduced, writes must be idempotent by source id, event type, and processing timestamp/window.

### Observability And SLOs

The MVP should expose signals that map to the real user journey:

- ingestion attempts;
- successful `UserCreated` publications;
- invalid-profile publications;
- publication failures;
- Random User API latency and failure reasons;
- Schema Registry and Kafka delivery failures.

Initial SLO candidates:

| SLI | Source | SLO | Alert | Runbook |
|---|---|---:|---|---|
| Ingestion success rate | app counter | >= 99% over 24h | burn rate > 2x for 30m | ingestion failures |
| Kafka publish latency | app histogram | p99 < 30s over 24h | p99 > 30s for 15m | Kafka publication |
| Kafka publish success rate | app counter | >= 99% over 24h | burn rate > 2x for 30m | Kafka publication |
| Invalid event quality | app counter / test | 100% have rejection reasons | any missing reason | quality rules |
| Secret hygiene | CI secret scans | 0 known committed secrets | any finding | security incident |

Alerts should favor SLO or burn-rate style symptoms over raw CPU or disk thresholds.

### Continuous Security

Security is part of delivery, not a separate final step:

- dependency audit with `pip-audit`;
- container and filesystem scanning;
- secret scanning;
- pinned Docker image versions;
- environment-based configuration;
- no hardcoded secrets;
- salted pseudonymization for invalid-event source ids outside development;
- future Kubernetes policy checks through OPA Gatekeeper or Kyverno when manifests exist.

Runtime hardening target:

- run as non-root;
- read-only root filesystem where practical;
- drop Linux capabilities;
- set CPU and memory requests and limits;
- mount secrets as environment variables or files from a managed secret source;
- avoid printing secrets or raw invalid payloads;
- restrict egress to Random User API, Kafka, Schema Registry, and telemetry endpoints;
- rotate Kafka and Schema Registry credentials before production use.

## Local Development

Install dependencies:

```bash
uv sync --all-groups
```

Install the pre-commit hook:

```bash
uv run nox -s dev
```

The pre-commit hook runs secret scanning plus `format`, `lint`, `typing`, and `test`. It does not run
Kafka, Airflow, or Spark integration sessions; use `uv run nox` or `uv run nox -s integration` for those.

Run all checks:

```bash
uv run nox
```

The default nox run includes the integration lane, so Docker must be running before this command.

Run a specific check:

```bash
uv run nox -s format
uv run nox -s lint
uv run nox -s typing
uv run nox -s test
uv run nox -s integration
uv run nox -s kafka-integration
uv run nox -s airflow-integration
uv run nox -s spark-integration
uv run nox -s audit
```

The `integration` session aggregates the integration sessions for each runtime brick and is part of the
default `uv run nox` lane.
Use `kafka-integration` to start Kafka and Schema Registry, wait for runtime health, run the real
produce/consume and Schema Registry contract tests, then tear the stack down. This test does not call
the Random User API and does not run the ingestion job. Use `airflow-integration` to build and start the
Airflow profile, check Airflow health, validate DAG import, run `airflow dags test`, validate the current
orchestration scaffold, and tear the stack down.
Use `spark-integration` to build and start the Spark profile, wait for Kafka and Schema Registry health,
run the bounded Kafka-to-Spark prepared-output test, and tear the stack down.

Airflow local deployment, DAG validation, and runbooks are documented in
[markdown/airflow.md](markdown/airflow.md).

Test the current CLI:

```bash
uv run realtimedatastreaming
```

## Application Configuration

Application variables are documented in [.env.example](.env.example), which is the source of truth for local configuration.

Important variables for profile contracts, messaging, and privacy:

- `SCHEMA_REGISTRY_URL`: Confluent Schema Registry endpoint.
- `SCHEMA_REGISTRY_BASIC_AUTH_USER_INFO`: optional Schema Registry basic auth value.
- `SCHEMA_REGISTRY_SSL_CA_LOCATION`: optional Schema Registry CA bundle path.
- `KAFKA_BOOTSTRAP_SERVERS`: Kafka bootstrap servers.
- `KAFKA_USERS_CREATED_TOPIC`: valid user profile event topic.
- `KAFKA_USERS_CREATED_INVALID_TOPIC`: invalid user profile event topic.
- `PII_PSEUDONYMIZATION_SALT`: required outside `development` to pseudonymize invalid-event source ids.

Airflow, Spark, Cassandra, Sentry, OpenTelemetry, and cloud secrets must be injected through environment variables or a managed secret store.

## Roadmap

The detailed roadmap is available in [markdown/development-roadmap.md](markdown/development-roadmap.md).

The main stages are now:

1. define the application modules under `realtimedatastreaming/`;
2. implement ingestion and quality rules;
3. publish and validate Kafka events;
4. optimize container image builds, image scans, and CI dependency installation;
5. add deployment automation and release gates;
6. add SRE runbooks, SLOs, and security controls;
7. add Airflow, Spark, and Cassandra as staged distributed-system capabilities with explicit operational gates.
