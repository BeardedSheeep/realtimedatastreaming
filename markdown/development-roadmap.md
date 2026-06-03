<!-- Copyright (c) 2026 BeardedSheeep -->

# Development Roadmap

This roadmap intentionally keeps the functional product small and moves the ambition to delivery, operations, SRE, and security.

The target is a distributed data platform built in controlled stages. The first stage is intentionally compact so it can prove the delivery, SRE, and security model before Airflow, Spark, and Cassandra are added.

## Target MVP Structure

```text
realtimedatastreaming/
    ingestion/
        random_user.py
        schemas.py
        quality.py
        schema_registry.py
        schema_registry_schemas/
    messaging/
        kafka_producer.py
        topics.py
    orchestration/
        airflow_dags.py
    streaming/
        spark_job.py
    storage/
        cassandra.py
        schema.cql
    settings.py
    observability.py
    docker-compose.integration.yml
tests/
    integration/
        messaging/
```

The first deployable slice uses ingestion plus messaging. `orchestration/`, `streaming/`, and `storage/` are target platform layers and should be added when their implementation and operational runbooks are ready.

## Product Boundary

### In Scope

1. Fetch demo profiles from Random User.
2. Normalize profiles into `UserCreated`.
3. Validate structural and semantic quality.
4. Publish valid events to Kafka.
5. Publish invalid events to a dedicated invalid-events topic.
6. Use Schema Registry JSON Schema contracts.
7. Keep invalid events privacy-reduced.
8. Orchestrate the pipeline with Airflow.
9. Process and enrich events with Spark Streaming.
10. Persist queryable profile and quality views in Cassandra.
11. Provide deterministic unit tests and optional Kafka/Schema Registry integration tests.

### Out Of Scope

1. Multi-source ingestion.
2. Full analytics dashboards.
3. Complex enrichment pipelines.

Airflow orchestration, Spark Streaming, and Cassandra persistence are in scope. They are staged after the first production-shaped ingestion slice so the distributed system grows with tests, runbooks, deployment gates, and observability.

## Step 1 - Application Foundation

Status: implemented.

Goal: keep the repository clean and easy to validate before adding platform surface area.

Actions:

1. Keep `realtimedatastreaming` as the Python package entrypoint.
2. Keep dependency management on `uv`.
3. Keep `nox` sessions for format, lint, typing, tests, audit, Docker, and integration.
4. Keep Ruff, mypy strict mode, pytest, coverage, and pip-audit green.
5. Keep the application `Dockerfile` minimal and reproducible.

Deliverables:

- importable package;
- CLI smoke test;
- quality checks;
- minimal container image;
- protected `main` branch when hosted.

## Step 2 - Runtime Configuration

Status: implemented for the MVP.

Goal: centralize runtime configuration and keep secrets outside code.

Actions:

1. Configure Random User API URL and timeout.
2. Configure Kafka bootstrap servers and topic names.
3. Configure Schema Registry URL and optional auth/TLS.
4. Configure privacy settings such as `PII_PSEUDONYMIZATION_SALT`.
5. Keep `.env.example` as the documented local source of truth.
6. Keep secrets injected through environment variables or a managed secret store.

Deliverables:

- typed `Settings`;
- `.env.example`;
- settings tests for defaults, validation, and environment overrides.

## Step 3 - Ingestion And Quality

Status: implemented.

Goal: make profile ingestion predictable, bounded, and easy to reason about.

Actions:

1. Fetch Random User profiles with explicit timeout handling.
2. Normalize payloads into a typed profile contract.
3. Require only the fields needed for the MVP: source id, name, country, email, username, and date of birth.
4. Preserve optional non-critical source fields as nullable values.
5. Classify ingestion failures explicitly.
6. Rate-limit source API calls.
7. Retry transient failures with bounded backoff.
8. Validate profile quality with explicit rejection reasons.
9. Build privacy-safe invalid events.

Deliverables:

- `RandomUserClient`;
- `UserCreated`;
- `UserProfileInvalid`;
- quality validation functions;
- invalid-event builder;
- deterministic unit tests without real network calls.

## Step 4 - Kafka Messaging

Status: implemented for the MVP.

Goal: publish events through Kafka with explicit contracts and resilient producer behavior.

Actions:

1. Keep a small topic catalog.
2. Serialize values with Schema Registry JSON Schema contracts.
3. Use topic-value subject naming, including configured topic names.
4. Configure reliable producer defaults: idempotence, `acks=all`, retries, timeouts, compression, and client id.
5. Handle local producer backpressure.
6. Provide async publish, single-message sync publish, and batch sync publish paths.
7. Surface delivery failures as application errors when sync or batch sync publishing is used.
8. Use batch sync publishing for short-lived ingestion jobs so delivery callbacks are observed before process exit.
9. Keep producer unit tests and contract tests.

Deliverables:

- `UserProfileEventProducer`;
- `SchemaRegistryValueSerializer`;
- topic defaults and settings-derived topics;
- unit tests;
- real Kafka/Schema Registry integration test.

## Step 5 - Container Image And CI Build Optimization

Goal: keep Docker builds, image scans, and quality gates fast enough before heavier Airflow, Spark, and Cassandra runtimes are introduced.

Actions:

1. Reorder the application `Dockerfile` so dependency installation is cached separately from application code changes.
2. Add BuildKit cache mounts for `uv` during Docker builds.
3. Keep `nox` sessions scoped so routine format, lint, typing, and unit-test lanes do not install heavy optional runtime profiles unless required.
4. Add registry-backed Docker build cache for CI builds.
5. Define a differentiated scan policy: fast scans for normal application PRs, full image scans when dependency or image inputs change, and scheduled full scans.
6. Add a stronger image smoke test that validates critical runtime imports and profile-specific entrypoints.
7. Introduce a prebuilt heavy-dependencies base image only when Spark, Airflow, or Cassandra runtimes become part of an executable image.
8. Produce an SBOM for published images.

Deliverables:

- Dockerfile cache-layer optimization;
- BuildKit `uv` cache usage;
- scoped `nox` dependency installation strategy;
- CI registry cache configuration;
- image scan policy;
- image smoke tests;
- SBOM artifact for released images.

## Step 6 - Airflow Orchestration

Goal: make Airflow the pipeline orchestrator once the ingestion path has more than one operational step.

Actions:

1. Create the Airflow DAG entrypoint.
2. Keep business logic outside DAG files.
3. Configure retries, schedules, catchup, tags, and backfill policy explicitly.
4. Manage Airflow connections and secrets outside code.
5. Add DAG import tests.
6. Keep the application `Dockerfile` dedicated to the app/job runtime.
7. Add a dedicated Airflow image or Docker target for scheduler and webserver runtime.
8. Extend the existing integration compose stack with Postgres, `airflow-init`, `airflow-webserver`, and `airflow-scheduler`.
9. Keep Kafka and Schema Registry in the same integration compose stack.
10. Mount or copy DAGs into the Airflow runtime and configure `AIRFLOW__CORE__DAGS_FOLDER` explicitly.
11. Add scheduler and failed-run runbooks.
12. Document the DAG deployment path.

Deliverables:

- minimal Airflow DAG;
- DAG import tests;
- retry and backfill policy;
- Airflow connection and secret handling notes;
- dedicated Airflow runtime image;
- integration compose stack with Airflow webserver and scheduler;
- failed DAG runbook.
- DAG deployment path documentation.

## Step 7 - Spark Streaming

Status: in progress. Current slice prepares validated profile records for CassandraDB with a dummy sink.

Goal: process Kafka user profile events with Spark, validate them against Kafka-facing contracts,
and prepare records for the first CassandraDB query pattern.

Actions:

1. Create a Spark Streaming entrypoint with stream processing logic kept outside orchestration code.
2. Consume `users_created` with explicit Kafka and Schema Registry configuration.
3. Validate payloads against Kafka-facing contracts before preparing records for storage.
4. Prepare valid `user_profiles_by_source_id` records and write them to a dummy sink until CassandraDB is connected.
5. Emit local batch signals: input records, prepared Cassandra writes,
   invalid records, batch duration, and sink failures.
6. Configure checkpointing explicitly with a single `SPARK_CHECKPOINT_LOCATION`.
7. Keep replay manual through checkpoint control and require future Cassandra writes to use the stable
   `source`, `source_user_id`, `event_type` idempotence key before production scheduling.
8. Add Spark driver runtime packaging and compose services only when containerized validation is needed.
9. Add a bounded integration test that produces one Kafka event, runs one Spark micro-batch, verifies prepared
    Cassandra-shaped output, and checks basic local monitoring output.
10. Document stuck-stream recovery, checkpoint recovery, and Spark deployment.

Deliverables:

- executable Spark Streaming job;
- transformation unit tests;
- explicit `SPARK_CHECKPOINT_LOCATION` configuration;
- manual checkpoint replay policy and stable Cassandra idempotence key;
- Cassandra-shaped valid profile records;
- dummy sink for the current Spark module slice;
- schema compatibility tests;
- dedicated Spark runtime image;
- integration compose stack for the first executable Spark slice;
- bounded Kafka-to-Spark integration test that verifies prepared Cassandra-shaped output;
- minimum local monitoring signal set;
- stuck or failing streaming job runbook;
- Spark deployment path documentation.

## Step 8 - CassandraDB Integration

Goal: replace the dummy Spark sink with real idempotent CassandraDB writes for the first queryable
profile view.

Actions:

1. Add CassandraDB to the integration compose stack with an explicit service endpoint.
2. Version the CQL schema for `user_profiles_by_source_id` from the query pattern.
3. Implement the Spark Cassandra sink using the stable `source`, `source_user_id`, `event_type`
   idempotence key.
4. Keep writes retry-safe before enabling scheduled streaming execution.
5. Extend the Spark integration test to verify real CassandraDB output.
6. Emit local sink success and failure signals for Cassandra writes.
7. Document CassandraDB recovery, replay safety, and schema migration steps.

Deliverables:

- CassandraDB compose service for integration validation;
- versioned CQL schema;
- real Spark Cassandra sink;
- idempotent write path;
- CassandraDB output verification in the bounded Spark integration test;
- CassandraDB recovery and migration notes.

## Step 9 - CI/CD Baseline

Goal: encode the release process instead of documenting manual rituals.

Actions:

1. Run format, lint, typing, unit tests, and dependency audit on every PR.
2. Build the container image on mainline changes.
3. Scan the image and filesystem for vulnerabilities.
4. Run secret scanning.
5. Run the Kafka/Schema Registry integration lane on demand or on a scheduled cadence.
6. Publish test and security reports as CI artifacts where practical.
7. Keep all promotion steps traceable in CI/CD logs.

Target CI/CD matrix:

| Stage | Trigger | Required | Checks |
|---|---|---|---|
| PR quality | `pull_request` | Yes | format, lint, typing, unit tests, dependency audit |
| Mainline security | push to `main` | Yes | image build, image smoke test, Trivy, OSV, Gitleaks |
| Integration | manual or scheduled | Before release | real Kafka and Schema Registry publish/consume test |
| Promotion | release approval | Yes | deploy immutable image, run smoke checks, watch SLO indicators |

Deliverables:

- quality workflow;
- security workflow;
- container build workflow;
- optional integration workflow;
- documented release gate list.

## Step 10 - Release Automation And Documentation Publishing

Goal: make each promoted version traceable, documented, and easy to consume.

Actions:

1. Add an explicit versioning step at the start of the release path.
2. Choose and document the versioning strategy, such as SemVer for public releases or CalVer for operational releases.
3. Generate release notes automatically from merged PRs, commits, tags, or curated changelog entries.
4. Create a GitHub prerelease before production promotion when the release still needs validation.
5. Promote a prerelease to a stable GitHub release only after required gates pass.
6. Attach or link the immutable image SHA, security report, integration result, and deployment target to each release.
7. Build project documentation in CI so broken docs fail before publication.
8. Publish versioned documentation, for example through GitHub Pages, once the release gates pass.
9. Keep release, documentation, image, and deployment metadata connected in CI/CD logs.

Target release flow:

| Stage | Trigger | Required | Output |
|---|---|---|---|
| Version | release workflow or tag | Yes | release version, immutable image tag, changelog scope |
| Release notes | after quality and security gates | Yes | generated notes reviewed in CI logs |
| Documentation | before release publication | Yes | built docs artifact and published docs site |
| Prerelease | release candidate approval | When applicable | GitHub prerelease linked to image SHA |
| Stable release | final promotion approval | Yes | GitHub release linked to docs, image, security report, and deployment evidence |

Deliverables:

- documented versioning strategy;
- release notes generation workflow;
- GitHub prerelease and release workflow;
- documentation build workflow;
- documentation publishing workflow;
- release artifact checklist.

## Step 11 - Deployment Model

Goal: deploy the small service using a professional, repeatable path.

Actions:

1. Use Kubernetes CronJob as the first production-shaped deployment target.
2. Version deployment manifests in Git.
3. Prefer immutable image tags.
4. Keep the workload stateless.
5. Use `concurrencyPolicy: Forbid` for the MVP.
6. Inject config through ConfigMap and Secret or an external secret manager.
7. Document rollback as a first-class release operation.
8. Add blue/green or canary only when a router or consumer migration strategy exists.

MVP deployment decisions:

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

Deliverables:

- deployment manifest or documented runtime command;
- environment-specific configuration pattern;
- rollback command;
- smoke-test command after deployment.
- runtime hardening checklist.

Target platform progression:

| Phase | Runtime | Purpose |
|---|---|---|
| 1 | Kubernetes CronJob + Kafka + Schema Registry | production-shaped ingestion and publication |
| 2 | Airflow + Kafka + Schema Registry | scheduled orchestration, retries, backfills, ownership |
| 3 | Airflow + Kafka + Spark local mode | stream validation and enrichment |
| 4 | Airflow + Kafka + Spark + Cassandra | queryable profiles and quality views |

## Step 12 - SRE And Observability

Goal: make the service diagnosable and govern releases with operational signals.

Actions:

1. Keep structured logs.
2. Add correlation ids where events cross boundaries.
3. Emit counters for ingestion attempts, valid events, invalid events, publish failures, and source API failures.
4. Emit latency histograms for Random User calls and Kafka publication.
5. Define SLIs before adding dashboards.
6. Define initial SLOs and alerting rules.
7. Alert on user-visible symptoms and error-budget burn, not only CPU or disk.
8. Add post-release monitoring checks.

Initial SLO candidates:

| SLI | Source | SLO | Alert | Runbook |
|---|---|---:|---|---|
| Ingestion success rate | app counter | >= 99% over 24h | burn rate > 2x for 30m | ingestion failures |
| Kafka publish latency | app histogram | p99 < 30s over 24h | p99 > 30s for 15m | Kafka publication |
| Kafka publish success rate | app counter | >= 99% over 24h | burn rate > 2x for 30m | Kafka publication |
| Invalid event quality | app counter / test | 100% have rejection reasons | any missing reason | quality rules |
| Secret hygiene | CI secret scans | 0 known committed secrets | any finding | security incident |

Deliverables:

- SLI/SLO document;
- metrics naming convention;
- dashboard notes;
- alerting policy;
- post-release verification checklist.

## Step 13 - Runbooks And Day-2 Operations

Goal: make common operations repeatable and auditable.

Actions:

1. Document local startup and shutdown.
2. Document Kafka topic diagnostics.
3. Document Schema Registry subject diagnostics.
4. Document how to replay or re-run ingestion safely.
5. Document how to respond to source API failure.
6. Document how to respond to Kafka publication failures.
7. Document patch cadence for dependencies and base images.
8. Add backup and restore runbooks only when persistent storage is introduced.

Rollback policy:

1. Stop or suspend the CronJob if the current image is actively producing bad events.
2. Redeploy the previous known-good image SHA.
3. Run the post-deploy smoke check.
4. Verify Kafka publication success rate and invalid-event rate.
5. Document whether replay is required.

Data loss and duplicate policy:

1. Kafka producer idempotence is enabled, but end-to-end exactly-once processing is not claimed.
2. The event key is the source user id when available.
3. Retries may produce duplicates if the caller retries after an ambiguous delivery outcome.
4. Consumers must tolerate duplicate `source_user_id` events.
5. Replay is manual until a dedicated replay command exists.
6. Once Cassandra is introduced, writes must be idempotent by source id, event type, and processing timestamp/window.

Deliverables:

- runbook markdown files;
- incident response checklist;
- rollback checklist;
- patching cadence.

## Step 14 - Continuous Security

Goal: make security checks part of delivery and operations.

Actions:

1. Keep dependency scanning.
2. Keep container image scanning.
3. Keep filesystem scanning.
4. Keep secret scanning.
5. Keep Docker image versions pinned.
6. Keep secrets out of code and logs.
7. Keep invalid profile events privacy-reduced.
8. Review Kafka and Schema Registry credentials before production use.
9. Add OPA Gatekeeper or Kyverno policies if Kubernetes manifests are introduced.

Runtime hardening target:

1. Run as non-root.
2. Use a read-only root filesystem where practical.
3. Drop Linux capabilities.
4. Set CPU and memory requests and limits.
5. Mount secrets from Kubernetes Secret or an external secret manager.
6. Avoid logging secrets or raw invalid payloads.
7. Restrict egress to Random User API, Kafka, Schema Registry, and telemetry endpoints.
8. Rotate Kafka and Schema Registry credentials before production use.

Deliverables:

- security workflow;
- `.trivyignore.yaml` expiry checks;
- documented secret handling;
- security exception policy;
- production hardening checklist.

## Step 15 - CassandraDB Operations Hardening

Goal: harden the CassandraDB storage path after the Step 8 integration is executable.

Actions:

1. Define TTL and retention policy.
2. Add backup and restore runbook.
3. Add compaction and repair guidance.
4. Track write latency, failed writes, storage growth, and tombstones.

Deliverables:

- retention policy;
- backup and restore runbook;
- compaction and repair notes;
- operational metrics and alerting notes.

## Definition Of Done For The MVP

The MVP is production-shaped when:

1. `uv run nox` is green.
2. `uv run nox -s integration` can publish and consume a real Kafka event.
3. The application image builds and scans cleanly.
4. Runtime configuration is environment-driven.
5. No secrets are hardcoded.
6. Invalid events are privacy-reduced.
7. Deployment and rollback are documented.
8. Initial SLOs are documented.
9. Runbooks exist for Kafka, Schema Registry, ingestion failures, and release rollback.
10. Releases have generated notes and link to their immutable image SHA, security report, and deployment evidence.
11. Documentation is built in CI and published as versioned release documentation.
12. The README explains both the staged product scope and the professional delivery target.
