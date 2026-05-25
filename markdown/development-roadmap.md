# Development Roadmap

This roadmap keeps one part per functional scope or application module under `realtimedatastreaming/`.

## Target Structure

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
```

## Step 1 - Application Foundation

Goal: keep a clean repository before adding the data stack.

Actions:

1. Keep the `realtimedatastreaming` package as the Python entrypoint.
2. Keep `uv`, `nox`, Ruff, mypy, pytest, coverage, and pip-audit.
3. Keep GitHub Actions workflows green.
4. Verify that `uv run nox` stays green before every major feature.

Deliverables:

- up-to-date README;
- working quality checks;
- protected `main` branch on GitHub.

## Step 2 - Settings

Goal: centralize runtime configuration before adding infrastructure code.

Actions:

1. Add Random User API URL and HTTP timeout settings.
2. Add Kafka bootstrap servers and topic names.
3. Add Schema Registry URL.
4. Add privacy settings such as `PII_PSEUDONYMIZATION_SALT`.
5. Add Spark checkpoint and application settings.
6. Add Cassandra host and keyspace settings.
7. Keep secrets injected through environment variables.

Deliverables:

- typed `Settings`;
- updated `.env.example`;
- tests for defaults and environment overrides.

## Step 3 - Ingestion

Goal: ingest user profiles from the demo source without coupling business logic to Airflow.

Actions:

1. Create `realtimedatastreaming/ingestion/random_user.py`.
2. Implement an HTTP client with a timeout, including when a shared `httpx.Client` is injected.
3. Normalize Random User payloads into a typed `NormalizedUserProfile`.
4. Validate Random User responses strictly before returning a user profile.
5. Keep critical source fields required: source user id, first name, last name, country, email, username, and date of birth.
6. Derive ISO-3166 alpha-2 `country_code` for supported Random User countries.
7. Treat non-critical nested source objects as optional when their normalized fields are nullable.
8. Classify ingestion failures explicitly, including HTTP status codes, timeouts, invalid JSON, empty `results`, malformed responses, and missing required fields.
9. Limit Random User calls to approximately 2 requests per second with a local thread-safe client-side rate limiter.
10. Retry transient failures at most 2 times with backoff, including timeouts, HTTP `5xx`, HTTP `429`, and empty `results`.
11. Reuse the final configured backoff delay if the retry count is higher than the provided backoff schedule.
12. Fail cleanly with the final classified reason when all retry attempts are exhausted.
13. Keep unit tests free from real network calls.

Deliverables:

- `RandomUserClient`;
- typed normalized profile contract;
- source-to-profile mapping function;
- country code derivation for Random User countries;
- strict payload validation and explicit failure classification;
- client-side rate limiting for Random User;
- bounded retry policy with backoff;
- deterministic test fixtures.

## Step 4 - Schemas And Quality Rules

Status: implemented.

Goal: define the user profile quality contract before publishing or processing events.

Actions:

1. Create `realtimedatastreaming/ingestion/schemas.py`.
2. Define `UserCreated` with typed structured fields: email, datetimes, coordinates, picture URLs, and optional `country_code`.
3. Define `UserProfileInvalid`.
4. Create `realtimedatastreaming/ingestion/quality.py`.
5. Validate semantic profile quality after schema validation.
6. Return explicit rejection reasons.
7. Create versioned Kafka JSON Schema artifacts under `realtimedatastreaming/ingestion/schema_registry_schemas/`.
8. Register Kafka value contracts through the official Confluent Schema Registry client.
9. Use topic-value subject naming, including subjects derived from configured topic names.
10. Route invalid events with privacy-safe payloads and salted source id pseudonymization.

Deliverables:

- Pydantic event models;
- Kafka-facing JSON Schema artifacts;
- Schema Registry registration helpers;
- quality rule functions;
- privacy-safe invalid event builder;
- unit and contract tests for valid and invalid profiles.

Implementation notes:

- `UserCreated` is the normalized profile event accepted by the ingestion boundary.
- `UserProfileInvalid` carries rejection reasons and a small allowlisted payload for DLQ/debug use.
- Invalid payloads intentionally keep only `schema_version`, `event_type`, `source`, and `country_code`.
- `PII_PSEUDONYMIZATION_SALT` is required outside `development`.
- Current tests are deterministic and avoid real Kafka or Schema Registry network calls.

## Step 5 - Messaging

Goal: publish user profile events to Kafka through a small application wrapper.

Actions:

1. Create `realtimedatastreaming/messaging/topics.py`.
2. Define `users_created` and `users_created_invalid`.
3. Create `realtimedatastreaming/messaging/kafka_producer.py`.
4. Encapsulate producer creation, publish, flush, and close.
5. Serialize values using the Schema Registry contracts.
6. Log publication outcomes.
7. Add producer tests with mocks.

Deliverables:

- topic catalog;
- reusable Kafka producer;
- Schema Registry-aware serialization;
- publication tests.

## Step 6 - Orchestration

Goal: make Airflow the orchestrator, not the container for all business logic.

Actions:

1. Create a root-level `dags/` directory if needed.
2. Import a `user_automation` DAG.
3. Configure `start_date`, `schedule`, `catchup`, and tags.
4. Call an application function from the DAG.
5. Avoid hardcoded secrets in the DAG.
6. Add Airflow startup documentation.
7. Add a DAG import test if Airflow is installed in the appropriate dependency group.

Deliverables:

- readable and minimal DAG;
- business logic outside the DAG;
- configuration injected from the environment.

## Step 7 - Streaming

Goal: validate and enrich profile events in Spark Streaming.

Actions:

1. Create `realtimedatastreaming/streaming/spark_job.py`.
2. Consume the `users_created` topic.
3. Parse event payloads.
4. Apply profile schema and quality rules.
5. Enrich valid profiles with age, country, email domain, and processing timestamp.
6. Route invalid records with rejection reasons and privacy-safe invalid payloads.
7. Configure checkpoints.

Deliverables:

- executable Spark Streaming job;
- valid profile stream;
- invalid profile stream;
- processing logs.

## Step 8 - Storage

Goal: persist valid profiles and quality monitoring views in Cassandra.

Actions:

1. Create `realtimedatastreaming/storage/schema.cql`.
2. Define a valid profiles table.
3. Define an invalid profile events table.
4. Define quality counters by time window.
5. Choose partition keys from concrete query patterns.
6. Add Cassandra writer code if needed.
7. Add schema or writer tests where practical.

Deliverables:

- versioned CQL schema;
- visible valid profile data;
- visible invalid event data;
- visible quality counters.

## Step 9 - Local Infrastructure

Goal: provide a reproducible local environment.

Actions:

1. Add Docker Compose services for Kafka, Schema Registry, Control Center, Airflow, Postgres, Spark, and Cassandra.
2. Pin Docker image versions.
3. Add healthchecks.
4. Add named volumes.
5. Document local ports.
6. Add a smoke-test command.

Deliverables:

- maintainable `docker-compose.yml`;
- documented startup flow;
- healthy local stack.

## Step 10 - Observability And Operations

Goal: make the pipeline understandable during local execution.

Actions:

1. Keep structured application logs.
2. Log ingestion batches.
3. Log Kafka publication results.
4. Log Spark micro-batch summaries.
5. Expose ingested, valid, and invalid profile counts.
6. Document Airflow, Control Center, Spark UI, and Cassandra diagnostic commands.

Deliverables:

- operational logs;
- quality monitoring signals;
- troubleshooting notes.

## Step 11 - Definition Of Done

The project is integrated when:

1. `docker compose up` starts the local stack.
2. Airflow triggers profile ingestion.
3. Kafka receives `users_created` messages.
4. Spark validates and enriches profiles.
5. Invalid profiles are routed with rejection reasons.
6. Cassandra contains valid profiles and quality views.
7. `uv run nox` is green.
8. Secrets are not hardcoded.
9. The README explains how to demonstrate the quality monitoring use case.
