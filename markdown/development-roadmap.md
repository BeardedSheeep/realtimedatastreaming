# Development Roadmap

This roadmap keeps one part per functional scope or application module under `realtimedatastreaming/`.

## Target Structure

```text
realtimedatastreaming/
    ingestion/
        random_user.py
        schemas.py
        quality.py
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
3. Add Spark checkpoint and application settings.
4. Add Cassandra host and keyspace settings.
5. Keep secrets injected through environment variables.

Deliverables:

- typed `Settings`;
- updated `.env.example`;
- tests for defaults and environment overrides.

## Step 3 - Ingestion

Goal: ingest user profiles from the demo source without coupling business logic to Airflow.

Actions:

1. Create `realtimedatastreaming/ingestion/random_user.py`.
2. Implement an HTTP client with a timeout.
3. Normalize Random User payloads into internal data.
4. Handle HTTP errors, malformed responses, and missing fields.
5. Keep unit tests free from real network calls.

Deliverables:

- `RandomUserClient`;
- source-to-event mapping function;
- deterministic test fixtures.

## Step 4 - Schemas And Quality Rules

Goal: define the user profile quality contract before publishing or processing events.

Actions:

1. Create `realtimedatastreaming/ingestion/schemas.py`.
2. Define `UserCreated`.
3. Define `UserProfileInvalid`.
4. Create `realtimedatastreaming/ingestion/quality.py`.
5. Validate required identity fields, email shape, username, country, and plausible age.
6. Return explicit rejection reasons.

Deliverables:

- Pydantic event models;
- quality rule functions;
- unit tests for valid and invalid profiles.

## Step 5 - Messaging

Goal: publish user profile events to Kafka through a small application wrapper.

Actions:

1. Create `realtimedatastreaming/messaging/topics.py`.
2. Define `users_created` and `users_created_invalid`.
3. Create `realtimedatastreaming/messaging/kafka_producer.py`.
4. Encapsulate producer creation, publish, flush, and close.
5. Log publication outcomes.
6. Add producer tests with mocks.

Deliverables:

- topic catalog;
- reusable Kafka producer;
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
4. Apply quality rules.
5. Enrich valid profiles with age, country, email domain, and processing timestamp.
6. Route invalid records with rejection reasons.
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
