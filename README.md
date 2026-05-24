# Realtime User Profile Quality Streaming

`realtimedatastreaming` is a real-time user profile ingestion and quality monitoring platform.

The project ingests user profiles, turns them into explicit events, validates and enriches them in a streaming pipeline, stores queryable profiles, and exposes enough operational signals to understand data quality in near real time.

## Goal

The functional goal is to provide a small, demonstrable platform that answers this question:

> Are user profiles being ingested correctly, and which profiles are valid, invalid, enriched, or failing quality checks?

The target pipeline follows this flow:

```text
Random User API
    |
    v
Airflow DAG
    |
    v
Kafka topic: users_created
    |
    v
Spark Streaming
    |--------------------|
    v                    v
Cassandra           Kafka topic: users_created_invalid
```

The platform should make it possible to:

- orchestrate scheduled ingestion with Airflow;
- fetch user profiles from `https://randomuser.me/api/` as a reproducible demo source;
- normalize useful user fields into a stable `UserCreated` event;
- publish valid ingestion events to Kafka;
- validate required fields and basic data quality rules;
- separate invalid profiles into a dead-letter or invalid-events topic;
- enrich valid profiles with simple derived fields such as age, country, email domain, and ingestion timestamp;
- observe topics with Confluent Control Center;
- process and monitor events with Spark Streaming;
- persist cleaned, queryable user profiles and quality counters in Cassandra;
- keep a tested, typed, linted, and secure Python foundation ready for CI/CD.

The initial business use case is intentionally narrow: real-time monitoring of user profile quality. The data source can later be replaced by a real product API, CRM feed, signup stream, or partner feed without changing the pipeline shape.

## Current Repository State

The repository already contains the Python application foundation:

- importable `realtimedatastreaming` package;
- `realtimedatastreaming` smoke-test CLI;
- typed application settings with Pydantic Settings;
- Random User ingestion boundary with typed normalized profiles;
- HTTP timeout, retry, backoff, rate limiting, and failure classification for ingestion;
- JSON/text logging with correlation context;
- dependency management with `uv`;
- developer checks orchestrated with `nox`;
- Ruff, mypy, pytest, coverage, and pip-audit;
- minimal application Dockerfile;
- GitHub Actions workflows for quality, CI, and CI/CD.

## Planned Architecture

### Functional Domain

The core domain is user profile ingestion quality:

- `UserCreated`: normalized user profile accepted by the ingestion boundary;
- `UserProfileValidated`: enriched profile that passed stream validation;
- `UserProfileInvalid`: rejected profile with a reason and enough context to debug;
- quality metrics: counts by validation status, country, source, and processing window.

### Orchestration

Airflow drives scheduled ingestion. The DAG stays thin and calls application code responsible for fetching, normalizing, validating at the boundary, and publishing user profile events.

### Ingestion

The initial source is the public Random User API. Raw data is filtered into a stable business payload:

- identity;
- gender;
- address;
- country;
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
- optional handling for non-critical nested source objects such as street, coordinates, timezone, registration, and pictures;
- HTTP timeout enforcement, including when a shared `httpx.Client` is injected;
- explicit failure classification through `RandomUserError.reason`;
- bounded retries for transient failures: timeouts, HTTP `429`, HTTP `5xx`, and empty `results`;
- backoff between retries, reusing the last configured delay if the retry count exceeds the schedule;
- a local thread-safe client-side rate limiter, defaulting to approximately 2 requests per second;
- deterministic unit tests with `httpx.MockTransport`, without real network calls.

The current normalized profile requires source identity, name, country, email, username, and date of birth. Address details, coordinates, timezone, registration date, phone numbers, pictures, and nationality are preserved when present and returned as `None` when optional source data is missing.

### Messaging

Kafka is the event bus. The initial target topic is:

```text
users_created
```

Invalid events are published to:

```text
users_created_invalid
```

The Confluent stack includes Schema Registry and Control Center to prepare for explicit message schemas and better topic observability.

### Processing

Spark Streaming will consume Kafka messages, validate payloads, enrich valid profiles, emit invalid profiles with reasons, and prepare rows for storage.

Initial quality rules should stay simple:

- required identity fields are present;
- email has a valid shape;
- date of birth is parseable and produces a plausible age;
- username is present;
- country is present;
- duplicate handling is explicit for the chosen key.

### Storage

Cassandra will store processed user profiles and quality monitoring views. The schema must be designed from expected query patterns, not only as a copy of the source JSON.

Candidate query patterns:

- latest valid profiles by ingestion time;
- valid profiles by country;
- invalid profile events by reason and processing time;
- quality counters by time window.

## Planned Local Services

| Service | Role | Local port |
|---|---|---:|
| Zookeeper | Historical Kafka coordination | `2181` |
| Kafka broker | Event bus | `9092` |
| Schema Registry | Schema registry | `8081` |
| Control Center | Confluent UI | `9021` |
| Airflow webserver | Airflow UI | `8080` |
| Postgres | Airflow metadata | internal |
| Spark master | Spark cluster | `9090`, `7077` |
| Spark worker | Spark execution | internal |
| Cassandra | Final storage | `9042` |

## Local Development

Install dependencies:

```bash
uv sync --all-groups
```

Install the pre-commit hook:

```bash
uv run nox -s dev
```

Run all checks:

```bash
uv run nox
```

Run a specific check:

```bash
uv run nox -s format
uv run nox -s lint
uv run nox -s typing
uv run nox -s test
uv run nox -s audit
```

Test the current CLI:

```bash
uv run realtimedatastreaming
```

## Application Configuration

Application variables are documented in [.env.example](.env.example), which is the source of truth for local configuration.

Future Airflow, Kafka, Cassandra, Sentry, and OpenTelemetry secrets must be removed from code and injected through environment variables.

## Roadmap

The detailed roadmap is available in [markdown/development-roadmap.md](markdown/development-roadmap.md).

The main stages are:

1. define the application modules under `realtimedatastreaming/`;
2. implement ingestion and quality rules;
3. publish and validate profile events;
4. process streams and persist quality views;
5. keep orchestration, infra, tests, and documentation clean.
