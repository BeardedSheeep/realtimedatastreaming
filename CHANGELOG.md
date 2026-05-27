# Changelog

## [Unreleased]

### Added

- Added `UserCreated` and `UserProfileInvalid` profile contracts with stricter Pydantic validation for email, datetimes, coordinates, URLs, and country codes.
- Added profile quality rules with explicit rejection reasons for age plausibility, registration dates, source-specific country validation, ISO country codes, nationality, timezone offsets, text fields, and picture URLs.
- Added versioned Kafka JSON Schema artifacts for `users_created-value` and `users_created_invalid-value`.
- Added Confluent Schema Registry helpers using topic-value subject naming and configurable topic-derived subjects.
- Added a Kafka messaging package with a topic catalog and Schema Registry-aware `UserProfileEventProducer`.
- Added reliable Kafka producer defaults for idempotence, `acks=all`, retries, request and delivery timeouts, compression, and client id.
- Added async and sync Kafka publish paths with explicit delivery failure propagation for synchronous publishing.
- Added local Kafka producer backpressure handling with bounded retry behavior.
- Added optional Schema Registry basic auth and CA bundle settings.
- Added a Docker Compose integration stack for Kafka and Schema Registry.
- Added an optional `nox -s integration` session and integration test that publishes and consumes a real Schema Registry-framed Kafka event.
- Added Random User `country_code` derivation for supported source countries.
- Added `SCHEMA_REGISTRY_URL` and `PII_PSEUDONYMIZATION_SALT` settings.
- Added tests for profile contracts, quality rules, Schema Registry contracts, invalid-event privacy behavior, and the new privacy setting.
- Added messaging unit and contract tests for producer configuration, serialization, delivery handling, backpressure, topic naming, and Schema Registry payload compatibility.

### Security

- Added privacy-safe invalid profile events with strict allowlisted payloads.
- Added salted HMAC-SHA256 pseudonymization for invalid-event `source_user_id`.
- Required `PII_PSEUDONYMIZATION_SALT` outside `development`.
- Added CI image quality checks with Trivy ignore expiry validation before Docker image scanning.

### Changed

- Updated README and development roadmap to describe the completed profile quality and Schema Registry feature.
- Updated README and development roadmap to narrow the functional MVP while keeping Airflow orchestration, Spark Streaming, and Cassandra persistence as staged distributed-system capabilities.
- Updated CI/CD workflows to install pinned `uv` with an executable fallback instead of depending on `astral-sh/setup-uv` action archive downloads.
- Updated the roadmap order so Kafka messaging precedes Airflow orchestration, Spark Streaming, and integration testing.
- Strengthened invalid picture URL rejection reasons with field-level details such as `invalid_picture_url:picture_large`.

### Fixed

- Fixed CI failures caused by GitHub Actions being unable to download `astral-sh/setup-uv` archives from `codeload.github.com`.
- Fixed Kafka topic subject naming so Schema Registry subjects are derived from configured topic names.

## [0.1.0] - 2026-05-24

### Added

- Initialized the `realtimedatastreaming` Python project with package metadata, CLI entrypoint, `uv` dependency management, and Docker support.
- Added typed settings for application, logging, observability, Random User ingestion, Kafka, Spark, Cassandra, Sentry, and OpenTelemetry.
- Added structured logging with JSON/text formats and correlation context.
- Added the Random User ingestion module with typed normalized profiles, payload validation, timeout, retries, backoff, rate limiting, and classified errors.
- Added automated tests for settings, observability, packaging, quality configuration, and ingestion behavior.
- Added developer tooling with Nox, Ruff, mypy, pytest, coverage, pip-audit, and pre-commit.
- Added GitHub Actions workflows for quality checks, CI, CI/CD, Docker smoke tests, and Trivy security scanning.
- Added project documentation, `.env.example`, and a development roadmap.

### Security

- Added secret scanning, dependency auditing, container/filesystem vulnerability scanning, and expiry checks for temporary Trivy ignore entries.
