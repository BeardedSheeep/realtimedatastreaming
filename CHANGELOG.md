# Changelog

## [Unreleased]

### Added

- Added `UserCreated` and `UserProfileInvalid` profile contracts with stricter Pydantic validation for email, datetimes, coordinates, URLs, and country codes.
- Added profile quality rules with explicit rejection reasons for age plausibility, registration dates, source-specific country validation, ISO country codes, nationality, timezone offsets, text fields, and picture URLs.
- Added versioned Kafka JSON Schema artifacts for `users_created-value` and `users_created_invalid-value`.
- Added Confluent Schema Registry helpers using topic-value subject naming and configurable topic-derived subjects.
- Added Random User `country_code` derivation for supported source countries.
- Added `SCHEMA_REGISTRY_URL` and `PII_PSEUDONYMIZATION_SALT` settings.
- Added tests for profile contracts, quality rules, Schema Registry contracts, invalid-event privacy behavior, and the new privacy setting.

### Security

- Added privacy-safe invalid profile events with strict allowlisted payloads.
- Added salted HMAC-SHA256 pseudonymization for invalid-event `source_user_id`.
- Required `PII_PSEUDONYMIZATION_SALT` outside `development`.

### Changed

- Updated README and development roadmap to describe the completed profile quality and Schema Registry feature.
- Strengthened invalid picture URL rejection reasons with field-level details such as `invalid_picture_url:picture_large`.

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
