# Changelog

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
