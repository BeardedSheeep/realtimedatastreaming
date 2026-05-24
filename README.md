# Realtime Data Streaming

Python project scaffold for the `realtimedatastreaming` repository.

The repository is initialized with:

- an importable `realtimedatastreaming` package;
- a `realtimedatastreaming` console entrypoint;
- typed and validated environment settings;
- structured JSON/text logging helpers;
- lightweight correlation context for request and trace IDs;
- `uv` dependency management;
- `nox` developer commands;
- Ruff, mypy, pytest, coverage, and pip-audit wiring;
- Docker and GitHub Actions quality/security workflows.

This is the local foundation that will receive the real-time data engineering code later.

## Layout

```text
realtimedatastreaming/        Python package
markdown/                     Supporting project notes
scripts/                      Repository maintenance scripts
.github/workflows/            CI and CI/CD workflows
Dockerfile                    Container build scaffold
noxfile.py                    Local task runner
pyproject.toml                Package and tool configuration
uv.lock                       Locked dependency graph
```

## Setup

This project targets Python 3.12.

```bash
uv sync --all-groups
```

Install the pre-commit hook:

```bash
uv run nox -s dev
```

Run the default checks:

```bash
uv run nox
```

Run one check:

```bash
uv run nox -s format
uv run nox -s lint
uv run nox -s typing
uv run nox -s test
uv run nox -s audit
```

## CLI

```bash
realtimedatastreaming
```

Default output:

```text
realtimedatastreaming [development]
```

The current CLI is a packaging and configuration smoke test. It should be replaced by the real application entrypoint when the data streaming code is integrated.

## Configuration

Environment variables:

```env
APP_NAME=realtimedatastreaming
APP_ENV=development
APP_DEBUG=false

LOG_LEVEL=INFO
LOG_FORMAT=json
SERVICE_NAME=realtimedatastreaming
SERVICE_VERSION=0.1.0

OTEL_ENABLED=false
OTEL_EXPORTER_OTLP_ENDPOINT=
OTEL_SERVICE_NAME=realtimedatastreaming

SENTRY_DSN=
SENTRY_ENVIRONMENT=development
SENTRY_TRACES_SAMPLE_RATE=0.0
```

Settings are read from environment variables. `.env` files are not loaded automatically.

Validation rules:

- `LOG_LEVEL` must be one of `CRITICAL`, `ERROR`, `WARNING`, `INFO`, `DEBUG`, or `NOTSET`;
- `LOG_FORMAT` must be `json` or `text`;
- `SENTRY_TRACES_SAMPLE_RATE` must be between `0.0` and `1.0`.

## Observability

The project includes a lightweight observability baseline for Python CLI, worker, or service code:

- configure application logs through `realtimedatastreaming.observability.configure_observability`;
- call observability configuration from process entrypoints;
- use `logging` for application logs and keep `print` for intentional user-facing output only;
- prefer `LOG_FORMAT=json` outside local debugging;
- include stable context fields such as service, environment, version, request ID, and trace ID;
- never log secrets, credentials, tokens, or raw sensitive user data;
- add request ID middleware when turning the project into a web service;
- add metrics, healthchecks, and distributed tracing when the project becomes a web service or long-running worker.

OpenTelemetry and Sentry settings are present as placeholders for future service runtime integration. The SDKs are not initialized by default.

## Docker

Build the image:

```bash
uv run nox -s docker_build
```

Run the container smoke test:

```bash
uv run nox -s docker_smoke
```

Run the full local image quality gate:

```bash
uv run nox -s image_quality
```

Before publishing an image, review the base image, entrypoint, runtime packages, ports, user, registry, tags, and publication rules.

## CI/CD

Workflows:

- `.github/workflows/quality.yaml`: runs audit, format, lint, typing, and tests;
- `.github/workflows/ci.yaml`: runs quality checks on pushes and pull requests;
- `.github/workflows/cicd.yaml`: builds the image, runs security scans, and prepares GHCR publishing behavior.

Quality checks run on Python 3.12, matching the package metadata and Nox sessions.

## Integration Notes

When integrating the external real-time data engineering code, keep the initialized project boundaries clean:

1. Put reusable Python code under `realtimedatastreaming/`.
2. Keep generated artifacts, virtual environments, caches, and local IDE files out of git.
3. Add tests around real behavior as each pipeline component is imported.
4. Pin runtime dependencies intentionally in `pyproject.toml` and refresh `uv.lock`.
5. Review Docker and CI/CD before enabling image publication.
