# Template Doc

Python project template with:

- a small importable package;
- a console entrypoint;
- typed and validated environment settings;
- structured JSON/text logging helpers;
- lightweight correlation context for request and trace IDs;
- `uv` dependency management;
- `nox` developer commands;
- Ruff, mypy, pytest, coverage, and pip-audit wiring;
- Docker and GitHub Actions scaffolding with security scan examples.

This repository is meant to be copied, renamed, and adapted. It is not a finished application.

## Layout

```text
template_doc/                  Python package placeholder
markdown/                      Supporting template notes
scripts/                       Repository maintenance scripts
.github/workflows/             CI and CI/CD examples
Dockerfile                     Container build scaffold
noxfile.py                     Local task runner
pyproject.toml                 Package and tool configuration
uv.lock                        Locked dependency graph
```

## Setup

This template targets Python 3.12.

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

The test suite covers package importability, the console script, environment settings, observability formatting, packaging smoke checks, and quality-tool configuration.

## CLI

```bash
template-doc
```

Default output:

```text
template-doc [development]
```

The CLI is only a packaging and configuration smoke test.

## Configuration

Environment variables:

```env
APP_NAME=template-doc
APP_ENV=development
APP_DEBUG=false

LOG_LEVEL=INFO
LOG_FORMAT=json
SERVICE_NAME=template-doc
SERVICE_VERSION=0.1.0

OTEL_ENABLED=false
OTEL_EXPORTER_OTLP_ENDPOINT=
OTEL_SERVICE_NAME=template-doc

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

The template includes a lightweight observability baseline for Python CLI, worker, or service projects:

- configure application logs through `template_doc.observability.configure_observability`;
- call observability configuration from the process entrypoint; it is idempotent and preserves externally registered logging handlers;
- use `logging` for application logs and keep `print` for intentional user-facing output only;
- prefer `LOG_FORMAT=json` outside local debugging;
- include stable context fields such as service, environment, version, request ID, and trace ID;
- never log secrets, credentials, tokens, or raw sensitive user data;
- add a request ID middleware when turning the template into a web service;
- let OpenTelemetry populate trace IDs when tracing is enabled in a real runtime;
- add metrics, healthchecks, and distributed tracing when the project becomes a web service or long-running worker.

OpenTelemetry and Sentry settings are present as placeholders for future service projects. The template does not initialize those SDKs by default.

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

Before using Docker for a real project, review the base image, entrypoint, runtime packages, ports, user, registry, tags, and publication rules.

## CI/CD

Workflows:

- `.github/workflows/quality.yaml`: runs audit, format, lint, typing, and tests;
- `.github/workflows/ci.yaml`: runs quality checks on pushes and pull requests;
- `.github/workflows/cicd.yaml`: builds the image, runs security scans, and publishes to GHCR after blocking checks pass.

Quality checks run on Python 3.12, matching the package metadata and Nox sessions.

Before enabling CI/CD for a real project, review branch rules, workflow permissions, registry naming, image promotion, secrets, environments, and release policy.

## Renaming Checklist

Replace the placeholders before using this as a real project:

- `template_doc/` package directory;
- `template_doc` imports;
- `template-doc` package name;
- `template-doc` CLI command;
- `PROJECT_PACKAGE` in `noxfile.py`;
- Hatch package and source distribution settings in `pyproject.toml`;
- Docker `COPY` path and `ENTRYPOINT`;
- `APP_NAME` in `.env.example`;
- project URLs containing `CHANGE_ME`;
- README and documentation text that still describes the template.

Search for leftovers:

```bash
rg "template_doc|template-doc|CHANGE_ME"
```

## Before Real Use

At minimum:

1. Rename the package and command.
2. Update `pyproject.toml` metadata.
3. Replace the example CLI with the real entrypoint.
4. Add tests for real behavior.
5. Review Docker and CI/CD before publishing anything.
6. Rewrite this README for the actual project.
