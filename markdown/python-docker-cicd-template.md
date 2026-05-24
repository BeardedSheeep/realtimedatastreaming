# Python Docker CI/CD Template

## Objective

This document describes the Docker and GitHub Actions scaffolding included in this Python template.

The workflow is intended to be copied and adapted by derived projects. It provides a secure-by-default starting point, but it is not a universal production release policy.

Before using it in a real project, review:

- image name and registry;
- branch protection;
- workflow triggers;
- release and versioning policy;
- GHCR publishing behavior;
- GitHub permissions;
- repository secrets and environments;
- security exception policy.

## Current Scope

The repository includes:

- a Dockerfile that builds the minimal Python package;
- Nox sessions for image build and smoke test;
- a reusable quality workflow for dependency audit, formatting, linting, typing, and tests;
- a CI workflow for validation;
- a CI/CD workflow for build, security scans, and image publishing;
- Trivy, OSV Scanner, and Gitleaks security checks;
- `.trivyignore.yaml` for temporary vulnerability exceptions;
- a shell check that fails CI/CD when a Trivy exception is expired.

## Tooling

| Need | Tool |
|---|---|
| Environment and dependencies | `uv` |
| Local automation | `nox` |
| Formatting and linting | `ruff` |
| Static typing | `mypy` |
| Tests and coverage tooling | `pytest`, `coverage` |
| Python dependency audit | `pip-audit` |
| Secret detection | `gitleaks` |
| Container build | Docker |
| Container registry | GHCR |
| Image and filesystem scanning | Trivy |
| Dependency scanning | OSV Scanner |
| CI/CD | GitHub Actions |

## Local Workflow

```bash
uv sync --all-groups
uv run nox -s dev
uv run nox
```

Docker-specific commands:

```bash
uv run nox -s docker_build
uv run nox -s docker_smoke
```

## Nox Sessions

| Session | Purpose |
|---|---|
| `dev` | Bootstrap the local environment and install pre-commit hooks |
| `format` | Check formatting and import ordering with Ruff |
| `lint` | Run Ruff lint checks |
| `typing` | Run mypy |
| `test` | Run pytest when `tests/` exists; pass for the scaffold while no tests exist |
| `audit` | Run `pip-audit` |
| `docker_build` | Build the Docker image |
| `docker_smoke` | Run a minimal container smoke test |

## Shared Quality Workflow

File:

```text
.github/workflows/quality.yaml
```

The quality workflow is called by both CI and CI/CD. It runs:

- dependency audit;
- formatting check;
- lint check;
- typing check;
- test session.

Keeping these checks in one reusable workflow prevents drift between pull-request validation and the main-branch delivery path.

The test session is intentionally permissive while this repository is still a template scaffold. Derived projects should add real tests once stable behavior exists.

## CI Workflow

File:

```text
.github/workflows/ci.yaml
```

Current triggers:

```yaml
on:
  workflow_dispatch:
  push:
    branches: [main]
  pull_request:
    branches: [main]
```

The CI workflow calls `.github/workflows/quality.yaml`.

## Local Image Quality

Run the local image quality gate with:

```bash
uv run nox -s image_quality
```

The session checks Trivy ignore expiry, builds the image, runs the container smoke test, and scans the image with Trivy.

## CI/CD Workflow

File:

```text
.github/workflows/cicd.yaml
```

Current triggers:

```yaml
on:
  workflow_dispatch:
  push:
    branches: [main]
```

The CI/CD workflow performs:

1. Shared quality workflow.
2. Trivy ignore exception expiry check.
3. Docker image build.
4. Docker image smoke test.
5. Trivy image scan.
6. Trivy filesystem scan for vulnerabilities, secrets, and misconfigurations.
7. OSV Scanner dependency scan.
8. Gitleaks Git history scan.
9. GHCR image publishing only after security checks pass.
10. Security report artifact generation.

## Security Scan Ordering

The workflow follows a scan-before-publish model:

```text
quality checks
  -> build image
  -> smoke image
  -> security scans
  -> publish image
```

Repository scans that do not need the image are decoupled from the image build when possible, so they can provide faster feedback.

The filesystem scan writes SARIF for GitHub code scanning and also runs blocking table-output checks:

- repository CVEs with Trivy `vuln`;
- repository secrets and misconfigurations with Trivy `secret,misconfig`.

## Trivy Exceptions

Temporary vulnerability exceptions are stored in:

```text
.trivyignore.yaml
```

Each entry must include an `expired_at` date.

The CI/CD workflow runs:

```bash
scripts/check-trivyignore-expiry.sh .trivyignore.yaml
```

If an exception is missing an expiration date, has an invalid date, or is expired, the workflow fails before image publishing.

The decision is documented in:

```text
markdown/adr-trivyignore-expiry-check.md
```

## Publishing

The publish job pushes:

- the commit SHA tag;
- `latest`.

Derived projects must decide whether publishing `latest` is appropriate. For stricter release processes, publish `latest` only from versioned releases or tags.

## Adaptation Checklist

Before enabling this workflow in a derived project, review:

- package directory and CLI names;
- Docker runtime command;
- exposed ports;
- registry and image name;
- branch protection rules;
- shared quality workflow contents;
- required CI checks;
- release strategy;
- whether `latest` should be published;
- GitHub environments and secrets;
- security exception ownership and review cadence.

## Acceptance Criteria For Derived Projects

- Local setup works with `uv sync --all-groups`.
- `uv run nox` runs the expected quality sessions.
- Docker build and smoke test pass if the project uses containers.
- CI runs automatically for pull requests to `main`.
- CI/CD publishes only from approved branches or release tags.
- Security scans, including high/critical filesystem secrets and misconfigurations, are blocking before image publishing.
- Trivy exceptions are temporary and checked for expiration.
