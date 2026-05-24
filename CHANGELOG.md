# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows semantic versioning.

## [Unreleased]

### Added

- Add complete README documentation for using, renaming, configuring, validating, and adapting the template.
- Add reusable GitHub Actions quality workflow for dependency audit, formatting, linting, typing, and tests.
- Add CI/CD workflow with manual dispatch, Docker image build, smoke test, Trivy image scan, Trivy filesystem scan, OSV Scanner, Gitleaks history scan, GHCR publishing, and security report artifact generation.
- Add `.trivyignore.yaml`, expiry checks, and ADR documentation for temporary Trivy exceptions.
- Add expanded project metadata, classifiers, keywords, project URLs, and README packaging metadata in `pyproject.toml`.
- Add supporting template documentation for project structure, Docker/CI-CD behavior, requirements, and VS Code extensions.
- Add typed and validated application settings for logging, service metadata, OpenTelemetry placeholders, and Sentry placeholders.
- Add structured JSON/text logging, correlation IDs, idempotent observability setup, and CLI lifecycle logs.
- Add tests for settings, CLI, observability, packaging, `.env.example`, and quality configuration.

### Changed

- Move the Python package from `src/template_doc` to direct package layout under `template_doc`.
- Update Hatch wheel and source distribution configuration for the direct `template_doc` package layout.
- Update Nox source path detection to use `template_doc` instead of `src`.
- Replace duplicated CI quality jobs with the shared `quality.yaml` workflow.
- Update CI triggers to target `main`, include manual dispatch, and call the shared quality workflow.
- Replace the previous CD workflow with the consolidated CI/CD workflow.
- Pin Docker base images more explicitly, copy `README.md` during builds, and build the project as a non-editable runtime install.
- Add targeted runtime security upgrades in the Dockerfile.
- Update settings to ignore extra environment variables via `SettingsConfigDict(extra="ignore")`.
- Limit the package, Nox sessions, Docker runtime, and CI quality checks to Python 3.12.
- Bound runtime Pydantic dependencies to the supported major version.
- Update README documentation for quality workflows, security checks, configuration validation, and observability placeholders.

### Fixed

- Fix Docker image build failure caused by missing `README.md` during package build metadata validation.
- Fix stale documentation that referred to `src/template_doc` after the package moved to direct layout.
- Fix misleading README text that said no real test suite existed.

### Removed

- Remove the old standalone `.github/workflows/cd.yaml` workflow in favor of `.github/workflows/cicd.yaml`.
- Remove repository-level VS Code settings from the template baseline.
