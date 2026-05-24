# Specification - Generic Python Template

## 1. Objective

This repository provides a clean, minimal, and reusable Python foundation for new projects.

It is intended to be forked, copied, or used as a template. It is not intended to be consumed unchanged as a finished production application.

The template is intentionally neutral:

- no imposed web framework;
- no imposed database;
- no imposed message queue;
- no imposed cloud provider;
- no imposed business logic;
- no premature application architecture.

The guiding principle is simple: provide common foundations, then let each derived project add only what it needs.

## 2. Functional Scope

### Included

- Python project management with `uv`.
- Reproducible lock file with `uv.lock`.
- Local automation with `nox`.
- Minimal package scaffold in `template_doc/`.
- CLI smoke-test entrypoint.
- Typed settings with `pydantic-settings`.
- Environment-variable-based runtime configuration.
- `.env.example` as documentation for local configuration.
- Formatting and linting with `ruff`.
- Static typing with `mypy`.
- Test and coverage tooling for future tests.
- Dependency auditing with `pip-audit`.
- Secret detection with Gitleaks.
- Dockerfile scaffold.
- Reusable GitHub Actions quality workflow.
- GitHub Actions CI workflow.
- GitHub Actions CI/CD workflow with image build, security scans, and GHCR publishing.
- Trivy, OSV Scanner, and Gitleaks security scanning.
- Trivy exception expiry check.
- Markdown documentation.

### Explicitly Out Of Scope

- A finished application.
- A framework-specific architecture.
- Production deployment configuration.
- Organization-specific branch protection rules.
- Organization-specific release approval process.
- A universal security exception policy for all derived projects.

Derived projects must adapt those parts to their own context.

## 3. Current Repository Structure

```text
template-doc/
├── .github/
│   └── workflows/
│       ├── ci.yaml
│       ├── cicd.yaml
│       └── quality.yaml
├── markdown/
│   ├── project-template.md
│   ├── python-docker-cicd-template.md
│   ├── requirements-specification.md
│   └── vscode-extensions.md
├── scripts/
│   └── check-trivyignore-expiry.sh
├── template_doc/
│   ├── __init__.py
│   ├── cli.py
│   ├── py.typed
│   └── settings.py
├── .dockerignore
├── .env.example
├── .gitignore
├── .pre-commit-config.yaml
├── .trivyignore.yaml
├── CHANGELOG.md
├── Dockerfile
├── LICENSE
├── README.md
├── noxfile.py
├── pyproject.toml
└── uv.lock
```

Local cache and environment directories such as `.venv/`, `.nox/`, `.mypy_cache/`, `.ruff_cache/`, and `__pycache__/` are local artifacts and must remain ignored by Git.

## 4. Current Tooling

### 4.1 Dependency Management

The project uses `uv`.

Runtime dependencies are intentionally minimal:

- `pydantic`;
- `pydantic-settings`.

Developer and tooling dependencies are separated into dependency groups:

| Group | Usage |
|---|---|
| `dev` | General development tools |
| `format` | Formatting and import sorting |
| `lint` | Static analysis |
| `typing` | Type checking |
| `test` | Test and coverage tooling |

### 4.2 Runtime Configuration

Settings are defined in `template_doc/settings.py`.

Runtime configuration comes from environment variables. The application does not implicitly load `.env`, so behavior does not depend on the current working directory.

`get_settings()` is cached for stable runtime behavior. Tests that mutate environment variables must clear this cache before reading settings again.

`.env.example` documents expected variables:

```env
APP_NAME=template-doc
APP_ENV=development
APP_DEBUG=false
```

### 4.3 Nox Sessions

| Session | Status | Role |
|---|---|---|
| `dev` | Implemented | Synchronizes the environment and installs pre-commit hooks |
| `format` | Implemented | Checks import sorting and Ruff formatting |
| `lint` | Implemented | Runs Ruff on detected source paths |
| `typing` | Implemented | Runs mypy |
| `test` | Implemented | Runs pytest when tests exist; passes for the scaffold while no tests exist |
| `audit` | Implemented | Runs `pip-audit` |
| `docker_build` | Implemented | Builds the Docker image |
| `docker_smoke` | Implemented | Runs a minimal container smoke test |

### 4.4 Code Quality

Ruff is configured with a pragmatic baseline:

- Python errors (`E`, `F`);
- import sorting (`I`);
- common best practices (`B`, `UP`, `SIM`);
- line length set to 120 characters.

Mypy is configured with a non-strict baseline:

- target Python 3.12;
- check untyped function bodies;
- report unnecessary casts and ignores;
- report unused configuration.

This baseline is acceptable for the generic template. Derived projects should either keep this choice documented or adopt a stricter profile once real application modules exist:

```toml
[tool.mypy]
python_version = "3.12"
strict = true
```

### 4.5 Security

Security tooling includes:

- Gitleaks in pre-commit;
- `pip-audit` in Nox;
- Trivy image scan;
- Trivy filesystem scan for vulnerabilities, secrets, and misconfigurations;
- OSV Scanner;
- Gitleaks Git history scan;
- `.trivyignore.yaml` for temporary exceptions;
- `scripts/check-trivyignore-expiry.sh` to fail CI/CD when exceptions expire.

The Trivy filesystem SARIF is uploaded for GitHub code scanning. The CI/CD workflow also runs blocking table-output checks for repository CVEs and for high/critical secrets or misconfigurations.

The ADR for Trivy exception expiration is:

```text
markdown/adr-trivyignore-expiry-check.md
```

### 4.6 CI/CD

The shared quality workflow runs dependency audit, formatting, linting, typing, and tests. CI/CD keeps a blocking scan-before-publish image gate.

The CI workflow runs on:

- manual dispatch;
- push to `main`;
- pull request to `main`.

The CI/CD workflow runs on:

- manual dispatch;
- push to `main`.

The CI/CD workflow runs the shared quality workflow, builds and smoke-tests an image, runs security scans, and publishes to GHCR only after blocking checks pass.

Docker, GHCR publishing, and the security workflow are scaffolding for derived projects. They must be reviewed before a real project relies on them.

## 5. Existing vs Remaining Work

| Area | Current State | Remaining Work For Derived Projects |
|---|---|---|
| Project metadata | Standard template metadata in `pyproject.toml` | Replace placeholder authors, classifiers, keywords, and URLs |
| Runtime dependencies | Minimal typed settings dependencies | Add only project-specific runtime dependencies |
| Dev dependencies | Organized dependency groups | Add specialized tooling only when needed |
| Local automation | Main Nox sessions implemented | Adapt sessions to project workflows |
| Formatting | Ruff configured | Adjust rules as maturity increases |
| Linting | Ruff configured | Add stricter rule families if useful |
| Typing | Mypy non-strict baseline | Enable stricter typing when ready |
| Testing | Test tooling present; scaffold passes without tests | Add real tests as soon as behavior exists |
| Configuration | Environment-variable based settings | Add project-specific settings |
| Docker | Dockerfile scaffold present | Adapt runtime command, ports, base image, and dependencies |
| CI | Validation workflow present and backed by shared quality workflow | Configure required checks and branch protection |
| CI/CD | Build, scan, publish scaffold present | Adapt registry, release strategy, environments, and secrets |
| Security exceptions | `.trivyignore.yaml` with expiry check | Review ownership and renewal policy |
| Documentation | README and supporting Markdown present | Keep docs aligned with real project behavior |
| Release process | Changelog present | Define release policy for derived project |

## 6. Recommendations For Derived Projects

### Immediate Adaptation

1. Rename the package directory from `template_doc/` to the real project name.
2. Rename the CLI command from `template-doc` if needed.
3. Update project metadata.
4. Update README content.
5. Review runtime dependencies.
6. Add tests once behavior exists.
7. Review Dockerfile and workflows before enabling publishing.

### Before Production Use

1. Define branch protection and review rules.
2. Define release and versioning policy.
3. Decide whether `latest` should be published.
4. Configure GitHub environments and secrets.
5. Review GHCR image names and registry.
6. Review `.trivyignore.yaml` ownership and expiry policy.
7. Add project-specific observability and error-handling conventions.

## 7. Reference Commands

Install all dependency groups:

```bash
uv sync --all-groups
```

Set up local development:

```bash
uv run nox -s dev
```

Run local quality checks:

```bash
uv run nox
```

Build and smoke-test the image:

```bash
uv run nox -s docker_build docker_smoke
```

Check Trivy exception expiry:

```bash
scripts/check-trivyignore-expiry.sh .trivyignore.yaml
```

## 8. Conclusion

The current repository is a reusable Python template with Docker and CI/CD scaffolding included.

The template provides useful defaults but intentionally stays generic. Derived projects must adapt the package directory name, metadata, tests, runtime dependencies, Docker settings, CI/CD publishing behavior, security exceptions, and release policy before treating it as production-ready.
