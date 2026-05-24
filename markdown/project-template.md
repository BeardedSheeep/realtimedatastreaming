# Personal Python Template

This repository is a neutral starting point for Python projects.

It is designed to be forked, copied, or used as a template. It is not a finished application and should not be consumed unchanged as a production project.

The goal is to provide a practical foundation:

- a minimal Python package under `template_doc/`;
- a small CLI entrypoint used as a smoke test;
- typed settings loaded from environment variables;
- repeatable local automation with Nox;
- dependency management with uv;
- formatting, linting, typing, auditing, and test tooling;
- Docker and GitHub Actions scaffolding for projects that need them.

The template intentionally does not impose a web framework, database, message queue, cloud provider, application architecture, or business domain.

## Source Of Truth

`README.md` is the human-facing entrypoint for this repository.

This document gives additional template-oriented notes, but the README should be updated first whenever the repository behavior changes.

## Project Management

### uv

`uv` is used to manage the environment, dependencies, and lock file.

Useful commands:

```bash
uv sync --all-groups
uv add package-name
uv add --group dev package-name
```

The project currently contains a real minimal package in `template_doc/`, so it behaves like a normal importable Python project from the beginning.

This template intentionally uses a direct package layout instead of a `src/` layout. When a derived project starts, `template_doc/` should be renamed to the real project name. Future code should then grow inside that package, for example:

```text
your_project/
├── module1/
├── module2/
├── cli.py
└── settings.py
```

### Dependency Groups

| Group | Purpose |
|---|---|
| `dev` | General development tools |
| `format` | Formatting with Ruff |
| `lint` | Linting with Ruff |
| `typing` | Static typing with mypy |
| `test` | Test and coverage tooling for future application tests |

Runtime dependencies should stay minimal. A dependency belongs in `[project.dependencies]` only when application code needs it at runtime.

## Nox Sessions

| Session | Purpose |
|---|---|
| `dev` | Syncs the environment and installs pre-commit hooks |
| `format` | Checks import ordering and formatting with Ruff |
| `lint` | Runs Ruff lint checks |
| `typing` | Runs mypy |
| `test` | Runs pytest when a `tests/` directory exists; passes for the scaffold while no tests exist |
| `audit` | Runs `pip-audit` |
| `docker_build` | Builds the Docker image |
| `docker_smoke` | Runs a minimal container smoke test |

Common commands:

```bash
uv run nox -s dev
uv run nox
uv run nox -s format lint typing test audit
```

## Configuration

Application settings are defined in `template_doc/settings.py`.

The settings model reads environment variables such as:

```env
APP_NAME=template-doc
APP_ENV=development
APP_DEBUG=false
```

`.env.example` documents expected variables, but runtime code does not implicitly load `.env`. This avoids behavior that depends on the current working directory.

`get_settings()` caches the settings object. Tests that mutate environment variables should call `get_settings.cache_clear()` before loading settings again.

Derived projects can choose their own local `.env` loading strategy if needed.

## Python Code Standards

### Formatting

- Line length: 120 characters.
- Formatting: `ruff format`.
- Import sorting: `ruff check --select I`.

### Linting

Ruff is configured with a pragmatic baseline:

- `E` and `F` for common Python errors;
- `I` for import ordering;
- `B` for frequent bug patterns;
- `UP` for Python modernization;
- `SIM` for code simplification.

### Typing

Mypy is enabled with a non-strict baseline:

- `check_untyped_defs = true`
- `warn_unused_configs = true`
- `warn_redundant_casts = true`
- `warn_unused_ignores = true`

This is deliberate for the template: it keeps the starting point useful without being too rigid before real application code exists.

Derived projects should document whether they keep this baseline or move toward strict typing. A recommended strict profile is:

```toml
[tool.mypy]
python_version = "3.12"
strict = true
```

For larger projects, strict mode can be introduced module by module.

## Security And Delivery Scaffolding

The repository includes:

- Gitleaks in pre-commit;
- `pip-audit` through Nox;
- Trivy, OSV Scanner, and Gitleaks in CI/CD;
- `.trivyignore.yaml` with expiring vulnerability exceptions;
- a reusable GitHub Actions quality workflow shared by CI and CI/CD;
- Docker and GHCR publishing workflow examples.

These are reusable defaults, not mandatory production policy. A derived project must review registry names, branch rules, release strategy, secrets, environments, and image promotion before relying on the workflows.

## Base Files

| File | Purpose |
|---|---|
| `README.md` | Main human-facing documentation |
| `pyproject.toml` | Project metadata, dependencies, and tool configuration |
| `uv.lock` | Reproducible lock file |
| `noxfile.py` | Local automation sessions |
| `.pre-commit-config.yaml` | Pre-commit hooks |
| `.gitignore` | Git exclusions for Python and local tools |
| `.env.example` | Example application environment variables |
| `.trivyignore.yaml` | Temporary vulnerability exceptions |
| `Dockerfile` | Container build scaffold |
| `.github/workflows/quality.yaml` | Shared audit, formatting, linting, typing, and test workflow |
| `.github/workflows/ci.yaml` | Validation workflow scaffold |
| `.github/workflows/cicd.yaml` | Container CI/CD and security scanning scaffold |
| `LICENSE` | MIT license placeholder |
| `markdown/` | Supporting documentation |
| `template_doc/` | Minimal importable package scaffold |

## Before Reusing The Template

When forking or copying this repository, review and adapt:

- package directory name and CLI name;
- project metadata in `pyproject.toml`;
- runtime dependencies;
- README contents;
- Docker image name, registry, tags, and runtime command;
- shared quality workflow contents;
- CI/CD triggers and required checks;
- GHCR publishing behavior;
- branch protection and review rules;
- release policy and changelog process;
- `.trivyignore.yaml` exceptions;
- secrets and GitHub environments;
- license ownership.

## Visual Summary

```text
Personal Python template
├── Package scaffold       -> template_doc
├── Configuration          -> typed settings from environment variables
├── Environment            -> uv + uv.lock
├── Automation             -> nox
├── Quality                -> ruff + mypy + pytest tooling
├── Security               -> pip-audit + gitleaks + Trivy + OSV
├── Container scaffold     -> Dockerfile
├── CI/CD scaffold         -> GitHub Actions
└── Documentation          -> README.md + markdown/
```
