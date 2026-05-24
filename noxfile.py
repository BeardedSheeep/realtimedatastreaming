import os
import re
from pathlib import Path

import nox
from nox.sessions import Session

PROJECT_DIR = Path(__file__).parent
PROJECT_NAME = os.getenv("PROJECT_NAME", PROJECT_DIR.name)
PROJECT_PACKAGE = "template_doc"
PYTHON_VERSION = "3.12"
SOURCE_PATHS = [path for path in (PROJECT_PACKAGE, "tests") if (PROJECT_DIR / path).exists()] or ["."]

nox.needs_version = ">=2025.5.1"
nox.options.reuse_existing_virtualenvs = False
nox.options.error_on_missing_interpreters = True
nox.options.default_venv_backend = "uv"
nox.options.sessions = ["format", "lint", "typing", "test"]


def docker_image_name() -> str:
    default_image_name = re.sub(r"[^a-z0-9_.-]+", "-", PROJECT_NAME.lower()).strip("-") or "app"
    image_name = os.getenv("DOCKER_IMAGE", default_image_name)
    image_tag = os.getenv("DOCKER_TAG", "latest")
    return f"{image_name}:{image_tag}"


@nox.session(venv_backend="none")
def dev(session: Session) -> None:
    session.run("uv", "sync", "--all-extras", "--all-groups", external=True)
    session.run("uv", "run", "pre-commit", "install")


@nox.session(python=PYTHON_VERSION)
def format(session: Session) -> None:
    session.run("uv", "sync", "--active", "--all-extras", "--all-groups", "--locked", external=True)
    session.run("ruff", "check", *SOURCE_PATHS, "--select", "I")
    session.run("ruff", "format", *SOURCE_PATHS, "--check")


@nox.session(python=PYTHON_VERSION)
def lint(session: Session) -> None:
    session.run("uv", "sync", "--active", "--all-extras", "--all-groups", "--locked", external=True)
    session.run("ruff", "check", *SOURCE_PATHS)


@nox.session(python=PYTHON_VERSION)
def typing(session: Session) -> None:
    session.run("uv", "sync", "--active", "--all-extras", "--all-groups", "--locked", external=True)
    session.run("mypy", *SOURCE_PATHS)


@nox.session(python=PYTHON_VERSION)
def test(session: Session) -> None:
    tests_dir = PROJECT_DIR / "tests"
    if not tests_dir.exists():
        session.log("tests directory not found; nothing to run")
        return

    session.run("uv", "sync", "--active", "--all-extras", "--all-groups", "--locked", external=True)
    session.run("pytest", str(tests_dir), "--cov", PROJECT_PACKAGE, "--cov-report", "term-missing", *session.posargs)


@nox.session(python=PYTHON_VERSION)
def audit(session: Session) -> None:
    session.run("uv", "sync", "--active", "--all-extras", "--all-groups", "--locked", external=True)
    session.run("pip-audit")


@nox.session(python=PYTHON_VERSION)
def docker_build(session: Session) -> None:
    dockerfile = Path(os.getenv("DOCKERFILE", "Dockerfile"))
    if not dockerfile.exists():
        session.error(f"Dockerfile not found: {dockerfile}")

    image_name = docker_image_name()
    docker_target = os.getenv("DOCKER_TARGET")

    command = ["docker", "build", "-f", str(dockerfile), "-t", image_name]
    if docker_target:
        command.extend(["--target", docker_target])
    command.extend(session.posargs)
    command.append(".")

    session.run(*command, external=True)


@nox.session(python=PYTHON_VERSION)
def docker_smoke(session: Session) -> None:
    image_name = docker_image_name()
    command = ["docker", "run", "--rm", image_name]
    command.extend(session.posargs)
    session.run(*command, external=True)


@nox.session
def image_quality(session: Session) -> None:
    image_name = docker_image_name()

    session.run("bash", "scripts/check-trivyignore-expiry.sh", ".trivyignore.yaml", external=True)

    dockerfile = Path(os.getenv("DOCKERFILE", "Dockerfile"))
    if not dockerfile.exists():
        session.error(f"Dockerfile not found: {dockerfile}")

    build_command = ["docker", "build", "-f", str(dockerfile), "-t", image_name]
    docker_target = os.getenv("DOCKER_TARGET")
    if docker_target:
        build_command.extend(["--target", docker_target])
    build_command.append(".")

    session.run(*build_command, external=True)
    session.run("docker", "run", "--rm", image_name, external=True)
    session.run(
        "docker",
        "run",
        "--rm",
        "-v",
        "/var/run/docker.sock:/var/run/docker.sock",
        "-v",
        f"{PROJECT_DIR}:/workspace",
        "aquasec/trivy:0.70.0",
        "image",
        "--scanners",
        "vuln",
        "--ignorefile",
        "/workspace/.trivyignore.yaml",
        "--severity",
        "HIGH,CRITICAL",
        "--exit-code",
        "1",
        image_name,
        external=True,
    )
