# Copyright (c) 2026 BeardedSheeep

import os
import re
import subprocess
import tomllib
from datetime import UTC, datetime
from pathlib import Path

import nox
from nox.sessions import Session

PROJECT_DIR = Path(__file__).parent
PROJECT_NAME = os.getenv("PROJECT_NAME", PROJECT_DIR.name)
PROJECT_PACKAGE = "realtimedatastreaming"
PYTHON_VERSION = "3.12"
SOURCE_PATHS = [path for path in (PROJECT_PACKAGE, "tests") if (PROJECT_DIR / path).exists()] or ["."]
DOCKER_BUILD_ENV = {"DOCKER_BUILDKIT": "1"}
TRIVY_TIMEOUT = os.getenv("TRIVY_TIMEOUT", "20m")
DOCKER_RUNTIME_SMOKE_SCRIPT = """
import json

import cassandra.cluster
import confluent_kafka
import jsonschema
import pyspark

import realtimedatastreaming
import realtimedatastreaming.messaging.kafka_producer
from realtimedatastreaming.ingestion.quality import validate_user_profile_quality
from realtimedatastreaming.ingestion.schema_registry import (
    USERS_CREATED_INVALID_VALUE_CONTRACT,
    USERS_CREATED_VALUE_CONTRACT,
)
from realtimedatastreaming.ingestion.schemas import UserCreated, UserProfileInvalid
from realtimedatastreaming.settings import get_settings

settings = get_settings()
assert settings.app_name == "realtimedatastreaming"

payload = {
    "source": "random_user",
    "source_user_id": "2f4c4f6e-743b-4c8e-82df-35b2c789f35f",
    "gender": "female",
    "title": "Ms",
    "first_name": "Ada",
    "last_name": "Lovelace",
    "street_number": 42,
    "street_name": "Analytical Engine Road",
    "city": "London",
    "state": "Greater London",
    "country": "United Kingdom",
    "country_code": "GB",
    "postcode": "SW1A 1AA",
    "latitude": "51.5072",
    "longitude": "-0.1276",
    "timezone_offset": "+0:00",
    "timezone_description": "London",
    "email": "ada.lovelace@randomuser.me",
    "username": "adal",
    "date_of_birth": "2000-01-01T00:00:00Z",
    "registered_at": "2024-01-01T12:00:00Z",
    "phone": "020 7946 0958",
    "cell": "07123 456789",
    "picture_large": "https://example.com/large.jpg",
    "picture_medium": "https://example.com/medium.jpg",
    "picture_thumbnail": "https://example.com/thumb.jpg",
    "nationality": "GB",
}

user = UserCreated.model_validate(payload)
assert validate_user_profile_quality(user) == ()

created_schema = json.loads(USERS_CREATED_VALUE_CONTRACT.schema_text())
invalid_schema = json.loads(USERS_CREATED_INVALID_VALUE_CONTRACT.schema_text())
jsonschema.Draft202012Validator.check_schema(created_schema)
jsonschema.Draft202012Validator.check_schema(invalid_schema)
jsonschema.validate(user.model_dump(mode="json"), created_schema)

invalid_event = UserProfileInvalid(rejection_reasons=("invalid_email",), payload={"email": "redacted"})
jsonschema.validate(invalid_event.model_dump(mode="json"), invalid_schema)

assert realtimedatastreaming.__name__ == "realtimedatastreaming"
assert confluent_kafka.__name__ == "confluent_kafka"
assert cassandra.cluster.__name__ == "cassandra.cluster"
assert pyspark.__name__ == "pyspark"
print("runtime smoke ok")
"""

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


def git_revision() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_DIR,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def project_metadata() -> dict[str, object]:
    with (PROJECT_DIR / "pyproject.toml").open("rb") as pyproject:
        return tomllib.load(pyproject)["project"]


def github_repository_url() -> str | None:
    server_url = os.getenv("GITHUB_SERVER_URL")
    repository = os.getenv("GITHUB_REPOSITORY")
    if server_url and repository:
        return f"{server_url}/{repository}"
    return None


def docker_build_args() -> dict[str, str]:
    metadata = project_metadata()
    urls = metadata.get("urls", {})
    if not isinstance(urls, dict):
        urls = {}

    source = os.getenv("OCI_SOURCE") or github_repository_url() or str(urls.get("Repository", "unknown"))
    version = os.getenv("OCI_VERSION") or os.getenv("GITHUB_REF_NAME") or str(metadata.get("version", "unknown"))

    return {
        "OCI_AUTHORS": os.getenv("OCI_AUTHORS", "BeardedSheeep"),
        "OCI_COPYRIGHT": os.getenv("OCI_COPYRIGHT", "Copyright (c) 2026 BeardedSheeep"),
        "OCI_CREATED": os.getenv("OCI_CREATED", datetime.now(UTC).isoformat(timespec="seconds")),
        "OCI_DESCRIPTION": os.getenv("OCI_DESCRIPTION", str(metadata.get("description", ""))),
        "OCI_LICENSES": os.getenv("OCI_LICENSES", str(metadata.get("license", "unknown"))),
        "OCI_REF_NAME": os.getenv("OCI_REF_NAME") or os.getenv("GITHUB_REF_NAME", version),
        "OCI_REVISION": os.getenv("OCI_REVISION") or os.getenv("GITHUB_SHA", git_revision()),
        "OCI_SOURCE": source,
        "OCI_TITLE": os.getenv("OCI_TITLE", str(metadata.get("name", PROJECT_NAME))),
        "OCI_URL": os.getenv("OCI_URL", str(urls.get("Homepage", source))),
        "OCI_VERSION": version,
    }


def docker_build_command(dockerfile: Path, image_name: str) -> list[str]:
    docker_target = os.getenv("DOCKER_TARGET")
    cache_from = os.getenv("DOCKER_CACHE_FROM")
    cache_to = os.getenv("DOCKER_CACHE_TO")
    use_buildx = bool(cache_from or cache_to or os.getenv("DOCKER_BUILDX") == "1")

    if use_buildx:
        command = ["docker", "buildx", "build", "--load", "-f", str(dockerfile), "-t", image_name]
        if cache_from:
            command.extend(["--cache-from", cache_from])
        if cache_to:
            command.extend(["--cache-to", cache_to])
    else:
        command = ["docker", "build", "-f", str(dockerfile), "-t", image_name]

    if docker_target:
        command.extend(["--target", docker_target])
    for name, value in docker_build_args().items():
        command.extend(["--build-arg", f"{name}={value}"])
    return command


def sync(session: Session, *groups: str, install_project: bool = True) -> None:
    command = ["uv", "sync", "--active", "--locked"]
    if not install_project:
        command.append("--no-install-project")
    for group in groups:
        command.extend(["--group", group])
    session.run(*command, external=True)


def sync_only_group(session: Session, group: str) -> None:
    session.run("uv", "sync", "--active", "--locked", "--only-group", group, "--no-install-project", external=True)


@nox.session(venv_backend="none")
def dev(session: Session) -> None:
    session.run(
        "uv",
        "sync",
        "--group",
        "dev",
        "--group",
        "format",
        "--group",
        "lint",
        "--group",
        "typing",
        "--group",
        "test",
        external=True,
    )
    session.run("uv", "run", "pre-commit", "install")


@nox.session(python=PYTHON_VERSION)
def format(session: Session) -> None:
    sync_only_group(session, "format")
    session.run("ruff", "check", *SOURCE_PATHS, "--select", "I")
    session.run("ruff", "format", *SOURCE_PATHS, "--check")


@nox.session(python=PYTHON_VERSION)
def lint(session: Session) -> None:
    sync_only_group(session, "lint")
    session.run("ruff", "check", *SOURCE_PATHS)


@nox.session(python=PYTHON_VERSION)
def typing(session: Session) -> None:
    sync(session, "typing", "test")
    session.run("mypy", *SOURCE_PATHS)


@nox.session(python=PYTHON_VERSION)
def test(session: Session) -> None:
    tests_dir = PROJECT_DIR / "tests"
    if not tests_dir.exists():
        session.log("tests directory not found; nothing to run")
        return

    sync(session, "test")
    session.run("pytest", str(tests_dir), "--cov", PROJECT_PACKAGE, "--cov-report", "term-missing", *session.posargs)


@nox.session(python=PYTHON_VERSION)
def integration(session: Session) -> None:
    compose_file = PROJECT_DIR / PROJECT_PACKAGE / "docker-compose.integration.yml"
    if not compose_file.exists():
        session.error(f"Integration compose file not found: {compose_file}")

    sync(session, "test")
    session.run("docker", "compose", "-f", str(compose_file), "up", "-d", external=True)
    try:
        session.run("pytest", "tests/integration", "-m", "integration", *session.posargs)
    finally:
        session.run("docker", "compose", "-f", str(compose_file), "down", "-v", external=True)


@nox.session(python=PYTHON_VERSION)
def audit(session: Session) -> None:
    sync(session, "dev")
    session.run("pip-audit")


@nox.session(python=PYTHON_VERSION)
def docker_build(session: Session) -> None:
    dockerfile = Path(os.getenv("DOCKERFILE", "Dockerfile"))
    if not dockerfile.exists():
        session.error(f"Dockerfile not found: {dockerfile}")

    image_name = docker_image_name()

    command = docker_build_command(dockerfile, image_name)
    command.extend(session.posargs)
    command.append(".")

    session.run(*command, env=DOCKER_BUILD_ENV, external=True)


@nox.session(python=PYTHON_VERSION)
def docker_smoke(session: Session) -> None:
    image_name = docker_image_name()
    command = ["docker", "run", "--rm", image_name]
    command.extend(session.posargs)
    session.run(*command, external=True)
    session.run(
        "docker",
        "run",
        "--rm",
        "--entrypoint",
        "python",
        image_name,
        "-c",
        DOCKER_RUNTIME_SMOKE_SCRIPT,
        external=True,
    )


@nox.session
def image_quality(session: Session) -> None:
    image_name = docker_image_name()

    session.run("bash", "scripts/check-trivyignore-expiry.sh", ".trivyignore.yaml", external=True)

    dockerfile = Path(os.getenv("DOCKERFILE", "Dockerfile"))
    if not dockerfile.exists():
        session.error(f"Dockerfile not found: {dockerfile}")

    build_command = docker_build_command(dockerfile, image_name)
    build_command.append(".")

    session.run(*build_command, env=DOCKER_BUILD_ENV, external=True)
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
        "--timeout",
        TRIVY_TIMEOUT,
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
