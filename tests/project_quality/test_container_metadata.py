# Copyright (c) 2026 BeardedSheeep

import subprocess
import tomllib
from pathlib import Path

import pytest
import yaml  # type: ignore[import-untyped]

import noxfile

REQUIRED_OCI_LABELS = {
    "org.opencontainers.image.authors",
    "org.opencontainers.image.copyright",
    "org.opencontainers.image.created",
    "org.opencontainers.image.description",
    "org.opencontainers.image.licenses",
    "org.opencontainers.image.ref.name",
    "org.opencontainers.image.revision",
    "org.opencontainers.image.source",
    "org.opencontainers.image.title",
    "org.opencontainers.image.url",
    "org.opencontainers.image.version",
}

REQUIRED_OCI_BUILD_ARGS = {
    "OCI_AUTHORS",
    "OCI_COPYRIGHT",
    "OCI_CREATED",
    "OCI_DESCRIPTION",
    "OCI_LICENSES",
    "OCI_REF_NAME",
    "OCI_REVISION",
    "OCI_SOURCE",
    "OCI_TITLE",
    "OCI_URL",
    "OCI_VERSION",
}

REQUIRED_SECURITY_PACKAGE_PINS = {
    "libcap2": "1:2.66-4+deb12u3+b1",
    "libgnutls30": "3.7.9-2+deb12u7",
    "libssl3": "3.0.19-1~deb12u2",
    "openssl": "3.0.19-1~deb12u2",
}

AIRFLOW_DOCKERFILE = Path("realtimedatastreaming/orchestration/Dockerfile.airflow")
INTEGRATION_COMPOSE_FILE = Path("realtimedatastreaming/docker-compose.integration.yml")


def test_tracked_yaml_and_toml_configs_parse() -> None:
    for path in _tracked_files():
        if path.suffix in {".yaml", ".yml"}:
            yaml.safe_load(path.read_text())
        elif path.name == "pyproject.toml":
            tomllib.loads(path.read_text())


def test_dependabot_is_limited_to_python_dependencies() -> None:
    dependabot = yaml.safe_load(Path(".github/dependabot.yml").read_text())

    ecosystems = {update["package-ecosystem"]: update["directory"] for update in dependabot["updates"]}

    assert ecosystems == {"uv": "/"}


def test_github_workflows_are_registered_with_expected_triggers() -> None:
    cicd = yaml.safe_load(Path(".github/workflows/cicd.yaml").read_text())
    quality = yaml.safe_load(Path(".github/workflows/quality.yaml").read_text())

    cicd_triggers = cicd[True]
    quality_triggers = quality[True]

    assert "pull_request" not in cicd_triggers
    assert "workflow_dispatch" in cicd_triggers
    assert cicd_triggers["push"]["branches"] == ["main"]
    assert "workflow_call" in quality_triggers
    assert quality["jobs"]["actionlint"]["steps"][1]["uses"].startswith("devops-actions/actionlint@")


def test_dockerfile_exposes_required_oci_labels() -> None:
    dockerfile = Path("Dockerfile").read_text()

    for build_arg in REQUIRED_OCI_BUILD_ARGS:
        assert f"ARG {build_arg}=" in dockerfile
        assert f"${{{build_arg}}}" in dockerfile

    for label in REQUIRED_OCI_LABELS:
        assert label in dockerfile


def test_airflow_dockerfile_keeps_airflow_runtime_separate_from_application_image() -> None:
    dockerfile = AIRFLOW_DOCKERFILE.read_text()
    app_dockerfile = Path("Dockerfile").read_text()

    assert "FROM apache/airflow:2.10.5-python3.12" in dockerfile
    assert "USER airflow" in dockerfile
    assert "pip install --no-cache-dir ." in dockerfile
    assert "apache/airflow" not in app_dockerfile


def test_integration_compose_includes_airflow_and_existing_runtime_services() -> None:
    compose = yaml.safe_load(INTEGRATION_COMPOSE_FILE.read_text())
    services = compose["services"]

    assert {
        "postgres",
        "kafka",
        "schema-registry",
        "airflow-init",
        "airflow-webserver",
        "airflow-scheduler",
    }.issubset(services)
    assert services["postgres"]["profiles"] == ["airflow"]
    assert services["airflow-init"]["profiles"] == ["airflow"]
    assert services["airflow-webserver"]["profiles"] == ["airflow"]
    assert services["airflow-scheduler"]["profiles"] == ["airflow"]
    assert services["airflow-webserver"]["ports"] == ["8080:8080"]
    assert services["kafka"]["ports"] == ["19092:19092"]
    assert services["schema-registry"]["ports"] == ["18081:8081"]


def test_integration_compose_configures_airflow_dag_folder_and_runtime_environment() -> None:
    compose = yaml.safe_load(INTEGRATION_COMPOSE_FILE.read_text())
    airflow_common = compose["x-airflow-common"]
    airflow_environment = airflow_common["environment"]

    assert airflow_common["build"] == {
        "context": "..",
        "dockerfile": "realtimedatastreaming/orchestration/Dockerfile.airflow",
    }
    assert airflow_environment["AIRFLOW__CORE__DAGS_FOLDER"] == (
        "/opt/airflow/project/realtimedatastreaming/orchestration"
    )
    assert airflow_environment["PYTHONPATH"] == "/opt/airflow/project"
    assert airflow_environment["KAFKA_BOOTSTRAP_SERVERS"] == "kafka:29092"
    assert airflow_environment["SCHEMA_REGISTRY_URL"] == "http://schema-registry:8081"
    assert airflow_environment["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] == (
        "postgresql+psycopg2://${AIRFLOW_POSTGRES_USER:-airflow}:"
        "${AIRFLOW_POSTGRES_PASSWORD:-local-dev-only-airflow-postgres-password}@postgres/"
        "${AIRFLOW_POSTGRES_DB:-airflow}"
    )
    assert airflow_environment["AIRFLOW__WEBSERVER__SECRET_KEY"] == (
        "${AIRFLOW_WEBSERVER_SECRET_KEY:-local-dev-only-airflow-webserver-secret-key}"
    )
    assert "KAFKA_SASL_PASSWORD" not in airflow_environment
    assert "PII_PSEUDONYMIZATION_SALT" not in airflow_environment


def test_integration_compose_checks_kafka_health_on_internal_listener() -> None:
    compose = yaml.safe_load(INTEGRATION_COMPOSE_FILE.read_text())
    kafka_healthcheck = compose["services"]["kafka"]["healthcheck"]["test"]

    assert kafka_healthcheck == ["CMD", "kafka-topics", "--bootstrap-server", "kafka:29092", "--list"]


def test_integration_compose_reads_local_airflow_credentials_from_environment() -> None:
    compose_text = INTEGRATION_COMPOSE_FILE.read_text()

    assert "POSTGRES_PASSWORD: ${AIRFLOW_POSTGRES_PASSWORD:-local-dev-only-airflow-postgres-password}" in compose_text
    assert '--password "${AIRFLOW_ADMIN_PASSWORD:-local-dev-only-airflow-admin-password}"' in compose_text
    assert "local-airflow-development-secret-key" not in compose_text
    assert "POSTGRES_PASSWORD: airflow" not in compose_text
    assert "--password airflow" not in compose_text


def test_nox_airflow_integration_commands_target_airflow_profile() -> None:
    compose_file = Path("realtimedatastreaming/docker-compose.integration.yml")

    assert noxfile.airflow_webserver_health_command(compose_file) == [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "--profile",
        "airflow",
        "exec",
        "-T",
        "airflow-webserver",
        "curl",
        "-fsS",
        "http://localhost:8080/health",
    ]
    assert noxfile.airflow_cli_command(compose_file, "dags", "list", "--output", "json") == [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "--profile",
        "airflow",
        "exec",
        "-T",
        "airflow-scheduler",
        "airflow",
        "dags",
        "list",
        "--output",
        "json",
    ]


def test_nox_kafka_integration_commands_target_default_integration_stack() -> None:
    compose_file = Path("realtimedatastreaming/docker-compose.integration.yml")

    assert noxfile.kafka_topics_command(compose_file) == [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        "kafka",
        "kafka-topics",
        "--bootstrap-server",
        "kafka:29092",
        "--list",
    ]
    assert noxfile.schema_registry_subjects_command(compose_file) == [
        "docker",
        "compose",
        "-f",
        str(compose_file),
        "exec",
        "-T",
        "schema-registry",
        "curl",
        "-fsS",
        "http://localhost:8081/subjects",
    ]


def test_nox_airflow_json_parser_tolerates_cli_warnings() -> None:
    assert noxfile._parse_json_from_command_output('[{"dag_id":"demo"}]') == [{"dag_id": "demo"}]
    assert noxfile._parse_json_from_command_output('WARNING: deprecated\n[{"dag_id":"demo"}]') == [{"dag_id": "demo"}]
    assert noxfile._parse_json_from_command_output('[{"dag_id":"demo"}]\nWARNING: deprecated') == [{"dag_id": "demo"}]


def test_nox_airflow_json_parser_rejects_output_without_json() -> None:
    with pytest.raises(ValueError, match="did not contain"):
        noxfile._parse_json_from_command_output("WARNING: deprecated\nno json here")


def test_nox_integration_session_aggregates_runtime_integration_sessions() -> None:
    noxfile_text = Path("noxfile.py").read_text()
    readme = Path("README.md").read_text()

    assert 'session.notify("kafka-integration")' in noxfile_text
    assert 'session.notify("spark-integration")' in noxfile_text
    assert 'session.notify("airflow-integration")' in noxfile_text
    assert '@nox.session(name="kafka-integration", python=PYTHON_VERSION)' in noxfile_text
    assert '@nox.session(name="spark-integration", venv_backend="none")' in noxfile_text
    assert '@nox.session(name="airflow-integration", venv_backend="none")' in noxfile_text
    assert "uv run nox -s kafka-integration" in readme
    assert "uv run nox -s spark-integration" in readme


def test_kafka_integration_session_is_documented_as_runtime_contract_test() -> None:
    readme = Path("README.md").read_text()

    assert "Use `kafka-integration` to start Kafka and Schema Registry" in readme
    assert "produce/consume and Schema Registry contract tests" in readme


def test_nox_builds_plain_docker_command_with_oci_metadata(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("DOCKER_BUILDX", raising=False)
    monkeypatch.delenv("DOCKER_CACHE_FROM", raising=False)
    monkeypatch.delenv("DOCKER_CACHE_TO", raising=False)
    monkeypatch.delenv("DOCKER_TARGET", raising=False)
    monkeypatch.setenv("OCI_CREATED", "2026-01-01T00:00:00+00:00")
    monkeypatch.setenv("GITHUB_SHA", "abc123")
    monkeypatch.setenv("GITHUB_REF_NAME", "main")

    command = noxfile.docker_build_command(Path("Dockerfile"), "example/app:test")

    assert command[:6] == ["docker", "build", "-f", "Dockerfile", "-t", "example/app:test"]
    for build_arg in REQUIRED_OCI_BUILD_ARGS:
        assert "--build-arg" in command
        assert any(argument.startswith(f"{build_arg}=") for argument in command)


def test_nox_builds_buildx_command_with_registry_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DOCKER_CACHE_FROM", "type=registry,ref=example/app:buildcache")
    monkeypatch.setenv("DOCKER_CACHE_TO", "type=registry,ref=example/app:buildcache,mode=max,ignore-error=true")
    monkeypatch.setenv("DOCKER_TARGET", "runtime")
    monkeypatch.setenv("OCI_CREATED", "2026-01-01T00:00:00+00:00")

    command = noxfile.docker_build_command(Path("Dockerfile"), "example/app:test")

    assert command[:7] == ["docker", "buildx", "build", "--load", "-f", "Dockerfile", "-t"]
    assert "example/app:test" in command
    assert "--cache-from" in command
    assert "type=registry,ref=example/app:buildcache" in command
    assert "--cache-to" in command
    assert "type=registry,ref=example/app:buildcache,mode=max,ignore-error=true" in command
    assert "--target" in command
    assert "runtime" in command


def test_cicd_generates_and_uploads_image_sbom() -> None:
    workflow = yaml.safe_load(Path(".github/workflows/cicd.yaml").read_text())
    trivy_image_steps = workflow["jobs"]["trivy_image"]["steps"]

    sbom_step = next(step for step in trivy_image_steps if step.get("name") == "Generate image SBOM")
    upload_step = next(step for step in trivy_image_steps if step.get("name") == "Upload image SBOM")

    assert "aquasec/trivy:0.70.0 image" in sbom_step["run"]
    assert "--format cyclonedx" in sbom_step["run"]
    assert "--output /workspace/sbom.cdx.json" in sbom_step["run"]
    assert upload_step["with"]["name"] == "image-sbom-${{ github.sha }}"
    assert "sbom.cdx.json" in upload_step["with"]["path"]
    assert "sbom-metadata.json" in upload_step["with"]["path"]
    for metadata_field in ("git_sha", "image_ref", "image_id", "sbom_format", "generated_at"):
        assert metadata_field in sbom_step["run"]


def test_dockerignore_excludes_local_and_generated_artifacts() -> None:
    dockerignore_entries = set(Path(".dockerignore").read_text().splitlines())

    assert {
        ".coverage",
        ".DS_Store",
        "*.db",
        "*.sqlite",
        "coverage.xml",
        "image.tar",
        "security-reports",
    }.issubset(dockerignore_entries)


def test_dockerfile_uses_only_reproducible_os_package_upgrades() -> None:
    dockerfile = Path("Dockerfile").read_text()

    assert "apt-get upgrade" not in dockerfile
    assert "apt-get dist-upgrade" not in dockerfile
    assert "apt-get install -y --no-install-recommends --only-upgrade" in dockerfile
    for package_name, package_version in REQUIRED_SECURITY_PACKAGE_PINS.items():
        assert f"{package_name}={package_version}" in dockerfile


def _tracked_files() -> list[Path]:
    output = subprocess.check_output(["git", "ls-files"], text=True)
    return [Path(path) for path in output.splitlines()]
