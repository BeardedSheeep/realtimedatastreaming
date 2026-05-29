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
    monkeypatch.setenv("DOCKER_CACHE_TO", "type=registry,ref=example/app:buildcache,mode=max")
    monkeypatch.setenv("DOCKER_TARGET", "runtime")
    monkeypatch.setenv("OCI_CREATED", "2026-01-01T00:00:00+00:00")

    command = noxfile.docker_build_command(Path("Dockerfile"), "example/app:test")

    assert command[:7] == ["docker", "buildx", "build", "--load", "-f", "Dockerfile", "-t"]
    assert "example/app:test" in command
    assert "--cache-from" in command
    assert "type=registry,ref=example/app:buildcache" in command
    assert "--cache-to" in command
    assert "type=registry,ref=example/app:buildcache,mode=max" in command
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
