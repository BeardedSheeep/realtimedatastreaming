import tomllib
from pathlib import Path


def test_coverage_threshold_is_enforced_in_pyproject() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text())

    assert pyproject["tool"]["coverage"]["report"]["fail_under"] == 80
    assert pyproject["tool"]["coverage"]["run"]["branch"] is True
    assert pyproject["tool"]["coverage"]["run"]["source"] == ["template_doc"]


def test_nox_test_session_runs_pytest_with_coverage() -> None:
    noxfile = Path("noxfile.py").read_text()

    assert "nox.options.reuse_existing_virtualenvs = False" in noxfile
    assert 'PYTHON_VERSION = "3.12"' in noxfile
    assert "@nox.session(python=PYTHON_VERSION)" in noxfile
    assert '"pytest"' in noxfile
    assert '"--cov"' in noxfile
    assert '"--cov-report"' in noxfile
    assert '"term-missing"' in noxfile


def test_python_support_is_limited_to_python_312() -> None:
    pyproject = tomllib.loads(Path("pyproject.toml").read_text())
    quality_workflow = Path(".github/workflows/quality.yaml").read_text()

    assert pyproject["project"]["requires-python"] == ">=3.12,<3.13"
    assert 'python-version: "3.12"' in quality_workflow
    assert "matrix.python-version" not in quality_workflow
