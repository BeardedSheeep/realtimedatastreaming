import importlib
import json
import shutil
import subprocess


def test_package_is_importable() -> None:
    module = importlib.import_module("realtimedatastreaming")

    assert module.__version__ == "0.1.0"


def test_console_script_runs_successfully() -> None:
    command = shutil.which("realtimedatastreaming")
    assert command is not None

    result = subprocess.run([command], capture_output=True, check=False, text=True)
    logs = [json.loads(line) for line in result.stderr.splitlines()]

    assert result.returncode == 0
    assert result.stdout == "realtimedatastreaming [development]\n"
    assert [log["message"] for log in logs] == ["application_started", "application_finished"]
