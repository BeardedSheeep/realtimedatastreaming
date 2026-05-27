# Copyright (c) 2026 BeardedSheeep

import importlib
import shutil
import subprocess
import sys
from pathlib import Path


def test_package_is_importable() -> None:
    module = importlib.import_module("realtimedatastreaming")

    assert module.__version__ == "0.1.0"


def test_console_script_runs_successfully() -> None:
    command = shutil.which("realtimedatastreaming") or str(Path(sys.executable).with_name("realtimedatastreaming"))
    assert command is not None

    result = subprocess.run([command], capture_output=True, check=False, text=True)

    assert result.returncode == 0
    assert result.stdout == "realtimedatastreaming [development]\n"
