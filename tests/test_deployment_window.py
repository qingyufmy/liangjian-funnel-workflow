"""Exercise only the extracted time guard, never the deployment body."""
from pathlib import Path
import shutil
import subprocess

import pytest


@pytest.mark.parametrize("stamp,expected", [
    ("2 0859", 0), ("2 0900", 3), ("2 1130", 3),
    ("2 1300", 3), ("2 1500", 3), ("2 1534", 3),
    ("2 1535", 0), ("6 1000", 0), ("bad", 3),
])
def test_deployment_window(stamp, expected):
    bash = shutil.which("bash")
    if not bash:
        pytest.skip("bash not installed")
    source = (Path(__file__).parents[1] / "deploy.sh").read_text(encoding="utf-8")
    guard = source.split("assert_deployment_window() {", 1)[1].split("\n}\n", 1)[0]
    script = f'date() {{ printf "%s\\n" "{stamp}"; }}\n'
    script += "assert_deployment_window() {" + guard + "\n}\nassert_deployment_window\n"
    result = subprocess.run([bash, "-c", script], capture_output=True, text=True, timeout=10)
    assert result.returncode == expected, result.stdout + result.stderr
