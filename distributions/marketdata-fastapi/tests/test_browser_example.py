"""Run browser recovery regressions without a frontend framework or network."""

import shutil
import subprocess

import pytest


def test_browser_recovery_example():
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is needed for browser example regressions")
    result = subprocess.run(
        [node, "--test", "tests/browser-marketdata.test.mjs"],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stdout + result.stderr
