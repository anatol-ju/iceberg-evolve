# conftest.py
import os
import pytest
import sys
import warnings

class ContainerSkipWarning(UserWarning):
    pass

_warned_about_skipping = False

def in_container() -> bool:
    if os.path.exists("/.dockerenv"):
        return True
    try:
        with open("/proc/1/cgroup", encoding="utf-8") as f:
            content = f.read()
        if any(kw in content for kw in ("docker", "kubepods", "containerd")):
            return True
    except Exception:
        pass
    return False

def should_run_integration() -> bool:
    # allow forcing integration tests even outside container
    if os.getenv("FORCE_INTEGRATION") == "1":
        return True
    return in_container()

def pytest_collection_modifyitems(config, items):
    global _warned_about_skipping
    run_allowed = should_run_integration()
    for item in items:
        if "integration" in item.keywords and not run_allowed:
            item.add_marker(
                pytest.mark.skip(
                    reason=(
                        "integration test requires containerized environment; "
                        "not detected. set FORCE_INTEGRATION=1 to override."
                    )
                )
            )
            if not _warned_about_skipping:
                warnings.warn(
                    "some integration-marked tests are being skipped because no containerized environment was detected; "
                    "set FORCE_INTEGRATION=1 to override.",
                    category=ContainerSkipWarning,
                    stacklevel=1,
                )
                # also echo to stderr so it's salient in logs
                print(
                    "[warning] integration tests skipped: no container detected; "
                    "set FORCE_INTEGRATION=1 to override.",
                    file=sys.stderr,
                )
                _warned_about_skipping = True
