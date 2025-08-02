import os
import pytest
import subprocess
import sys

def get_running_compose_services():
    try:
        # list only services that are currently running
        result = subprocess.run(
            ["docker", "compose", "ps", "--services", "--filter", "status=running"],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        services = [s.strip() for s in result.stdout.splitlines() if s.strip()]
        return services
    except Exception as e:
        print(f"[debug] failed to query docker compose services: {e}", file=sys.stderr)
        return []

def is_compose_up_enough():
    running = get_running_compose_services()
    if not running:
        # nothing running: treat as compose not up
        return False

    # optional: user can declare required services via env var, comma separated
    required = os.getenv("REQUIRED_COMPOSE_SERVICES")
    if required:
        required_list = [s.strip() for s in required.split(",") if s.strip()]
        missing = [s for s in required_list if s not in running]
        if missing:
            print(f"[debug] compose running services {running}, missing required {missing}", file=sys.stderr)
            return False

    # otherwise at least one service is running, consider sufficient
    print(f"[debug] compose running services: {running}", file=sys.stderr)
    return True

def pytest_collection_modifyitems(config, items):
    up = is_compose_up_enough()
    for item in items:
        if "integration" in item.keywords and not up:
            item.add_marker(
                pytest.mark.skip(
                    reason=(
                        "docker compose stack not detected as running. "
                        "start it with `docker compose up` or set REQUIRED_COMPOSE_SERVICES if you need specific services."
                    )
                )
            )
            print(f"[debug] skipping {item.name} because compose is not sufficiently up", file=sys.stderr)
