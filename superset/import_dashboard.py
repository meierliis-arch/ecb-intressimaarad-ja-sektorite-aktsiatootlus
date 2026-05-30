#!/usr/bin/env python3
"""
Superset dashboard auto-import script.
Runs as a one-shot Docker service after Superset is healthy.
Authenticates via the REST API, imports dashboard_export.zip,
and injects the real database password so the connection works immediately.
"""
import json
import os
import sys
import time

import requests

SUPERSET_URL = os.environ.get("SUPERSET_URL", "http://superset:8088")
ADMIN_USER   = os.environ["ADMIN_USERNAME"]
ADMIN_PASS   = os.environ["ADMIN_PASSWORD"]
DB_PASS      = os.environ["ANALYTICS_DB_PASSWORD"]
DASHBOARD_ZIP = "/app/dashboard_export.zip"

# The key must match the database YAML path inside the zip
# (path relative to the export root folder, not the full zip path)
DB_YAML_KEY = "databases/PostgreSQL.yaml"


def wait_for_superset(max_wait: int = 180) -> None:
    print(f"Waiting for Superset at {SUPERSET_URL} ...")
    waited = 0
    while waited < max_wait:
        try:
            r = requests.get(f"{SUPERSET_URL}/health", timeout=5)
            if r.status_code == 200:
                print("Superset is ready.")
                return
        except requests.exceptions.ConnectionError:
            pass
        time.sleep(5)
        waited += 5
    sys.exit("ERROR: Superset did not become ready within the timeout.")


def get_token() -> str:
    r = requests.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={
            "username": ADMIN_USER,
            "password": ADMIN_PASS,
            "provider": "db",
            "refresh": True,
        },
        timeout=15,
    )
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        sys.exit(f"ERROR: no access_token in login response: {r.text}")
    print("Authenticated with Superset.")
    return token


def import_assets(token: str) -> None:
    headers = {"Authorization": f"Bearer {token}"}
    passwords = json.dumps({DB_YAML_KEY: DB_PASS})

    with open(DASHBOARD_ZIP, "rb") as f:
        r = requests.post(
            f"{SUPERSET_URL}/api/v1/assets/import/",
            headers=headers,
            files={"bundle": ("dashboard_export.zip", f, "application/zip")},
            data={"passwords": passwords, "overwrite": "true"},
            timeout=60,
        )

    if r.status_code in (200, 201):
        print("Dashboard imported successfully.")
    else:
        # Pretty-print the error so it's visible in docker compose logs
        print(f"ERROR: import returned HTTP {r.status_code}")
        try:
            print(json.dumps(r.json(), indent=2))
        except Exception:
            print(r.text)
        sys.exit(1)


if __name__ == "__main__":
    wait_for_superset()
    token = get_token()
    import_assets(token)
