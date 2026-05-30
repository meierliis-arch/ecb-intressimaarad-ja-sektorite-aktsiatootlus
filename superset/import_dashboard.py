#!/usr/bin/env python3
"""
Superset dashboard auto-import script.
Runs as a one-shot Docker service after Superset is healthy.

Superset 6.0 removed /api/v1/assets/import/ — we use individual endpoints:
  1. POST /api/v1/database/   — create DB connection with real password
  2. POST /api/v1/dashboard/import/ — import dashboard zip (charts + datasets included)

CSRF token is required for all mutating requests even with Bearer auth in Superset 6.0.
"""
import json
import os
import sys
import time

import requests

SUPERSET_URL  = os.environ.get("SUPERSET_URL", "http://superset:8088")
ADMIN_USER    = os.environ["ADMIN_USERNAME"]
ADMIN_PASS    = os.environ["ADMIN_PASSWORD"]
DB_PASS       = os.environ["ANALYTICS_DB_PASSWORD"]
DASHBOARD_ZIP = "/app/dashboard_export.zip"

DB_NAME = "Our_Database_Display_Name"
DB_URI  = f"postgresql+psycopg2://praktikum:{DB_PASS}@ecb-analytics-db:5432/praktikum"


# ---------------------------------------------------------------------------
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


def authenticate(session: requests.Session) -> tuple[str, str]:
    """Returns (bearer_token, csrf_token). Session cookie is preserved automatically."""
    r = session.post(
        f"{SUPERSET_URL}/api/v1/security/login",
        json={"username": ADMIN_USER, "password": ADMIN_PASS,
              "provider": "db", "refresh": True},
        timeout=15,
    )
    r.raise_for_status()
    bearer = r.json().get("access_token")
    if not bearer:
        sys.exit(f"ERROR: no access_token: {r.text}")

    # CSRF token must be fetched with the same session so the cookie is tied to it
    r2 = session.get(
        f"{SUPERSET_URL}/api/v1/security/csrf_token/",
        headers={"Authorization": f"Bearer {bearer}"},
        timeout=15,
    )
    r2.raise_for_status()
    csrf = r2.json().get("result")
    if not csrf:
        sys.exit(f"ERROR: no csrf_token: {r2.text}")

    print("Authenticated (bearer + csrf + session cookie obtained).")
    return bearer, csrf


def api_headers(bearer: str, csrf: str) -> dict:
    return {
        "Authorization": f"Bearer {bearer}",
        "X-CSRFToken": csrf,
    }


# ---------------------------------------------------------------------------
def ensure_database(session: requests.Session, bearer: str, csrf: str) -> None:
    """Create the analytics DB connection, or update it if it already exists."""
    headers = api_headers(bearer, csrf)

    r = session.get(f"{SUPERSET_URL}/api/v1/database/",
                    headers={"Authorization": f"Bearer {bearer}"}, timeout=15)
    r.raise_for_status()
    databases = r.json().get("result", [])
    existing = next((d for d in databases if d["database_name"] == DB_NAME), None)

    if existing:
        db_id = existing["id"]
        print(f"Database '{DB_NAME}' exists (id={db_id}), updating password ...")
        r = session.put(
            f"{SUPERSET_URL}/api/v1/database/{db_id}",
            headers={**headers, "Content-Type": "application/json"},
            json={"sqlalchemy_uri": DB_URI},
            timeout=15,
        )
    else:
        print(f"Creating database connection '{DB_NAME}' ...")
        r = session.post(
            f"{SUPERSET_URL}/api/v1/database/",
            headers={**headers, "Content-Type": "application/json"},
            json={
                "database_name": DB_NAME,
                "sqlalchemy_uri": DB_URI,
                "expose_in_sqllab": True,
                "extra": json.dumps({"allows_virtual_table_explore": True}),
            },
            timeout=15,
        )

    print(f"  HTTP {r.status_code}")
    if r.status_code not in (200, 201):
        print(f"  ERROR: {r.text[:400]}")
        sys.exit(1)
    print("  Done.")


# ---------------------------------------------------------------------------
def import_dashboard(session: requests.Session, bearer: str, csrf: str) -> None:
    """Import dashboard zip via /api/v1/dashboard/import/."""
    headers = api_headers(bearer, csrf)
    print("Importing dashboard zip ...")
    with open(DASHBOARD_ZIP, "rb") as f:
        r = session.post(
            f"{SUPERSET_URL}/api/v1/dashboard/import/",
            headers=headers,
            files={"formData": ("dashboard_export.zip", f, "application/zip")},
            data={"overwrite": "true"},
            timeout=60,
        )
    print(f"  HTTP {r.status_code}")
    try:
        body = r.json()
        print(f"  Response: {json.dumps(body)}")
    except Exception:
        snippet = r.text[:300].replace("\n", " ")
        print(f"  Response (raw): {snippet}")

    if r.status_code not in (200, 201):
        sys.exit("ERROR: dashboard import failed.")
    print("  Done.")


# ---------------------------------------------------------------------------
def verify(session: requests.Session, bearer: str) -> None:
    headers = {"Authorization": f"Bearer {bearer}"}
    r = session.get(f"{SUPERSET_URL}/api/v1/dashboard/", headers=headers, timeout=15)
    dashboards = r.json().get("result", [])
    print(f"\nDashboards in Superset ({len(dashboards)}):")
    for d in dashboards:
        print(f"  id={d['id']}  '{d['dashboard_title']}'  published={d['published']}")

    r = session.get(f"{SUPERSET_URL}/api/v1/database/", headers=headers, timeout=15)
    dbs = r.json().get("result", [])
    print(f"Database connections ({len(dbs)}):")
    for d in dbs:
        print(f"  id={d['id']}  '{d['database_name']}'")


# ---------------------------------------------------------------------------
if __name__ == "__main__":
    wait_for_superset()
    session = requests.Session()
    bearer, csrf = authenticate(session)
    ensure_database(session, bearer, csrf)
    import_dashboard(session, bearer, csrf)
    verify(session, bearer)
