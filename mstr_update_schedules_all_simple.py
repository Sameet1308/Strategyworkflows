#!/usr/bin/env python3
"""mstr_update_schedules_all_simple.py

SIMPLE script to:
  1) Login to MicroStrategy/Strategy (MCE) REST
  2) Get list of ALL schedules accessible to the user
  3) For each schedule:
       - GET full schedule definition
       - Update startDate and endDate (or another end-field)
       - PUT schedule back
  4) Write a JSON report

Notes (important):
  - MicroStrategy schedule payload uses *startDate* in REST. "End date" field name may vary by tenant/version.
    If your server doesn't return an end-date field named 'endDate', set END_FIELD below
    to whatever your GET /api/schedules/{id} returns (e.g., expirationDate).
  - This script updates schedules ONE BY ONE (iterating), which is safer and easier to troubleshoot.

Endpoints:
  POST /api/auth/login
  GET  /api/v2/schedules   (preferred; fallback to /api/schedules)
  GET  /api/schedules/{id}
  PUT  /api/schedules/{id}
  POST /api/auth/logout
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests
import urllib3


# =========================
# CONFIG (edit these)
# =========================
@dataclass(frozen=True)
class Config:
    base_url: str = "https://YOUR_HOST/MicroStrategyLibrary"  # no /api at end
    username: str = "YOUR_USERNAME"
    password: str = "YOUR_PASSWORD"
    login_mode: int = 1
    verify_ssl: bool = False  # as requested (verify=False)


# What you want to set for ALL schedules (format must match what your server accepts)
NEW_START_DATE = "2026-02-01T00:00:00.000Z"
NEW_END_DATE   = "2026-12-31T23:59:59.000Z"

# End-date field name in your tenant's schedule JSON.
# If your GET /api/schedules/{id} returns "endDate" you're fine.
# If it returns something else (common), set it here, e.g. "expirationDate".
END_FIELD = "endDate"

# Safety
DRY_RUN = True   # set False to actually update
PAGE_SIZE = 1000 # paging size for /v2/schedules
REPORT_FILE = "schedule_update_all_report.json"


# =========================
# REST client
# =========================
class StrategyRest:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.api_base = cfg.base_url.rstrip("/") + "/api"
        self.session = requests.Session()
        self.token: Optional[str] = None

        if not cfg.verify_ssl:
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def _req(
        self,
        method: str,
        path: str,
        *,
        headers: Optional[Dict[str, str]] = None,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        timeout: int = 60,
        retries: int = 3,
    ) -> requests.Response:
        url = self.api_base + path
        h = {"Accept": "application/json"}
        if self.token:
            h["X-MSTR-AuthToken"] = self.token
        if headers:
            h.update(headers)

        last_err = None
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.request(
                    method=method,
                    url=url,
                    headers=h,
                    params=params,
                    json=json_body,
                    verify=self.cfg.verify_ssl,
                    timeout=timeout,
                )
                if resp.status_code in (429, 502, 503, 504):
                    time.sleep(1.5 * attempt)
                    continue
                return resp
            except Exception as e:
                last_err = e
                time.sleep(1.5 * attempt)

        raise RuntimeError(f"HTTP failed after retries: {method} {url}. Last error: {last_err}")

    def login(self) -> None:
        resp = self._req(
            "POST",
            "/auth/login",
            headers={"Content-Type": "application/json"},
            json_body={
                "username": self.cfg.username,
                "password": self.cfg.password,
                "loginMode": self.cfg.login_mode,
            },
        )
        if resp.status_code != 204:
            raise RuntimeError(f"Login failed ({resp.status_code}): {resp.text}")

        self.token = resp.headers.get("X-MSTR-AuthToken") or resp.headers.get("x-mstr-authtoken")
        if not self.token:
            raise RuntimeError("Login succeeded but X-MSTR-AuthToken header not found.")

    def logout(self) -> None:
        if not self.token:
            return
        try:
            self._req("POST", "/auth/logout")
        except Exception:
            pass

    def list_schedules(self, page_size: int = 1000) -> List[Dict[str, Any]]:
        # Preferred: /v2/schedules (paged). Fallback: /schedules.
        schedules: List[Dict[str, Any]] = []

        offset = 0
        while True:
            resp = self._req("GET", "/v2/schedules", params={"limit": page_size, "offset": offset})
            if resp.status_code == 404:
                schedules = []
                break
            if resp.status_code != 200:
                raise RuntimeError(f"GET /v2/schedules failed ({resp.status_code}): {resp.text}")

            payload = resp.json() or {}
            batch = payload.get("schedules", [])
            schedules.extend(batch)

            if len(batch) < page_size:
                return schedules
            offset += page_size

        # Fallback:
        resp = self._req("GET", "/schedules")
        if resp.status_code != 200:
            raise RuntimeError(f"GET /schedules failed ({resp.status_code}): {resp.text}")
        data = resp.json()
        if isinstance(data, dict) and "schedules" in data:
            return data["schedules"]
        if isinstance(data, list):
            return data
        return []

    def get_schedule(self, schedule_id: str) -> Dict[str, Any]:
        resp = self._req("GET", f"/schedules/{schedule_id}")
        if resp.status_code != 200:
            raise RuntimeError(f"GET /schedules/{schedule_id} failed ({resp.status_code}): {resp.text}")
        return resp.json()

    def put_schedule(self, schedule_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._req(
            "PUT",
            f"/schedules/{schedule_id}",
            headers={"Content-Type": "application/json"},
            json_body=payload,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"PUT /schedules/{schedule_id} failed ({resp.status_code}): {resp.text}")
        return resp.json()


def sanitize_for_put(schedule_def: Dict[str, Any]) -> Dict[str, Any]:
    # Remove common server-generated/read-only fields (varies by tenant; safe to drop if present)
    drop = {
        "id",
        "dateCreated",
        "dateModified",
        "owner",
        "creator",
        "lastModifiedBy",
        "links",
        "updater",
    }
    return {k: v for k, v in schedule_def.items() if k not in drop}


def main():
    cfg = Config()
    api = StrategyRest(cfg)

    report = {"dry_run": DRY_RUN, "updated": [], "failed": []}

    try:
        api.login()

        schedules = api.list_schedules(page_size=PAGE_SIZE)
        print(f"Schedules accessible to this user: {len(schedules)}")

        for s in schedules:
            sid = s.get("id")
            name = s.get("name")

            if not sid:
                continue

            try:
                full = api.get_schedule(sid)
                editable = sanitize_for_put(full)

                # Update start/end date (bulk)
                editable["startDate"] = NEW_START_DATE
                if END_FIELD not in editable:
                    # If your tenant doesn't have this key, you'll see failures or no-op.
                    # Fix by setting END_FIELD to the correct key from GET output.
                    pass
                editable[END_FIELD] = NEW_END_DATE

                if DRY_RUN:
                    report["updated"].append({"id": sid, "name": name, "dry_run": True})
                    continue

                api.put_schedule(sid, editable)
                report["updated"].append({"id": sid, "name": name, "dry_run": False})

                print(f"UPDATED: {name} | {sid}")

            except Exception as e:
                report["failed"].append({"id": sid, "name": name, "error": str(e)})
                print(f"FAILED: {name} | {sid} -> {e}")

        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print(f"Report saved: {REPORT_FILE}")
        print(f"Updated: {len(report['updated'])} | Failed: {len(report['failed'])}")

    finally:
        api.logout()


if __name__ == "__main__":
    main()
