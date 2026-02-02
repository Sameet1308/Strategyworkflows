#!/usr/bin/env python3
"""mstr_update_schedule_by_id.py

Update ONE MicroStrategy/Strategy schedule's start/end date by Schedule ID using REST API.

Endpoints used (Library base URL assumed):
  - POST  /api/auth/login
  - GET   /api/schedules/{id}
  - PUT   /api/schedules/{id}
  - POST  /api/auth/logout

Safe update pattern:
  GET full schedule JSON -> modify date fields -> PUT full JSON back.

SSL:
  Set VERIFY_SSL = False to use verify=False and suppress urllib3 warnings.
"""

import json
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional, List, Tuple

import requests
import urllib3


# =========================
# CONFIG (edit these)
# =========================
@dataclass(frozen=True)
class Config:
    base_url: str = "https://YOUR_HOST/MicroStrategyLibrary"  # MicroStrategyLibrary root (no /api)
    username: str = "YOUR_USERNAME"
    password: str = "YOUR_PASSWORD"
    login_mode: int = 1
    verify_ssl: bool = False  # SSL verify false as you asked


# =========================
# REST Client
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

    def get_schedule(self, schedule_id: str) -> Dict[str, Any]:
        resp = self._req("GET", f"/schedules/{schedule_id}")
        if resp.status_code != 200:
            raise RuntimeError(f"GET /schedules/{schedule_id} failed ({resp.status_code}): {resp.text}")
        return resp.json()

    def update_schedule(self, schedule_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        resp = self._req(
            "PUT",
            f"/schedules/{schedule_id}",
            headers={"Content-Type": "application/json"},
            json_body=payload,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"PUT /schedules/{schedule_id} failed ({resp.status_code}): {resp.text}")
        return resp.json()


# =========================
# Update helpers
# =========================
def sanitize_for_put(schedule_def: Dict[str, Any]) -> Dict[str, Any]:
    """Remove common read-only keys that can break PUT in some tenants."""
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

def detect_end_field(schedule_def: Dict[str, Any]) -> Optional[str]:
    """End-date key varies by version/tenant. We only set if known key exists."""
    candidates = ["endDate", "expirationDate", "expireDate", "end_date", "expiration_date"]
    for k in candidates:
        if k in schedule_def:
            return k
    return None

def apply_dates(
    schedule_def: Dict[str, Any],
    new_start: str,
    new_end: Optional[str],
    end_field_override: Optional[str] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []

    schedule_def["startDate"] = new_start

    if new_end is not None:
        end_key = end_field_override or detect_end_field(schedule_def)
        if not end_key:
            warnings.append(
                "End-date field not found in schedule JSON. "
                "End date was NOT updated. Use end_field_override to force a key if your tenant supports it."
            )
        else:
            schedule_def[end_key] = new_end

    return schedule_def, warnings


# =========================
# Main
# =========================
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Update a single MicroStrategy schedule (by ID) start/end dates.")
    parser.add_argument("--schedule-id", required=True, help="Schedule ID to update.")
    parser.add_argument("--start", required=True, help="New startDate (use the format your server expects).")
    parser.add_argument("--end", help="New end date (optional).")
    parser.add_argument("--end-field", help="Force end-date field key (e.g., endDate / expirationDate).")
    parser.add_argument("--dry-run", action="store_true", help="Do not PUT; only show what would change.")
    parser.add_argument("--show-json", action="store_true", help="Print current schedule JSON and exit.")
    args = parser.parse_args()

    cfg = Config()
    client = StrategyRest(cfg)

    try:
        client.login()
        current = client.get_schedule(args.schedule_id)

        if args.show_json:
            print(json.dumps(current, indent=2))
            return

        editable = sanitize_for_put(current)
        updated, warns = apply_dates(editable, args.start, args.end, args.end_field)

        if args.dry_run:
            print("DRY RUN: would update schedule to:")
            print(json.dumps(updated, indent=2))
            if warns:
                print("\nWARNINGS:")
                for w in warns:
                    print(f"- {w}")
            return

        resp = client.update_schedule(args.schedule_id, updated)
        print(f"UPDATED schedule: {resp.get('name', '(name not returned)')} | id={args.schedule_id}")
        if warns:
            print("WARNINGS:")
            for w in warns:
                print(f"- {w}")

    finally:
        client.logout()


if __name__ == "__main__":
    main()
