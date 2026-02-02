#!/usr/bin/env python3
"""mstr_update_schedules_bulk.py

List schedules and bulk-update schedule start/end dates using REST API.
Selection modes:
  - --schedule-id <id>          (single)
  - --name-exact "Name"         (bulk exact match)
  - --name-contains "text"      (bulk substring match)
  - --name-regex "regex"        (bulk regex match)
  - --all                       (bulk all accessible)

Endpoints used:
  - POST  /api/auth/login
  - GET   /api/v2/schedules  (preferred; falls back to /api/schedules if v2 not available)
  - GET   /api/schedules/{id}
  - PUT   /api/schedules/{id}
  - POST  /api/auth/logout

Safe update pattern:
  GET full schedule JSON -> modify date fields -> PUT full JSON back.

SSL:
  Set VERIFY_SSL = False to use verify=False and suppress urllib3 warnings.
"""

import argparse
import json
import re
import sys
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

    def list_schedules(self, page_size: int = 1000) -> List[Dict[str, Any]]:
        """Prefer /v2/schedules; fallback to /schedules if v2 not supported."""
        schedules: List[Dict[str, Any]] = []

        # Try v2
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

        # Fallback to v1
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
    candidates = ["endDate", "expirationDate", "expireDate", "end_date", "expiration_date"]
    for k in candidates:
        if k in schedule_def:
            return k
    return None

def apply_dates(
    schedule_def: Dict[str, Any],
    new_start: str,
    new_end: Optional[str],
    end_field_override: Optional[str],
    fail_if_end_missing: bool,
) -> Tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []
    schedule_def["startDate"] = new_start

    if new_end is not None:
        end_key = end_field_override or detect_end_field(schedule_def)
        if not end_key:
            msg = (
                "End-date field not found in schedule JSON. "
                "Use --end-field to force a key if your tenant supports it."
            )
            if fail_if_end_missing:
                raise RuntimeError(msg)
            warnings.append(msg)
        else:
            schedule_def[end_key] = new_end

    return schedule_def, warnings


def select_ids(
    schedules: List[Dict[str, Any]],
    schedule_id: Optional[str],
    name_exact: Optional[str],
    name_contains: Optional[str],
    name_regex: Optional[str],
    all_flag: bool,
) -> List[str]:
    if schedule_id:
        return [schedule_id]

    if all_flag:
        return [s["id"] for s in schedules if s.get("id")]

    ids: List[str] = []
    if name_exact:
        ids = [s["id"] for s in schedules if s.get("name") == name_exact and s.get("id")]
    elif name_contains:
        needle = name_contains.lower()
        ids = [s["id"] for s in schedules if needle in (s.get("name") or "").lower() and s.get("id")]
    elif name_regex:
        rx = re.compile(name_regex)
        ids = [s["id"] for s in schedules if rx.search(s.get("name") or "") and s.get("id")]

    return ids


# =========================
# Main
# =========================
def main():
    parser = argparse.ArgumentParser(description="Bulk update MicroStrategy schedules start/end dates via REST.")
    parser.add_argument("--list", action="store_true", help="List schedules (id | name) and exit.")

    sel = parser.add_mutually_exclusive_group(required=False)
    sel.add_argument("--schedule-id", help="Update one schedule by ID.")
    sel.add_argument("--name-exact", help="Update schedules matching this exact name.")
    sel.add_argument("--name-contains", help="Update schedules whose name contains this text (case-insensitive).")
    sel.add_argument("--name-regex", help="Update schedules whose name matches this regex.")
    sel.add_argument("--all", action="store_true", help="Update ALL schedules accessible to this user.")

    parser.add_argument("--start", help="New startDate (format as your server expects).")
    parser.add_argument("--end", help="New end date (format as your server expects).")
    parser.add_argument("--end-field", help="Force end-date field key (e.g., endDate/expirationDate).")
    parser.add_argument("--fail-if-end-missing", action="store_true",
                        help="Fail (instead of warn) if end-date key isn't found in schedule JSON.")
    parser.add_argument("--dry-run", action="store_true", help="Do not PUT; only show planned changes.")
    parser.add_argument("--report", default="schedule_bulk_update_report.json", help="Report JSON output path.")
    parser.add_argument("--page-size", type=int, default=1000, help="Paging size for /v2/schedules.")
    args = parser.parse_args()

    cfg = Config()
    client = StrategyRest(cfg)

    report = {"updated": [], "skipped": [], "failed": []}

    try:
        client.login()
        schedules = client.list_schedules(page_size=args.page_size)

        if args.list:
            for s in schedules:
                print(f"{s.get('id')} | {s.get('name')}")
            return

        # Must pick a selection mode if not listing
        if not (args.schedule_id or args.name_exact or args.name_contains or args.name_regex or args.all):
            raise RuntimeError("Choose one selection mode: --schedule-id / --name-exact / --name-contains / --name-regex / --all")

        # For updates, require start
        if not args.start:
            raise RuntimeError("Missing --start. Provide a new startDate value.")

        # If you want end updates too, provide --end
        ids = select_ids(schedules, args.schedule_id, args.name_exact, args.name_contains, args.name_regex, args.all)
        if not ids:
            raise RuntimeError("No schedules matched selection. Use --list to see available schedules and IDs.")

        for sid in ids:
            try:
                current = client.get_schedule(sid)
                editable = sanitize_for_put(current)

                updated, warns = apply_dates(
                    editable,
                    args.start,
                    args.end,
                    args.end_field,
                    args.fail_if_end_missing,
                )

                if args.dry_run:
                    report["skipped"].append({
                        "id": sid,
                        "name": current.get("name"),
                        "dry_run": True,
                        "warnings": warns,
                    })
                    continue

                resp = client.update_schedule(sid, updated)
                report["updated"].append({
                    "id": sid,
                    "name": resp.get("name", current.get("name")),
                    "warnings": warns,
                })

            except Exception as e:
                report["failed"].append({"id": sid, "name": None, "error": str(e)})

        with open(args.report, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

        print(f"Done. Updated={len(report['updated'])}, DryRun/Skipped={len(report['skipped'])}, Failed={len(report['failed'])}")
        print(f"Report saved: {args.report}")

        if report["failed"]:
            sys.exit(2)

    finally:
        client.logout()


if __name__ == "__main__":
    main()
