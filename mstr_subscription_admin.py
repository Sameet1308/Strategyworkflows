"""
mstr_subscription_admin.py
────────────────────────────────────────────────────────────────────────────────
Strategy ONE (Strategy REST API) — Subscription Admin Toolkit
Three subcommands, all scoped to a single project via --project:

  delete   Bulk-delete subscriptions filtered by owner, schedule,
           delivery type, or staleness (days since last modified).

  send     Trigger one or all subscriptions immediately via the
           /send endpoint (bypasses the schedule).

  chown    Change ownership of one subscription, or bulk-reassign
           every subscription from one user to another.

Required privileges on the service account:
  • DssXmlPrivilegesScheduleAdministration  (see all users' subscriptions)
  • DssXmlPrivilegesUseScheduleMonitor      (fallback — own subs only)
  • Standard Create/Modify/Delete subscription privileges

Usage examples
──────────────
  # List all subscriptions for a project (dry-run of delete — shows what exists)
  python mstr_subscription_admin.py delete --project "BI_RMIS" --dry-run

  # Delete all subscriptions owned by a specific user ID
  python mstr_subscription_admin.py delete --project "BI_RMIS" \
      --owner-id "ABC123DEF456"

  # Delete all EMAIL subscriptions
  python mstr_subscription_admin.py delete --project "BI_RMIS" \
      --delivery-type EMAIL

  # Delete all subscriptions tied to a specific schedule ID
  python mstr_subscription_admin.py delete --project "BI_RMIS" \
      --schedule-id "FF7BB3C811D501F0C00051916B98494F"

  # Delete subscriptions not modified in the last 180 days (stale)
  python mstr_subscription_admin.py delete --project "BI_RMIS" --stale-days 180

  # Send one subscription immediately by ID
  python mstr_subscription_admin.py send --project "BI_RMIS" \
      --sub-id "64E9995E4908CC6BFC6F1998D244FE55"

  # Re-trigger ALL subscriptions tied to a specific schedule ID
  python mstr_subscription_admin.py send --project "BI_RMIS" \
      --schedule-id "FF7BB3C811D501F0C00051916B98494F"

  # Re-trigger all time-based subscriptions (daily/weekly/monthly)
  python mstr_subscription_admin.py send --project "BI_RMIS" \
      --schedule-type time_based

  # Re-trigger all event-based subscriptions
  python mstr_subscription_admin.py send --project "BI_RMIS" \
      --schedule-type event_based

  # Combine: time-based + EMAIL + specific owner (filters are ANDed)
  python mstr_subscription_admin.py send --project "BI_RMIS" \
      --schedule-type time_based --delivery-type EMAIL \
      --owner-id "ABC123DEF456"

  # Send ALL subscriptions in a project immediately (with confirmation)
  python mstr_subscription_admin.py send --project "BI_RMIS" --all

  # Send all subscriptions owned by a specific user
  python mstr_subscription_admin.py send --project "BI_RMIS" \
      --owner-id "ABC123DEF456"

  # Change owner of one subscription
  python mstr_subscription_admin.py chown --project "BI_RMIS" \
      --sub-id "64E9995E4908CC6BFC6F1998D244FE55" \
      --new-owner-id "NEW_USER_ID"

  # Bulk reassign ALL subscriptions from one user to another
  python mstr_subscription_admin.py chown --project "BI_RMIS" \
      --from-owner-id "OLD_USER_ID" \
      --new-owner-id "NEW_USER_ID"

  # Update start and expiry date on a single subscription
  python mstr_subscription_admin.py update --project "BI_RMIS" \
      --sub-id "64E9995E4908CC6BFC6F1998D244FE55" \
      --start-date 2025-01-01 --expiry-date 2025-12-31

  # Bulk-update expiry date for all time-based subscriptions
  python mstr_subscription_admin.py update --project "BI_RMIS" \
      --schedule-type time_based --expiry-date 2025-12-31

  # Extend expiry for all EMAIL subs owned by a user
  python mstr_subscription_admin.py update --project "BI_RMIS" \
      --owner-id "ABC123DEF456" --delivery-type EMAIL \
      --expiry-date 2026-06-30

  # Dry-run — preview what would be updated
  python mstr_subscription_admin.py update --project "BI_RMIS" \
      --schedule-type event_based --expiry-date 2025-12-31 --dry-run

Environment variables (preferred over hard-coding):
  MSTR_BASE_URL     e.g. https://your-dev-server.com/MicroStrategyLibrarySTD
  MSTR_USERNAME
  MSTR_PASSWORD
  MSTR_LOGIN_MODE   1=standard (default), 16=LDAP
────────────────────────────────────────────────────────────────────────────────
"""

import argparse
import csv
import os
import sys
import urllib3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import requests

# ── Suppress self-signed-cert warnings (verify=False is intentional) ──────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Defaults (override via env vars or CLI flags) ─────────────────────────────
DEFAULT_BASE_URL   = os.getenv("MSTR_BASE_URL",    "https://your-dev-server.com/MicroStrategyLibrarySTD")
DEFAULT_USERNAME   = os.getenv("MSTR_USERNAME",    "service_account_placeholder")
DEFAULT_PASSWORD   = os.getenv("MSTR_PASSWORD",    "password_placeholder")
DEFAULT_LOGIN_MODE = int(os.getenv("MSTR_LOGIN_MODE", "1"))   # 1=standard, 16=LDAP

REQUEST_TIMEOUT = 30
PAGE_LIMIT      = 200

# Valid delivery mode values returned by the API
DELIVERY_MODES = {"EMAIL", "FILE", "SHARED_LINK", "MOBILE", "FTP", "CACHE", "PRINT"}


# ─────────────────────────────────────────────────────────────────────────────
# Summary container
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class OpSummary:
    total_found:    int = 0
    total_actioned: int = 0
    total_failed:   int = 0
    total_skipped:  int = 0
    errors: list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# Session / auth helpers
# ─────────────────────────────────────────────────────────────────────────────

def _api(base_url: str) -> str:
    return base_url.rstrip("/") + "/api"


def create_session() -> requests.Session:
    """
    MSTR REST API requires both X-MSTR-AuthToken AND session cookies on every
    call.  A requests.Session carries both automatically after login.
    Never use bare requests.get/post/etc — that drops cookies → ERR009.
    """
    session = requests.Session()
    session.verify = False
    session.headers.update({"Accept": "application/json"})
    return session


def login(base_url: str, session: requests.Session,
          username: str, password: str, login_mode: int) -> str:
    """POST /api/auth/login — pins token onto session headers."""
    resp = session.post(
        f"{_api(base_url)}/auth/login",
        json={"username": username, "password": password, "loginMode": login_mode},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    token = resp.headers.get("X-MSTR-AuthToken")
    if not token:
        raise RuntimeError("Login succeeded but X-MSTR-AuthToken header was absent.")
    session.headers.update({"X-MSTR-AuthToken": token})
    print(f"[AUTH]   Logged in as '{username}' — token acquired.")
    return token


def logout(base_url: str, session: requests.Session) -> None:
    """POST /api/auth/logout"""
    try:
        session.post(f"{_api(base_url)}/auth/logout", timeout=15)
        print("[AUTH]   Session closed (logout).")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN]   Logout call failed (non-fatal): {exc}")


# ─────────────────────────────────────────────────────────────────────────────
# Core API helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_projects(base_url: str, session: requests.Session) -> list[dict]:
    """GET /api/projects — returns all loaded projects."""
    resp = session.get(f"{_api(base_url)}/projects", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    # Handle both shapes: bare list OR {"projects": [...]}
    if isinstance(data, list):
        return data
    return data.get("projects", [])


def resolve_project_id(base_url: str, session: requests.Session,
                       project_name: str) -> str:
    """Return the project ID for the named project (case-insensitive)."""
    projects = get_projects(base_url, session)
    for p in projects:
        if p["name"].strip().lower() == project_name.strip().lower():
            return p["id"]
    available = [p["name"] for p in projects]
    raise ValueError(
        f"Project '{project_name}' not found or not loaded.\n"
        f"Available: {available}"
    )


def get_subscriptions(base_url: str, session: requests.Session,
                      project_id: str) -> list[dict]:
    """
    GET /api/subscriptions
    Requires X-MSTR-ProjectID header.
    Admin sees ALL users' subscriptions with ScheduleAdministration privilege.
    Paginates automatically.
    Response shape: {"subscriptions": [...]}  — always a dict.
    """
    url    = f"{_api(base_url)}/subscriptions"
    params = {"limit": PAGE_LIMIT, "offset": 0}
    # Project is passed as a header, not a query param
    session.headers.update({"X-MSTR-ProjectID": project_id})
    all_subs: list[dict] = []
    try:
        while True:
            resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            # Handle both response shapes defensively
            if isinstance(data, dict):
                batch = data.get("subscriptions", [])
            elif isinstance(data, list):
                batch = data
            else:
                batch = []
            all_subs.extend(batch)
            if len(batch) < PAGE_LIMIT:
                break
            params["offset"] += PAGE_LIMIT
    finally:
        # Remove project header after use so it doesn't leak into unrelated calls
        session.headers.pop("X-MSTR-ProjectID", None)
    return all_subs


def get_subscription(base_url: str, session: requests.Session,
                     project_id: str, sub_id: str) -> dict:
    """GET /api/subscriptions/{id} — fetch a single subscription's full body."""
    session.headers.update({"X-MSTR-ProjectID": project_id})
    try:
        resp = session.get(
            f"{_api(base_url)}/subscriptions/{sub_id}",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        session.headers.pop("X-MSTR-ProjectID", None)


def delete_subscription(base_url: str, session: requests.Session,
                        project_id: str, sub_id: str) -> None:
    """DELETE /api/subscriptions/{id} — returns 204 on success."""
    session.headers.update({"X-MSTR-ProjectID": project_id})
    try:
        resp = session.delete(
            f"{_api(base_url)}/subscriptions/{sub_id}",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    finally:
        session.headers.pop("X-MSTR-ProjectID", None)


def send_subscription(base_url: str, session: requests.Session,
                      project_id: str, sub_id: str) -> None:
    """POST /api/subscriptions/{id}/send — triggers delivery immediately."""
    session.headers.update({"X-MSTR-ProjectID": project_id})
    try:
        resp = session.post(
            f"{_api(base_url)}/subscriptions/{sub_id}/send",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    finally:
        session.headers.pop("X-MSTR-ProjectID", None)


def update_subscription(base_url: str, session: requests.Session,
                        project_id: str, sub_id: str, body: dict) -> dict:
    """
    PUT /api/subscriptions/{id}
    Full-body replacement required — GET first, modify owner, PUT back.
    Returns the updated subscription dict.
    """
    session.headers.update({
        "X-MSTR-ProjectID": project_id,
        "Content-Type":     "application/json",
    })
    try:
        resp = session.put(
            f"{_api(base_url)}/subscriptions/{sub_id}",
            json=body,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    finally:
        session.headers.pop("X-MSTR-ProjectID", None)
        session.headers.pop("Content-Type", None)



def get_schedules(base_url: str, session: requests.Session) -> list[dict]:
    """
    GET /api/schedules
    Returns all schedules accessible to the session.
    Each schedule has: id, name, type ("time_based" | "event_based"), nextDelivery, etc.
    Response handled defensively — both bare list and {"schedules": [...]} dict.
    """
    resp = session.get(f"{_api(base_url)}/schedules", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    if isinstance(data, dict):
        return data.get("schedules", [])
    elif isinstance(data, list):
        return data
    return []


def build_schedule_type_map(base_url: str,
                            session: requests.Session) -> dict[str, str]:
    """
    Returns {schedule_id: schedule_type} for all schedules.
    Values: "time_based" or "event_based".
    Fetched once per run and passed into apply_filters() to avoid
    per-subscription API calls.
    """
    schedules = get_schedules(base_url, session)
    return {
        s.get("id", ""): (
            s.get("type") or s.get("scheduleType") or "unknown"
        ).lower()
        for s in schedules
        if s.get("id")
    }


# ─────────────────────────────────────────────────────────────────────────────
# Subscription filtering helpers
# ─────────────────────────────────────────────────────────────────────────────

def _sub_delivery_mode(sub: dict) -> str:
    """Extract delivery mode string from a subscription dict."""
    return (sub.get("delivery") or {}).get("mode", "").upper()


def _sub_schedule_ids(sub: dict) -> list[str]:
    """Return list of schedule IDs attached to a subscription."""
    return [s.get("id", "") for s in (sub.get("schedules") or [])]


def _sub_owner_id(sub: dict) -> str:
    return (sub.get("owner") or {}).get("id", "")


def _sub_date_modified(sub: dict) -> Optional[datetime]:
    """Parse dateModified into a UTC-aware datetime, or None."""
    raw = sub.get("dateModified") or sub.get("dateCreated")
    if not raw:
        return None
    try:
        # Strip trailing timezone offset to normalise
        dt = datetime.fromisoformat(raw.replace("+0000", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None


def apply_filters(subs: list[dict],
                  owner_id:           Optional[str],
                  schedule_id:        Optional[str],
                  delivery_type:      Optional[str],
                  stale_days:         Optional[int],
                  schedule_type:      Optional[str]       = None,
                  schedule_type_map:  Optional[dict]      = None) -> list[dict]:
    """
    Return only subscriptions that match ALL supplied filters.
    Filters are ANDed together; omitting a filter means 'match all'.

    schedule_type filter:
      Pass schedule_type ("time_based" | "event_based") together with
      schedule_type_map ({id: type} from build_schedule_type_map()).
      A subscription matches if ANY of its attached schedules has
      the requested type.
    """
    result = subs
    if owner_id:
        result = [s for s in result if _sub_owner_id(s) == owner_id]
    if schedule_id:
        result = [s for s in result if schedule_id in _sub_schedule_ids(s)]
    if delivery_type:
        result = [s for s in result
                  if _sub_delivery_mode(s) == delivery_type.upper()]
    if stale_days is not None:
        cutoff = datetime.now(tz=timezone.utc)
        result = [
            s for s in result
            if (dt := _sub_date_modified(s)) is not None
            and (cutoff - dt).days >= stale_days
        ]
    if schedule_type and schedule_type_map:
        target_type = schedule_type.lower()
        def _matches_type(sub: dict) -> bool:
            for sid in _sub_schedule_ids(sub):
                if schedule_type_map.get(sid, "").lower() == target_type:
                    return True
            return False
        result = [s for s in result if _matches_type(s)]
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Printing helpers
# ─────────────────────────────────────────────────────────────────────────────

def _print_sub_row(sub: dict, prefix: str = "  ") -> None:
    sub_id    = sub.get("id", "unknown")
    name      = sub.get("name", "unnamed")
    owner     = (sub.get("owner") or {}).get("name", "unknown")
    owner_id  = _sub_owner_id(sub)
    mode      = _sub_delivery_mode(sub)
    schedules = ", ".join(
        s.get("name", s.get("id", "?")) for s in (sub.get("schedules") or [])
    ) or "—"
    modified  = sub.get("dateModified", "unknown")
    print(
        f"{prefix}ID: {sub_id[:16]}…  |  '{name}'\n"
        f"{prefix}    Owner: {owner} ({owner_id[:12]}…)  "
        f"Delivery: {mode}  Schedule: {schedules}  Modified: {modified}"
    )


def export_csv(subs: list[dict], path: str) -> None:
    """Write subscription list to a CSV file for audit purposes."""
    fieldnames = ["id", "name", "owner_name", "owner_id",
                  "delivery_mode", "schedules", "date_modified", "date_created"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for sub in subs:
            writer.writerow({
                "id":            sub.get("id", ""),
                "name":          sub.get("name", ""),
                "owner_name":    (sub.get("owner") or {}).get("name", ""),
                "owner_id":      _sub_owner_id(sub),
                "delivery_mode": _sub_delivery_mode(sub),
                "schedules":     "; ".join(
                    s.get("name", s.get("id", "")) for s in (sub.get("schedules") or [])
                ),
                "date_modified": sub.get("dateModified", ""),
                "date_created":  sub.get("dateCreated", ""),
            })
    print(f"[CSV]    Exported {len(subs)} subscription(s) → {path}")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: delete
# ─────────────────────────────────────────────────────────────────────────────

def cmd_delete(args: argparse.Namespace, base_url: str,
               session: requests.Session) -> None:
    """Bulk-delete subscriptions matching supplied filters."""

    project_id = resolve_project_id(base_url, session, args.project)
    print(f"\n[INFO]   Fetching subscriptions for '{args.project}' …")
    all_subs = get_subscriptions(base_url, session, project_id)
    print(f"[INFO]   {len(all_subs)} subscription(s) found.")

    # Fetch schedule type map only when --schedule-type is requested (saves an API call)
    sched_type_map: dict = {}
    schedule_type = getattr(args, "schedule_type", None)
    if schedule_type:
        print(f"[INFO]   Fetching schedule type map (filter: {schedule_type}) …")
        sched_type_map = build_schedule_type_map(base_url, session)
        print(f"[INFO]   {len(sched_type_map)} schedule(s) indexed.")

    targets = apply_filters(
        all_subs,
        owner_id          = args.owner_id,
        schedule_id       = args.schedule_id,
        delivery_type     = args.delivery_type,
        stale_days        = args.stale_days,
        schedule_type     = schedule_type,
        schedule_type_map = sched_type_map or None,
    )

    if not targets:
        print("[INFO]   No subscriptions match the supplied filters. Nothing to do.")
        return

    summary = OpSummary(total_found=len(targets))
    print(f"\n[DELETE] {len(targets)} subscription(s) match filters:")
    for sub in targets:
        _print_sub_row(sub)

    # Export to CSV before deleting (always — good audit trail)
    if not args.dry_run:
        ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = f"deleted_subscriptions_{ts}.csv"
        export_csv(targets, csv_path)

    if args.dry_run:
        print(f"\n[DRY-RUN] Would delete {len(targets)} subscription(s). No changes made.")
        return

    if not args.no_confirm:
        answer = input(
            f"\n  Type  YES  to delete {len(targets)} subscription(s), "
            f"or anything else to abort: "
        ).strip()
        if answer != "YES":
            print("[ABORTED]  No changes were made.")
            return

    print(f"\n[DELETE] Proceeding …")
    for sub in targets:
        sub_id = sub.get("id", "unknown")
        name   = sub.get("name", "unnamed")
        print(f"  Deleting '{name}' ({sub_id[:16]}…) …", end="")
        try:
            delete_subscription(base_url, session, project_id, sub_id)
            summary.total_actioned += 1
            print("  ✓")
        except requests.HTTPError as exc:
            summary.total_failed += 1
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:120]}"
            summary.errors.append(f"Delete failed — {sub_id}: {msg}")
            print(f"  ✗ {msg}")

    _print_summary("DELETE", summary)


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: send
# ─────────────────────────────────────────────────────────────────────────────

def cmd_send(args: argparse.Namespace, base_url: str,
             session: requests.Session) -> None:
    """Trigger one or more subscriptions for immediate delivery."""

    project_id = resolve_project_id(base_url, session, args.project)

    # Single subscription by ID
    if args.sub_id:
        print(f"\n[SEND]   Triggering subscription {args.sub_id} …", end="")
        try:
            send_subscription(base_url, session, project_id, args.sub_id)
            print("  ✓ sent")
        except requests.HTTPError as exc:
            print(f"  ✗ HTTP {exc.response.status_code}: {exc.response.text[:120]}")
            sys.exit(1)
        return

    # Bulk send — fetch all and filter
    print(f"\n[INFO]   Fetching subscriptions for '{args.project}' …")
    all_subs = get_subscriptions(base_url, session, project_id)
    print(f"[INFO]   {len(all_subs)} subscription(s) found.")

    # Fetch schedule type map only when --schedule-type filter is requested
    sched_type_map: dict = {}
    schedule_type = getattr(args, "schedule_type", None)
    if schedule_type:
        print(f"[INFO]   Fetching schedule type map (filter: {schedule_type}) …")
        sched_type_map = build_schedule_type_map(base_url, session)
        print(f"[INFO]   {len(sched_type_map)} schedule(s) indexed.")

    targets = apply_filters(
        all_subs,
        owner_id          = args.owner_id,
        schedule_id       = args.schedule_id,
        delivery_type     = args.delivery_type,
        stale_days        = None,   # stale filter not meaningful for send
        schedule_type     = schedule_type,
        schedule_type_map = sched_type_map or None,
    )

    if not targets:
        print("[INFO]   No subscriptions match filters. Nothing to send.")
        return

    summary = OpSummary(total_found=len(targets))
    print(f"\n[SEND]   {len(targets)} subscription(s) will be triggered:")
    for sub in targets:
        _print_sub_row(sub)

    if args.dry_run:
        print(f"\n[DRY-RUN] Would send {len(targets)} subscription(s). No changes made.")
        return

    if not args.no_confirm:
        answer = input(
            f"\n  Type  YES  to send {len(targets)} subscription(s) immediately, "
            f"or anything else to abort: "
        ).strip()
        if answer != "YES":
            print("[ABORTED]  No changes were made.")
            return

    print(f"\n[SEND]   Triggering …")
    for sub in targets:
        sub_id = sub.get("id", "unknown")
        name   = sub.get("name", "unnamed")
        print(f"  Sending '{name}' ({sub_id[:16]}…) …", end="")
        try:
            send_subscription(base_url, session, project_id, sub_id)
            summary.total_actioned += 1
            print("  ✓")
        except requests.HTTPError as exc:
            summary.total_failed += 1
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:120]}"
            summary.errors.append(f"Send failed — {sub_id}: {msg}")
            print(f"  ✗ {msg}")

    _print_summary("SEND", summary)


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: chown
# ─────────────────────────────────────────────────────────────────────────────

def cmd_chown(args: argparse.Namespace, base_url: str,
              session: requests.Session) -> None:
    """
    Change the owner of one subscription (--sub-id) or bulk-reassign all
    subscriptions from --from-owner-id to --new-owner-id.

    Strategy: GET the full subscription body → replace owner.id → PUT it back.
    The PUT endpoint requires the complete subscription object.
    """
    project_id = resolve_project_id(base_url, session, args.project)

    # ── Collect targets ──────────────────────────────────────────────────────
    if args.sub_id:
        # Single subscription
        print(f"\n[INFO]   Fetching subscription {args.sub_id} …")
        sub  = get_subscription(base_url, session, project_id, args.sub_id)
        targets = [sub]
    else:
        # Bulk — get all and filter by from_owner_id
        if not args.from_owner_id:
            print("[ERROR]  Provide either --sub-id or --from-owner-id for bulk chown.")
            sys.exit(1)
        print(f"\n[INFO]   Fetching subscriptions for '{args.project}' …")
        all_subs = get_subscriptions(base_url, session, project_id)
        targets  = [s for s in all_subs
                    if _sub_owner_id(s) == args.from_owner_id]
        print(f"[INFO]   {len(targets)} subscription(s) owned by '{args.from_owner_id}'.")

    if not targets:
        print("[INFO]   No matching subscriptions found. Nothing to do.")
        return

    summary = OpSummary(total_found=len(targets))
    print(f"\n[CHOWN]  {len(targets)} subscription(s) will be reassigned "
          f"→ new owner ID: {args.new_owner_id}")
    for sub in targets:
        _print_sub_row(sub)

    if args.dry_run:
        print(f"\n[DRY-RUN] Would reassign {len(targets)} subscription(s). No changes made.")
        return

    if not args.no_confirm:
        answer = input(
            f"\n  Type  YES  to reassign {len(targets)} subscription(s), "
            f"or anything else to abort: "
        ).strip()
        if answer != "YES":
            print("[ABORTED]  No changes were made.")
            return

    print(f"\n[CHOWN]  Reassigning …")
    for sub in targets:
        sub_id = sub.get("id", "unknown")
        name   = sub.get("name", "unnamed")
        old_owner = (sub.get("owner") or {}).get("name", "unknown")
        print(f"  '{name}' ({sub_id[:16]}…) "
              f"  {old_owner} → {args.new_owner_id} …", end="")

        # Build updated body: full subscription with owner.id replaced
        updated_body = dict(sub)
        if updated_body.get("owner") is None:
            updated_body["owner"] = {}
        updated_body["owner"]["id"] = args.new_owner_id
        # Clear the owner name — the server will resolve it from the ID
        updated_body["owner"].pop("name", None)

        try:
            update_subscription(base_url, session, project_id, sub_id, updated_body)
            summary.total_actioned += 1
            print("  ✓")
        except requests.HTTPError as exc:
            summary.total_failed += 1
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:120]}"
            summary.errors.append(f"Chown failed — {sub_id}: {msg}")
            print(f"  ✗ {msg}")

    _print_summary("CHOWN", summary)



# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: update
# ─────────────────────────────────────────────────────────────────────────────

def _parse_date_arg(value: Optional[str], label: str) -> Optional[str]:
    """
    Validate and normalise a date string to YYYY-MM-DD.
    Accepts: YYYY-MM-DD, YYYY/MM/DD, MM-DD-YYYY, MM/DD/YYYY.
    Returns the ISO date string, or raises ValueError on bad input.
    """
    if not value:
        return None
    import re
    # Try YYYY-MM-DD or YYYY/MM/DD
    m = re.fullmatch(r"(\d{4})[/-](\d{2})[/-](\d{2})", value)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    # Try MM-DD-YYYY or MM/DD/YYYY
    m = re.fullmatch(r"(\d{2})[/-](\d{2})[/-](\d{4})", value)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    raise ValueError(
        f"--{label} '{value}' is not a recognised date format. "
        f"Use YYYY-MM-DD (e.g. 2025-12-31)."
    )


def _apply_dates_to_body(body: dict,
                         start_date: Optional[str],
                         expiry_date: Optional[str]) -> tuple[dict, list[str]]:
    """
    Inject start/expiry dates into the subscription body.

    Strategy ONE stores schedule dates on the schedule objects inside
    the 'schedules' array.  Some versions also carry them at the top
    level.  We update both locations so the change sticks regardless
    of server version.

    Date field names handled:
      start  : startDate
      expiry : stopDate  (primary)  |  expirationDate  (fallback alias)

    Returns (updated_body, list_of_changes_made).
    """
    changes: list[str] = []
    body    = dict(body)   # shallow copy — avoid mutating the original

    # ── Top-level fields (some server versions) ───────────────────────────────
    if start_date:
        if "startDate" in body or start_date:
            body["startDate"] = start_date
            changes.append(f"top-level startDate → {start_date}")
    if expiry_date:
        if "stopDate" in body:
            body["stopDate"] = expiry_date
            changes.append(f"top-level stopDate → {expiry_date}")
        elif "expirationDate" in body:
            body["expirationDate"] = expiry_date
            changes.append(f"top-level expirationDate → {expiry_date}")
        else:
            # Set both — server will use whichever it recognises
            body["stopDate"] = expiry_date
            changes.append(f"top-level stopDate (new) → {expiry_date}")

    # ── Schedule-level fields (primary location in most versions) ─────────────
    for sched in body.get("schedules") or []:
        sched_name = sched.get("name", sched.get("id", "?"))
        if start_date:
            sched["startDate"] = start_date
            changes.append(f"schedule '{sched_name}' startDate → {start_date}")
        if expiry_date:
            # Prefer stopDate; fall back to expirationDate; create stopDate if absent
            if "stopDate" in sched:
                sched["stopDate"] = expiry_date
                changes.append(f"schedule '{sched_name}' stopDate → {expiry_date}")
            elif "expirationDate" in sched:
                sched["expirationDate"] = expiry_date
                changes.append(f"schedule '{sched_name}' expirationDate → {expiry_date}")
            else:
                sched["stopDate"] = expiry_date
                changes.append(f"schedule '{sched_name}' stopDate (new) → {expiry_date}")

    return body, changes


def cmd_update(args: argparse.Namespace, base_url: str,
               session: requests.Session) -> None:
    """
    Update startDate and/or stopDate (expiry) on one or more subscriptions.
    Pattern: GET full body → modify date fields → PUT back.
    """
    # ── Validate date inputs early — fail fast before any API calls ───────────
    start_date  = _parse_date_arg(getattr(args, "start_date",  None), "start-date")
    expiry_date = _parse_date_arg(getattr(args, "expiry_date", None), "expiry-date")

    if not start_date and not expiry_date:
        print("[ERROR]  Provide at least --start-date or --expiry-date.")
        sys.exit(1)

    project_id = resolve_project_id(base_url, session, args.project)

    # ── Collect targets ───────────────────────────────────────────────────────
    if args.sub_id and not any([args.owner_id, args.schedule_id,
                                args.schedule_type, args.delivery_type]):
        # Single subscription — fetch its full body directly
        print(f"\n[INFO]   Fetching subscription {args.sub_id} …")
        sub     = get_subscription(base_url, session, project_id, args.sub_id)
        targets = [sub]
    else:
        # Bulk — list all and apply filters
        print(f"\n[INFO]   Fetching subscriptions for '{args.project}' …")
        all_subs = get_subscriptions(base_url, session, project_id)
        print(f"[INFO]   {len(all_subs)} subscription(s) found.")

        sched_type_map: dict = {}
        schedule_type = getattr(args, "schedule_type", None)
        if schedule_type:
            print(f"[INFO]   Fetching schedule type map (filter: {schedule_type}) …")
            sched_type_map = build_schedule_type_map(base_url, session)
            print(f"[INFO]   {len(sched_type_map)} schedule(s) indexed.")

        targets = apply_filters(
            all_subs,
            owner_id          = args.owner_id,
            schedule_id       = args.schedule_id,
            delivery_type     = args.delivery_type,
            stale_days        = None,
            schedule_type     = schedule_type,
            schedule_type_map = sched_type_map or None,
        )

        if args.sub_id:
            # sub-id provided alongside filters — further narrow to that one ID
            targets = [s for s in targets if s.get("id") == args.sub_id]

    if not targets:
        print("[INFO]   No subscriptions match the supplied filters. Nothing to do.")
        return

    summary = OpSummary(total_found=len(targets))

    print(f"\n[UPDATE] {len(targets)} subscription(s) will be updated:")
    changes_label = []
    if start_date:  changes_label.append(f"startDate → {start_date}")
    if expiry_date: changes_label.append(f"stopDate  → {expiry_date}")
    print(f"         Fields: {', '.join(changes_label)}")
    for sub in targets:
        _print_sub_row(sub)

    if args.dry_run:
        print(f"\n[DRY-RUN] Would update {len(targets)} subscription(s). No changes made.")
        return

    if not args.no_confirm:
        answer = input(
            f"\n  Type  YES  to update {len(targets)} subscription(s), "
            f"or anything else to abort: "
        ).strip()
        if answer != "YES":
            print("[ABORTED]  No changes were made.")
            return

    print(f"\n[UPDATE] Applying …")
    for sub in targets:
        sub_id = sub.get("id", "unknown")
        name   = sub.get("name", "unnamed")
        print(f"  '{name}' ({sub_id[:16]}…) …", end="")

        # For bulk fetches, the list endpoint may return a lighter body.
        # Always re-fetch the full body before PUT to ensure completeness.
        try:
            full_body = get_subscription(base_url, session, project_id, sub_id)
        except requests.HTTPError as exc:
            summary.total_failed += 1
            msg = f"GET failed HTTP {exc.response.status_code}"
            summary.errors.append(f"Update skipped — {sub_id}: {msg}")
            print(f"  ✗ {msg}")
            continue

        updated_body, changes = _apply_dates_to_body(full_body, start_date, expiry_date)

        try:
            update_subscription(base_url, session, project_id, sub_id, updated_body)
            summary.total_actioned += 1
            print(f"  ✓  ({len(changes)} field(s) set)")
        except requests.HTTPError as exc:
            summary.total_failed += 1
            msg = f"HTTP {exc.response.status_code}: {exc.response.text[:120]}"
            summary.errors.append(f"Update failed — {sub_id}: {msg}")
            print(f"  ✗ {msg}")

    _print_summary("UPDATE", summary)


# ─────────────────────────────────────────────────────────────────────────────
# Summary printer
# ─────────────────────────────────────────────────────────────────────────────

def _print_summary(op: str, summary: OpSummary) -> None:
    print(f"\n{'='*60}")
    print(f"  {op} SUMMARY")
    print(f"{'='*60}")
    print(f"  Subscriptions found    : {summary.total_found}")
    print(f"  Successfully actioned  : {summary.total_actioned}")
    print(f"  Failed                 : {summary.total_failed}")
    if summary.errors:
        print(f"\n  Errors ({len(summary.errors)}):")
        for e in summary.errors:
            print(f"    • {e}")
    print(f"{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI argument parser
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog="mstr_subscription_admin.py",
        description="Strategy ONE Subscription Admin Toolkit",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── Shared / global flags ─────────────────────────────────────────────────
    root.add_argument("--base-url",    default=DEFAULT_BASE_URL,
        help="Base URL (stop at /MicroStrategyLibrarySTD). Override: MSTR_BASE_URL")
    root.add_argument("--username",    default=DEFAULT_USERNAME,
        help="Service account username. Override: MSTR_USERNAME")
    root.add_argument("--password",    default=DEFAULT_PASSWORD,
        help="Service account password. Override: MSTR_PASSWORD")
    root.add_argument("--login-mode",  type=int, default=DEFAULT_LOGIN_MODE,
        help="1=standard, 16=LDAP. Override: MSTR_LOGIN_MODE")

    subparsers = root.add_subparsers(dest="command", required=True,
                                     title="subcommands")

    # ── Shared filter arguments (reused across delete / send) ─────────────────
    def add_shared_filters(p: argparse.ArgumentParser) -> None:
        p.add_argument("--project",       required=True,
            help="Exact project name to scope the operation.")
        p.add_argument("--owner-id",      default=None,
            help="Filter: only subscriptions owned by this user ID.")
        p.add_argument("--schedule-id",   default=None,
            help="Filter: only subscriptions tied to this schedule ID.")
        p.add_argument("--schedule-type", default=None,
            choices=["time_based", "event_based"],
            help="Filter: only subscriptions whose schedule is time_based "
                 "or event_based. Combines with --schedule-id (AND logic).")
        p.add_argument("--delivery-type", default=None,
            choices=["EMAIL","FILE","SHARED_LINK","MOBILE","FTP","CACHE","PRINT"],
            help="Filter: only subscriptions of this delivery mode.")
        p.add_argument("--dry-run",       action="store_true",
            help="List what would be affected without making any changes.")
        p.add_argument("--no-confirm",    action="store_true",
            help="Skip the interactive YES/abort confirmation prompt.")

    # ── delete ────────────────────────────────────────────────────────────────
    p_del = subparsers.add_parser(
        "delete",
        help="Bulk-delete subscriptions by filter.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_shared_filters(p_del)
    p_del.add_argument("--stale-days", type=int, default=None,
        help="Delete subscriptions not modified in >= N days.")

    # ── send ──────────────────────────────────────────────────────────────────
    p_send = subparsers.add_parser(
        "send",
        help="Trigger one or more subscriptions for immediate delivery.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_shared_filters(p_send)
    p_send.add_argument("--sub-id",  default=None,
        help="Send a single subscription by ID (bypasses filters).")
    p_send.add_argument("--all",     action="store_true",
        help="Send ALL subscriptions in the project (no filters). "
             "Cannot be combined with other filters.")

    # ── chown ─────────────────────────────────────────────────────────────────
    p_own = subparsers.add_parser(
        "chown",
        help="Change ownership of subscription(s).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p_own.add_argument("--project",       required=True,
        help="Exact project name to scope the operation.")
    p_own.add_argument("--sub-id",        default=None,
        help="Change owner of a single subscription by ID.")
    p_own.add_argument("--from-owner-id", default=None,
        help="Bulk mode: reassign all subs currently owned by this user ID.")
    p_own.add_argument("--new-owner-id",  required=True,
        help="User ID of the new owner.")
    p_own.add_argument("--dry-run",       action="store_true",
        help="List what would be changed without making any changes.")
    p_own.add_argument("--no-confirm",    action="store_true",
        help="Skip the interactive YES/abort confirmation prompt.")

    # ── update ────────────────────────────────────────────────────────────────
    p_upd = subparsers.add_parser(
        "update",
        help="Update start date and/or expiry date on subscription(s).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    add_shared_filters(p_upd)
    p_upd.add_argument("--sub-id",       default=None,
        help="Target a single subscription by ID. "
             "Can be combined with filters for extra safety.")
    p_upd.add_argument("--start-date",   default=None, dest="start_date",
        metavar="YYYY-MM-DD",
        help="New start date for the subscription schedule(s).")
    p_upd.add_argument("--expiry-date",  default=None, dest="expiry_date",
        metavar="YYYY-MM-DD",
        help="New expiry / stop date for the subscription schedule(s).")

    return root


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    base_url = args.base_url

    print("=" * 65)
    print("  Strategy ONE — Subscription Admin Toolkit")
    print(f"  Command  : {args.command.upper()}")
    print(f"  Base URL : {base_url}")
    print(f"  Project  : {args.project}")
    if getattr(args, "dry_run", False):
        print("  Mode     : DRY-RUN (no changes will be made)")
    print("=" * 65)

    session = None
    try:
        session = create_session()
        login(base_url, session, args.username, args.password, args.login_mode)

        if args.command == "delete":
            cmd_delete(args, base_url, session)
        elif args.command == "send":
            cmd_send(args, base_url, session)
        elif args.command == "chown":
            cmd_chown(args, base_url, session)
        elif args.command == "update":
            cmd_update(args, base_url, session)

    except requests.HTTPError as exc:
        print(f"\n[ERROR]  HTTP {exc.response.status_code}: {exc.response.text}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"\n[ERROR]  {exc}")
        sys.exit(1)
    finally:
        if session:
            logout(base_url, session)


if __name__ == "__main__":
    main()
