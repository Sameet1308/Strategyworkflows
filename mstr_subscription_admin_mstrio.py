"""
mstr_subscription_admin_mstrio.py
────────────────────────────────────────────────────────────────────────────────
Strategy ONE — Complete Subscription Admin Toolkit (mstrio-py SDK)
Version : mstrio-py 11.6.5.101  (May 2026)
Install : pip install mstrio-py

Covers everything in mstr_subscription_admin.py PLUS additional SDK capabilities:

  SUBCOMMAND       DESCRIPTION
  ─────────────────────────────────────────────────────────────────
  list             List subscriptions with rich filters + CSV export
  delete           Bulk delete by type / owner / schedule / stale days
  execute          Fire one or more subscriptions immediately
  chown            Change ownership — single or bulk (sub.alter)
  update           Update any subscription field via alter()
  create           Create all 7 subscription types
  copy             Clone a subscription (manager.create_copy)
  recipients       Add / remove / list recipients
  audit            Fetch change journal entries (full history)
  bursting         List available bursting attributes for content
  drl              Dynamic Recipient List — list and create
  status           Delivery history check (history list via conn.get)
  prompts          Answer prompts on a prompted subscription

Why two layers still exist:
  • sub.alter(owner_id=...)  ←  subscription execution owner (SDK native)
  • conn.put(...flags=70)    ←  metadata object ACL (no SDK wrapper yet)
  • conn.get(/v2/historyList) ← delivery status (no SDK wrapper yet)

Usage examples
──────────────
  # List all cache subs with last-run info, export to CSV
  python mstr_subscription_admin_mstrio.py list \
      --project "BI_RMIS" --type CACHE --last-run --export subs.csv

  # Bulk delete stale email subs
  python mstr_subscription_admin_mstrio.py delete \
      --project "BI_RMIS" --delivery-type EMAIL --stale-days 180 --dry-run

  # Fire all time-based subs immediately
  python mstr_subscription_admin_mstrio.py execute \
      --project "BI_RMIS" --schedule-type time_based

  # Change owner — single sub
  python mstr_subscription_admin_mstrio.py chown \
      --project "BI_RMIS" --sub-id "SUB_ID" --new-owner-id "NEW_ID"

  # Bulk reassign from one user to another
  python mstr_subscription_admin_mstrio.py chown \
      --project "BI_RMIS" --from-owner-id "OLD_ID" --new-owner-id "NEW_ID"

  # Clone a subscription to same project
  python mstr_subscription_admin_mstrio.py copy \
      --project "BI_RMIS" --sub-id "SUB_ID" --new-name "Q3 Revenue Copy"

  # Add a recipient
  python mstr_subscription_admin_mstrio.py recipients \
      --project "BI_RMIS" --sub-id "SUB_ID" \
      --action add --recipient-id "USER_ID" --recipient-type USER

  # Audit trail for a subscription
  python mstr_subscription_admin_mstrio.py audit \
      --project "BI_RMIS" --sub-id "SUB_ID"

  # List available bursting attributes for content
  python mstr_subscription_admin_mstrio.py bursting \
      --project "BI_RMIS" --sub-id "SUB_ID"

  # List Dynamic Recipient Lists
  python mstr_subscription_admin_mstrio.py drl \
      --project "BI_RMIS" --action list

  # Check delivery status (history list)
  python mstr_subscription_admin_mstrio.py status \
      --project "BI_RMIS" --sub-id "SUB_ID" --last 10

Environment variables:
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
import time
import urllib3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── mstrio-py imports ─────────────────────────────────────────────────────────
try:
    from mstrio.connection import Connection
    from mstrio.distribution_services import (
        CacheUpdateSubscription,
        Content,
        EmailSubscription,
        FileSubscription,
        FTPSubscription,
        HistoryListSubscription,
        MobileSubscription,
        Subscription,
        SubscriptionManager,
        list_subscriptions,
    )
    from mstrio.distribution_services.subscription.dynamic_recipient_list import (
        DynamicRecipientList,
        list_dynamic_recipient_lists,
    )
    from mstrio.distribution_services.subscription.base_subscription import (
        RecipientsTypes,
    )
    from mstrio.distribution_services.subscription.content import Content as SubContent
except ImportError as exc:
    print(f"[ERROR]  mstrio-py not installed: {exc}")
    print("         Run: pip install mstrio-py")
    sys.exit(1)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_BASE_URL   = os.getenv("MSTR_BASE_URL",    "https://your-dev-server.com/MicroStrategyLibrarySTD")
DEFAULT_USERNAME   = os.getenv("MSTR_USERNAME",    "service_account_placeholder")
DEFAULT_PASSWORD   = os.getenv("MSTR_PASSWORD",    "password_placeholder")
DEFAULT_LOGIN_MODE = int(os.getenv("MSTR_LOGIN_MODE", "1"))

# Subscription type label → class map
SUB_TYPE_MAP = {
    "EMAIL":        EmailSubscription,
    "FILE":         FileSubscription,
    "FTP":          FTPSubscription,
    "CACHE":        CacheUpdateSubscription,
    "HISTORY_LIST": HistoryListSubscription,
    "MOBILE":       MobileSubscription,
}

# flags=70 — apply ownerId + ACL atomically on metadata objects
OBJECT_FLAGS = 70

# History list: seconds to pause after job vanishes before querying
HISTORY_WAIT_S = 3


# ─────────────────────────────────────────────────────────────────────────────
# Connection
# ─────────────────────────────────────────────────────────────────────────────

def make_connection(base_url: str, username: str, password: str,
                    login_mode: int, project_name: str) -> Connection:
    """
    mstrio-py Connection auto-manages:
      - X-MSTR-AuthToken header
      - Session cookies
      - Auto session renewal when using username/password auth
    base_url must stop at /MicroStrategyLibrarySTD — no /api suffix.
    """
    conn = Connection(
        base_url     = base_url,
        username     = username,
        password     = password,
        login_mode   = login_mode,
        project_name = project_name,
    )
    _con(f"Connected as '{username}' → project '{project_name}'", "AUTH")
    return conn


# ─────────────────────────────────────────────────────────────────────────────
# Subscription fetch helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_all_subs(conn: Connection, project_name: str,
                 last_run: bool = False) -> list:
    """
    list_subscriptions() returns typed objects:
    CacheUpdateSubscription, EmailSubscription, etc.
    last_run=True adds last execution timestamp to each object.
    """
    return list_subscriptions(
        connection   = conn,
        project_name = project_name,
        last_run     = last_run,
    )


def get_sub_by_id(conn: Connection, project_name: str,
                  sub_id: str) -> Subscription:
    """Fetch one subscription by ID. Raises ValueError if not found."""
    for sub in get_all_subs(conn, project_name):
        if sub.id == sub_id:
            return sub
    raise ValueError(
        f"Subscription '{sub_id}' not found in project '{project_name}'.\n"
        f"Check the ID and that the account has ScheduleAdministration privilege."
    )


def get_manager(conn: Connection, project_name: str) -> SubscriptionManager:
    return SubscriptionManager(connection=conn, project_name=project_name)


# ─────────────────────────────────────────────────────────────────────────────
# Filter helpers
# ─────────────────────────────────────────────────────────────────────────────

def _delivery_mode(sub) -> str:
    try:
        return str(sub.delivery.mode).upper()
    except Exception:
        return ""


def _owner_id(sub) -> str:
    try:
        return sub.owner.id
    except Exception:
        return ""


def _schedule_ids(sub) -> list[str]:
    try:
        return [s.id for s in (sub.schedules or [])]
    except Exception:
        return []


def _date_modified(sub) -> Optional[datetime]:
    raw = getattr(sub, "date_modified", None) or getattr(sub, "dateModified", None)
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw if raw.tzinfo else raw.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(raw).replace("+0000", "+00:00").replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _get_schedule_type_map(conn: Connection) -> dict[str, str]:
    """
    Fetch all schedules and return {id: type} map.
    Uses conn.get() — /api/schedules has no mstrio-py wrapper yet.
    """
    try:
        resp = conn.get(endpoint="/schedules")
        resp.raise_for_status()
        data = resp.json()
        scheds = data.get("schedules", []) if isinstance(data, dict) else data
        return {
            s.get("id", ""): (s.get("type") or s.get("scheduleType") or "").lower()
            for s in scheds if s.get("id")
        }
    except Exception:
        return {}


def apply_filters(subs: list, owner_id: Optional[str],
                  delivery_type: Optional[str], stale_days: Optional[int],
                  schedule_type: Optional[str],
                  schedule_type_map: Optional[dict],
                  sub_type: Optional[str]) -> list:
    """
    AND-logic filter across all supported dimensions.
    sub_type matches against Python class name (EMAIL, CACHE, etc.)
    """
    result = subs

    if owner_id:
        result = [s for s in result if _owner_id(s) == owner_id]

    if delivery_type:
        result = [s for s in result
                  if _delivery_mode(s) == delivery_type.upper()]

    if stale_days is not None:
        cutoff = datetime.now(tz=timezone.utc)
        result = [
            s for s in result
            if (dt := _date_modified(s)) and (cutoff - dt).days >= stale_days
        ]

    if schedule_type and schedule_type_map:
        target = schedule_type.lower()
        def _has_type(sub) -> bool:
            return any(
                schedule_type_map.get(sid, "").lower() == target
                for sid in _schedule_ids(sub)
            )
        result = [s for s in result if _has_type(s)]

    if sub_type:
        cls = SUB_TYPE_MAP.get(sub_type.upper())
        if cls:
            result = [s for s in result if isinstance(s, cls)]

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Print / export helpers
# ─────────────────────────────────────────────────────────────────────────────

def _con(msg: str, tag: str = "INFO") -> None:
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"  [{ts}] [{tag:<14}]  {msg}")


def _div(char: str = "─", w: int = 66) -> None:
    print(char * w)


def _section(title: str) -> None:
    print()
    _div()
    print(f"  {title}")
    _div()


def _print_sub(sub, show_last_run: bool = False) -> None:
    mode  = _delivery_mode(sub)
    owner = getattr(sub, "owner", None)
    o_name = getattr(owner, "name", "?") if owner else "?"
    o_id   = getattr(owner, "id",   "?") if owner else "?"
    scheds = ", ".join(
        getattr(s, "name", getattr(s, "id", "?"))
        for s in (getattr(sub, "schedules", []) or [])
    ) or "—"
    contents = getattr(sub, "contents", []) or []
    c_name = getattr(contents[0], "name", "?") if contents else "?"
    modified = getattr(sub, "date_modified", getattr(sub, "dateModified", "?"))
    print(f"  {sub.id[:16]}…  [{mode:<12}]  '{sub.name}'")
    print(f"               Owner   : {o_name} ({o_id[:12]}…)")
    print(f"               Content : {c_name}")
    print(f"               Schedule: {scheds}")
    print(f"               Modified: {str(modified)[:19]}")
    if show_last_run:
        lr = getattr(sub, "last_run", None)
        print(f"               Last run: {lr or 'never'}")


def export_to_csv(subs: list, path: str) -> None:
    fields = ["id", "name", "type", "owner_id", "owner_name",
              "delivery_mode", "schedules", "content_name",
              "date_modified", "last_run"]
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for sub in subs:
            owner    = getattr(sub, "owner", None)
            contents = getattr(sub, "contents", []) or []
            scheds   = "; ".join(
                getattr(s, "name", getattr(s, "id", "?"))
                for s in (getattr(sub, "schedules", []) or [])
            )
            writer.writerow({
                "id":           sub.id,
                "name":         sub.name,
                "type":         type(sub).__name__,
                "owner_id":     getattr(owner, "id",   "") if owner else "",
                "owner_name":   getattr(owner, "name", "") if owner else "",
                "delivery_mode":_delivery_mode(sub),
                "schedules":    scheds,
                "content_name": getattr(contents[0], "name", "") if contents else "",
                "date_modified":str(getattr(sub, "date_modified",
                                           getattr(sub, "dateModified", ""))),
                "last_run":     str(getattr(sub, "last_run", "")),
            })
    _con(f"Exported {len(subs)} subscription(s) → {path}")


def _confirm(prompt: str, no_confirm: bool) -> bool:
    if no_confirm:
        return True
    answer = input(f"\n  {prompt}\n  Type  YES  to proceed: ").strip()
    return answer == "YES"


def _parse_date(value: Optional[str], label: str) -> Optional[str]:
    """Validate and normalise date to YYYY-MM-DD."""
    if not value:
        return None
    import re
    m = re.fullmatch(r"(\d{4})[/-](\d{2})[/-](\d{2})", value)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.fullmatch(r"(\d{2})[/-](\d{2})[/-](\d{4})", value)
    if m:
        return f"{m.group(3)}-{m.group(1)}-{m.group(2)}"
    raise ValueError(f"--{label} '{value}' — use YYYY-MM-DD.")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: list
# ─────────────────────────────────────────────────────────────────────────────

def cmd_list(args: argparse.Namespace, conn: Connection) -> None:
    """
    list_subscriptions() returns typed objects.
    Filters: delivery type, owner, schedule type, sub class, stale days.
    last_run=True adds last execution timestamp.
    to_csv exports results to a file.
    """
    _section(f"LIST SUBSCRIPTIONS  —  project: {args.project}")

    _con("Fetching subscriptions …")
    subs = get_all_subs(conn, args.project, last_run=args.last_run)
    _con(f"{len(subs)} total subscription(s) found.")

    sched_map: dict = {}
    if args.schedule_type:
        _con("Fetching schedule type map …")
        sched_map = _get_schedule_type_map(conn)

    filtered = apply_filters(
        subs,
        owner_id          = args.owner_id,
        delivery_type     = args.delivery_type,
        stale_days        = args.stale_days,
        schedule_type     = args.schedule_type,
        schedule_type_map = sched_map or None,
        sub_type          = args.type,
    )

    print(f"\n  Matching: {len(filtered)}\n")

    for sub in filtered:
        _print_sub(sub, show_last_run=args.last_run)
        print()

    if args.export:
        export_to_csv(filtered, args.export)

    _div("═")
    print(f"  Total shown: {len(filtered)}")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: delete
# ─────────────────────────────────────────────────────────────────────────────

def cmd_delete(args: argparse.Namespace, conn: Connection) -> None:
    """
    Bulk delete using manager.delete() with typed filtering.
    Auto-exports matched subs to CSV before deletion (audit trail).
    """
    _section(f"BULK DELETE  —  project: {args.project}")

    subs = get_all_subs(conn, args.project)
    sched_map: dict = {}
    if args.schedule_type:
        sched_map = _get_schedule_type_map(conn)

    targets = apply_filters(
        subs,
        owner_id          = args.owner_id,
        delivery_type     = args.delivery_type,
        stale_days        = args.stale_days,
        schedule_type     = args.schedule_type,
        schedule_type_map = sched_map or None,
        sub_type          = args.type,
    )

    if not targets:
        _con("No subscriptions match filters. Nothing to do.")
        return

    print(f"\n  Matched {len(targets)} subscription(s):\n")
    for sub in targets:
        _print_sub(sub)
        print()

    if args.dry_run:
        _con(f"DRY-RUN — would delete {len(targets)}. No changes made.")
        return

    # Auto CSV backup before deleting
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = f"deleted_subs_{ts}.csv"
    export_to_csv(targets, csv_path)

    if not _confirm(f"Delete {len(targets)} subscription(s)?", args.no_confirm):
        print("[ABORTED]")
        return

    manager = get_manager(conn, args.project)
    manager.delete(subscriptions=targets, force=True)

    _con(f"✓ {len(targets)} subscription(s) deleted.")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: execute
# ─────────────────────────────────────────────────────────────────────────────

def cmd_execute(args: argparse.Namespace, conn: Connection) -> None:
    """
    Fire one subscription by ID, or bulk fire by filter.
    Uses sub.execute() (single) or manager.execute() (bulk).
    """
    _section(f"EXECUTE  —  project: {args.project}")

    if args.sub_id and not any([args.owner_id, args.delivery_type,
                                args.schedule_type, args.type]):
        # Single mode
        _con(f"Fetching subscription {args.sub_id} …")
        sub = get_sub_by_id(conn, args.project, args.sub_id)
        _print_sub(sub)

        if args.dry_run:
            _con("DRY-RUN — would call sub.execute().")
            return
        if not _confirm(f"Execute '{sub.name}'?", args.no_confirm):
            print("[ABORTED]")
            return
        sub.execute()
        _con(f"✓ '{sub.name}' sent.")
        return

    # Bulk mode
    subs = get_all_subs(conn, args.project)
    sched_map: dict = {}
    if args.schedule_type:
        sched_map = _get_schedule_type_map(conn)

    targets = apply_filters(
        subs,
        owner_id          = args.owner_id,
        delivery_type     = args.delivery_type,
        stale_days        = None,
        schedule_type     = args.schedule_type,
        schedule_type_map = sched_map or None,
        sub_type          = args.type,
    )

    if args.sub_id:
        targets = [s for s in targets if s.id == args.sub_id]

    if not targets:
        _con("No subscriptions match. Nothing to execute.")
        return

    print(f"\n  Will execute {len(targets)} subscription(s):\n")
    for sub in targets:
        _print_sub(sub)
        print()

    if args.dry_run:
        _con(f"DRY-RUN — would execute {len(targets)}. No changes made.")
        return

    if not _confirm(f"Execute {len(targets)} subscription(s) immediately?",
                    args.no_confirm):
        print("[ABORTED]")
        return

    manager = get_manager(conn, args.project)
    manager.execute(subscriptions=targets)

    _con(f"✓ {len(targets)} subscription(s) triggered.")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: chown
# ─────────────────────────────────────────────────────────────────────────────

def cmd_chown(args: argparse.Namespace, conn: Connection) -> None:
    """
    Change subscription owner via sub.alter(owner_id=...).
    Single mode: --sub-id
    Bulk mode:   --from-owner-id + --new-owner-id

    For CACHE subscriptions, also transfers the underlying object's
    ACL via conn.put() with flags=70 when --include-object-acl is set.
    """
    _section(f"CHANGE OWNERSHIP  —  project: {args.project}")

    if args.sub_id:
        _con(f"Fetching subscription {args.sub_id} …")
        targets = [get_sub_by_id(conn, args.project, args.sub_id)]
    elif args.from_owner_id:
        subs    = get_all_subs(conn, args.project)
        targets = [s for s in subs if _owner_id(s) == args.from_owner_id]
    else:
        print("[ERROR]  Provide --sub-id or --from-owner-id.")
        sys.exit(1)

    if not targets:
        _con("No subscriptions found. Nothing to do.")
        return

    print(f"\n  {len(targets)} subscription(s) to reassign → {args.new_owner_id}\n")
    for sub in targets:
        _print_sub(sub)
        print()

    if args.dry_run:
        _con(f"DRY-RUN — would call sub.alter(owner_id='{args.new_owner_id}').")
        return

    if not _confirm(
        f"Reassign {len(targets)} subscription(s) → {args.new_owner_id}?",
        args.no_confirm,
    ):
        print("[ABORTED]")
        return

    ok = failed = 0
    for sub in targets:
        print(f"  '{sub.name}' …", end="")
        try:
            # For CACHE subs: optionally transfer object ACL too
            if args.include_object_acl and isinstance(sub, CacheUpdateSubscription):
                contents = getattr(sub, "contents", []) or []
                if contents:
                    c_id = getattr(contents[0], "id", "")
                    if c_id and args.from_owner_id:
                        conn.put(
                            endpoint = f"/objects/{c_id}",
                            params   = {"type": 2048, "flags": OBJECT_FLAGS},
                            json     = {
                                "ownerId": args.new_owner_id,
                                "acl": [
                                    {"op":"remove","trustee":args.from_owner_id,
                                     "rights":255,"denied":False,
                                     "inheritable":False,"type":1},
                                    {"op":"add","trustee":args.new_owner_id,
                                     "rights":255,"denied":False,
                                     "inheritable":False,"type":1},
                                ]
                            },
                        ).raise_for_status()
                        print(" [object ACL ✓]", end="")

            sub.alter(owner_id=args.new_owner_id)
            ok += 1
            print("  ✓")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"  ✗  {exc}")

    _con(f"Done — {ok} transferred, {failed} failed.")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: update
# ─────────────────────────────────────────────────────────────────────────────

def cmd_update(args: argparse.Namespace, conn: Connection) -> None:
    """
    Update any combination of subscription fields via sub.alter().
    Supports: name, schedule, dates, expiry, format, zip, email subject,
    cache type, mobile device, delivery mode, recipient changes.
    Single or bulk (by owner / delivery type / schedule type / stale days).
    """
    _section(f"UPDATE  —  project: {args.project}")

    start_date  = _parse_date(args.start_date,  "start-date")
    expiry_date = _parse_date(args.expiry_date, "expiry-date")

    # Collect alter kwargs — only pass what was explicitly provided
    alter_kwargs: dict = {}
    if args.new_name:                    alter_kwargs["name"]                       = args.new_name
    if args.schedule_id:                 alter_kwargs["schedules"]                  = [args.schedule_id]
    if expiry_date:                      alter_kwargs["delivery_expiration_date"]    = expiry_date
    if args.expiry_tz:                   alter_kwargs["delivery_expiration_timezone"]= args.expiry_tz
    if args.email_subject:               alter_kwargs["email_subject"]               = args.email_subject
    if args.filename:                    alter_kwargs["filename"]                    = args.filename
    if args.zip_filename:                alter_kwargs["zip_filename"]                = args.zip_filename
    if args.zip_password:                alter_kwargs["zip_password"]                = args.zip_password
    if args.zip_password_protect is not None: alter_kwargs["zip_password_protect"]  = args.zip_password_protect
    if args.cache_type:                  alter_kwargs["cache_cache_type"]            = args.cache_type
    if args.mobile_client_type:          alter_kwargs["mobile_client_type"]          = args.mobile_client_type
    if args.device_id:                   alter_kwargs["device_id"]                   = args.device_id
    if args.send_now:                    alter_kwargs["send_now"]                    = True
    if args.allow_unsub is not None:     alter_kwargs["allow_unsubscribe"]           = args.allow_unsub
    if args.overwrite is not None:       alter_kwargs["overwrite_older_version"]     = args.overwrite
    if args.rerun_hl is not None:        alter_kwargs["re_run_hl"]                   = args.rerun_hl
    if args.notify is not None:          alter_kwargs["delivery_notification_enabled"] = args.notify

    if not alter_kwargs:
        print("[ERROR]  No update fields provided. "
              "Specify at least one field to change.")
        sys.exit(1)

    # Collect targets
    if args.sub_id and not any([args.owner_id, args.delivery_type,
                                args.schedule_type]):
        targets = [get_sub_by_id(conn, args.project, args.sub_id)]
    else:
        subs = get_all_subs(conn, args.project)
        sched_map: dict = {}
        if args.schedule_type:
            sched_map = _get_schedule_type_map(conn)
        targets = apply_filters(
            subs,
            owner_id          = args.owner_id,
            delivery_type     = args.delivery_type,
            stale_days        = None,
            schedule_type     = args.schedule_type,
            schedule_type_map = sched_map or None,
            sub_type          = None,
        )
        if args.sub_id:
            targets = [s for s in targets if s.id == args.sub_id]

    if not targets:
        _con("No subscriptions match. Nothing to update.")
        return

    print(f"\n  {len(targets)} subscription(s) to update:")
    print(f"  Fields: {list(alter_kwargs.keys())}\n")
    for sub in targets:
        _print_sub(sub)
        print()

    if args.dry_run:
        _con(f"DRY-RUN — would call sub.alter({alter_kwargs}).")
        return

    if not _confirm(f"Update {len(targets)} subscription(s)?", args.no_confirm):
        print("[ABORTED]")
        return

    ok = failed = 0
    for sub in targets:
        print(f"  '{sub.name}' …", end="")
        try:
            sub.alter(**alter_kwargs)
            ok += 1
            print("  ✓")
        except Exception as exc:  # noqa: BLE001
            failed += 1
            print(f"  ✗  {exc}")

    _con(f"Done — {ok} updated, {failed} failed.")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: create
# ─────────────────────────────────────────────────────────────────────────────

def cmd_create(args: argparse.Namespace, conn: Connection) -> None:
    """
    Create any of the 7 subscription types using the typed .create() class methods.
    Confirms payload before POST unless --no-confirm.
    """
    _section(f"CREATE  —  {args.delivery_type}  —  project: {args.project}")

    # Build shared content object
    content = SubContent(
        id   = args.content_id,
        type = SubContent.Type(args.content_type.upper()),
        personalization = SubContent.Properties(
            format_type = SubContent.Properties.FormatType(
                (args.format_type or "PDF").upper()
            )
        ),
    )

    common = dict(
        connection   = conn,
        name         = args.name,
        project_name = args.project,
        contents     = content,
        schedules    = [args.schedule_id],
        send_now     = args.send_now,
    )

    if args.delivery_type == "EMAIL":
        create_kwargs = dict(**common,
            recipients    = [args.recipient_id],
            email_subject = args.subject  or args.name,
            filename      = args.filename or args.name,
        )
        cls = EmailSubscription

    elif args.delivery_type == "FILE":
        create_kwargs = dict(**common,
            recipients = [args.recipient_id],
            filename   = args.filename or args.name,
        )
        cls = FileSubscription

    elif args.delivery_type == "FTP":
        create_kwargs = dict(**common,
            recipients = [args.recipient_id],
            filename   = args.filename or args.name,
        )
        cls = FTPSubscription

    elif args.delivery_type == "HISTORY_LIST":
        create_kwargs = dict(**common,
            recipients                  = [args.recipient_id],
            do_not_create_update_caches = False,
            overwrite_older_version     = True,
            re_run_hl                   = True,
        )
        cls = HistoryListSubscription

    elif args.delivery_type == "MOBILE":
        if not args.device_id or not args.library_url:
            print("[ERROR]  MOBILE requires --device-id and --library-url.")
            sys.exit(1)
        create_kwargs = dict(**common,
            recipients = [args.recipient_id],
            device_id  = args.device_id,
        )
        cls = MobileSubscription

    elif args.delivery_type == "CACHE":
        from mstrio.distribution_services import CacheType as CType
        create_kwargs = dict(**common,
            cache_cache_type = CType.RESERVED,
        )
        if args.recipient_id:
            create_kwargs["recipients"] = [args.recipient_id]
        cls = CacheUpdateSubscription

    else:
        print(f"[ERROR]  Unknown delivery type '{args.delivery_type}'.")
        sys.exit(1)

    print(f"  Name         : {args.name}")
    print(f"  Content      : {args.content_type} {args.content_id}")
    print(f"  Schedule     : {args.schedule_id}")
    if args.recipient_id:
        print(f"  Recipient    : {args.recipient_id}")

    if args.dry_run:
        _con(f"DRY-RUN — would call {cls.__name__}.create(). No API call made.")
        return

    if not _confirm(f"Create {args.delivery_type} subscription '{args.name}'?",
                    args.no_confirm):
        print("[ABORTED]")
        return

    created = cls.create(**create_kwargs)
    _con(f"✓ Created — ID: {created.id}")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: copy
# ─────────────────────────────────────────────────────────────────────────────

def cmd_copy(args: argparse.Namespace, conn: Connection) -> None:
    """
    Clone a subscription using manager.create_copy().
    Can copy to a different project with --target-project.
    """
    _section(f"COPY  —  project: {args.project}")

    sub = get_sub_by_id(conn, args.project, args.sub_id)
    _print_sub(sub)
    target_project = args.target_project or args.project
    new_name       = args.new_name or f"{sub.name} (copy)"

    print(f"\n  Copy to project : {target_project}")
    print(f"  New name        : {new_name}")
    if args.send_now:
        print(f"  Execute after copy: YES")

    if args.dry_run:
        _con("DRY-RUN — would call manager.create_copy(). No API call made.")
        return

    if not _confirm(f"Clone '{sub.name}' → '{new_name}'?", args.no_confirm):
        print("[ABORTED]")
        return

    manager = get_manager(conn, args.project)
    copy = manager.create_copy(
        subscription  = sub,
        name          = new_name,
        project_name  = target_project,
        send_now      = args.send_now,
    )
    _con(f"✓ Cloned — new ID: {copy.id}  name: '{copy.name}'")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: recipients
# ─────────────────────────────────────────────────────────────────────────────

def cmd_recipients(args: argparse.Namespace, conn: Connection) -> None:
    """
    add    → sub.add_recipient(recipient_id, recipient_type, include_type)
    remove → sub.remove_recipient(recipient_id)
    list   → sub.available_recipients()  [who can be added]
    show   → print current recipients on this subscription
    """
    _section(f"RECIPIENTS  —  {args.action.upper()}  —  project: {args.project}")

    sub = get_sub_by_id(conn, args.project, args.sub_id)
    _print_sub(sub)

    if args.action == "show":
        recipients = getattr(sub, "recipients", []) or []
        print(f"\n  Current recipients ({len(recipients)}):")
        for r in recipients:
            r_id   = getattr(r, "id",          r.get("id",          "?") if isinstance(r, dict) else "?")
            r_name = getattr(r, "name",        r.get("name",        "?") if isinstance(r, dict) else "?")
            r_type = getattr(r, "type",        r.get("type",        "?") if isinstance(r, dict) else "?")
            r_incl = getattr(r, "include_type",r.get("includeType", "?") if isinstance(r, dict) else "?")
            print(f"     {r_id[:16]}…  {r_name:<30}  {r_type}  {r_incl}")
        return

    if args.action == "list":
        available = sub.available_recipients()
        print(f"\n  Available recipients ({len(available)}):")
        for r in available[:50]:
            print(f"     {r.get('id','?')[:16]}…  {r.get('name','?'):<30}  {r.get('type','?')}")
        if len(available) > 50:
            print(f"     … {len(available)-50} more")
        return

    if args.action == "add":
        if not args.recipient_id:
            print("[ERROR]  --recipient-id required for add.")
            sys.exit(1)
        if args.dry_run:
            _con(f"DRY-RUN — would add {args.recipient_id} as {args.recipient_type}.")
            return
        sub.add_recipient(
            recipient_id          = args.recipient_id,
            recipient_type        = args.recipient_type or "USER",
            recipient_include_type= args.include_type  or "TO",
        )
        _con(f"✓ Recipient {args.recipient_id} added.")
        return

    if args.action == "remove":
        if not args.recipient_id:
            print("[ERROR]  --recipient-id required for remove.")
            sys.exit(1)
        if args.dry_run:
            _con(f"DRY-RUN — would remove {args.recipient_id}.")
            return
        sub.remove_recipient(recipient_id=args.recipient_id)
        _con(f"✓ Recipient {args.recipient_id} removed.")
        return

    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: audit
# ─────────────────────────────────────────────────────────────────────────────

def cmd_audit(args: argparse.Namespace, conn: Connection) -> None:
    """
    sub.fetch_all_change_journal_entries()
    Returns the full history of changes made to this subscription:
    who changed what, when, old value, new value.
    """
    _section(f"AUDIT TRAIL  —  project: {args.project}")

    sub = get_sub_by_id(conn, args.project, args.sub_id)
    _print_sub(sub)

    _con("Fetching change journal …")
    sub.fetch_all_change_journal_entries()

    entries = getattr(sub, "change_journal_entries", []) or []
    if not entries:
        _con("No change journal entries found.")
        return

    print(f"\n  {len(entries)} change(s) recorded:\n")
    for e in entries:
        ts     = getattr(e, "date",       getattr(e, "timestamp", "?"))
        author = getattr(e, "author",     getattr(e, "user",      "?"))
        a_name = getattr(author, "name",  str(author)) if author else "?"
        field  = getattr(e, "field",      getattr(e, "property", "?"))
        old    = getattr(e, "old_value",  getattr(e, "oldValue",  ""))
        new    = getattr(e, "new_value",  getattr(e, "newValue",  ""))
        print(f"  {str(ts)[:19]}  {a_name:<25}  {field}")
        if old or new:
            print(f"               {old!r}  →  {new!r}")

    if args.export:
        with open(args.export, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=["timestamp","author","field","old","new"])
            w.writeheader()
            for e in entries:
                ts     = getattr(e, "date", getattr(e, "timestamp", ""))
                author = getattr(e, "author", getattr(e, "user", ""))
                w.writerow({
                    "timestamp": str(ts),
                    "author":    getattr(author, "name", str(author)),
                    "field":     getattr(e, "field",     getattr(e, "property", "")),
                    "old":       getattr(e, "old_value", getattr(e, "oldValue",  "")),
                    "new":       getattr(e, "new_value", getattr(e, "newValue",  "")),
                })
        _con(f"Exported → {args.export}")

    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: bursting
# ─────────────────────────────────────────────────────────────────────────────

def cmd_bursting(args: argparse.Namespace, conn: Connection) -> None:
    """
    sub.available_bursting_attributes()
    Lists attributes that can be used to burst (personalise) this subscription
    per recipient — e.g. burst by Region so each user gets their own slice.
    """
    _section(f"BURSTING ATTRIBUTES  —  project: {args.project}")

    sub = get_sub_by_id(conn, args.project, args.sub_id)
    _print_sub(sub)

    _con("Fetching available bursting attributes …")
    attrs = sub.available_bursting_attributes()

    if not attrs:
        _con("No bursting attributes available for this subscription's content.")
        return

    if isinstance(attrs, dict):
        attrs_list = attrs.get("attributes", attrs.get("result", list(attrs.values())))
    elif isinstance(attrs, list):
        attrs_list = attrs
    else:
        attrs_list = [attrs]

    print(f"\n  {len(attrs_list)} attribute(s) available for bursting:\n")
    print(f"  {'ID':<36}  NAME")
    _div()
    for a in attrs_list:
        a_id   = a.get("id",   "?") if isinstance(a, dict) else getattr(a, "id",   "?")
        a_name = a.get("name", "?") if isinstance(a, dict) else getattr(a, "name", "?")
        print(f"  {a_id:<36}  {a_name}")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: drl (Dynamic Recipient Lists)
# ─────────────────────────────────────────────────────────────────────────────

def cmd_drl(args: argparse.Namespace, conn: Connection) -> None:
    """
    Dynamic Recipient Lists — data-driven recipient management.
    A DRL is backed by a report — each row in the report becomes a recipient.
    Actions: list | create | delete
    """
    _section(f"DYNAMIC RECIPIENT LISTS  —  {args.action.upper()}  —  {args.project}")

    if args.action == "list":
        _con("Fetching DRLs …")
        drls = list_dynamic_recipient_lists(
            connection   = conn,
            project_name = args.project,
        )
        if not drls:
            _con("No Dynamic Recipient Lists found.")
            return
        print(f"\n  {len(drls)} DRL(s) found:\n")
        print(f"  {'ID':<36}  {'NAME':<40}  SOURCE_REPORT_ID")
        _div()
        for d in drls:
            d_id    = getattr(d, "id",             "?")
            d_name  = getattr(d, "name",           "?")
            d_src   = getattr(d, "source_report_id", "?")
            print(f"  {d_id:<36}  {d_name[:38]:<40}  {d_src}")
        return

    if args.action == "create":
        required = ["drl_name", "source_report_id",
                    "phys_addr_attr", "phys_addr_form",
                    "linked_user_attr", "linked_user_form",
                    "device_attr", "device_form"]
        missing = [r for r in required if not getattr(args, r, None)]
        if missing:
            print(f"[ERROR]  Missing for DRL create: {missing}")
            sys.exit(1)

        if args.dry_run:
            _con("DRY-RUN — would call DynamicRecipientList.create().")
            return

        drl = DynamicRecipientList.create(
            connection       = conn,
            name             = args.drl_name,
            project_name     = args.project,
            source_report_id = args.source_report_id,
            physical_address = DynamicRecipientList.MappingField(
                attribute_id      = args.phys_addr_attr,
                attribute_form_id = args.phys_addr_form,
            ),
            linked_user = DynamicRecipientList.MappingField(
                attribute_id      = args.linked_user_attr,
                attribute_form_id = args.linked_user_form,
            ),
            device = DynamicRecipientList.MappingField(
                attribute_id      = args.device_attr,
                attribute_form_id = args.device_form,
            ),
        )
        _con(f"✓ DRL created — ID: {drl.id}  name: '{drl.name}'")
        return

    if args.action == "delete":
        if not args.drl_id:
            print("[ERROR]  --drl-id required for delete.")
            sys.exit(1)
        drl = DynamicRecipientList(
            connection   = conn,
            id           = args.drl_id,
            project_name = args.project,
        )
        if not _confirm(f"Delete DRL '{args.drl_id}'?", args.no_confirm):
            print("[ABORTED]")
            return
        drl.delete(force=True)
        _con(f"✓ DRL {args.drl_id} deleted.")

    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: status
# ─────────────────────────────────────────────────────────────────────────────

def cmd_status(args: argparse.Namespace, conn: Connection) -> None:
    """
    Delivery history for a subscription.
    Uses conn.get('/v2/historyList') directly — no mstrio-py wrapper yet.

    With --trigger: fires the subscription, watches the job monitor,
    then reads the history list for the final outcome.
    Without --trigger: just shows last N history records.
    """
    _section(f"STATUS  —  project: {args.project}")

    sub = get_sub_by_id(conn, args.project, args.sub_id)
    _print_sub(sub)

    contents     = getattr(sub, "contents", []) or []
    content_name = getattr(contents[0], "name", "") if contents else ""
    owner_id_val = _owner_id(sub)

    def _get_history(content_name_: str, limit_: int = 20) -> list:
        try:
            resp = conn.get(
                endpoint = "/v2/historyList",
                params   = {
                    "scope":     "all_users",
                    "projectId": conn.project_id,
                    "limit":     limit_,
                    **({"targetInfo.name": content_name_} if content_name_ else {}),
                    **({"ownerId": owner_id_val} if owner_id_val else {}),
                },
            )
            resp.raise_for_status()
            data = resp.json()
            return (
                data.get("historyList", []) if isinstance(data, dict)
                else data if isinstance(data, list)
                else []
            )
        except Exception as exc:  # noqa: BLE001
            _con(f"History list error: {exc}", "WARN")
            return []

    def _outcome(msg: dict) -> str:
        text = (msg.get("messageText") or "").lower()
        if "completed" in text:
            return "✓ DELIVERED"
        if any(k in text for k in ["failed","error","unable","timeout","denied"]):
            return "✗ FAILED"
        if isinstance(msg.get("requestStatus"), int):
            return "✓ DELIVERED" if msg["requestStatus"] <= 1 else "✗ FAILED"
        return "? UNKNOWN"

    # ── History-only mode ─────────────────────────────────────────────────────
    if not args.trigger:
        last_n  = args.last or 10
        records = _get_history(content_name, last_n * 2)
        records.sort(key=lambda m: m.get("startTime", ""), reverse=True)
        records = records[:last_n]

        if not records:
            _con("No delivery records found.")
            return

        print(f"\n  Last {len(records)} delivery record(s) for '{content_name}':\n")
        for i, msg in enumerate(records, 1):
            outcome  = _outcome(msg)
            start    = msg.get("startTime",  "?")[:19]
            finish   = msg.get("finishTime", "?")[:19]
            text     = msg.get("messageText", "")[:80]
            duration = "?"
            try:
                from datetime import datetime as _dt
                s = _dt.fromisoformat(msg["startTime"].replace("+0000","+00:00"))
                f = _dt.fromisoformat(msg["finishTime"].replace("+0000","+00:00"))
                duration = f"{int((f-s).total_seconds())}s"
            except Exception:
                pass
            print(f"  [{i:02d}] {outcome:<15}  Start: {start}  "
                  f"Finish: {finish}  ({duration})")
            if text and "FAILED" in outcome or "UNKNOWN" in outcome:
                print(f"        Detail: {text}")
        _div("═")
        return

    # ── Trigger + watch mode ──────────────────────────────────────────────────
    if not _confirm(f"Trigger '{sub.name}' and watch for delivery?",
                    args.no_confirm):
        print("[ABORTED]")
        return

    triggered_at = datetime.now(tz=timezone.utc)
    _con(f"Executing subscription …", "TRIGGER")
    sub.execute()
    _con("Accepted — watching job monitor …", "TRIGGER")

    # Poll job monitor via conn.get()
    elapsed      = 0
    job_found    = False
    job_vanished = False
    timeout      = args.timeout or 180
    poll         = args.poll_interval or 5

    while elapsed < timeout:
        time.sleep(poll)
        elapsed += poll
        try:
            resp  = conn.get(
                endpoint = f"/monitors/projects/{conn.project_id}/jobs",
                params   = {"limit": 200},
            )
            resp.raise_for_status()
            data  = resp.json()
            jobs  = data.get("jobs", []) if isinstance(data, dict) else data
            match = next((
                j for j in jobs
                if content_name.lower() in
                   (j.get("objectName") or j.get("name") or "").lower()
                and (lambda t: not t or t >= triggered_at)(
                    __import__("datetime").datetime.fromisoformat(
                        (j.get("startTime","") or "").replace("+0000","+00:00")
                        or "2000-01-01T00:00:00+00:00"
                    )
                )
            ), None)
        except Exception:
            match = None

        if match:
            job_found = True
            status    = match.get("status", "?")
            _con(f"[{elapsed:04d}s] Job found — status: {status}", "PHASE1")
        elif job_found:
            _con(f"[{elapsed:04d}s] Job vanished → execution finished", "PHASE1")
            job_vanished = True
            break
        else:
            _con(f"[{elapsed:04d}s] Waiting for job …", "PHASE1")

    if not job_vanished:
        _con(f"Timed out after {timeout}s.", "WARN")
    else:
        _con(f"Pausing {3}s then checking history …", "PHASE2")
        time.sleep(3)
        records = _get_history(content_name, 10)
        fresh   = [
            m for m in records
            if (t := m.get("startTime")) and
               __import__("datetime").datetime.fromisoformat(
                   t.replace("+0000","+00:00")) >= triggered_at
        ] or (sorted(records, key=lambda m: m.get("startTime",""), reverse=True)[:1])

        if fresh:
            best    = fresh[0]
            outcome = _outcome(best)
            dur_s   = "?"
            try:
                s = __import__("datetime").datetime.fromisoformat(
                    best["startTime"].replace("+0000","+00:00"))
                f = __import__("datetime").datetime.fromisoformat(
                    best["finishTime"].replace("+0000","+00:00"))
                dur_s = f"{int((f-s).total_seconds())}s"
            except Exception:
                pass
            wall = int((datetime.now(tz=timezone.utc)-triggered_at).total_seconds())
            print(f"\n  {'═'*60}")
            print(f"  RESULT:  {outcome}")
            print(f"  Execution: {dur_s}  Wall-clock: {wall}s")
            if "FAILED" in outcome:
                print(f"  Error: {best.get('messageText','')}")
            print(f"  {'═'*60}")
        else:
            _con("No history record found — possible silent crash.", "CRASH")

    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# Subcommand: prompts
# ─────────────────────────────────────────────────────────────────────────────

def cmd_prompts(args: argparse.Namespace, conn: Connection) -> None:
    """
    sub.answer_prompts(prompt_answers, force=True)
    Pre-answer prompts on a prompted subscription so it delivers without
    user interaction. Requires a Prompt object per prompt on the content.
    Shows current prompt state with --show only.
    """
    from mstrio.distribution_services.subscription.content import Content as C

    _section(f"PROMPTS  —  project: {args.project}")

    sub = get_sub_by_id(conn, args.project, args.sub_id)
    _print_sub(sub)

    contents = getattr(sub, "contents", []) or []
    if not contents:
        _con("No content found on subscription.")
        return

    all_prompts = []
    for c in contents:
        pp = getattr(getattr(c, "personalization", None), "prompt", None)
        if pp:
            all_prompts.append(pp)

    if args.show or not args.prompt_id:
        if not all_prompts:
            _con("No prompted content on this subscription.")
        else:
            print(f"\n  Prompts ({len(all_prompts)}):")
            for p in all_prompts:
                print(f"     instanceId : {getattr(p, 'instance_id', getattr(p, 'instanceId', '?'))}")
                print(f"     enabled    : {getattr(p, 'enabled', '?')}")
        return

    if args.dry_run:
        _con(f"DRY-RUN — would answer prompt {args.prompt_id}.")
        return

    prompt_obj = C.Properties.Prompt(
        instance_id = args.prompt_id,
        enabled     = True,
    )
    result = sub.answer_prompts(prompt_answers=[prompt_obj], force=args.force)
    _con(f"{'✓ Prompts answered.' if result else '✗ answer_prompts returned False.'}")
    _div("═")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(
        prog        = "mstr_subscription_admin_mstrio.py",
        description = "Strategy ONE — Subscription Admin  (mstrio-py 11.6.5.101)",
        formatter_class = argparse.ArgumentDefaultsHelpFormatter,
    )
    root.add_argument("--base-url",   default=DEFAULT_BASE_URL)
    root.add_argument("--username",   default=DEFAULT_USERNAME)
    root.add_argument("--password",   default=DEFAULT_PASSWORD)
    root.add_argument("--login-mode", type=int, default=DEFAULT_LOGIN_MODE,
        help="1=standard, 16=LDAP.")

    subs = root.add_subparsers(dest="command", required=True)

    def _proj(p):
        p.add_argument("--project", required=True)

    def _sub_id(p, required=False):
        p.add_argument("--sub-id", required=required, dest="sub_id")

    def _dry(p):
        p.add_argument("--dry-run",    action="store_true")
        p.add_argument("--no-confirm", action="store_true")

    def _filters(p):
        p.add_argument("--owner-id",      default=None, dest="owner_id")
        p.add_argument("--delivery-type", default=None, dest="delivery_type",
            choices=["EMAIL","FILE","FTP","CACHE","HISTORY_LIST","MOBILE"])
        p.add_argument("--schedule-type", default=None, dest="schedule_type",
            choices=["time_based","event_based"])
        p.add_argument("--stale-days",    default=None, type=int, dest="stale_days")
        p.add_argument("--type", default=None,
            choices=["EMAIL","FILE","FTP","CACHE","HISTORY_LIST","MOBILE"])

    # ── list ──────────────────────────────────────────────────────────────────
    p_list = subs.add_parser("list", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="List subscriptions with filters, last-run, CSV export.")
    _proj(p_list); _filters(p_list)
    p_list.add_argument("--last-run", action="store_true", dest="last_run",
        help="Include last execution timestamp.")
    p_list.add_argument("--export", default=None, metavar="FILE.csv")

    # ── delete ────────────────────────────────────────────────────────────────
    p_del = subs.add_parser("delete", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Bulk delete by filter. Auto CSV backup before deletion.")
    _proj(p_del); _filters(p_del); _dry(p_del)

    # ── execute ───────────────────────────────────────────────────────────────
    p_exe = subs.add_parser("execute", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Fire one or more subscriptions immediately.")
    _proj(p_exe); _sub_id(p_exe); _filters(p_exe); _dry(p_exe)
    p_exe.add_argument("--send-now", action="store_true", dest="send_now")

    # ── chown ─────────────────────────────────────────────────────────────────
    p_chown = subs.add_parser("chown", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Change subscription owner via sub.alter(owner_id=...).")
    _proj(p_chown); _sub_id(p_chown); _dry(p_chown)
    p_chown.add_argument("--from-owner-id",      default=None, dest="from_owner_id")
    p_chown.add_argument("--new-owner-id",        required=True, dest="new_owner_id")
    p_chown.add_argument("--include-object-acl",  action="store_true", dest="include_object_acl",
        help="For CACHE subs: also transfer underlying object ACL (needs --from-owner-id).")

    # ── update ────────────────────────────────────────────────────────────────
    p_upd = subs.add_parser("update", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Update any subscription field(s) via sub.alter().")
    _proj(p_upd); _sub_id(p_upd); _filters(p_upd); _dry(p_upd)
    p_upd.add_argument("--new-name",          default=None, dest="new_name")
    p_upd.add_argument("--schedule-id",       default=None, dest="schedule_id")
    p_upd.add_argument("--start-date",        default=None, dest="start_date", metavar="YYYY-MM-DD")
    p_upd.add_argument("--expiry-date",       default=None, dest="expiry_date", metavar="YYYY-MM-DD")
    p_upd.add_argument("--expiry-tz",         default=None, dest="expiry_tz")
    p_upd.add_argument("--email-subject",     default=None, dest="email_subject")
    p_upd.add_argument("--filename",          default=None)
    p_upd.add_argument("--zip-filename",      default=None, dest="zip_filename")
    p_upd.add_argument("--zip-password",      default=None, dest="zip_password")
    p_upd.add_argument("--zip-password-protect", default=None, dest="zip_password_protect",
        type=lambda x: x.lower() == "true")
    p_upd.add_argument("--cache-type",        default=None, dest="cache_type",
        choices=["RESERVED","SHORTCUT","SHORTCUTWITHBOOKMARK"])
    p_upd.add_argument("--mobile-client-type",default=None, dest="mobile_client_type",
        choices=["PHONE","TABLET"])
    p_upd.add_argument("--device-id",         default=None, dest="device_id")
    p_upd.add_argument("--send-now",          action="store_true", dest="send_now")
    p_upd.add_argument("--allow-unsub",       default=None, dest="allow_unsub",
        type=lambda x: x.lower() == "true")
    p_upd.add_argument("--overwrite",         default=None,
        type=lambda x: x.lower() == "true")
    p_upd.add_argument("--rerun-hl",          default=None, dest="rerun_hl",
        type=lambda x: x.lower() == "true")
    p_upd.add_argument("--notify",            default=None,
        type=lambda x: x.lower() == "true")

    # ── create ────────────────────────────────────────────────────────────────
    p_cre = subs.add_parser("create", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Create any subscription type via typed .create() methods.")
    _proj(p_cre); _dry(p_cre)
    p_cre.add_argument("--delivery-type", required=True, dest="delivery_type",
        choices=list(SUB_TYPE_MAP.keys()))
    p_cre.add_argument("--name",          required=True)
    p_cre.add_argument("--schedule-id",   required=True, dest="schedule_id")
    p_cre.add_argument("--content-id",    required=True, dest="content_id")
    p_cre.add_argument("--content-type",  required=True, dest="content_type",
        choices=["report","document","dossier"])
    p_cre.add_argument("--recipient-id",  default=None, dest="recipient_id")
    p_cre.add_argument("--format-type",   default="PDF", dest="format_type",
        choices=["PDF","HTML","EXCEL","CSV","PLAIN_TEXT"])
    p_cre.add_argument("--subject",       default=None)
    p_cre.add_argument("--filename",      default=None)
    p_cre.add_argument("--device-id",     default=None, dest="device_id")
    p_cre.add_argument("--library-url",   default=None, dest="library_url")
    p_cre.add_argument("--send-now",      action="store_true", dest="send_now")

    # ── copy ──────────────────────────────────────────────────────────────────
    p_copy = subs.add_parser("copy", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Clone a subscription (manager.create_copy).")
    _proj(p_copy); _sub_id(p_copy, required=True); _dry(p_copy)
    p_copy.add_argument("--new-name",       default=None, dest="new_name")
    p_copy.add_argument("--target-project", default=None, dest="target_project")
    p_copy.add_argument("--send-now",       action="store_true", dest="send_now")

    # ── recipients ────────────────────────────────────────────────────────────
    p_rec = subs.add_parser("recipients", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Add / remove / list / show recipients.")
    _proj(p_rec); _sub_id(p_rec, required=True); _dry(p_rec)
    p_rec.add_argument("--action", required=True, choices=["add","remove","list","show"])
    p_rec.add_argument("--recipient-id",   default=None, dest="recipient_id")
    p_rec.add_argument("--recipient-type", default="USER", dest="recipient_type",
        choices=["USER","USER_GROUP","CONTACT","CONTACT_GROUP","PERSONAL_ADDRESS"])
    p_rec.add_argument("--include-type",   default="TO",  dest="include_type",
        choices=["TO","CC","BCC"])

    # ── audit ─────────────────────────────────────────────────────────────────
    p_aud = subs.add_parser("audit", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Fetch change journal entries for a subscription.")
    _proj(p_aud); _sub_id(p_aud, required=True)
    p_aud.add_argument("--export", default=None, metavar="FILE.csv")

    # ── bursting ──────────────────────────────────────────────────────────────
    p_bur = subs.add_parser("bursting", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="List available bursting attributes for a subscription's content.")
    _proj(p_bur); _sub_id(p_bur, required=True)

    # ── drl ───────────────────────────────────────────────────────────────────
    p_drl = subs.add_parser("drl", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Dynamic Recipient List management.")
    _proj(p_drl); _dry(p_drl)
    p_drl.add_argument("--action", required=True, choices=["list","create","delete"])
    p_drl.add_argument("--drl-id",          default=None, dest="drl_id")
    p_drl.add_argument("--drl-name",        default=None, dest="drl_name")
    p_drl.add_argument("--source-report-id",default=None, dest="source_report_id")
    p_drl.add_argument("--phys-addr-attr",  default=None, dest="phys_addr_attr")
    p_drl.add_argument("--phys-addr-form",  default=None, dest="phys_addr_form")
    p_drl.add_argument("--linked-user-attr",default=None, dest="linked_user_attr")
    p_drl.add_argument("--linked-user-form",default=None, dest="linked_user_form")
    p_drl.add_argument("--device-attr",     default=None, dest="device_attr")
    p_drl.add_argument("--device-form",     default=None, dest="device_form")

    # ── status ────────────────────────────────────────────────────────────────
    p_stat = subs.add_parser("status", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Delivery history check via /v2/historyList. Optionally trigger+watch.")
    _proj(p_stat); _sub_id(p_stat, required=True)
    p_stat.add_argument("--trigger",      action="store_true")
    p_stat.add_argument("--last",         type=int, default=10)
    p_stat.add_argument("--timeout",      type=int, default=180)
    p_stat.add_argument("--poll-interval",type=int, default=5,  dest="poll_interval")
    p_stat.add_argument("--no-confirm",   action="store_true")

    # ── prompts ───────────────────────────────────────────────────────────────
    p_pmt = subs.add_parser("prompts", formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        help="Answer prompts on a prompted subscription.")
    _proj(p_pmt); _sub_id(p_pmt, required=True); _dry(p_pmt)
    p_pmt.add_argument("--prompt-id", default=None, dest="prompt_id",
        help="Prompt instance ID to answer.")
    p_pmt.add_argument("--show",  action="store_true",
        help="Show current prompt state only.")
    p_pmt.add_argument("--force", action="store_true",
        help="Overwrite existing prompt answers.")

    return root


def main() -> None:
    parser = build_parser()
    args   = parser.parse_args()

    project = getattr(args, "project", "")

    print("=" * 66)
    print("  Strategy ONE — Subscription Admin  [mstrio-py 11.6.5.101]")
    print(f"  Command  : {args.command.upper()}")
    print(f"  Project  : {project}")
    if getattr(args, "dry_run", False):
        print("  Mode     : DRY-RUN")
    print("=" * 66)

    conn = None
    try:
        conn = make_connection(
            args.base_url, args.username, args.password,
            args.login_mode, project,
        )

        dispatch = {
            "list":       cmd_list,
            "delete":     cmd_delete,
            "execute":    cmd_execute,
            "chown":      cmd_chown,
            "update":     cmd_update,
            "create":     cmd_create,
            "copy":       cmd_copy,
            "recipients": cmd_recipients,
            "audit":      cmd_audit,
            "bursting":   cmd_bursting,
            "drl":        cmd_drl,
            "status":     cmd_status,
            "prompts":    cmd_prompts,
        }
        dispatch[args.command](args, conn)

    except Exception as exc:  # noqa: BLE001
        print(f"\n[ERROR]  {exc}")
        sys.exit(1)
    finally:
        if conn:
            try:
                conn.close()
                _con("Connection closed.", "AUTH")
            except Exception:  # noqa: BLE001
                pass


if __name__ == "__main__":
    main()
