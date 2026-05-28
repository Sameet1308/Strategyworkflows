"""
mstr_job_tracker.py
────────────────────────────────────────────────────────────────────────────────
Strategy ONE — Subscription Job Tracker
Triggers a subscription, watches the job through its full lifecycle,
reads the history list for the final outcome, and writes a structured
log entry for every run — especially errors.

Flow:
  1. POST /api/subscriptions/{id}/send
  2. Poll GET /api/monitors/projects/{id}/jobs  (Phase 1 — job monitor)
  3. Once job vanishes → GET /api/v2/historyList  (Phase 2 — history list)
  4. Verdict: DELIVERED | FAILED | SILENT_CRASH
  5. Write JSON log entry to ./mstr_job_tracker.log

Usage
─────
  # Trigger and watch (interactive confirm)
  python mstr_job_tracker.py --project "BI_RMIS" --sub-id "64E9995E4908CC6B"

  # No confirm — for automation / pipelines
  python mstr_job_tracker.py --project "BI_RMIS" --sub-id "64E9995E4908CC6B" \
      --no-confirm

  # Custom timeout and poll speed
  python mstr_job_tracker.py --project "BI_RMIS" --sub-id "64E9995E4908CC6B" \
      --timeout 300 --poll-interval 3

  # Custom log file location
  python mstr_job_tracker.py --project "BI_RMIS" --sub-id "64E9995E4908CC6B" \
      --log-file /var/log/mstr/job_tracker.log

  # Print last N log entries
  python mstr_job_tracker.py --show-log --last 20

Environment variables:
  MSTR_BASE_URL     e.g. https://your-dev-server.com/MicroStrategyLibrarySTD
  MSTR_USERNAME
  MSTR_PASSWORD
  MSTR_LOGIN_MODE   1=standard (default), 16=LDAP
────────────────────────────────────────────────────────────────────────────────
"""

import argparse
import json
import os
import sys
import time
import urllib3
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_BASE_URL   = os.getenv("MSTR_BASE_URL",    "https://your-dev-server.com/MicroStrategyLibrarySTD")
DEFAULT_USERNAME   = os.getenv("MSTR_USERNAME",    "service_account_placeholder")
DEFAULT_PASSWORD   = os.getenv("MSTR_PASSWORD",    "password_placeholder")
DEFAULT_LOGIN_MODE = int(os.getenv("MSTR_LOGIN_MODE", "1"))
DEFAULT_LOG_FILE   = os.getenv("MSTR_LOG_FILE",    "mstr_job_tracker.log")

REQUEST_TIMEOUT = 30
PAGE_LIMIT      = 200
HISTORY_WAIT_S  = 3      # seconds to wait after job vanishes before querying history


# ── Verdict constants ─────────────────────────────────────────────────────────
DELIVERED    = "DELIVERED"
FAILED       = "FAILED"
SILENT_CRASH = "SILENT_CRASH"
TIMED_OUT    = "TIMED_OUT"
UNKNOWN      = "UNKNOWN"


# ─────────────────────────────────────────────────────────────────────────────
# Log entry dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class JobLogEntry:
    """One log record written per tracker run."""
    # When
    run_id:              str  = ""
    triggered_at:        str  = ""
    finished_at:         str  = ""

    # What
    subscription_id:     str  = ""
    subscription_name:   str  = ""
    project_name:        str  = ""
    project_id:          str  = ""
    content_name:        str  = ""
    content_id:          str  = ""
    delivery_mode:       str  = ""
    owner_name:          str  = ""

    # Outcome
    verdict:             str  = UNKNOWN
    job_id:              str  = ""
    job_start:           str  = ""
    job_duration_s:      int  = 0
    wall_clock_s:        int  = 0

    # Error details
    error_message:       str  = ""
    error_code:          str  = ""
    history_message_id:  str  = ""

    # Phase tracking
    phase1_job_seen:     bool = False
    phase2_record_found: bool = False

    # Cluster health snapshot (filled on SILENT_CRASH only)
    cluster_check:       dict = field(default_factory=dict)

    # Raw history record (for deep debugging)
    raw_history:         dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Session / auth
# ─────────────────────────────────────────────────────────────────────────────

def _api(base_url: str) -> str:
    return base_url.rstrip("/") + "/api"


def create_session() -> requests.Session:
    """
    REQUIRED: Session carries both X-MSTR-AuthToken AND session cookies.
    Never use bare requests.get/post — that drops cookies and causes ERR009.
    """
    s = requests.Session()
    s.verify = False
    s.headers.update({"Accept": "application/json"})
    return s


def login(base_url: str, session: requests.Session,
          username: str, password: str, login_mode: int) -> None:
    """POST /api/auth/login — pins token onto session headers."""
    resp = session.post(
        f"{_api(base_url)}/auth/login",
        json={"username": username, "password": password, "loginMode": login_mode},
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    token = resp.headers.get("X-MSTR-AuthToken")
    if not token:
        raise RuntimeError("Login succeeded but X-MSTR-AuthToken was absent.")
    session.headers.update({"X-MSTR-AuthToken": token})
    _log_console("Logged in as '{}'".format(username))


def logout(base_url: str, session: requests.Session) -> None:
    """POST /api/auth/logout"""
    try:
        session.post(f"{_api(base_url)}/auth/logout", timeout=15)
        _log_console("Session closed")
    except Exception:  # noqa: BLE001
        pass


# ─────────────────────────────────────────────────────────────────────────────
# API helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_projects(base_url: str, session: requests.Session) -> list[dict]:
    resp = session.get(f"{_api(base_url)}/projects", timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    return data if isinstance(data, list) else data.get("projects", [])


def resolve_project_id(base_url: str, session: requests.Session,
                       project_name: str) -> str:
    projects = get_projects(base_url, session)
    for p in projects:
        if p["name"].strip().lower() == project_name.strip().lower():
            return p["id"]
    raise ValueError(
        "Project '{}' not found. Available: {}".format(
            project_name, [p["name"] for p in projects]
        )
    )


def get_subscription(base_url: str, session: requests.Session,
                     project_id: str, sub_id: str) -> dict:
    """GET /api/subscriptions/{id}"""
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


def send_subscription(base_url: str, session: requests.Session,
                      project_id: str, sub_id: str) -> None:
    """POST /api/subscriptions/{id}/send — async, server returns 202."""
    session.headers.update({"X-MSTR-ProjectID": project_id})
    try:
        resp = session.post(
            f"{_api(base_url)}/subscriptions/{sub_id}/send",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
    finally:
        session.headers.pop("X-MSTR-ProjectID", None)


def get_jobs(base_url: str, session: requests.Session,
             project_id: str) -> list[dict]:
    """
    GET /api/monitors/projects/{projectId}/jobs
    Returns ONLY currently active (running/queued) jobs.
    Jobs disappear from this endpoint the moment they complete.
    """
    url    = f"{_api(base_url)}/monitors/projects/{project_id}/jobs"
    params = {"limit": PAGE_LIMIT, "offset": 0}
    all_jobs: list[dict] = []
    while True:
        resp = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data  = resp.json()
        batch = (
            data.get("jobs", []) if isinstance(data, dict)
            else data if isinstance(data, list)
            else []
        )
        all_jobs.extend(batch)
        if len(batch) < PAGE_LIMIT:
            break
        params["offset"] += PAGE_LIMIT
    return all_jobs


def get_history_list(base_url: str, session: requests.Session,
                     project_id: str,
                     content_name: Optional[str] = None,
                     owner_id:     Optional[str] = None,
                     limit:        int            = 20) -> list[dict]:
    """
    GET /api/v2/historyList
    scope=all_users lets admin see all users' completed delivery records.
    Available since 2021 Update 8.
    """
    params: dict = {
        "scope":     "all_users",
        "projectId": project_id,
        "limit":     limit,
        "offset":    0,
    }
    if content_name:
        params["targetInfo.name"] = content_name
    if owner_id:
        params["ownerId"] = owner_id

    session.headers.update({"X-MSTR-ProjectID": project_id})
    try:
        resp = session.get(
            f"{_api(base_url)}/v2/historyList",
            params=params,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        return (
            data.get("historyList", []) if isinstance(data, dict)
            else data if isinstance(data, list)
            else []
        )
    finally:
        session.headers.pop("X-MSTR-ProjectID", None)


def get_cluster_health(base_url: str, session: requests.Session,
                       project_id: str) -> dict:
    """
    GET /api/monitors/iServer/nodes
    Returns compact health snapshot used when a silent crash is detected.
    """
    try:
        resp = session.get(
            f"{_api(base_url)}/monitors/iServer/nodes",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        nodes      = resp.json().get("nodes", [])
        running    = [n for n in nodes if n.get("status") == "running"]
        loaded_on  = [
            n["name"] for n in nodes
            for p in n.get("projects", [])
            if p.get("id") == project_id and p.get("status") == "loaded"
        ]
        return {
            "nodes_total":       len(nodes),
            "nodes_running":     len(running),
            "project_loaded_on": loaded_on,
            "healthy":           len(running) > 0 and len(loaded_on) > 0,
        }
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc), "healthy": None}


# ─────────────────────────────────────────────────────────────────────────────
# Pure logic helpers
# ─────────────────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_time(raw: Optional[str]) -> Optional[datetime]:
    if not raw:
        return None
    try:
        dt = datetime.fromisoformat(
            raw.replace("+0000", "+00:00").replace("Z", "+00:00")
        )
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _find_job(jobs: list[dict], content_name: str,
              triggered_after: datetime) -> Optional[dict]:
    """
    Match a live job to our subscription by content name (fuzzy)
    and start time >= trigger time.
    """
    for job in jobs:
        name = (job.get("objectName") or job.get("name") or "").strip()
        if not name:
            continue
        name_match = (
            content_name.lower() in name.lower() or
            name.lower() in content_name.lower()
        )
        if not name_match:
            continue
        start = _parse_time(job.get("startTime") or job.get("createTime"))
        if start and start < triggered_after:
            continue   # pre-existing job — skip
        return job
    return None


def _read_outcome(msg: dict) -> tuple[str, str, str]:
    """
    Parse a history list message into (verdict, error_message, error_code).

    Three outcomes:
      DELIVERED    — messageText says completed
      FAILED       — messageText contains error language
      UNKNOWN      — can't determine from available fields
    """
    text   = (msg.get("messageText") or "").lower().strip()
    status = msg.get("requestStatus")

    # Explicit success
    if "completed" in text:
        return DELIVERED, "", ""

    # Explicit failure keywords
    error_keywords = (
        "failed", "error", "unable", "exception",
        "timeout", "timed out", "invalid", "denied",
        "could not", "cannot", "no privilege"
    )
    if any(k in text for k in error_keywords):
        return FAILED, msg.get("messageText", ""), str(status or "")

    # Numeric requestStatus
    if isinstance(status, int):
        return (DELIVERED, "", "") if status <= 1 else (
            FAILED, msg.get("messageText", "requestStatus={}".format(status)), str(status)
        )

    # Non-empty unrecognised text — lean towards delivered
    if text:
        return DELIVERED, "", ""

    return UNKNOWN, "", str(status or "")


def _duration_s(start_raw: Optional[str], finish_raw: Optional[str]) -> int:
    s = _parse_time(start_raw)
    f = _parse_time(finish_raw)
    return max(0, int((f - s).total_seconds())) if s and f else 0


# ─────────────────────────────────────────────────────────────────────────────
# Console output helpers
# ─────────────────────────────────────────────────────────────────────────────

def _log_console(msg: str, prefix: str = "INFO") -> None:
    ts = _now_utc().strftime("%H:%M:%S")
    print("  [{}] [{:<12}]  {}".format(ts, prefix, msg))


def _banner(text: str, char: str = "─", width: int = 62) -> None:
    print("\n" + char * width)
    print("  " + text)
    print(char * width)


def _print_verdict(entry: JobLogEntry) -> None:
    icons = {
        DELIVERED:    "✓",
        FAILED:       "✗",
        SILENT_CRASH: "✗",
        TIMED_OUT:    "⏱",
        UNKNOWN:      "?",
    }
    icon = icons.get(entry.verdict, "?")
    _banner("RESULT:  {}  {}".format(icon, entry.verdict), "═")
    print("  Subscription   : {}".format(entry.subscription_name))
    print("  Content        : {}".format(entry.content_name))
    print("  Delivery mode  : {}".format(entry.delivery_mode))
    print("  Job ID         : {}".format(entry.job_id or "(not captured)"))
    print("  Execution time : {}s".format(entry.job_duration_s))
    print("  Wall-clock     : {}s  (trigger → verdict)".format(entry.wall_clock_s))

    if entry.error_message:
        print("\n  ERROR DETAIL:")
        print("     {}".format(entry.error_message))
    if entry.error_code:
        print("     Code: {}".format(entry.error_code))

    if entry.verdict == SILENT_CRASH:
        h = entry.cluster_check
        print("\n  CLUSTER HEALTH (at time of crash):")
        print("     Nodes running       : {}/{}".format(
            h.get("nodes_running", "?"), h.get("nodes_total", "?")))
        print("     Project loaded on   : {}".format(
            h.get("project_loaded_on", "?")))
        healthy = h.get("healthy")
        print("     Healthy             : {}".format(
            "YES" if healthy else "NO — likely cause of silent crash"))

    print("\n  Log entry  : {}".format(entry.run_id))
    print("═" * 62)


# ─────────────────────────────────────────────────────────────────────────────
# Log file (JSONL — one JSON object per line)
# ─────────────────────────────────────────────────────────────────────────────

def write_log(entry: JobLogEntry, log_file: str) -> None:
    """
    Append one JSON line to the log file.
    JSONL format — easy to grep, tail, or ingest into
    Splunk / CloudWatch / ELK / any log aggregator.
    """
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry)) + "\n")
    _log_console("Logged to {}".format(path.resolve()))


def read_log(log_file: str, last_n: int = 10) -> None:
    """Print last N log entries in readable format."""
    path = Path(log_file)
    if not path.exists():
        print("[INFO]  Log file not found: {}".format(path))
        return

    lines   = [l for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    entries = lines[-last_n:]

    print("\n" + "═" * 62)
    print("  LAST {} LOG ENTRIES  ←  {}".format(len(entries), path.resolve()))
    print("═" * 62)

    icons = {
        DELIVERED:    "✓",
        FAILED:       "✗",
        SILENT_CRASH: "✗",
        TIMED_OUT:    "⏱",
        UNKNOWN:      "?",
    }

    for raw in entries:
        try:
            e = json.loads(raw)
        except json.JSONDecodeError:
            continue
        verdict = e.get("verdict", "?")
        icon    = icons.get(verdict, "?")
        print("\n  {} {:<14}  {}  [{}s wall]".format(
            icon,
            verdict,
            e.get("triggered_at", "?")[:19],
            e.get("wall_clock_s", "?"),
        ))
        print("     Sub  : {}".format(e.get("subscription_name", "?")))
        print("     Cont : {}".format(e.get("content_name", "?")))
        print("     Mode : {}".format(e.get("delivery_mode", "?")))
        print("     Job  : {}".format(e.get("job_id") or "(not captured)"))
        if e.get("error_message"):
            print("     ERR  : {}".format(e["error_message"]))
        if e.get("error_code"):
            print("     CODE : {}".format(e["error_code"]))

    print("\n" + "═" * 62 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Main tracker
# ─────────────────────────────────────────────────────────────────────────────

def track(base_url: str, session: requests.Session,
          project_name: str, sub_id: str,
          timeout: int, poll_interval: int,
          log_file: str) -> JobLogEntry:
    """
    Full three-step lifecycle tracker.
    Returns a completed JobLogEntry and writes it to log_file.
    """
    run_id       = _now_utc().strftime("RUN-%Y%m%d-%H%M%S")
    triggered_at = _now_utc()

    entry = JobLogEntry(
        run_id          = run_id,
        triggered_at    = _iso(triggered_at),
        project_name    = project_name,
        subscription_id = sub_id,
    )

    # ── Resolve project ───────────────────────────────────────────────────────
    _log_console("Resolving project …")
    project_id       = resolve_project_id(base_url, session, project_name)
    entry.project_id = project_id

    # ── Fetch subscription metadata ───────────────────────────────────────────
    _log_console("Fetching subscription {} …".format(sub_id))
    sub          = get_subscription(base_url, session, project_id, sub_id)
    contents     = sub.get("contents") or []
    content_name = (contents[0].get("name") or "") if contents else ""
    content_id   = (contents[0].get("id")   or "") if contents else ""
    owner_id     = (sub.get("owner") or {}).get("id")

    entry.subscription_name = sub.get("name", sub_id)
    entry.content_name      = content_name
    entry.content_id        = content_id
    entry.delivery_mode     = (sub.get("delivery") or {}).get("mode", "UNKNOWN")
    entry.owner_name        = (sub.get("owner") or {}).get("name", "")

    _log_console("Subscription : '{}'".format(entry.subscription_name))
    _log_console("Content      : '{}'".format(content_name))
    _log_console("Mode         : {}".format(entry.delivery_mode))

    # ── STEP 1 — Trigger ─────────────────────────────────────────────────────
    _banner("STEP 1 — Triggering subscription")
    _log_console("POST /subscriptions/{}/send …".format(sub_id), "SEND")

    try:
        send_subscription(base_url, session, project_id, sub_id)
        triggered_at       = _now_utc()          # re-stamp after send returns
        entry.triggered_at = _iso(triggered_at)
        _log_console("Accepted (HTTP 202) — job queued", "SEND")
    except requests.HTTPError as exc:
        entry.verdict       = FAILED
        entry.error_message = "POST /send failed: HTTP {} — {}".format(
            exc.response.status_code, exc.response.text[:200]
        )
        entry.finished_at   = _iso(_now_utc())
        entry.wall_clock_s  = int((_now_utc() - triggered_at).total_seconds())
        _log_console(entry.error_message, "ERROR")
        write_log(entry, log_file)
        return entry

    # ── STEP 2 — Phase 1: job monitor ────────────────────────────────────────
    _banner("STEP 2 — Watching Job Monitor (Phase 1)")
    _log_console("Polling every {}s  |  timeout: {}s".format(poll_interval, timeout))
    _log_console("Matching content: '{}'".format(content_name))

    job_found    = False
    job_vanished = False
    elapsed      = 0
    last_status  = ""

    while elapsed < timeout:
        time.sleep(poll_interval)
        elapsed += poll_interval

        try:
            jobs = get_jobs(base_url, session, project_id)
        except requests.HTTPError as exc:
            _log_console("Job monitor poll error: {} — retrying".format(exc), "WARN")
            continue

        matched = _find_job(jobs, content_name, triggered_at)

        if matched:
            job_found             = True
            entry.phase1_job_seen = True
            entry.job_id          = matched.get("id", "")
            entry.job_start       = (
                matched.get("startTime") or matched.get("createTime") or ""
            )
            curr_status = matched.get("status", "?")
            if curr_status != last_status:
                _log_console(
                    "Job {}…  status: {}".format(entry.job_id[:16], curr_status),
                    "JOB"
                )
                last_status = curr_status

        elif job_found:
            _log_console(
                "Job vanished from monitor after {}s — execution finished".format(elapsed),
                "JOB"
            )
            job_vanished = True
            break

        else:
            _log_console("[{:04d}s] waiting for job to appear …".format(elapsed))

    # Handle timeout
    if not job_vanished:
        if job_found:
            _log_console("Job still running after {}s — TIMED_OUT".format(timeout), "WARN")
        else:
            _log_console(
                "Job never appeared in monitor within {}s".format(timeout), "WARN"
            )
        entry.verdict      = TIMED_OUT
        entry.finished_at  = _iso(_now_utc())
        entry.wall_clock_s = elapsed
        write_log(entry, log_file)
        return entry

    # ── STEP 3 — Phase 2: history list ───────────────────────────────────────
    _banner("STEP 3 — Reading History List (Phase 2)")
    _log_console("Waiting {}s for history record …".format(HISTORY_WAIT_S))
    time.sleep(HISTORY_WAIT_S)

    records = get_history_list(
        base_url, session, project_id,
        content_name = content_name or None,
        owner_id     = owner_id,
        limit        = 20,
    )

    # Keep only records that started at or after our trigger
    fresh = [
        m for m in records
        if (t := _parse_time(m.get("startTime"))) and t >= triggered_at
    ]

    # Fallback: if nothing strictly newer, use most recent record
    if not fresh and records:
        records.sort(key=lambda m: m.get("startTime", ""), reverse=True)
        fresh = [records[0]]

    # ── STEP 4 — Verdict ─────────────────────────────────────────────────────
    _banner("STEP 4 — Verdict")

    finished_at        = _now_utc()
    entry.finished_at  = _iso(finished_at)
    entry.wall_clock_s = int((finished_at - triggered_at).total_seconds())

    if not fresh:
        # ── SILENT CRASH ─────────────────────────────────────────────────────
        entry.verdict             = SILENT_CRASH
        entry.phase2_record_found = False
        entry.error_message       = (
            "Job disappeared from monitor but no history record was found. "
            "Job likely crashed before completion."
        )
        _log_console("No history record found — checking cluster health …", "CRASH")
        entry.cluster_check = get_cluster_health(base_url, session, project_id)
        h = entry.cluster_check
        if not h.get("healthy"):
            entry.error_message += (
                " Cluster: {}/{} nodes running. Project loaded on: {}.".format(
                    h.get("nodes_running", 0),
                    h.get("nodes_total", 0),
                    h.get("project_loaded_on", []),
                )
            )

    else:
        # ── DELIVERED or FAILED ───────────────────────────────────────────────
        fresh.sort(key=lambda m: m.get("startTime", ""), reverse=True)
        best = fresh[0]

        entry.phase2_record_found = True
        entry.history_message_id  = best.get("messageId", "")
        entry.job_duration_s      = _duration_s(
            best.get("startTime"), best.get("finishTime")
        )
        entry.raw_history = best

        verdict, err_msg, err_code = _read_outcome(best)
        entry.verdict       = verdict
        entry.error_message = err_msg
        entry.error_code    = err_code

        _log_console(
            "requestStatus={}  messageText='{}'".format(
                best.get("requestStatus"),
                (best.get("messageText") or "")[:80],
            ),
            verdict,
        )

    # ── Write log ─────────────────────────────────────────────────────────────
    write_log(entry, log_file)
    return entry


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Strategy ONE — Subscription Job Tracker",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--base-url",      default=DEFAULT_BASE_URL,
        help="Base URL stopping at /MicroStrategyLibrarySTD.")
    p.add_argument("--username",      default=DEFAULT_USERNAME,
        help="Service account username.")
    p.add_argument("--password",      default=DEFAULT_PASSWORD,
        help="Service account password.")
    p.add_argument("--login-mode",    type=int, default=DEFAULT_LOGIN_MODE,
        help="1=standard, 16=LDAP.")
    p.add_argument("--project",       default=None,
        help="Project name (required unless --show-log).")
    p.add_argument("--sub-id",        default=None, dest="sub_id",
        help="Subscription ID to trigger and track.")
    p.add_argument("--timeout",       type=int, default=180,
        help="Max seconds to watch Phase 1 before TIMED_OUT.")
    p.add_argument("--poll-interval", type=int, default=5, dest="poll_interval",
        help="Seconds between job monitor polls.")
    p.add_argument("--log-file",      default=DEFAULT_LOG_FILE, dest="log_file",
        help="Path to the JSONL log file.")
    p.add_argument("--no-confirm",    action="store_true",
        help="Skip interactive YES/abort prompt.")
    p.add_argument("--show-log",      action="store_true", dest="show_log",
        help="Print recent log entries and exit.")
    p.add_argument("--last",          type=int, default=10,
        help="With --show-log: number of entries to show.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.show_log:
        read_log(args.log_file, args.last)
        return

    if not args.project or not args.sub_id:
        print("[ERROR]  --project and --sub-id are required.")
        sys.exit(1)

    print("=" * 62)
    print("  Strategy ONE — Subscription Job Tracker")
    print("  Project  : {}".format(args.project))
    print("  Sub ID   : {}".format(args.sub_id))
    print("  Timeout  : {}s  |  Poll: {}s".format(args.timeout, args.poll_interval))
    print("  Log file : {}".format(args.log_file))
    print("=" * 62)

    if not args.no_confirm:
        answer = input(
            "\n  Type  YES  to trigger and track this subscription: "
        ).strip()
        if answer != "YES":
            print("[ABORTED]")
            return

    session = None
    try:
        session = create_session()
        login(args.base_url, session,
              args.username, args.password, args.login_mode)

        entry = track(
            base_url      = args.base_url,
            session       = session,
            project_name  = args.project,
            sub_id        = args.sub_id,
            timeout       = args.timeout,
            poll_interval = args.poll_interval,
            log_file      = args.log_file,
        )

        _print_verdict(entry)

        # Non-zero exit code on anything other than DELIVERED — useful in pipelines
        if entry.verdict != DELIVERED:
            sys.exit(1)

    except requests.HTTPError as exc:
        print("[ERROR]  HTTP {}: {}".format(
            exc.response.status_code, exc.response.text[:300]))
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print("[ERROR]  {}".format(exc))
        sys.exit(1)
    finally:
        if session:
            logout(args.base_url, session)


if __name__ == "__main__":
    main()
