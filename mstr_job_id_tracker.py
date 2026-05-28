"""
mstr_job_id_tracker.py
────────────────────────────────────────────────────────────────────────────────
Strategy ONE — Job ID Tracker
Track any job by its ID through its full lifecycle and log the outcome.

Unlike mstr_job_tracker.py (which triggers a subscription then watches),
this script starts from a job ID that already exists — either currently
running or recently completed — and tells you exactly what happened to it.

Two scenarios handled:
  A. Job is still running when you start tracking
     → Find it in the monitor by ID
     → Capture content name + start time
     → Watch until it vanishes
     → Search history list for the final record
     → Verdict: DELIVERED / FAILED / SILENT_CRASH

  B. Job already finished before you start tracking
     → Not found in monitor
     → Search history list in a time window around when the job ran
     → Match by content name (if provided) or return all recent records
     → Verdict: DELIVERED / FAILED / NOT_FOUND

Limitation:
  The MSTR history list API has no jobId filter.
  Correlation is done via content object name + start timestamp proximity.
  Providing --content-name improves match accuracy when the job is already gone.

Usage
─────
  # Track a running or recently completed job
  python mstr_job_id_tracker.py \
      --project "BI_RMIS" \
      --job-id "D7A3BC9E11D5C49EC0000C881FDA1A4F"

  # Help match if job already finished — provide the content name
  python mstr_job_id_tracker.py \
      --project "BI_RMIS" \
      --job-id "D7A3BC9E11D5C49EC0000C881FDA1A4F" \
      --content-name "Unit Tracking by Supplier"

  # Track multiple job IDs at once
  python mstr_job_id_tracker.py \
      --project "BI_RMIS" \
      --job-id "JOB1ID" --job-id "JOB2ID" --job-id "JOB3ID"

  # Load job IDs from a file (one per line)
  python mstr_job_id_tracker.py \
      --project "BI_RMIS" \
      --job-file jobs_to_track.txt

  # Look back further for already-finished jobs (default 30 min)
  python mstr_job_id_tracker.py \
      --project "BI_RMIS" \
      --job-id "D7A3BC9E11D5C49EC0000C881FDA1A4F" \
      --lookback-minutes 120

  # View recent log entries
  python mstr_job_id_tracker.py --show-log --last 20

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
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_BASE_URL   = os.getenv("MSTR_BASE_URL",    "https://your-dev-server.com/MicroStrategyLibrarySTD")
DEFAULT_USERNAME   = os.getenv("MSTR_USERNAME",    "service_account_placeholder")
DEFAULT_PASSWORD   = os.getenv("MSTR_PASSWORD",    "password_placeholder")
DEFAULT_LOGIN_MODE = int(os.getenv("MSTR_LOGIN_MODE", "1"))
DEFAULT_LOG_FILE   = os.getenv("MSTR_LOG_FILE",    "mstr_job_id_tracker.log")

REQUEST_TIMEOUT    = 30
PAGE_LIMIT         = 200
HISTORY_WAIT_S     = 3       # seconds to pause before querying history
HISTORY_MATCH_WINDOW_S = 120 # seconds either side of job startTime for history match

# ── Verdict constants ─────────────────────────────────────────────────────────
DELIVERED    = "DELIVERED"
FAILED       = "FAILED"
SILENT_CRASH = "SILENT_CRASH"
TIMED_OUT    = "TIMED_OUT"
NOT_FOUND    = "NOT_FOUND"
UNKNOWN      = "UNKNOWN"


# ─────────────────────────────────────────────────────────────────────────────
# Log entry
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class JobTrackEntry:
    """One record per job ID tracked."""
    # Identity
    run_id:              str  = ""
    tracked_at:          str  = ""
    job_id:              str  = ""
    project_name:        str  = ""
    project_id:          str  = ""

    # Captured from monitor (Scenario A only)
    content_name:        str  = ""
    content_id:          str  = ""
    job_owner:           str  = ""
    job_status_snapshot: str  = ""   # last status seen in monitor
    job_start_time:      str  = ""

    # Tracking path
    scenario:            str  = ""   # "A_running" | "B_already_finished"
    phase1_found:        bool = False
    phase1_watch_s:      int  = 0
    phase2_record_found: bool = False

    # Final outcome
    verdict:             str  = UNKNOWN
    job_duration_s:      int  = 0
    wall_clock_s:        int  = 0

    # Error details
    error_message:       str  = ""
    error_code:          str  = ""

    # Cluster health (silent crash only)
    cluster_check:       dict = field(default_factory=dict)

    # Raw history record
    raw_history:         dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Session / auth
# ─────────────────────────────────────────────────────────────────────────────

def _api(base_url: str) -> str:
    return base_url.rstrip("/") + "/api"


def create_session() -> requests.Session:
    """
    Session carries both X-MSTR-AuthToken AND cookies.
    Never use bare requests.get/post — drops cookies → ERR009.
    """
    s = requests.Session()
    s.verify = False
    s.headers.update({"Accept": "application/json"})
    return s


def login(base_url: str, session: requests.Session,
          username: str, password: str, login_mode: int) -> None:
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
    _con("Logged in as '{}'".format(username))


def logout(base_url: str, session: requests.Session) -> None:
    try:
        session.post(f"{_api(base_url)}/auth/logout", timeout=15)
        _con("Session closed")
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
    for p in get_projects(base_url, session):
        if p["name"].strip().lower() == project_name.strip().lower():
            return p["id"]
    raise ValueError("Project '{}' not found.".format(project_name))


def get_all_jobs(base_url: str, session: requests.Session,
                 project_id: str) -> list[dict]:
    """
    GET /api/monitors/projects/{projectId}/jobs
    Returns all CURRENTLY ACTIVE jobs for the project.
    A job disappears from this endpoint the moment it finishes.
    Response shape handled defensively.
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


def find_job_by_id(base_url: str, session: requests.Session,
                   project_id: str, job_id: str) -> Optional[dict]:
    """
    Scan the live job monitor and return the job with the given ID, or None.
    Returns None if the job is not currently running
    (completed, never existed, or belongs to a different project).
    """
    jobs = get_all_jobs(base_url, session, project_id)
    for job in jobs:
        if job.get("id", "").lower() == job_id.lower():
            return job
    return None


def get_history_list(base_url: str, session: requests.Session,
                     project_id: str,
                     content_name: Optional[str] = None,
                     owner_id:     Optional[str] = None,
                     limit:        int            = 30) -> list[dict]:
    """
    GET /api/v2/historyList
    scope=all_users gives admin view of all users' completed delivery records.
    NOTE: No jobId filter exists in this API — correlation is by content name
    and timestamp proximity.
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
    """GET /api/monitors/iServer/nodes — compact health snapshot."""
    try:
        resp = session.get(
            f"{_api(base_url)}/monitors/iServer/nodes",
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        nodes     = resp.json().get("nodes", [])
        running   = [n for n in nodes if n.get("status") == "running"]
        loaded_on = [
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


def _duration_s(start_raw: Optional[str], finish_raw: Optional[str]) -> int:
    s = _parse_time(start_raw)
    f = _parse_time(finish_raw)
    return max(0, int((f - s).total_seconds())) if s and f else 0


def _read_outcome(msg: dict) -> tuple[str, str, str]:
    """
    Parse a history list record into (verdict, error_message, error_code).

    Three outcomes:
      DELIVERED — messageText says completed
      FAILED    — messageText contains error language or requestStatus > 1
      UNKNOWN   — cannot determine
    """
    text   = (msg.get("messageText") or "").lower().strip()
    status = msg.get("requestStatus")

    if "completed" in text:
        return DELIVERED, "", ""

    error_keywords = (
        "failed", "error", "unable", "exception",
        "timeout", "timed out", "invalid", "denied",
        "could not", "cannot", "no privilege",
    )
    if any(k in text for k in error_keywords):
        return FAILED, msg.get("messageText", ""), str(status or "")

    if isinstance(status, int):
        return (
            (DELIVERED, "", "") if status <= 1
            else (FAILED,
                  msg.get("messageText", "requestStatus={}".format(status)),
                  str(status))
        )

    return (DELIVERED, "", "") if text else (UNKNOWN, "", str(status or ""))


def _best_history_match(records: list[dict],
                        job_start_time: Optional[str],
                        content_name:   str,
                        window_s:       int = HISTORY_MATCH_WINDOW_S) -> Optional[dict]:
    """
    Find the history record that best matches our job.

    Matching strategy (in order of confidence):
      1. Content name match + start time within ±window_s of job start
      2. Content name match + most recent record
      3. Most recent record overall (fallback when no content name known)
    """
    if not records:
        return None

    job_start = _parse_time(job_start_time)

    # Score each record
    scored: list[tuple[int, dict]] = []
    for rec in records:
        rec_name  = (rec.get("targetInfo") or {}).get("name", "")
        rec_start = _parse_time(rec.get("startTime"))
        score     = 0

        # Name match
        if content_name and rec_name:
            if (content_name.lower() in rec_name.lower() or
                    rec_name.lower() in content_name.lower()):
                score += 10

        # Time proximity
        if job_start and rec_start:
            delta = abs((rec_start - job_start).total_seconds())
            if delta <= window_s:
                score += 5
            if delta <= 30:
                score += 5     # tight match bonus

        scored.append((score, rec))

    # Sort by score desc, then by startTime desc (most recent first)
    scored.sort(
        key=lambda x: (x[0], x[1].get("startTime", "")),
        reverse=True,
    )

    best_score, best_rec = scored[0]

    # Only return if we have at least some confidence
    if best_score == 0 and content_name:
        # We have a content name but nothing matched — return None
        return None

    return best_rec


# ─────────────────────────────────────────────────────────────────────────────
# Console helpers
# ─────────────────────────────────────────────────────────────────────────────

def _con(msg: str, tag: str = "INFO") -> None:
    ts = _now_utc().strftime("%H:%M:%S")
    print("  [{}] [{:<14}]  {}".format(ts, tag, msg))


def _section(title: str) -> None:
    print("\n" + "─" * 64)
    print("  {}".format(title))
    print("─" * 64)


def _print_job_snapshot(job: dict) -> None:
    print("     ID      : {}".format(job.get("id", "?")))
    print("     Name    : {}".format(job.get("objectName") or job.get("name", "?")))
    print("     Status  : {}".format(job.get("status", "?")))
    print("     Start   : {}".format(job.get("startTime") or job.get("createTime", "?")))
    print("     Owner   : {}".format(job.get("userName") or job.get("userId", "?")))


def _print_verdict(entry: JobTrackEntry) -> None:
    icons = {
        DELIVERED:    "✓",
        FAILED:       "✗",
        SILENT_CRASH: "✗",
        TIMED_OUT:    "⏱",
        NOT_FOUND:    "∅",
        UNKNOWN:      "?",
    }
    icon = icons.get(entry.verdict, "?")
    print("\n" + "═" * 64)
    print("  RESULT:  {}  {}".format(icon, entry.verdict))
    print("═" * 64)
    print("  Job ID         : {}".format(entry.job_id))
    print("  Content        : {}".format(entry.content_name or "(unknown)"))
    print("  Scenario       : {}".format(entry.scenario))
    print("  Phase 1 (monitor) found  : {}".format("YES" if entry.phase1_found else "NO"))
    print("  Phase 2 (history) found  : {}".format("YES" if entry.phase2_record_found else "NO"))
    print("  Execution time : {}s".format(entry.job_duration_s))
    print("  Wall-clock     : {}s".format(entry.wall_clock_s))

    if entry.error_message:
        print("\n  ERROR:")
        print("     {}".format(entry.error_message))
    if entry.error_code:
        print("     Code: {}".format(entry.error_code))

    if entry.verdict == SILENT_CRASH:
        h = entry.cluster_check
        print("\n  CLUSTER HEALTH:")
        print("     Nodes running       : {}/{}".format(
            h.get("nodes_running","?"), h.get("nodes_total","?")))
        print("     Project loaded on   : {}".format(
            h.get("project_loaded_on","?")))
        print("     Healthy             : {}".format(
            "YES" if h.get("healthy") else "NO — likely cause of silent crash"))

    if entry.verdict == NOT_FOUND:
        print("\n  POSSIBLE REASONS:")
        print("     • Job ID belongs to a different project")
        print("     • Job finished before history window (try --lookback-minutes N)")
        print("     • Content name mismatch (try --content-name 'exact name')")
        print("     • History list was purged on this server")

    print("  Log entry      : {}".format(entry.run_id))
    print("═" * 64)


# ─────────────────────────────────────────────────────────────────────────────
# Log file (JSONL)
# ─────────────────────────────────────────────────────────────────────────────

def write_log(entry: JobTrackEntry, log_file: str) -> None:
    path = Path(log_file)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(asdict(entry)) + "\n")
    _con("Logged → {}".format(path.resolve()))


def read_log(log_file: str, last_n: int = 10) -> None:
    path = Path(log_file)
    if not path.exists():
        print("[INFO]  Log file not found: {}".format(path))
        return
    lines   = [l for l in path.read_text(encoding="utf-8").splitlines() if l.strip()]
    entries = lines[-last_n:]
    icons   = {DELIVERED:"✓", FAILED:"✗", SILENT_CRASH:"✗",
               TIMED_OUT:"⏱", NOT_FOUND:"∅", UNKNOWN:"?"}
    print("\n" + "═" * 64)
    print("  LAST {} LOG ENTRIES  ←  {}".format(len(entries), path.resolve()))
    print("═" * 64)
    for raw in entries:
        try:
            e = json.loads(raw)
        except json.JSONDecodeError:
            continue
        v    = e.get("verdict", "?")
        icon = icons.get(v, "?")
        print("\n  {} {:<14}  {}  [{}s wall]".format(
            icon, v, e.get("tracked_at","?")[:19], e.get("wall_clock_s","?")))
        print("     Job ID   : {}".format(e.get("job_id","?")))
        print("     Content  : {}".format(e.get("content_name") or "(unknown)"))
        print("     Scenario : {}".format(e.get("scenario","?")))
        if e.get("error_message"):
            print("     ERR      : {}".format(e["error_message"]))
        if e.get("error_code"):
            print("     CODE     : {}".format(e["error_code"]))
    print("\n" + "═" * 64 + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Core tracker — one job ID
# ─────────────────────────────────────────────────────────────────────────────

def track_job(base_url: str, session: requests.Session,
              project_name:     str,
              job_id:           str,
              content_name_hint: Optional[str],
              timeout:          int,
              poll_interval:    int,
              lookback_minutes: int,
              log_file:         str) -> JobTrackEntry:
    """
    Track a single job ID from wherever it is in its lifecycle.
    """
    started_at   = _now_utc()
    run_id       = started_at.strftime("RUN-%Y%m%d-%H%M%S-") + job_id[:8]

    entry = JobTrackEntry(
        run_id       = run_id,
        tracked_at   = _iso(started_at),
        job_id       = job_id,
        project_name = project_name,
    )

    # ── Resolve project ───────────────────────────────────────────────────────
    project_id       = resolve_project_id(base_url, session, project_name)
    entry.project_id = project_id

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 1 — Check job monitor
    # ─────────────────────────────────────────────────────────────────────────
    _section("PHASE 1  —  Job Monitor  (job ID: {}…)".format(job_id[:20]))
    _con("Looking for job in monitor …")

    job = find_job_by_id(base_url, session, project_id, job_id)

    if job:
        # ── SCENARIO A: job is currently running ──────────────────────────────
        entry.scenario            = "A_running"
        entry.phase1_found        = True
        entry.content_name        = (
            content_name_hint or
            job.get("objectName") or
            job.get("name", "")
        )
        entry.content_id          = job.get("objectId", "")
        entry.job_owner           = job.get("userName") or job.get("userId", "")
        entry.job_start_time      = (
            job.get("startTime") or job.get("createTime", "")
        )
        entry.job_status_snapshot = job.get("status", "")

        _con("SCENARIO A — Job is currently running", "FOUND")
        _print_job_snapshot(job)
        _con("Watching until it vanishes (timeout: {}s) …".format(timeout))

        elapsed     = 0
        last_status = entry.job_status_snapshot
        job_gone    = False

        while elapsed < timeout:
            time.sleep(poll_interval)
            elapsed += poll_interval

            current = find_job_by_id(base_url, session, project_id, job_id)

            if current:
                curr_status = current.get("status", "?")
                if curr_status != last_status:
                    _con("status changed: {} → {}".format(
                        last_status, curr_status), "JOB")
                    entry.job_status_snapshot = curr_status
                    last_status = curr_status
                else:
                    _con("[{:04d}s] still {} …".format(elapsed, curr_status))
            else:
                _con("Job vanished after {}s → execution finished".format(elapsed), "JOB")
                entry.phase1_watch_s = elapsed
                job_gone = True
                break

        if not job_gone:
            entry.verdict      = TIMED_OUT
            entry.wall_clock_s = elapsed
            entry.error_message = (
                "Job still {} in monitor after {}s timeout.".format(
                    last_status, timeout)
            )
            write_log(entry, log_file)
            return entry

    else:
        # ── SCENARIO B: job not in monitor — already finished ─────────────────
        entry.scenario     = "B_already_finished"
        entry.phase1_found = False
        entry.content_name = content_name_hint or ""
        _con("SCENARIO B — Job not found in monitor", "NOT RUNNING")
        _con("Job either finished before tracking started or belongs to a different project.")
        _con("Searching history list …")

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2 — History list
    # ─────────────────────────────────────────────────────────────────────────
    _section("PHASE 2  —  History List")

    # Brief pause for Scenario A so the server has time to write the record
    if entry.scenario == "A_running":
        _con("Pausing {}s for history record to be written …".format(HISTORY_WAIT_S))
        time.sleep(HISTORY_WAIT_S)

    # Fetch history — use content name hint for tighter results
    records = get_history_list(
        base_url, session, project_id,
        content_name = entry.content_name or None,
        limit        = 50,
    )

    # For Scenario B: filter to a lookback window from now
    if entry.scenario == "B_already_finished":
        cutoff  = _now_utc() - timedelta(minutes=lookback_minutes)
        records = [
            r for r in records
            if (t := _parse_time(r.get("startTime"))) and t >= cutoff
        ]
        _con("History records in last {} min: {}".format(
            lookback_minutes, len(records)))
    else:
        _con("History records fetched: {}".format(len(records)))

    # Match the best record to our job
    best = _best_history_match(
        records,
        job_start_time = entry.job_start_time or None,
        content_name   = entry.content_name,
    )

    # ─────────────────────────────────────────────────────────────────────────
    # VERDICT
    # ─────────────────────────────────────────────────────────────────────────
    finished_at        = _now_utc()
    entry.finished_at  = _iso(finished_at) if hasattr(entry, "finished_at") else ""
    entry.wall_clock_s = int((finished_at - started_at).total_seconds())

    if not best:
        if entry.scenario == "A_running":
            # Job ran, we watched it, but no history record → SILENT_CRASH
            entry.verdict             = SILENT_CRASH
            entry.phase2_record_found = False
            entry.error_message       = (
                "Job vanished from monitor but no matching history record found. "
                "Job likely crashed before writing completion record."
            )
            _con("No history record — checking cluster health …", "CRASH")
            entry.cluster_check = get_cluster_health(base_url, session, project_id)
            h = entry.cluster_check
            if not h.get("healthy"):
                entry.error_message += (
                    " Cluster: {}/{} nodes running. Loaded on: {}.".format(
                        h.get("nodes_running",0),
                        h.get("nodes_total",0),
                        h.get("project_loaded_on",[]),
                    )
                )
        else:
            # Scenario B + no history record → NOT_FOUND
            entry.verdict             = NOT_FOUND
            entry.phase2_record_found = False
            entry.error_message       = (
                "Job not in monitor and no history record found "
                "in last {} minutes.".format(lookback_minutes)
            )

    else:
        entry.phase2_record_found = True
        entry.raw_history         = best
        entry.job_duration_s      = _duration_s(
            best.get("startTime"), best.get("finishTime")
        )
        # Fill content name from history if we didn't know it before
        if not entry.content_name:
            entry.content_name = (best.get("targetInfo") or {}).get("name", "")

        verdict, err_msg, err_code = _read_outcome(best)
        entry.verdict       = verdict
        entry.error_message = err_msg
        entry.error_code    = err_code

        _con("requestStatus={}  messageText='{}'".format(
            best.get("requestStatus"),
            (best.get("messageText") or "")[:80],
        ), verdict)

        _con("Execution: {}s  |  Start: {}  |  Finish: {}".format(
            entry.job_duration_s,
            best.get("startTime","?")[:19],
            best.get("finishTime","?")[:19],
        ))

    write_log(entry, log_file)
    return entry


# ─────────────────────────────────────────────────────────────────────────────
# Batch tracker — multiple job IDs
# ─────────────────────────────────────────────────────────────────────────────

def track_batch(base_url: str, session: requests.Session,
                project_name:     str,
                job_ids:          list[str],
                content_name_hint: Optional[str],
                timeout:          int,
                poll_interval:    int,
                lookback_minutes: int,
                log_file:         str) -> list[JobTrackEntry]:
    """Track multiple job IDs sequentially and return all entries."""
    results: list[JobTrackEntry] = []
    total = len(job_ids)

    for idx, job_id in enumerate(job_ids, start=1):
        print("\n" + "█" * 64)
        print("  JOB {}/{}  —  {}".format(idx, total, job_id))
        print("█" * 64)

        entry = track_job(
            base_url          = base_url,
            session           = session,
            project_name      = project_name,
            job_id            = job_id,
            content_name_hint = content_name_hint,
            timeout           = timeout,
            poll_interval     = poll_interval,
            lookback_minutes  = lookback_minutes,
            log_file          = log_file,
        )
        _print_verdict(entry)
        results.append(entry)

    # Batch summary
    print("\n" + "═" * 64)
    print("  BATCH SUMMARY  ({} jobs)".format(total))
    print("═" * 64)
    icons = {DELIVERED:"✓", FAILED:"✗", SILENT_CRASH:"✗",
             TIMED_OUT:"⏱", NOT_FOUND:"∅", UNKNOWN:"?"}
    counts: dict[str, int] = {}
    for e in results:
        counts[e.verdict] = counts.get(e.verdict, 0) + 1
        icon = icons.get(e.verdict, "?")
        print("  {} {:<14}  {}  {}".format(
            icon, e.verdict,
            e.job_id[:20],
            "ERR: " + e.error_message[:50] if e.error_message else "",
        ))
    print()
    for verdict, count in sorted(counts.items()):
        print("  {:<14}: {}".format(verdict, count))
    print("═" * 64)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Strategy ONE — Track any job by its ID",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--base-url",         default=DEFAULT_BASE_URL)
    p.add_argument("--username",         default=DEFAULT_USERNAME)
    p.add_argument("--password",         default=DEFAULT_PASSWORD)
    p.add_argument("--login-mode",       type=int, default=DEFAULT_LOGIN_MODE,
        help="1=standard, 16=LDAP.")
    p.add_argument("--project",          default=None,
        help="Project name (required unless --show-log).")
    p.add_argument("--job-id",           action="append", dest="job_ids",
        metavar="JOB_ID",
        help="Job ID to track. Repeat for multiple: --job-id A --job-id B")
    p.add_argument("--job-file",         default=None, dest="job_file",
        metavar="FILE",
        help="Text file with one job ID per line.")
    p.add_argument("--content-name",     default=None, dest="content_name",
        help="Content name hint — improves history matching for finished jobs.")
    p.add_argument("--timeout",          type=int, default=180,
        help="Max seconds to watch a running job before TIMED_OUT.")
    p.add_argument("--poll-interval",    type=int, default=5, dest="poll_interval",
        help="Seconds between job monitor polls.")
    p.add_argument("--lookback-minutes", type=int, default=30, dest="lookback_minutes",
        help="How far back to search history for already-finished jobs.")
    p.add_argument("--log-file",         default=DEFAULT_LOG_FILE, dest="log_file")
    p.add_argument("--show-log",         action="store_true", dest="show_log")
    p.add_argument("--last",             type=int, default=10)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.show_log:
        read_log(args.log_file, args.last)
        return

    # Collect job IDs
    job_ids: list[str] = list(args.job_ids or [])
    if args.job_file:
        path = Path(args.job_file)
        if not path.exists():
            print("[ERROR]  --job-file not found: {}".format(path))
            sys.exit(1)
        for line in path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#"):
                job_ids.append(line)

    if not job_ids:
        print("[ERROR]  Provide at least one --job-id or a --job-file.")
        sys.exit(1)

    if not args.project:
        print("[ERROR]  --project is required.")
        sys.exit(1)

    print("=" * 64)
    print("  Strategy ONE — Job ID Tracker")
    print("  Project  : {}".format(args.project))
    print("  Jobs     : {}".format(len(job_ids)))
    print("  Timeout  : {}s  |  Poll: {}s  |  Lookback: {}min".format(
        args.timeout, args.poll_interval, args.lookback_minutes))
    print("  Log file : {}".format(args.log_file))
    print("=" * 64)

    session = None
    try:
        session = create_session()
        login(args.base_url, session,
              args.username, args.password, args.login_mode)

        if len(job_ids) == 1:
            entry = track_job(
                base_url          = args.base_url,
                session           = session,
                project_name      = args.project,
                job_id            = job_ids[0],
                content_name_hint = args.content_name,
                timeout           = args.timeout,
                poll_interval     = args.poll_interval,
                lookback_minutes  = args.lookback_minutes,
                log_file          = args.log_file,
            )
            _print_verdict(entry)
            if entry.verdict != DELIVERED:
                sys.exit(1)
        else:
            results = track_batch(
                base_url          = args.base_url,
                session           = session,
                project_name      = args.project,
                job_ids           = job_ids,
                content_name_hint = args.content_name,
                timeout           = args.timeout,
                poll_interval     = args.poll_interval,
                lookback_minutes  = args.lookback_minutes,
                log_file          = args.log_file,
            )
            # Exit 1 if any job failed
            if any(e.verdict != DELIVERED for e in results):
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
