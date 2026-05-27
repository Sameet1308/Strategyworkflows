"""
mstr_clear_jobs_connections.py
────────────────────────────────────────────────────────────────────────────────
Strategy ONE (Strategy REST API) — Server-wide Job & Connection Cleaner
Cancels all active jobs and closes all user connections across every project
on the server.

Required privileges on the service account:
  • Monitor cluster           (GET /api/monitors/iServer/nodes)
  • Monitor Jobs              (GET /api/monitors/projects/{id}/jobs)
  • Administer Job Monitor    (DELETE /api/monitors/projects/{id}/jobs/{jobId})
  • Monitor User Connections  (GET /api/monitors/projects/{id}/userConnections)
  • Administer User Monitor   (DELETE /api/monitors/projects/{id}/userConnections/{connId})

Usage examples
──────────────
  # Cancel everything on all projects (default — with confirmation prompt)
  python mstr_clear_jobs_connections.py

  # Skip the interactive prompt (use carefully in automation)
  python mstr_clear_jobs_connections.py --no-confirm

  # Target a single project by name
  python mstr_clear_jobs_connections.py --project "My Project"

  # Dry-run — report what would be cancelled without touching anything
  python mstr_clear_jobs_connections.py --dry-run

  # Only cancel jobs, leave connections alone
  python mstr_clear_jobs_connections.py --jobs-only

  # Only close connections, leave jobs alone
  python mstr_clear_jobs_connections.py --connections-only

Environment variables (preferred over hard-coding):
  MSTR_BASE_URL   e.g. https://your-dev-server.com/MicroStrategyLibrarySTD
  MSTR_USERNAME
  MSTR_PASSWORD
  MSTR_LOGIN_MODE 1=standard (default), 16=LDAP
────────────────────────────────────────────────────────────────────────────────
"""

import argparse
import os
import sys
import urllib3
from dataclasses import dataclass, field
from typing import Optional

import requests

# ── Suppress self-signed-cert warnings (verify=False is intentional) ──────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Defaults (override via env vars or CLI flags) ─────────────────────────────
DEFAULT_BASE_URL   = os.getenv("MSTR_BASE_URL",    "https://your-dev-server.com/MicroStrategyLibrarySTD")
DEFAULT_USERNAME   = os.getenv("MSTR_USERNAME",    "service_account_placeholder")
DEFAULT_PASSWORD   = os.getenv("MSTR_PASSWORD",    "password_placeholder")
DEFAULT_LOGIN_MODE = int(os.getenv("MSTR_LOGIN_MODE", "1"))   # 1=standard, 16=LDAP

REQUEST_TIMEOUT    = 30   # seconds per individual REST call
PAGE_LIMIT         = 200  # items per page for list endpoints

# ─────────────────────────────────────────────────────────────────────────────
# Data containers
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ClearSummary:
    """Accumulates per-run stats for final reporting."""
    jobs_found:        int = 0
    jobs_cancelled:    int = 0
    jobs_failed:       int = 0
    conns_found:       int = 0
    conns_closed:      int = 0
    conns_failed:      int = 0
    projects_scanned:  int = 0
    errors:            list = field(default_factory=list)


# ─────────────────────────────────────────────────────────────────────────────
# REST helper wrappers
# ─────────────────────────────────────────────────────────────────────────────

def _api(base_url: str) -> str:
    """Append /api to the base URL (base URL must NOT include /api)."""
    return base_url.rstrip("/") + "/api"


def login(base_url: str, username: str, password: str, login_mode: int) -> str:
    """
    POST /api/auth/login
    Returns the X-MSTR-AuthToken header value.
    """
    url = f"{_api(base_url)}/auth/login"
    payload = {"username": username, "password": password, "loginMode": login_mode}
    resp = requests.post(url, json=payload, verify=False, timeout=REQUEST_TIMEOUT)
    resp.raise_for_status()
    token = resp.headers.get("X-MSTR-AuthToken")
    if not token:
        raise RuntimeError("Login succeeded but X-MSTR-AuthToken header was absent.")
    print(f"[AUTH]   Logged in as '{username}' — token acquired.")
    return token


def logout(base_url: str, token: str) -> None:
    """POST /api/auth/logout"""
    try:
        requests.post(
            f"{_api(base_url)}/auth/logout",
            headers={"X-MSTR-AuthToken": token},
            verify=False,
            timeout=15,
        )
        print("[AUTH]   Session closed (logout).")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN]   Logout call failed (non-fatal): {exc}")


def _headers(token: str) -> dict:
    return {"X-MSTR-AuthToken": token, "Accept": "application/json"}


def get_projects(base_url: str, token: str) -> list[dict]:
    """
    GET /api/projects
    Returns list of { id, name, status, … } dicts for all accessible projects.
    status=0 means loaded and accessible.
    """
    resp = requests.get(
        f"{_api(base_url)}/projects",
        headers=_headers(token),
        verify=False,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json()   # list of project objects


def get_jobs(base_url: str, token: str, project_id: str) -> list[dict]:
    """
    GET /api/monitors/projects/{projectId}/jobs
    Returns all active jobs for the project (paginates automatically).
    Each job dict includes at least: id, status, objectName, userId.
    """
    url    = f"{_api(base_url)}/monitors/projects/{project_id}/jobs"
    params = {"limit": PAGE_LIMIT, "offset": 0}
    all_jobs: list[dict] = []
    while True:
        resp = requests.get(
            url,
            headers=_headers(token),
            params=params,
            verify=False,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
        batch = data.get("jobs", [])
        all_jobs.extend(batch)
        # Stop if we got fewer items than the page size (last page)
        if len(batch) < PAGE_LIMIT:
            break
        params["offset"] += PAGE_LIMIT
    return all_jobs


def cancel_job(base_url: str, token: str, project_id: str, job_id: str) -> None:
    """
    DELETE /api/monitors/projects/{projectId}/jobs/{jobId}
    Sends a cancel request for the specified job.
    Returns 204 No Content on success.
    """
    url = f"{_api(base_url)}/monitors/projects/{project_id}/jobs/{job_id}"
    resp = requests.delete(
        url,
        headers=_headers(token),
        verify=False,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()


def get_user_connections(base_url: str, token: str, project_id: str) -> list[dict]:
    """
    GET /api/monitors/projects/{projectId}/userConnections
    Returns all active user connections for the project (paginates automatically).
    Each connection dict includes at least: id, username, clientType, lastActivityTime.
    """
    url    = f"{_api(base_url)}/monitors/projects/{project_id}/userConnections"
    params = {"limit": PAGE_LIMIT, "offset": 0}
    all_conns: list[dict] = []
    while True:
        resp = requests.get(
            url,
            headers=_headers(token),
            params=params,
            verify=False,
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data  = resp.json()
        batch = data.get("userConnections", [])
        all_conns.extend(batch)
        if len(batch) < PAGE_LIMIT:
            break
        params["offset"] += PAGE_LIMIT
    return all_conns


def close_connection(base_url: str, token: str, project_id: str, conn_id: str) -> None:
    """
    DELETE /api/monitors/projects/{projectId}/userConnections/{connectionId}
    Closes the specified user connection.
    Returns 204 No Content on success.
    """
    url = f"{_api(base_url)}/monitors/projects/{project_id}/userConnections/{conn_id}"
    resp = requests.delete(
        url,
        headers=_headers(token),
        verify=False,
        timeout=REQUEST_TIMEOUT,
    )
    resp.raise_for_status()


# ─────────────────────────────────────────────────────────────────────────────
# Core logic
# ─────────────────────────────────────────────────────────────────────────────

def _clear_project_jobs(
    base_url: str,
    token: str,
    project_id: str,
    project_name: str,
    dry_run: bool,
    summary: ClearSummary,
) -> None:
    """Cancel all active jobs on one project."""
    try:
        jobs = get_jobs(base_url, token, project_id)
    except requests.HTTPError as exc:
        msg = f"  [WARN]   Could not list jobs for '{project_name}': HTTP {exc.response.status_code}"
        print(msg)
        summary.errors.append(msg)
        return

    summary.jobs_found += len(jobs)

    if not jobs:
        print(f"  [JOBS]   No active jobs.")
        return

    print(f"  [JOBS]   Found {len(jobs)} active job(s) — cancelling …")
    for job in jobs:
        job_id   = job.get("id", "unknown")
        obj_name = job.get("objectName") or job.get("name", "unnamed")
        status   = job.get("status", "unknown")
        print(f"           Job {job_id[:12]}… | '{obj_name}' | status={status}", end="")
        if dry_run:
            print("  [DRY-RUN — skipped]")
            continue
        try:
            cancel_job(base_url, token, project_id, job_id)
            summary.jobs_cancelled += 1
            print("  ✓ cancelled")
        except requests.HTTPError as exc:
            summary.jobs_failed += 1
            msg = f"  ✗ HTTP {exc.response.status_code}"
            print(msg)
            summary.errors.append(
                f"Job cancel failed — project={project_name} job={job_id}: {exc.response.status_code}"
            )


def _clear_project_connections(
    base_url: str,
    token: str,
    project_id: str,
    project_name: str,
    dry_run: bool,
    summary: ClearSummary,
) -> None:
    """Close all user connections on one project."""
    try:
        conns = get_user_connections(base_url, token, project_id)
    except requests.HTTPError as exc:
        msg = f"  [WARN]   Could not list connections for '{project_name}': HTTP {exc.response.status_code}"
        print(msg)
        summary.errors.append(msg)
        return

    summary.conns_found += len(conns)

    if not conns:
        print(f"  [CONNS]  No active user connections.")
        return

    print(f"  [CONNS]  Found {len(conns)} active connection(s) — closing …")
    for conn in conns:
        conn_id  = conn.get("id", "unknown")
        username = conn.get("username") or conn.get("loginName", "unknown user")
        client   = conn.get("clientType", "")
        print(f"           Conn {conn_id[:12]}… | user='{username}' | client={client}", end="")
        if dry_run:
            print("  [DRY-RUN — skipped]")
            continue
        try:
            close_connection(base_url, token, project_id, conn_id)
            summary.conns_closed += 1
            print("  ✓ closed")
        except requests.HTTPError as exc:
            summary.conns_failed += 1
            msg = f"  ✗ HTTP {exc.response.status_code}"
            print(msg)
            summary.errors.append(
                f"Connection close failed — project={project_name} conn={conn_id}: {exc.response.status_code}"
            )


def clear_server(
    base_url:         str,
    token:            str,
    filter_project:   Optional[str],
    dry_run:          bool,
    jobs_only:        bool,
    connections_only: bool,
) -> ClearSummary:
    """
    Main orchestrator.
      1. Enumerate all loaded projects on the server.
      2. Optionally filter to one project by name.
      3. Per project: cancel jobs, close connections (as requested).
    """
    summary = ClearSummary()

    print(f"\n[INFO]   Fetching project list …")
    projects = get_projects(base_url, token)

    # Filter to loaded projects only (status 0 = loaded/active)
    loaded = [p for p in projects if p.get("status", -1) == 0]
    print(f"[INFO]   {len(loaded)} loaded project(s) found on server.")

    if filter_project:
        loaded = [
            p for p in loaded
            if p["name"].strip().lower() == filter_project.strip().lower()
        ]
        if not loaded:
            all_names = [p["name"] for p in projects]
            raise ValueError(
                f"Project '{filter_project}' not found or not loaded.\n"
                f"Available: {all_names}"
            )

    for proj in loaded:
        proj_id   = proj["id"]
        proj_name = proj["name"]
        summary.projects_scanned += 1
        print(f"\n{'─'*60}")
        print(f"[PROJECT] '{proj_name}'  (id={proj_id[:12]}…)")

        if not connections_only:
            _clear_project_jobs(base_url, token, proj_id, proj_name, dry_run, summary)

        if not jobs_only:
            _clear_project_connections(base_url, token, proj_id, proj_name, dry_run, summary)

    return summary


def print_summary(summary: ClearSummary, dry_run: bool) -> None:
    tag = "  [DRY-RUN]" if dry_run else ""
    print(f"\n{'='*60}")
    print(f"  SUMMARY{tag}")
    print(f"{'='*60}")
    print(f"  Projects scanned      : {summary.projects_scanned}")
    print(f"  Jobs found            : {summary.jobs_found}")
    print(f"  Jobs cancelled        : {summary.jobs_cancelled}   (failed: {summary.jobs_failed})")
    print(f"  Connections found     : {summary.conns_found}")
    print(f"  Connections closed    : {summary.conns_closed}   (failed: {summary.conns_failed})")
    if summary.errors:
        print(f"\n  Errors ({len(summary.errors)}):")
        for e in summary.errors:
            print(f"    • {e}")
    print(f"{'='*60}")


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Cancel all active jobs and close all user connections "
            "across every loaded project on a Strategy ONE server."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--base-url",    default=DEFAULT_BASE_URL,
        help="Base URL stopping at the web-context root. Override with MSTR_BASE_URL.")
    parser.add_argument("--username",    default=DEFAULT_USERNAME,
        help="Service-account username. Override with MSTR_USERNAME.")
    parser.add_argument("--password",    default=DEFAULT_PASSWORD,
        help="Service-account password. Override with MSTR_PASSWORD.")
    parser.add_argument("--login-mode",  type=int, default=DEFAULT_LOGIN_MODE,
        help="Login mode: 1=standard, 16=LDAP. Override with MSTR_LOGIN_MODE.")
    parser.add_argument("--project",     default=None,
        help="Restrict to one specific project name. Omit for all projects.")
    parser.add_argument("--no-confirm",  action="store_true",
        help="Skip the interactive safety confirmation prompt.")
    parser.add_argument("--dry-run",     action="store_true",
        help="List what would be cancelled/closed without making any changes.")
    parser.add_argument("--jobs-only",   action="store_true",
        help="Only cancel jobs; leave user connections untouched.")
    parser.add_argument("--connections-only", action="store_true",
        help="Only close connections; leave jobs untouched.")
    return parser.parse_args()


def confirm_action(args: argparse.Namespace) -> bool:
    """Interactive safety gate — prints a clear summary of what will happen."""
    scope    = f"'{args.project}'" if args.project else "ALL loaded projects"
    actions  = []
    if not args.connections_only:
        actions.append("cancel ALL active jobs")
    if not args.jobs_only:
        actions.append("close ALL user connections")
    action_str = " AND ".join(actions)

    print("\n" + "!" * 60)
    print("  ⚠   DESTRUCTIVE OPERATION — PLEASE READ CAREFULLY")
    print("!" * 60)
    print(f"  Server  : {args.base_url}")
    print(f"  Scope   : {scope}")
    print(f"  Action  : {action_str}")
    print("!" * 60)
    answer = input("\n  Type  YES  to proceed, or anything else to abort: ").strip()
    return answer == "YES"


def main() -> None:
    args = parse_args()

    print("=" * 60)
    print("  Strategy ONE — Server Job & Connection Cleaner")
    print(f"  Base URL : {args.base_url}")
    print(f"  Project  : {args.project or '(all loaded projects)'}")
    mode_parts = []
    if args.dry_run:           mode_parts.append("DRY-RUN")
    if args.jobs_only:         mode_parts.append("jobs only")
    if args.connections_only:  mode_parts.append("connections only")
    if mode_parts:
        print(f"  Mode     : {', '.join(mode_parts)}")
    print("=" * 60)

    # Safety gate
    if not args.dry_run and not args.no_confirm:
        if not confirm_action(args):
            print("\n[ABORTED]  No changes were made.")
            sys.exit(0)

    token = None
    try:
        token   = login(args.base_url, args.username, args.password, args.login_mode)
        summary = clear_server(
            base_url         = args.base_url,
            token            = token,
            filter_project   = args.project,
            dry_run          = args.dry_run,
            jobs_only        = args.jobs_only,
            connections_only = args.connections_only,
        )
        print_summary(summary, args.dry_run)

    except requests.HTTPError as exc:
        print(f"\n[ERROR]  HTTP {exc.response.status_code}: {exc.response.text}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"\n[ERROR]  {exc}")
        sys.exit(1)
    finally:
        if token:
            logout(args.base_url, token)


if __name__ == "__main__":
    main()
