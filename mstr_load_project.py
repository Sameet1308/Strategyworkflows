"""
mstr_load_project.py
────────────────────────────────────────────────────────────────────────────────
Strategy ONE (Strategy REST API) — Project Loader
Loads a named project on one or all cluster nodes in minutes.

Required privileges on the service account:
  • Monitor cluster          (GET /api/monitors/iServer/nodes)
  • Load and Unload project  (PATCH /api/monitors/iServer/nodes/{node}/projects/{id})

Usage examples
──────────────
  # Load on every node (default)
  python mstr_load_project.py

  # Load on a specific node only
  python mstr_load_project.py --node "my-node-name"

  # Override base URL / project at run-time
  python mstr_load_project.py --base-url https://my-server.example.com/MicroStrategyLibrarySTD \
                               --project "My Project Name"

Environment variables (preferred over hard-coding):
  MSTR_BASE_URL   e.g. https://your-dev-server.com/MicroStrategyLibrarySTD
  MSTR_USERNAME
  MSTR_PASSWORD
  MSTR_PROJECT    project name to load
  MSTR_LOGIN_MODE 1 = standard (default), 16 = LDAP
────────────────────────────────────────────────────────────────────────────────
"""

import argparse
import os
import sys
import time
import urllib3

import requests

# ── Suppress self-signed-cert warnings (verify=False is intentional) ──────────
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Defaults (override via env vars or CLI flags) ─────────────────────────────
DEFAULT_BASE_URL    = os.getenv("MSTR_BASE_URL",    "https://your-dev-server.com/MicroStrategyLibrarySTD")
DEFAULT_USERNAME    = os.getenv("MSTR_USERNAME",    "service_account_placeholder")
DEFAULT_PASSWORD    = os.getenv("MSTR_PASSWORD",    "password_placeholder")
DEFAULT_PROJECT     = os.getenv("MSTR_PROJECT",     "Your Project Name")
DEFAULT_LOGIN_MODE  = int(os.getenv("MSTR_LOGIN_MODE", "1"))   # 1=standard, 16=LDAP

POLL_INTERVAL_SEC   = 5      # seconds between status-check polls
MAX_POLL_ATTEMPTS   = 24     # 24 × 5 s = 2 minutes timeout per node

LOADABLE_STATUSES = {
    "unloaded",
    "unloaded_pending",  # transitional — treat as needs-loading
}

# Status values accepted by the PATCH endpoint
TARGET_STATUS = "loaded"


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
    payload = {
        "username":  username,
        "password":  password,
        "loginMode": login_mode,
    }
    resp = requests.post(url, json=payload, verify=False, timeout=30)
    resp.raise_for_status()
    token = resp.headers.get("X-MSTR-AuthToken")
    if not token:
        raise RuntimeError("Login succeeded but X-MSTR-AuthToken header was absent.")
    print(f"[AUTH]  Logged in as '{username}' — token acquired.")
    return token


def logout(base_url: str, token: str) -> None:
    """POST /api/auth/logout"""
    url = f"{_api(base_url)}/auth/logout"
    headers = {"X-MSTR-AuthToken": token}
    try:
        requests.post(url, headers=headers, verify=False, timeout=15)
        print("[AUTH]  Session closed (logout).")
    except Exception as exc:  # noqa: BLE001
        print(f"[WARN]  Logout call failed (non-fatal): {exc}")


def get_nodes(base_url: str, token: str) -> list[dict]:
    """
    GET /api/monitors/iServer/nodes
    Returns the full list of node dicts from the cluster response.
    """
    url = f"{_api(base_url)}/monitors/iServer/nodes"
    headers = {"X-MSTR-AuthToken": token, "Accept": "application/json"}
    resp = requests.get(url, headers=headers, verify=False, timeout=30)
    resp.raise_for_status()
    return resp.json().get("nodes", [])


def patch_project_status(
    base_url: str, token: str, node_name: str, project_id: str, status: str
) -> dict:
    """
    PATCH /api/monitors/iServer/nodes/{nodeName}/projects/{projectId}
    Requests a status change (loaded / unloaded / exec_idle / etc.).
    Returns the response JSON.
    """
    url = f"{_api(base_url)}/monitors/iServer/nodes/{node_name}/projects/{project_id}"
    headers = {
        "X-MSTR-AuthToken": token,
        "Content-Type":     "application/json",
        "Accept":           "application/json",
    }
    payload = {
        "operationList": [
            {"op": "replace", "path": "/status", "value": status}
        ]
    }
    resp = requests.patch(url, json=payload, headers=headers, verify=False, timeout=30)
    resp.raise_for_status()
    return resp.json()


# ─────────────────────────────────────────────────────────────────────────────
# Core logic
# ─────────────────────────────────────────────────────────────────────────────

def find_project_in_nodes(
    nodes: list[dict], project_name: str
) -> dict:
    """
    Scan all nodes to build a map of:
        { node_name: {"id": str, "status": str} }
    for nodes that contain the named project.
    Raises if the project is not found on any node.
    """
    result: dict[str, dict] = {}
    for node in nodes:
        node_name = node["name"]
        for proj in node.get("projects", []):
            if proj["name"].strip().lower() == project_name.strip().lower():
                result[node_name] = {
                    "id":     proj["id"],
                    "status": proj["status"],
                }
    if not result:
        available = sorted(
            {p["name"] for n in nodes for p in n.get("projects", [])}
        )
        raise ValueError(
            f"Project '{project_name}' was not found on any cluster node.\n"
            f"Available projects: {available}"
        )
    return result


def wait_for_loaded(
    base_url: str,
    token: str,
    node_name: str,
    project_id: str,
    project_name: str,
) -> bool:
    """
    Poll GET /api/monitors/iServer/nodes until the project on this node
    reaches 'loaded' status, or until MAX_POLL_ATTEMPTS is exhausted.
    Returns True if loaded, False on timeout.
    """
    for attempt in range(1, MAX_POLL_ATTEMPTS + 1):
        nodes = get_nodes(base_url, token)
        for node in nodes:
            if node["name"] != node_name:
                continue
            for proj in node.get("projects", []):
                if proj["id"] == project_id:
                    current = proj["status"]
                    print(
                        f"  [{attempt:02d}/{MAX_POLL_ATTEMPTS}] "
                        f"Node '{node_name}' — '{project_name}' status: {current}"
                    )
                    if current == TARGET_STATUS:
                        return True
        time.sleep(POLL_INTERVAL_SEC)

    print(
        f"[WARN]  Timed out waiting for '{project_name}' to reach "
        f"'{TARGET_STATUS}' on node '{node_name}'."
    )
    return False


def load_project(
    base_url: str,
    token: str,
    project_name: str,
    target_node: str | None = None,
) -> None:
    """
    Main orchestrator:
      1. Discover nodes and find the project.
      2. Optionally filter to one node.
      3. PATCH any non-loaded nodes to 'loaded'.
      4. Poll until loaded.
    """
    print(f"\n[INFO]  Fetching cluster node information …")
    nodes = get_nodes(base_url, token)
    print(f"[INFO]  {len(nodes)} node(s) found in cluster.")

    project_map = find_project_in_nodes(nodes, project_name)
    print(f"[INFO]  Project '{project_name}' found on {len(project_map)} node(s).")

    # Optional: restrict to one node
    if target_node:
        if target_node not in project_map:
            raise ValueError(
                f"Node '{target_node}' was specified but does not host "
                f"project '{project_name}'.  Available nodes: {list(project_map)}"
            )
        project_map = {target_node: project_map[target_node]}

    overall_success = True

    for node_name, info in project_map.items():
        project_id     = info["id"]
        current_status = info["status"]

        print(f"\n[NODE]  '{node_name}'")
        print(f"        Project id   : {project_id}")
        print(f"        Current status: {current_status}")

        if current_status == TARGET_STATUS:
            print(f"        ✓ Already loaded — skipping.")
            continue

        if current_status not in LOADABLE_STATUSES and current_status != TARGET_STATUS:
            # e.g. exec_idle, partial_idle, loaded_pending — still effectively
            # accessible; just log and skip to avoid a redundant PATCH.
            print(
                f"        ⚠  Status is '{current_status}' — project is "
                f"accessible, not sending load request."
            )
            continue

        print(f"        → Sending PATCH to load …")
        response = patch_project_status(
            base_url, token, node_name, project_id, TARGET_STATUS
        )
        resp_status = (
            response.get("project", {}).get("status")
            or response.get("status", "unknown")
        )
        print(f"        PATCH accepted — initial response status: {resp_status}")

        print(f"        Polling for '{TARGET_STATUS}' …")
        success = wait_for_loaded(base_url, token, node_name, project_id, project_name)

        if success:
            print(f"        ✓ Project is now loaded on '{node_name}'.")
        else:
            print(f"        ✗ Load did NOT complete within the timeout window.")
            overall_success = False

    print()
    if overall_success:
        print(f"[DONE]  '{project_name}' is loaded and ready.")
    else:
        print(
            f"[WARN]  Some node(s) did not reach 'loaded' within "
            f"{MAX_POLL_ATTEMPTS * POLL_INTERVAL_SEC} seconds.  "
            f"Check Intelligence Server logs for details."
        )


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry-point
# ─────────────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load a Strategy ONE project on all (or a specific) cluster node.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help="Base URL stopping at the web-context root (e.g. …/MicroStrategyLibrarySTD). "
             "Override with MSTR_BASE_URL env var.",
    )
    parser.add_argument(
        "--username",
        default=DEFAULT_USERNAME,
        help="Service-account username.  Override with MSTR_USERNAME.",
    )
    parser.add_argument(
        "--password",
        default=DEFAULT_PASSWORD,
        help="Service-account password.  Override with MSTR_PASSWORD.",
    )
    parser.add_argument(
        "--login-mode",
        type=int,
        default=DEFAULT_LOGIN_MODE,
        help="Login mode: 1=standard, 16=LDAP.  Override with MSTR_LOGIN_MODE.",
    )
    parser.add_argument(
        "--project",
        default=DEFAULT_PROJECT,
        help="Exact project name to load.  Override with MSTR_PROJECT.",
    )
    parser.add_argument(
        "--node",
        default=None,
        help="Restrict load operation to one specific cluster node name. "
             "Omit to apply to all nodes.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    print("=" * 72)
    print("  Strategy ONE — Project Loader")
    print(f"  Base URL : {args.base_url}")
    print(f"  Project  : {args.project}")
    print(f"  Node     : {args.node or '(all cluster nodes)'}")
    print("=" * 72)

    token = None
    try:
        token = login(args.base_url, args.username, args.password, args.login_mode)
        load_project(
            base_url=args.base_url,
            token=token,
            project_name=args.project,
            target_node=args.node,
        )
    except requests.HTTPError as exc:
        print(f"\n[ERROR] HTTP {exc.response.status_code}: {exc.response.text}")
        sys.exit(1)
    except Exception as exc:  # noqa: BLE001
        print(f"\n[ERROR] {exc}")
        sys.exit(1)
    finally:
        if token:
            logout(args.base_url, token)


if __name__ == "__main__":
    main()
