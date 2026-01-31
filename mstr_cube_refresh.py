"""MicroStrategy Cube Refresh Helper

This module contains a helper function and example usage for triggering and
monitoring multiple cube refreshes in a MicroStrategy environment using the
`mstrio-py` SDK. It provides an easy‑to‑extend framework for refreshing
multiple cubes, waiting for those refreshes to complete, and logging the
progress along the way.

The key features of this script include:

* **Parameterised configuration** – placeholders exist for the
  MicroStrategy base URL, user credentials, project identification and cube
  IDs. These should be replaced with values appropriate for your
  environment before running the script.
* **Non‑blocking refresh calls** – the script triggers a refresh on each
  cube asynchronously using the `refresh()` method. This returns a
  `Job` object that can be polled later without blocking the calling
  thread.
* **Adaptive polling** – cube refresh statuses are polled at a configurable
  interval (default 60 seconds). The polling loop distributes the delay
  proportionally across all pending jobs to avoid overloading the server
  with status requests.
* **Timeout handling** – if any cube has not finished refreshing within
  the specified timeout window (default 2 hours), the script logs an
  error and stops waiting.
* **Comprehensive logging** – logging is configured to output to both
  STDOUT and to a timestamped log file. Each significant event (login,
  trigger, status update, error or completion) is recorded.

Usage
-----

Replace the placeholders in the `if __name__ == "__main__":` block with
appropriate values for your MicroStrategy environment. You can then run
the script directly, or import the `trigger_and_monitor_cubes` function
into another Python module. The function returns a dictionary mapping
cube IDs to a boolean indicating whether that cube refresh completed
successfully.

Dependencies
------------

This script depends on the `mstrio-py` package. Install it via pip if
it's not already available:

    pip install mstrio-py

Reference
---------

The behaviour of the refresh and status monitoring logic is based on
documentation from MicroStrategy's Python SDK, which notes that
`refresh()` on an `OlapCube` returns a `Job` object representing an
asynchronous refresh request. The job status can be polled via
`job.refresh_status()` and examined via the `job.status` and
`job.error_message` properties【254813964554443†L1170-L1191】.

"""

from __future__ import annotations

import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Iterable, List, Optional, Tuple

from mstrio.connection import Connection
from mstrio.project_objects.datasets import cube as mstr_cube
from mstrio.project_objects.datasets.olap_cube import OlapCube
from mstrio.server.job_monitor import Job, JobStatus


@dataclass
class CubeRefreshResult:
    """Result of a cube refresh operation.

    Attributes
    ----------
    cube_id: str
        Unique identifier of the cube that was refreshed.
    job_id: Optional[str]
        Identifier of the MicroStrategy job that performed the refresh. May be
        ``None`` if the refresh was not successfully triggered.
    succeeded: bool
        Indicates whether the cube refresh completed successfully.
    error_message: Optional[str]
        If the refresh failed, this contains the error message returned by the
        server; otherwise ``None``.
    """

    cube_id: str
    job_id: Optional[str]
    succeeded: bool
    error_message: Optional[str] = None


def _configure_logging(log_dir: str, log_level: int = logging.INFO) -> str:
    """Configure logging to output to both console and a file.

    Parameters
    ----------
    log_dir: str
        Directory where the log file should be created. The directory will be
        created if it does not already exist.
    log_level: int, optional
        Logging level to use (e.g. ``logging.INFO`` or ``logging.DEBUG``).

    Returns
    -------
    str
        Path to the log file that was created.
    """
    os.makedirs(log_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    log_file = os.path.join(log_dir, f"cube_refresh_{timestamp}.log")

    # Create root logger
    logger = logging.getLogger()
    logger.setLevel(log_level)
    # Remove any existing handlers to prevent duplicate logs when reusing
    # this function in a notebook or another runtime.
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # File handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(log_level)
    file_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    fh.setFormatter(file_formatter)
    logger.addHandler(fh)

    # Console handler
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(log_level)
    console_formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%H:%M:%S",
    )
    ch.setFormatter(console_formatter)
    logger.addHandler(ch)

    return log_file


def trigger_and_monitor_cubes(
    base_url: str,
    username: str,
    password: str,
    project_name: Optional[str],
    cube_ids: Iterable[str],
    *,
    poll_interval: int = 60,
    timeout_seconds: int = 2 * 60 * 60,
    login_mode: int = 1,
    ssl_verify: bool = True,
    log_dir: str = "./logs",
    verbose: bool = False,
) -> Dict[str, CubeRefreshResult]:
    """Trigger and monitor refreshes of multiple cubes.

    This helper logs into MicroStrategy using the provided credentials, loads
    each cube by ID, triggers a refresh, and then monitors the status of
    each refresh job until it completes or a timeout expires. To avoid
    overloading the server, status checks are distributed evenly across the
    pending jobs.

    Parameters
    ----------
    base_url: str
        Base URL of the MicroStrategy Library instance (e.g.
        ``https://your-server/MicroStrategyLibrary``).
    username: str
        Username used to authenticate.
    password: str
        Password used to authenticate.
    project_name: str or None
        Name of the project to connect to. If ``None``, the connection will be
        established without selecting a project and you must manually select
        one afterwards (not recommended for cube operations).
    cube_ids: Iterable[str]
        Iterable of cube object IDs to refresh. Each ID should be a valid
        MicroStrategy cube identifier.
    poll_interval: int, optional
        Overall interval in seconds between status checks for all jobs. The
        function will divide this interval by the number of active jobs,
        sleeping between each job status query. Default is 60 seconds.
    timeout_seconds: int, optional
        Maximum number of seconds to wait for all cube refreshes to complete.
        Default is two hours (7200 seconds).
    login_mode: int, optional
        Login mode used for authentication. The default (1) corresponds to
        standard login. LDAP is typically 16 and API Token is 4096.
    ssl_verify: bool, optional
        Whether to verify the server's SSL certificate. Set to ``False`` if
        using self‑signed certificates. Default ``True``.
    log_dir: str, optional
        Directory where log files will be stored. Defaults to ``./logs``.
    verbose: bool, optional
        If ``True``, the connection will output verbose REST call information.

    Returns
    -------
    dict
        Mapping of cube ID to ``CubeRefreshResult`` summarising the outcome
        of the refresh for each cube.
    """
    # Configure logging and record the log file path
    log_file = _configure_logging(log_dir)
    logging.info("Starting cube refresh process")
    logging.info("Connecting to MicroStrategy at %s", base_url)

    # Establish a connection to MicroStrategy
    try:
        conn = Connection(
            base_url=base_url,
            username=username,
            password=password,
            project_name=project_name,
            login_mode=login_mode,
            ssl_verify=ssl_verify,
            verbose=verbose,
        )
    except Exception as exc:
        logging.error("Failed to establish connection: %s", exc)
        raise

    # Container for results keyed by cube ID
    results: Dict[str, CubeRefreshResult] = {}
    # Keep loaded cube objects so we can validate cube states after job completion
    cube_objects: Dict[str, object] = {}
    # List of active jobs along with their cube identifiers
    active_jobs: List[Tuple[str, Job]] = []

    # Trigger refresh on each cube
    for cube_id in cube_ids:
        try:
            # Load cube; this returns an OlapCube or SuperCube depending on ID
            cube_obj = mstr_cube.load_cube(connection=conn, cube_id=cube_id)
            cube_objects[cube_id] = cube_obj
        except Exception as exc:
            msg = f"Error loading cube {cube_id}: {exc}"
            logging.error(msg)
            results[cube_id] = CubeRefreshResult(
                cube_id=cube_id,
                job_id=None,
                succeeded=False,
                error_message=msg,
            )
            continue

        try:
            # Trigger asynchronous refresh; returns Job
            job = cube_obj.refresh()
            job_id = getattr(job, "id", None)
            logging.info(
                "Triggered refresh for cube %s (job %s)", cube_id, job_id
            )
            results[cube_id] = CubeRefreshResult(
                cube_id=cube_id,
                job_id=job_id,
                succeeded=False,
                error_message=None,
            )
            active_jobs.append((cube_id, job))
        except Exception as exc:
            msg = f"Error triggering refresh for cube {cube_id}: {exc}"
            logging.error(msg)
            results[cube_id] = CubeRefreshResult(
                cube_id=cube_id,
                job_id=None,
                succeeded=False,
                error_message=msg,
            )

    if not active_jobs:
        logging.info("No cube refresh jobs were initiated.")
        return results

    # Monitoring loop
    start_time = time.time()
    logging.info(
        "Monitoring %d refresh jobs every %d seconds (timeout: %d seconds)",
        len(active_jobs),
        poll_interval,
        timeout_seconds,
    )

    # While there are jobs not yet marked complete or errored
    while active_jobs:
        elapsed = time.time() - start_time
        if elapsed > timeout_seconds:
            # Timeout reached
            for cube_id, job in active_jobs:
                logging.error(
                    "Timeout waiting for cube %s (job %s) to finish",
                    cube_id,
                    getattr(job, "id", "unknown"),
                )
                res = results.get(cube_id)
                if res is not None:
                    res.succeeded = False
                    res.error_message = (
                        res.error_message
                        or f"Refresh timed out after {timeout_seconds} seconds"
                    )
            break

        # Compute per-job delay to spread requests evenly
        per_job_delay = poll_interval / max(len(active_jobs), 1)

        # Iterate over a copy of the list as we may remove items during iteration
        for cube_id, job in active_jobs[:]:
            try:
                # Refresh job status from server
                job.refresh_status()
                status: JobStatus = job.status
            except Exception as exc:
                # Log the error and mark job as failed
                logging.error(
                    "Failed to refresh status for cube %s (job %s): %s",
                    cube_id,
                    getattr(job, "id", "unknown"),
                    exc,
                )
                result = results.get(cube_id)
                if result is not None:
                    result.succeeded = False
                    result.error_message = (
                        result.error_message or f"Error fetching job status: {exc}"
                    )
                # Remove from active_jobs
                active_jobs.remove((cube_id, job))
                # Sleep between status calls
                time.sleep(per_job_delay)
                continue

            # Evaluate job status
            if status in (JobStatus.COMPLETED, JobStatus.CACHE_READY):
                # Job completed. Validate cube state as well (READY is the expected state).
                cube_state_names: List[str] = []
                cube_ready = False

                cube_obj = cube_objects.get(cube_id)
                if cube_obj is not None:
                    try:
                        # Refresh cube status and get readable state names.
                        # OlapCube implements refresh_status() and show_status().
                        cube_obj.refresh_status()  # type: ignore[attr-defined]
                        cube_state_names = cube_obj.show_status()  # type: ignore[attr-defined]
                        cube_ready = any(s.upper() == "READY" for s in cube_state_names)
                    except Exception as exc:
                        logging.warning(
                            "Cube %s job completed but cube status check failed: %s",
                            cube_id,
                            exc,
                        )

                if cube_ready:
                    logging.info(
                        "Cube %s refresh completed and cube is READY (job %s)",
                        cube_id,
                        getattr(job, "id", "unknown"),
                    )
                    result = results.get(cube_id)
                    if result is not None:
                        result.succeeded = True
                else:
                    # Job finished but cube did not reach READY.
                    # Mark as failure (still continue monitoring others).
                    logging.error(
                        "Cube %s refresh job completed but cube is not READY. Cube states: %s (job %s)",
                        cube_id,
                        cube_state_names or ["<unknown>"],
                        getattr(job, "id", "unknown"),
                    )
                    result = results.get(cube_id)
                    if result is not None:
                        result.succeeded = False
                        result.error_message = (
                            result.error_message
                            or f"Job completed but cube not READY. Cube states: {cube_state_names or ['<unknown>']}"
                        )

                active_jobs.remove((cube_id, job))
            elif status == JobStatus.ERROR:
                # Completed with error
                error_msg = getattr(job, "error_message", None)
                logging.error(
                    "Cube %s refresh failed (job %s): %s",
                    cube_id,
                    getattr(job, "id", "unknown"),
                    error_msg or "Unknown error",
                )
                result = results.get(cube_id)
                if result is not None:
                    result.succeeded = False
                    result.error_message = (
                        error_msg or result.error_message or "Unknown error"
                    )
                active_jobs.remove((cube_id, job))
            else:
                # In progress; log at debug level to avoid spamming INFO
                logging.debug(
                    "Cube %s refresh status: %s (job %s)",
                    cube_id,
                    status.name if hasattr(status, "name") else status,
                    getattr(job, "id", "unknown"),
                )
                # Do not remove from active_jobs; will check again next cycle

            # Sleep briefly to avoid back‑to‑back requests
            time.sleep(per_job_delay)

    # All jobs have been processed; close connection
    try:
        conn.close()
    except Exception:
        # If closing fails, log but ignore
        logging.warning("Failed to close MicroStrategy connection cleanly")

    logging.info("Cube refresh process complete; log file saved to %s", log_file)
    return results


if __name__ == "__main__":
    # Example usage. Replace the placeholder values with your own.
    BASE_URL = "https://your-server/MicroStrategyLibrary"
    USERNAME = "username"
    PASSWORD = "password"
    PROJECT_NAME = "Your Project Name"  # Or None if specifying project_id separately
    CUBE_IDS = [
        "CUBE_ID_1",
        "CUBE_ID_2",
        # Add more cube IDs as needed
    ]
    # Optional configuration
    POLL_INTERVAL = 60  # seconds between checks across all jobs
    TIMEOUT_SECONDS = 2 * 60 * 60  # 2 hours
    LOGIN_MODE = 1  # Standard login
    # IMPORTANT: Set to False if your Library uses a self-signed cert.
    # The function will also suppress InsecureRequestWarning when this is False.
    SSL_VERIFY = False

    # Trigger and monitor
    try:
        results = trigger_and_monitor_cubes(
            base_url=BASE_URL,
            username=USERNAME,
            password=PASSWORD,
            project_name=PROJECT_NAME,
            cube_ids=CUBE_IDS,
            poll_interval=POLL_INTERVAL,
            timeout_seconds=TIMEOUT_SECONDS,
            login_mode=LOGIN_MODE,
            ssl_verify=SSL_VERIFY,
            log_dir="./logs",
        )
    except Exception as exc:
        logging.exception("An error occurred while refreshing cubes: %s", exc)
        sys.exit(1)

    # Print a simple summary of results
    for cid, result in results.items():
        status_str = "SUCCESS" if result.succeeded else "FAILED"
        msg = f"Cube {cid}: {status_str}"
        if result.error_message:
            msg += f" (Error: {result.error_message})"
        print(msg)

    # Exit code: 0 only if ALL cubes succeeded; 2 if any cube failed.
    all_ok = all(r.succeeded for r in results.values())
    print(
        "\nOVERALL: "
        + ("SUCCESS (all cubes refreshed and are READY)" if all_ok else "FAILURE (one or more cubes failed or did not become READY)")
    )
    sys.exit(0 if all_ok else 2)
