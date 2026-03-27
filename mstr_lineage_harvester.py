#!/usr/bin/env python3
"""
mstr_lineage_harvester.py
-----------------------------------------------------------------------------
MicroStrategy Full-Chain Data Lineage Harvester
Version: 2.0.0

Pipeline:
  1. Discover all projects (with optional exclusion list)
  2. Harvest each project into separate normalized DataFrames
  3. Join chain -> single final lineage DataFrame (edge grain)
  4. Publish as Intelligent Cube on target dev server

Edge grain:
  One row = one relationship in the lineage chain.
  A cube with 10 attributes produces 10 Cube->Attribute edges.
  Each attribute with 3 forms produces 3 Attribute->Form edges.
  Each form tied to a table with 15 columns produces 15 Table->Column edges.

-----------------------------------------------------------------------------
"""

import time
import logging
import warnings
from datetime import datetime
from typing import Optional

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# -----------------------------------------------------------------------------
# CONFIGURATION  -- replace the YOUR_* placeholders before running
# -----------------------------------------------------------------------------

# SOURCE = PRODUCTION SERVER  (lineage metadata is harvested FROM here)
SOURCE_BASE_URL = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"

# -- Project scope -------------------------------------------------------------
# Add one or more project IDs below to harvest only those projects.
# Leave the list empty [] to automatically harvest ALL projects in the environment.
#
# Example -- test with one project first:
#   RUN_ONLY_PROJECT_IDS = ["XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX"]
#
# Example -- full run across everything:
#   RUN_ONLY_PROJECT_IDS = []
RUN_ONLY_PROJECT_IDS = [
    # "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
]

# Shared credentials -- same username and password work on both prod and dev.
MSTR_USERNAME = "YOUR_USERNAME"
MSTR_PASSWORD = "YOUR_PASSWORD"

# TARGET = DEV SERVER  (lineage cube is created / updated HERE)
# Script runs on dev; the cube lives in dev; prod is never written to.
TARGET_BASE_URL   = "https://YOUR_DEV_SERVER/MicroStrategyLibrarySTD"
TARGET_PROJECT_ID = "YOUR_DEV_PROJECT_ID"   # GUID of the dev project

# Folder inside the dev project where the cube will be created.
# Leave "" to publish to the project root.
TARGET_FOLDER_ID  = ""

# -- Cube name ------------------------------------------------------------------
# The cube uses a FIXED name. On every run the script checks if this cube already
# exists in the dev project:
#   - Found  -> updates it in-place (Replace policy) -- no duplicates
#   - Not found -> creates it fresh
# Change this name if you want a different cube name in Workstation.
CUBE_NAME  = "MSTR_Lineage_Harvest"
TABLE_NAME = "LineageEdges"

PAGE_SIZE         = 200
REQUEST_DELAY     = 0.15
SQL_MAX_CHARS     = 800
EXPR_MAX_CHARS    = 600

# -----------------------------------------------------------------------------
# MSTR OBJECT TYPE CONSTANTS
# -----------------------------------------------------------------------------
TYPE_REPORT    = 3
TYPE_METRIC    = 4
TYPE_ATTRIBUTE = 12
TYPE_CUBE      = 776
TYPE_DOCUMENT  = 55

# Report subtypes -- returned in the subtype field of the report definition
SUBTYPE_REPORT_GRID        = 768   # standard grid report (schema-based)
SUBTYPE_REPORT_GRAPH       = 769   # graph report (schema-based)
SUBTYPE_REPORT_FREEFORM    = 772   # freeform SQL report -- lineage IS the SQL
SUBTYPE_REPORT_CUBE        = 774   # OLAP / cube-sourced report -- lineage -> Cube object
SUBTYPE_REPORT_TRANSACTION = 775   # transaction report (write-back)

# -----------------------------------------------------------------------------
# LOGGING
# -----------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("lineage")


# -----------------------------------------------------------------------------
# PROGRESS TRACKER
# -----------------------------------------------------------------------------

class ProgressTracker:
    """
    Lightweight progress tracker. Prints a visible status bar to the console
    so you can see exactly where the script is at all times.

    Usage:
        tracker = ProgressTracker(total_steps=5)
        tracker.step("Discovering projects")
        tracker.step("Harvesting reports")
        tracker.done()
    """

    def __init__(self, total_steps: int = 0, label: str = ""):
        self.total   = total_steps
        self.current = 0
        self.label   = label
        self.start   = datetime.now()

    def step(self, message: str, current: int = None, total: int = None):
        if current is not None:
            self.current = current
        else:
            self.current += 1
        if total is not None:
            self.total = total

        elapsed = (datetime.now() - self.start).seconds
        if self.total > 0:
            pct   = int((self.current / self.total) * 100)
            bar   = ("=" * (pct // 5)).ljust(20)
            label = f"[{bar}] {pct:3d}%  step {self.current}/{self.total}"
        else:
            label = f"[step {self.current}]"

        sep = "=" * 70
        print(f"\n{sep}")
        print(f"  {self.label + ' | ' if self.label else ''}{message}")
        print(f"  {label}  |  elapsed: {elapsed}s")
        print(sep)
        log.info(f"[PROGRESS] {message}")

    def item(self, message: str):
        """Log a sub-item within a step -- visible but less prominent."""
        log.info(f"  --> {message}")

    def done(self, total_rows: int = 0):
        elapsed = (datetime.now() - self.start).seconds
        sep = "=" * 70
        print(f"\n{sep}")
        print(f"  COMPLETE{' | ' + self.label if self.label else ''}")
        if total_rows:
            print(f"  Total lineage edges: {total_rows:,}")
        print(f"  Total elapsed      : {elapsed}s")
        print(f"{sep}\n")


def progress(phase: str, message: str):
    """Simple one-liner progress log -- used outside tracker context."""
    print(f"\n  [{phase}] {message}")
    log.info(f"[{phase}] {message}")


# -----------------------------------------------------------------------------
# HELPERS
# -----------------------------------------------------------------------------

def trunc(val, max_len: int) -> str:
    s = str(val).strip() if val else ""
    return s[:max_len] + "..." if len(s) > max_len else s

def safe(val) -> str:
    return str(val).strip() if val is not None else ""

def tokens_to_str(token_list: list) -> str:
    if not isinstance(token_list, list):
        return ""
    return "".join(t.get("value", "") for t in token_list if isinstance(t, dict))


# -----------------------------------------------------------------------------
# MSTR REST CLIENT
# -----------------------------------------------------------------------------

class MSTRClient:
    """
    Thin REST client. Base URL stops at /MicroStrategyLibrarySTD.
    /api/ always appended in code. loginMode=1. verify=False.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.api      = base_url + "/api"
        self.username = username
        self.password = password
        self.token: Optional[str] = None
        self._session = requests.Session()
        self._session.verify = False

    def login(self):
        r = self._session.post(
            f"{self.api}/auth/login",
            json={"username": self.username, "password": self.password, "loginMode": 1},
            headers={"Content-Type": "application/json"},
            verify=False
        )
        r.raise_for_status()
        self.token = r.headers["X-MSTR-AuthToken"]
        log.info(f"[AUTH] Connected -> {self.base_url}")

    def logout(self):
        if self.token:
            try:
                self._session.post(f"{self.api}/auth/logout",
                                   headers=self._h(), verify=False)
            except Exception:
                pass
            self.token = None

    def _h(self, pid: str = "") -> dict:
        h = {
            "X-MSTR-AuthToken": self.token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        if pid:
            h["X-MSTR-ProjectID"] = pid
        return h

    def _req(self, method: str, path: str, pid: str = "",
             params: dict = None, body: dict = None) -> dict:
        time.sleep(REQUEST_DELAY)
        url = f"{self.api}{path}"
        try:
            r = self._session.request(
                method, url,
                headers=self._h(pid),
                params=params,
                json=body,
                verify=False,
                timeout=60
            )
            if r.status_code in (400, 403, 404, 500):
                log.debug(f"[{r.status_code}] {method} {path}")
                return {}
            r.raise_for_status()
            return r.json() if r.text.strip() else {}
        except Exception as e:
            log.warning(f"[ERR] {method} {path} -> {e}")
            return {}

    def get(self, path: str, pid: str = "", params: dict = None) -> dict:
        return self._req("GET", path, pid=pid, params=params)

    def post(self, path: str, body: dict = None, pid: str = "") -> dict:
        return self._req("POST", path, pid=pid, body=body)

    def put(self, path: str, body: dict = None, pid: str = "") -> dict:
        return self._req("PUT", path, pid=pid, body=body)

    def search_all(self, obj_type: int, pid: str) -> list:
        results, offset = [], 0
        while True:
            data  = self.get("/searches/results", pid=pid,
                             params={"type": obj_type, "limit": PAGE_SIZE, "offset": offset})
            items = data.get("result", [])
            results.extend(items)
            if len(items) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
        log.info(f"    [SEARCH] type={obj_type} -> {len(results)} objects")
        return results

    def get_projects(self) -> list:
        data = self.get("/projects")
        return data if isinstance(data, list) else data.get("projects", [])

    def get_datasources(self) -> list:
        data = self.get("/datasources")
        return data.get("datasources", [])


# -----------------------------------------------------------------------------
# PHASE 1 -- PROJECT DISCOVERY
# -----------------------------------------------------------------------------

def discover_projects(client: MSTRClient, run_only_ids: list) -> pd.DataFrame:
    progress("PHASE 1", "Discovering projects...")
    all_projects = client.get_projects()

    if run_only_ids:
        progress("PHASE 1", f"Targeted run -- {len(run_only_ids)} project(s) specified")
    else:
        progress("PHASE 1", f"Full run -- harvesting all {len(all_projects)} project(s) found")

    rows = []
    for p in all_projects:
        pid    = safe(p.get("id"))
        name   = safe(p.get("name"))
        status = safe(p.get("status", ""))

        if run_only_ids and pid not in run_only_ids:
            log.debug(f"  [SKIP] {name} ({pid})")
            continue

        rows.append({"project_id": pid, "project_name": name, "project_status": status})
        log.info(f"  [QUEUED] {name} ({pid})")

    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["project_id","project_name","project_status"])
    progress("PHASE 1", f"{len(df)} project(s) queued for harvest")
    return df


# -----------------------------------------------------------------------------
# PHASE 2 -- HARVEST INTO NORMALIZED DataFrames
# -----------------------------------------------------------------------------

class HarvestEngine:
    """
    Harvests every project into typed, normalized DataFrames.

    DataFrames:
      df_reports          -- one row per report
      df_cubes            -- one row per cube (includes SQL preview)
      df_documents        -- one row per document / dossier
      df_metrics          -- one row per unique metric + formula
      df_attributes       -- one row per unique attribute
      df_attr_forms       -- one row per attribute-form-expression (the key grain expansion)
      df_br_rpt_metric    -- bridge: report <-> metric
      df_br_rpt_attr      -- bridge: report <-> attribute
      df_br_cube_metric   -- bridge: cube <-> metric
      df_br_cube_attr     -- bridge: cube <-> attribute
      df_br_doc_ds        -- bridge: document <-> dataset
      df_datasources      -- one row per DB instance / DSN
      df_tables           -- one row per logical/physical table
      df_columns          -- one row per column
    """

    def __init__(self, client: MSTRClient):
        self.c = client
        # Caches
        self._metric_cache:    dict = {}
        self._attribute_cache: dict = {}
        self._table_cache:     dict = {}
        # Accumulators
        self._reports:        list = []
        self._cubes:          list = []
        self._documents:      list = []
        self._metrics:        list = []
        self._attributes:     list = []
        self._attr_forms:     list = []
        self._br_rpt_metric:  list = []
        self._br_rpt_attr:    list = []
        self._br_cube_metric: list = []
        self._br_cube_attr:   list = []
        self._br_doc_ds:          list = []
        self._freeform_sqls:      list = []   # freeform SQL reports
        self._cube_sourced_reports: list = [] # OLAP / cube-sourced reports
        self._datasources:        list = []
        self._tables:         list = []
        self._columns:        list = []

    # -- Metric ----------------------------------------------------------------

    def _fetch_metric(self, mid: str, pid: str) -> dict:
        key = f"{pid}:{mid}"
        if key not in self._metric_cache:
            self._metric_cache[key] = self.c.get(f"/metrics/{mid}", pid=pid)
        return self._metric_cache[key]

    def _parse_formula(self, detail: dict) -> tuple:
        expr = detail.get("expression", {})
        if isinstance(expr, dict):
            formula    = tokens_to_str(expr.get("tokens", []))
            expression = safe(expr.get("text", ""))
        else:
            formula = expression = ""
        if not formula:
            expr2 = detail.get("definition", {}).get("expression", {})
            if isinstance(expr2, dict):
                formula = tokens_to_str(expr2.get("tokens", []))
        return trunc(formula, EXPR_MAX_CHARS), trunc(expression, EXPR_MAX_CHARS)

    def _register_metric(self, mid: str, mname: str, pid: str):
        key = f"{pid}:{mid}"
        if any(f"{r['project_id']}:{r['metric_id']}" == key for r in self._metrics):
            return
        detail = self._fetch_metric(mid, pid)
        formula, expression = self._parse_formula(detail)
        self._metrics.append({
            "project_id":        pid,
            "metric_id":         mid,
            "metric_name":       mname or safe(detail.get("name","")),
            "metric_formula":    formula,
            "metric_expression": expression
        })

    # -- Attribute -------------------------------------------------------------

    def _fetch_attribute(self, aid: str, pid: str) -> dict:
        key = f"{pid}:{aid}"
        if key not in self._attribute_cache:
            self._attribute_cache[key] = self.c.get(f"/attributes/{aid}", pid=pid)
        return self._attribute_cache[key]

    def _register_attribute(self, aid: str, aname: str, pid: str):
        key = f"{pid}:{aid}"
        if any(f"{r['project_id']}:{r['attribute_id']}" == key for r in self._attributes):
            return

        detail = self._fetch_attribute(aid, pid)
        aname  = aname or safe(detail.get("name",""))
        self._attributes.append({"project_id": pid, "attribute_id": aid, "attribute_name": aname})

        # Each form -> each expression -> table -> register columns
        for form in detail.get("forms", []):
            form_name = safe(form.get("name",""))
            for expr in form.get("expressions", []):
                expr_obj = expr.get("expression", {})
                if isinstance(expr_obj, dict):
                    expr_str = tokens_to_str(expr_obj.get("tokens",[])) or safe(expr_obj.get("text",""))
                else:
                    expr_str = safe(expr_obj)

                tbl_info = expr.get("table", {})
                tbl_id   = safe(tbl_info.get("id",""))
                tbl_name = safe(tbl_info.get("name",""))

                self._attr_forms.append({
                    "project_id":      pid,
                    "attribute_id":    aid,
                    "attribute_name":  aname,
                    "form_name":       form_name,
                    "form_expression": trunc(expr_str, EXPR_MAX_CHARS),
                    "table_id":        tbl_id,
                    "table_name":      tbl_name
                })
                if tbl_id:
                    self._register_table(tbl_id, tbl_name, pid)

        # Also register tables listed at attribute level
        for tbl in detail.get("tables", []):
            tid = safe(tbl.get("id",""))
            if tid:
                self._register_table(tid, safe(tbl.get("name","")), pid)

    # -- Table / Column --------------------------------------------------------

    def _register_table(self, tbl_id: str, tbl_name: str, pid: str, ds_id: str = ""):
        key = f"{pid}:{tbl_id}"
        if any(f"{r['project_id']}:{r['table_id']}" == key for r in self._tables):
            return
        detail  = self.c.get(f"/tables/{tbl_id}", pid=pid)
        ds_id   = ds_id or safe(detail.get("dataSource",{}).get("id",""))
        columns = detail.get("physicalTable",{}).get("columns",[])
        self._tables.append({
            "project_id":    pid,
            "table_id":      tbl_id,
            "table_name":    tbl_name or safe(detail.get("name","")),
            "datasource_id": ds_id
        })
        for col in columns:
            self._columns.append({
                "project_id":       pid,
                "table_id":         tbl_id,
                "table_name":       tbl_name,
                "column_name":      safe(col.get("columnName", col.get("name",""))),
                "column_data_type": safe(col.get("dataType",""))
            })

    # -- Datasources -----------------------------------------------------------

    def harvest_datasources(self):
        log.info("  [DATASOURCES] Loading...")
        seen = set()
        for ds in self.c.get_datasources():
            did = safe(ds.get("id"))
            if did in seen:
                continue
            seen.add(did)
            conn = ds.get("datasourceConnection",{})
            self._datasources.append({
                "datasource_id":    did,
                "db_instance_name": safe(ds.get("name","")),
                "dsn_name":         safe(conn.get("name","")),
                "db_type":          safe(ds.get("dbType",""))
            })
        log.info(f"    -> {len(self._datasources)} datasources")

    # -- Reports ---------------------------------------------------------------

    def _get_report_sql(self, rid: str, pid: str) -> str:
        """Fetch SQL preview for a report (freeform or standard)."""
        sql_data = self.c.get(f"/reports/{rid}/sqlView", pid=pid)
        if sql_data:
            passes = sql_data.get("sqlStatements", [])
            return " | ".join(p.get("sql", "") for p in passes if isinstance(p, dict))
        return ""

    def harvest_reports(self, pid: str, pname: str):
        """
        Handles three report types by subtype:
          SUBTYPE_REPORT_GRID / GRAPH  -> schema-based: harvest metrics, attributes, tables, columns
          SUBTYPE_REPORT_FREEFORM      -> no schema objects: capture raw SQL as lineage
          SUBTYPE_REPORT_CUBE          -> sourced from a cube: edge Report->Cube, no table traversal
        """
        log.info(f"  [REPORTS] {pname}...")
        for obj in self.c.search_all(TYPE_REPORT, pid):
            rid     = safe(obj.get("id"))
            rname   = safe(obj.get("name"))
            subtype = int(obj.get("subtype", SUBTYPE_REPORT_GRID) or SUBTYPE_REPORT_GRID)

            self._reports.append({
                "project_id":    pid,
                "report_id":     rid,
                "report_name":   rname,
                "report_subtype": str(subtype)
            })

            detail = self.c.get(f"/reports/{rid}", pid=pid)
            defn   = detail.get("definition", {})

            # -- FREEFORM SQL report -------------------------------------------
            # No schema objects -- the SQL IS the full lineage.
            # Captured as a single FreeformSQL node; attribute/metric traversal skipped.
            if subtype == SUBTYPE_REPORT_FREEFORM:
                sql_text = self._get_report_sql(rid, pid)
                if not sql_text:
                    # Also try pulling from definition directly
                    sql_text = safe(defn.get("freeFormSQL", {}).get("sql", ""))
                self._freeform_sqls.append({
                    "project_id":  pid,
                    "report_id":   rid,
                    "report_name": rname,
                    "freeform_sql": trunc(sql_text, SQL_MAX_CHARS)
                })
                log.debug(f"    [FREEFORM] {rname}")
                continue

            # -- CUBE-SOURCED report (OLAP Services) ---------------------------
            # Report queries an Intelligent Cube, not the DB directly.
            # Lineage edge: Report -> Cube (cube lineage handles the rest).
            if subtype == SUBTYPE_REPORT_CUBE:
                dataset = defn.get("dataPartition", {}) or defn.get("dataset", {})
                cube_id   = safe(dataset.get("id", ""))
                cube_name = safe(dataset.get("name", ""))
                # Fallback: check availableObjects for a cube reference
                if not cube_id:
                    for ds in defn.get("availableObjects", {}).get("datasets", []):
                        if int(ds.get("type", 0)) == TYPE_CUBE:
                            cube_id   = safe(ds.get("id", ""))
                            cube_name = safe(ds.get("name", ""))
                            break
                self._cube_sourced_reports.append({
                    "project_id":  pid,
                    "report_id":   rid,
                    "report_name": rname,
                    "source_cube_id":   cube_id,
                    "source_cube_name": cube_name
                })
                log.debug(f"    [CUBE-SOURCED] {rname} -> cube {cube_name}")
                continue

            # -- STANDARD GRID / GRAPH report ---------------------------------
            # Schema-based: harvest metrics and attributes normally.
            avail = defn.get("availableObjects", {})
            grid  = defn.get("grid", {})

            metrics = (avail.get("metrics", []) or
                       [o for o in grid.get("columns", []) + grid.get("rows", [])
                        if o.get("type") == "metric"])
            for m in metrics:
                mid = safe(m.get("id"))
                if not mid:
                    continue
                self._br_rpt_metric.append({"project_id": pid, "report_id": rid, "metric_id": mid})
                self._register_metric(mid, safe(m.get("name", "")), pid)

            attrs = (avail.get("attributes", []) or
                     [o for o in grid.get("columns", []) + grid.get("rows", [])
                      if o.get("type") == "attribute"])
            for a in attrs:
                aid = safe(a.get("id"))
                if not aid:
                    continue
                self._br_rpt_attr.append({"project_id": pid, "report_id": rid, "attribute_id": aid})
                self._register_attribute(aid, safe(a.get("name", "")), pid)

    # -- Cubes -----------------------------------------------------------------

    def harvest_cubes(self, pid: str, pname: str):
        log.info(f"  [CUBES] {pname}...")
        for obj in self.c.search_all(TYPE_CUBE, pid):
            cid   = safe(obj.get("id"))
            cname = safe(obj.get("name"))

            # SQL preview
            sql_data = self.c.get(f"/cubes/{cid}/sqlView", pid=pid)
            sql_text = ""
            if sql_data:
                passes   = sql_data.get("sqlStatements",[])
                sql_text = " | ".join(p.get("sql","") for p in passes if isinstance(p,dict))

            self._cubes.append({
                "project_id":       pid,
                "cube_id":          cid,
                "cube_name":        cname,
                "cube_sql_preview": trunc(sql_text, SQL_MAX_CHARS)
            })

            detail = self.c.get(f"/v2/cubes/{cid}", pid=pid) or self.c.get(f"/cubes/{cid}", pid=pid)
            if not detail:
                continue

            avail = detail.get("definition",{}).get("availableObjects",{})

            for m in avail.get("metrics",[]):
                mid = safe(m.get("id"))
                if not mid:
                    continue
                self._br_cube_metric.append({"project_id": pid, "cube_id": cid, "metric_id": mid})
                self._register_metric(mid, safe(m.get("name","")), pid)

            for a in avail.get("attributes",[]):
                aid = safe(a.get("id"))
                if not aid:
                    continue
                self._br_cube_attr.append({"project_id": pid, "cube_id": cid, "attribute_id": aid})
                self._register_attribute(aid, safe(a.get("name","")), pid)

    # -- Documents / Dossiers --------------------------------------------------

    def harvest_documents(self, pid: str, pname: str):
        log.info(f"  [DOCUMENTS] {pname}...")
        for obj in self.c.search_all(TYPE_DOCUMENT, pid):
            did    = safe(obj.get("id"))
            dname  = safe(obj.get("name"))
            subtype = "Dossier" if obj.get("subtype") == 14081 else "Document"
            self._documents.append({
                "project_id": pid, "doc_id": did,
                "doc_name": dname, "doc_subtype": subtype
            })
            detail = (self.c.get(f"/dossiers/{did}/definition", pid=pid) or
                      self.c.get(f"/documents/{did}/definition", pid=pid))
            if not detail:
                continue
            for ds in detail.get("datasets",[]):
                ds_id = safe(ds.get("id"))
                if ds_id:
                    self._br_doc_ds.append({
                        "project_id":   pid,
                        "doc_id":       did,
                        "dataset_id":   ds_id,
                        "dataset_type": safe(ds.get("type",""))
                    })

    # -- Standalone passes -----------------------------------------------------

    def harvest_standalone_metrics(self, pid: str, pname: str):
        log.info(f"  [STANDALONE METRICS] {pname}...")
        for m in self.c.search_all(TYPE_METRIC, pid):
            self._register_metric(safe(m.get("id")), safe(m.get("name","")), pid)

    def harvest_standalone_attributes(self, pid: str, pname: str):
        log.info(f"  [STANDALONE ATTRIBUTES] {pname}...")
        for a in self.c.search_all(TYPE_ATTRIBUTE, pid):
            self._register_attribute(safe(a.get("id")), safe(a.get("name","")), pid)

    # -- Datasource table topology ---------------------------------------------

    def harvest_datasource_topology(self):
        log.info("  [DS TOPOLOGY] Fetching datasource table/column lists...")
        for ds in self._datasources:
            did = ds["datasource_id"]
            resp = self.c.get(f"/datasources/{did}/tables")
            for tbl in resp.get("tables",[]):
                tid   = safe(tbl.get("id"))
                tname = safe(tbl.get("name"))
                cols  = tbl.get("columns",[])
                key   = f"__global__:{tid}"
                if any(f"{r['project_id']}:{r['table_id']}" == key for r in self._tables):
                    continue
                self._tables.append({
                    "project_id":    "__global__",
                    "table_id":      tid,
                    "table_name":    tname,
                    "datasource_id": did
                })
                for col in cols:
                    self._columns.append({
                        "project_id":       "__global__",
                        "table_id":         tid,
                        "table_name":       tname,
                        "column_name":      safe(col.get("columnName", col.get("name",""))),
                        "column_data_type": safe(col.get("dataType",""))
                    })

    # -- Per-project orchestrator ----------------------------------------------

    def harvest_project(self, pid: str, pname: str):
        print(f"\n    Project : {pname}")
        print(f"    ID      : {pid}")
        print(f"    {'-'*55}")
        log.info(f"\n  == Project: {pname} ({pid}) ==")
        self.harvest_reports(pid, pname)
        self.harvest_cubes(pid, pname)
        self.harvest_documents(pid, pname)
        self.harvest_standalone_metrics(pid, pname)
        self.harvest_standalone_attributes(pid, pname)
        rpt_count  = sum(1 for r in self._reports   if r["project_id"] == pid)
        cube_count = sum(1 for c in self._cubes      if c["project_id"] == pid)
        doc_count  = sum(1 for d in self._documents  if d["project_id"] == pid)
        met_count  = sum(1 for m in self._metrics    if m["project_id"] == pid)
        attr_count = sum(1 for a in self._attributes if a["project_id"] == pid)
        ff_count   = sum(1 for f in self._freeform_sqls if f["project_id"] == pid)
        cs_count   = sum(1 for c in self._cube_sourced_reports if c["project_id"] == pid)
        print(f"    Reports (grid/graph): {rpt_count - ff_count - cs_count}  |  Freeform SQL: {ff_count}  |  Cube-sourced: {cs_count}")
        print(f"    Cubes: {cube_count}  |  Docs/Dossiers: {doc_count}  |  Metrics: {met_count}  |  Attributes: {attr_count}")

    # -- Build DataFrames ------------------------------------------------------

    def build_dataframes(self) -> dict:
        log.info("\n[PHASE 2 -> DFs] Normalizing accumulators into DataFrames...")
        empty_df = lambda cols: pd.DataFrame(columns=cols)

        def to_df(rows, dedup_cols=None):
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            if dedup_cols:
                df = df.drop_duplicates(subset=dedup_cols)
            else:
                df = df.drop_duplicates()
            return df

        dfs = {
            "df_reports":        to_df(self._reports,        ["project_id","report_id"]),
            "df_cubes":          to_df(self._cubes,          ["project_id","cube_id"]),
            "df_documents":      to_df(self._documents,      ["project_id","doc_id"]),
            "df_metrics":        to_df(self._metrics,        ["project_id","metric_id"]),
            "df_attributes":     to_df(self._attributes,     ["project_id","attribute_id"]),
            "df_attr_forms":     to_df(self._attr_forms),
            "df_br_rpt_metric":  to_df(self._br_rpt_metric),
            "df_br_rpt_attr":    to_df(self._br_rpt_attr),
            "df_br_cube_metric": to_df(self._br_cube_metric),
            "df_br_cube_attr":   to_df(self._br_cube_attr),
            "df_br_doc_ds":           to_df(self._br_doc_ds),
            "df_freeform_sqls":        to_df(self._freeform_sqls,       ["project_id","report_id"]),
            "df_cube_sourced_reports": to_df(self._cube_sourced_reports, ["project_id","report_id"]),
            "df_datasources":          to_df(self._datasources,          ["datasource_id"]),
            "df_tables":         to_df(self._tables,         ["project_id","table_id"]),
            "df_columns":        to_df(self._columns)
        }

        for name, df in dfs.items():
            log.info(f"  {name:<22}: {len(df):>7,} rows")

        return dfs


# -----------------------------------------------------------------------------
# PHASE 3 -- JOIN CHAIN -> FINAL LINEAGE DataFrame
# -----------------------------------------------------------------------------

class LineageJoiner:
    """
    Joins normalized DataFrames into one edge-grain lineage DataFrame.

    Edge grain: one row = one directional relationship.
    If a cube has 10 attributes, there are 10 Cube->Attribute rows.
    If attribute A has 2 forms, there are 2 Attribute->Form rows under it.
    If a form maps to a table with 15 columns, there are 15 Table->Column rows.
    """

    FINAL_COLS = [
        "lineage_row_id",
        "project_id", "project_name",
        "top_object_type", "top_object_id", "top_object_name",
        "edge_type",
        "parent_node_type", "parent_node_id", "parent_node_name",
        "child_node_type",  "child_node_id",  "child_node_name",
        "node_level",
        "metric_formula", "metric_expression",
        "form_name", "form_expression",
        "cube_sql_preview",
        "datasource_id", "db_instance_name", "dsn_name", "db_type",
        "table_id", "table_name",
        "column_name", "column_data_type",
        "report_subtype", "doc_subtype", "dataset_type",
        "harvested_at"
    ]

    def __init__(self, dfs: dict, df_projects: pd.DataFrame):
        self.dfs      = dfs
        self.projects = df_projects
        self.ts       = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self._edges: list = []

    def _pname(self, pid: str) -> str:
        row = self.projects[self.projects["project_id"] == pid]
        return row["project_name"].iloc[0] if not row.empty else pid

    def _ds_info(self, ds_id: str) -> dict:
        ds = self.dfs["df_datasources"]
        row = ds[ds["datasource_id"] == ds_id] if ds_id else pd.DataFrame()
        if not row.empty:
            return {
                "datasource_id":    ds_id,
                "db_instance_name": row["db_instance_name"].iloc[0],
                "dsn_name":         row["dsn_name"].iloc[0],
                "db_type":          row["db_type"].iloc[0]
            }
        return {"datasource_id": ds_id, "db_instance_name": "", "dsn_name": "", "db_type": ""}

    def _get_table_ds(self, tbl_id: str, pid: str) -> str:
        tbls = self.dfs["df_tables"]
        row  = tbls[(tbls["project_id"] == pid) & (tbls["table_id"] == tbl_id)]
        if row.empty:
            row = tbls[(tbls["project_id"] == "__global__") & (tbls["table_id"] == tbl_id)]
        return row["datasource_id"].iloc[0] if not row.empty else ""

    def _get_columns(self, tbl_id: str, pid: str) -> pd.DataFrame:
        cols = self.dfs["df_columns"]
        res  = cols[(cols["project_id"] == pid) & (cols["table_id"] == tbl_id)]
        if res.empty:
            res = cols[(cols["project_id"] == "__global__") & (cols["table_id"] == tbl_id)]
        return res

    def _e(self, **kwargs) -> dict:
        """Build one edge row with all FINAL_COLS defaulted."""
        defaults = {c: "" for c in self.FINAL_COLS}
        defaults["node_level"]    = 0
        defaults["harvested_at"]  = self.ts
        defaults.update(kwargs)
        return defaults

    # -- Report edges ----------------------------------------------------------

    def _report_edges(self):
        br_m  = self.dfs["df_br_rpt_metric"]
        br_a  = self.dfs["df_br_rpt_attr"]
        rpts  = self.dfs["df_reports"]
        mets  = self.dfs["df_metrics"]
        attrs = self.dfs["df_attributes"]
        forms = self.dfs["df_attr_forms"]

        # Report -> Metric -> Formula
        m_merged = (br_m.merge(rpts, on=["project_id","report_id"], how="left")
                        .merge(mets, on=["project_id","metric_id"], how="left"))
        for _, r in m_merged.iterrows():
            pid   = r["project_id"]
            pname = self._pname(pid)
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type="Report", top_object_id=r["report_id"], top_object_name=r.get("report_name",""),
                edge_type="Report->Metric",
                parent_node_type="Report",  parent_node_id=r["report_id"], parent_node_name=r.get("report_name",""),
                child_node_type="Metric",   child_node_id=r["metric_id"],  child_node_name=r.get("metric_name",""),
                node_level=2,
                report_subtype=str(r.get("report_subtype","")),
                metric_formula=r.get("metric_formula",""),
                metric_expression=r.get("metric_expression","")
            ))
            if r.get("metric_formula"):
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Report", top_object_id=r["report_id"], top_object_name=r.get("report_name",""),
                    edge_type="Metric->Formula",
                    parent_node_type="Metric",        parent_node_id=r["metric_id"], parent_node_name=r.get("metric_name",""),
                    child_node_type="MetricFormula",  child_node_id=r["metric_id"],  child_node_name=f"{r.get('metric_name','')} [Formula]",
                    node_level=3,
                    metric_formula=r.get("metric_formula",""),
                    metric_expression=r.get("metric_expression","")
                ))

        # Report -> Attribute -> Form -> Table -> Column
        a_merged = (br_a.merge(rpts, on=["project_id","report_id"], how="left")
                        .merge(attrs, on=["project_id","attribute_id"], how="left"))
        for _, r in a_merged.iterrows():
            pid   = r["project_id"]
            pname = self._pname(pid)
            aid   = r["attribute_id"]
            aname = r.get("attribute_name","")
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type="Report", top_object_id=r["report_id"], top_object_name=r.get("report_name",""),
                edge_type="Report->Attribute",
                parent_node_type="Report",    parent_node_id=r["report_id"], parent_node_name=r.get("report_name",""),
                child_node_type="Attribute",  child_node_id=aid,             child_node_name=aname,
                node_level=2
            ))
            self._attribute_form_column_edges(
                aid, aname, "Report", r["report_id"], r.get("report_name",""),
                pid, pname, forms, 3
            )

    # -- Freeform SQL report edges ---------------------------------------------

    def _freeform_sql_edges(self):
        """
        Freeform SQL reports have no attribute/metric/table lineage.
        Emit one edge: Report -> FreeformSQL  with the SQL captured as enrichment.
        """
        df = self.dfs.get("df_freeform_sqls", pd.DataFrame())
        if df.empty:
            return
        rpts = self.dfs["df_reports"]
        for _, r in df.iterrows():
            pid   = r["project_id"]
            pname = self._pname(pid)
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type="Report", top_object_id=r["report_id"], top_object_name=r["report_name"],
                edge_type="Report->FreeformSQL",
                parent_node_type="Report",      parent_node_id=r["report_id"], parent_node_name=r["report_name"],
                child_node_type="FreeformSQL",  child_node_id=r["report_id"],  child_node_name=f"{r['report_name']} [Freeform SQL]",
                node_level=2,
                cube_sql_preview=r.get("freeform_sql","")   # reusing sql preview column
            ))

    # -- Cube-sourced report edges ----------------------------------------------

    def _cube_sourced_report_edges(self):
        """
        OLAP / cube-sourced reports point to an Intelligent Cube, not directly to tables.
        Emit one edge: Report -> Cube  (the cube's own lineage covers the rest).
        """
        df = self.dfs.get("df_cube_sourced_reports", pd.DataFrame())
        if df.empty:
            return
        for _, r in df.iterrows():
            pid   = r["project_id"]
            pname = self._pname(pid)
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type="Report", top_object_id=r["report_id"], top_object_name=r["report_name"],
                edge_type="Report->Cube(OLAP)",
                parent_node_type="Report", parent_node_id=r["report_id"], parent_node_name=r["report_name"],
                child_node_type="Cube",    child_node_id=r.get("source_cube_id",""),  child_node_name=r.get("source_cube_name",""),
                node_level=2
            ))

    # -- Cube edges ------------------------------------------------------------

    def _cube_edges(self):
        cubes = self.dfs["df_cubes"]
        br_m  = self.dfs["df_br_cube_metric"]
        br_a  = self.dfs["df_br_cube_attr"]
        mets  = self.dfs["df_metrics"]
        attrs = self.dfs["df_attributes"]
        forms = self.dfs["df_attr_forms"]

        for _, cube in cubes.iterrows():
            pid   = cube["project_id"]
            cid   = cube["cube_id"]
            cname = cube["cube_name"]
            pname = self._pname(pid)

            if cube.get("cube_sql_preview"):
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Cube", top_object_id=cid, top_object_name=cname,
                    edge_type="Cube->SQL",
                    parent_node_type="Cube",    parent_node_id=cid, parent_node_name=cname,
                    child_node_type="CubeSQL",  child_node_id=cid,  child_node_name=f"{cname} [SQL]",
                    node_level=2,
                    cube_sql_preview=cube["cube_sql_preview"]
                ))

            # Cube -> Metric -> Formula
            cmet = br_m[(br_m["project_id"]==pid)&(br_m["cube_id"]==cid)].merge(mets, on=["project_id","metric_id"], how="left")
            for _, m in cmet.iterrows():
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Cube", top_object_id=cid, top_object_name=cname,
                    edge_type="Cube->Metric",
                    parent_node_type="Cube",   parent_node_id=cid,           parent_node_name=cname,
                    child_node_type="Metric",  child_node_id=m["metric_id"],  child_node_name=m.get("metric_name",""),
                    node_level=2,
                    metric_formula=m.get("metric_formula",""),
                    metric_expression=m.get("metric_expression","")
                ))
                if m.get("metric_formula"):
                    self._edges.append(self._e(
                        project_id=pid, project_name=pname,
                        top_object_type="Cube", top_object_id=cid, top_object_name=cname,
                        edge_type="Metric->Formula",
                        parent_node_type="Metric",       parent_node_id=m["metric_id"], parent_node_name=m.get("metric_name",""),
                        child_node_type="MetricFormula", child_node_id=m["metric_id"],  child_node_name=f"{m.get('metric_name','')} [Formula]",
                        node_level=3,
                        metric_formula=m.get("metric_formula",""),
                        metric_expression=m.get("metric_expression","")
                    ))

            # Cube -> Attribute -> Form -> Table -> Column
            cattr = br_a[(br_a["project_id"]==pid)&(br_a["cube_id"]==cid)].merge(
                self.dfs["df_attributes"], on=["project_id","attribute_id"], how="left")
            for _, a in cattr.iterrows():
                aid   = a["attribute_id"]
                aname = a.get("attribute_name","")
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Cube", top_object_id=cid, top_object_name=cname,
                    edge_type="Cube->Attribute",
                    parent_node_type="Cube",      parent_node_id=cid, parent_node_name=cname,
                    child_node_type="Attribute",  child_node_id=aid,  child_node_name=aname,
                    node_level=2
                ))
                self._attribute_form_column_edges(
                    aid, aname, "Cube", cid, cname,
                    pid, pname, forms, 3
                )

    # -- Shared: Attribute -> Form -> Table -> Column ---------------------------

    def _attribute_form_column_edges(self, aid, aname, top_type, top_id, top_name,
                                      pid, pname, forms_df, base_level):
        """
        For a given attribute, emit:
          - one Attribute->Form edge per form expression  (base_level)
          - one Table->Column edge per column in that form's table  (base_level+1)
        """
        attr_forms = forms_df[(forms_df["project_id"]==pid) & (forms_df["attribute_id"]==aid)]
        for _, f in attr_forms.iterrows():
            tbl_id   = f.get("table_id","")
            tbl_name = f.get("table_name","")
            ds_id    = self._get_table_ds(tbl_id, pid) if tbl_id else ""
            ds_info  = self._ds_info(ds_id)

            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type=top_type, top_object_id=top_id, top_object_name=top_name,
                edge_type="Attribute->Form",
                parent_node_type="Attribute",    parent_node_id=aid,  parent_node_name=aname,
                child_node_type="AttributeForm", child_node_id=aid,
                child_node_name=f"{aname} [{f.get('form_name','')}]",
                node_level=base_level,
                form_name=f.get("form_name",""),
                form_expression=f.get("form_expression",""),
                table_id=tbl_id, table_name=tbl_name,
                **ds_info
            ))

            if tbl_id:
                for _, col in self._get_columns(tbl_id, pid).iterrows():
                    self._edges.append(self._e(
                        project_id=pid, project_name=pname,
                        top_object_type=top_type, top_object_id=top_id, top_object_name=top_name,
                        edge_type="Table->Column",
                        parent_node_type="Table",  parent_node_id=tbl_id, parent_node_name=tbl_name,
                        child_node_type="Column",  child_node_id="",      child_node_name=col["column_name"],
                        node_level=base_level+1,
                        table_id=tbl_id, table_name=tbl_name,
                        column_name=col["column_name"],
                        column_data_type=col["column_data_type"],
                        **ds_info
                    ))

    # -- Document edges --------------------------------------------------------

    def _document_edges(self):
        docs  = self.dfs["df_documents"]
        br    = self.dfs["df_br_doc_ds"]
        rpts  = self.dfs["df_reports"]
        cubes = self.dfs["df_cubes"]

        merged = br.merge(docs, on=["project_id","doc_id"], how="left")
        for _, r in merged.iterrows():
            pid   = r["project_id"]
            pname = self._pname(pid)
            child_type = "Cube" if str(r.get("dataset_type","")) == str(TYPE_CUBE) else "Report"
            ds_name = ""
            if child_type == "Report":
                row = rpts[(rpts["project_id"]==pid)&(rpts["report_id"]==r["dataset_id"])]
                if not row.empty:
                    ds_name = row["report_name"].iloc[0]
            else:
                row = cubes[(cubes["project_id"]==pid)&(cubes["cube_id"]==r["dataset_id"])]
                if not row.empty:
                    ds_name = row["cube_name"].iloc[0]

            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type=r.get("doc_subtype","Document"),
                top_object_id=r["doc_id"],
                top_object_name=r.get("doc_name",""),
                edge_type=f"{r.get('doc_subtype','Document')}->{child_type}",
                parent_node_type=r.get("doc_subtype","Document"),
                parent_node_id=r["doc_id"], parent_node_name=r.get("doc_name",""),
                child_node_type=child_type, child_node_id=r["dataset_id"], child_node_name=ds_name,
                node_level=2,
                doc_subtype=r.get("doc_subtype",""),
                dataset_type=str(r.get("dataset_type",""))
            ))

    # -- Standalone metric edges ------------------------------------------------

    def _standalone_metric_edges(self):
        covered = {e["child_node_id"] for e in self._edges
                   if e.get("edge_type","") in ("Report->Metric","Cube->Metric")}
        for _, m in self.dfs["df_metrics"].iterrows():
            if m["metric_id"] in covered:
                continue
            pid   = m["project_id"]
            pname = self._pname(pid)
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type="Metric", top_object_id=m["metric_id"], top_object_name=m["metric_name"],
                edge_type="Metric->Formula",
                parent_node_type="Metric",       parent_node_id=m["metric_id"], parent_node_name=m["metric_name"],
                child_node_type="MetricFormula", child_node_id=m["metric_id"],  child_node_name=f"{m['metric_name']} [Formula]",
                node_level=1,
                metric_formula=m.get("metric_formula",""),
                metric_expression=m.get("metric_expression","")
            ))

    # -- Datasource topology edges ---------------------------------------------

    def _datasource_topology_edges(self):
        for _, ds in self.dfs["df_datasources"].iterrows():
            did   = ds["datasource_id"]
            dname = ds["db_instance_name"]
            ds_info = self._ds_info(did)

            self._edges.append(self._e(
                project_id="__global__",
                top_object_type="DBInstance", top_object_id=did, top_object_name=dname,
                edge_type="DBInstance->DSN",
                parent_node_type="DBInstance", parent_node_id=did, parent_node_name=dname,
                child_node_type="DSN",         child_node_id=did,  child_node_name=ds["dsn_name"],
                node_level=1,
                **ds_info
            ))

            for _, t in self.dfs["df_tables"][self.dfs["df_tables"]["datasource_id"]==did].iterrows():
                tid   = t["table_id"]
                tname = t["table_name"]
                self._edges.append(self._e(
                    project_id="__global__",
                    top_object_type="DBInstance", top_object_id=did, top_object_name=dname,
                    edge_type="DSN->Table",
                    parent_node_type="DSN",   parent_node_id=did, parent_node_name=ds["dsn_name"],
                    child_node_type="Table",  child_node_id=tid,  child_node_name=tname,
                    node_level=2,
                    table_id=tid, table_name=tname,
                    **ds_info
                ))
                for _, col in self._get_columns(tid, "__global__").iterrows():
                    self._edges.append(self._e(
                        project_id="__global__",
                        top_object_type="DBInstance", top_object_id=did, top_object_name=dname,
                        edge_type="Table->Column",
                        parent_node_type="Table",  parent_node_id=tid, parent_node_name=tname,
                        child_node_type="Column",  child_node_id="",   child_node_name=col["column_name"],
                        node_level=3,
                        table_id=tid, table_name=tname,
                        column_name=col["column_name"],
                        column_data_type=col["column_data_type"],
                        **ds_info
                    ))

    # -- Build -----------------------------------------------------------------

    def build(self) -> pd.DataFrame:
        log.info("\n[PHASE 3] Joining DataFrames -> lineage edge DataFrame...")

        self._report_edges()            # standard grid/graph reports
        self._freeform_sql_edges()      # freeform SQL reports
        self._cube_sourced_report_edges()  # OLAP / cube-sourced reports
        self._cube_edges()
        self._document_edges()
        self._standalone_metric_edges()
        self._datasource_topology_edges()

        df = pd.DataFrame(self._edges)

        # Ensure all columns present
        for col in self.FINAL_COLS:
            if col not in df.columns:
                df[col] = ""

        df = df[self.FINAL_COLS].copy()
        df["lineage_row_id"] = [str(i+1).zfill(8) for i in range(len(df))]
        df["harvested_at"]   = self.ts
        df["node_level"]     = pd.to_numeric(df["node_level"], errors="coerce").fillna(0).astype(int)

        str_cols = [c for c in df.columns if c != "node_level"]
        df[str_cols] = df[str_cols].fillna("").astype(str).apply(lambda s: s.str.strip())

        df = df.drop_duplicates().reset_index(drop=True)
        log.info(f"  -> Final lineage DataFrame: {len(df):,} edge rows across {df['edge_type'].nunique()} edge types")
        log.info(f"  -> Edge type breakdown:")
        for et, cnt in df["edge_type"].value_counts().items():
            log.info(f"       {et:<35}: {cnt:>7,}")
        return df


# -----------------------------------------------------------------------------
# PHASE 4 -- PUBLISH AS INTELLIGENT CUBE (Push Data API)
# -----------------------------------------------------------------------------

class CubePublisher:
    """
    Publishes df_lineage as an Intelligent Cube via the MSTR Push Data API.

    Flow:
      POST /api/datasets                                     -> create definition
      PUT  /api/datasets/{id}/tables/{table}  (chunked)     -> upload rows
      POST /api/datasets/{id}/publish                        -> publish
    """

    METRIC_COL = "node_level"
    CHUNK_SIZE  = 50_000

    def __init__(self, client: MSTRClient, project_id: str,
                 cube_name: str, table_name: str, folder_id: str = ""):
        self.c          = client
        self.project_id = project_id
        self.cube_name  = cube_name
        self.table_name = table_name
        self.folder_id  = folder_id

    def _attr_cols(self, df: pd.DataFrame) -> list:
        return [c for c in df.columns if c != self.METRIC_COL]

    def _find_existing_cube(self) -> str:
        """
        Search the dev project for a cube whose name exactly matches CUBE_NAME.
        Returns the dataset ID if found, empty string if not found.
        Every run uses this to decide UPDATE vs CREATE - no duplicate cubes.
        """
        data = self.c.get(
            "/searches/results", pid=self.project_id,
            params={"type": TYPE_CUBE, "name": self.cube_name, "limit": 50}
        )
        for obj in data.get("result", []):
            if safe(obj.get("name")) == self.cube_name:
                existing_id = safe(obj.get("id"))
                log.info(f"  [FOUND] Existing cube '{self.cube_name}' -> ID: {existing_id}  (will UPDATE)")
                return existing_id
        log.info(f"  [NOT FOUND] Cube '{self.cube_name}' does not exist yet  (will CREATE)")
        return ""

    def _definition_body(self, df: pd.DataFrame) -> dict:
        attr_cols = self._attr_cols(df)
        all_cols  = attr_cols + [self.METRIC_COL]
        body = {
            "name":   self.cube_name,
            "tables": [{"name": self.table_name, "columnHeaders": all_cols}],
            "attributes": [
                {
                    "name": col,
                    "attributeForms": [{
                        "category": "ID",
                        "expressions": [{"formula": f"{self.table_name}.{col}"}]
                    }]
                }
                for col in attr_cols
            ],
            "metrics": [{
                "name": "Node Level",
                "dataType": "integer",
                "expressions": [{"formula": f"{self.table_name}.{self.METRIC_COL}"}]
            }]
        }
        if self.folder_id:
            body["folderId"] = self.folder_id
        return body

    def _serialize(self, df_chunk: pd.DataFrame, policy: str = "Replace") -> dict:
        attr_cols = self._attr_cols(df_chunk)
        all_cols  = attr_cols + [self.METRIC_COL]
        headers   = {col: idx for idx, col in enumerate(all_cols)}
        raw_data  = []
        for _, row in df_chunk.iterrows():
            record = [str(row.get(c,"") or "") for c in attr_cols]
            record.append(str(int(row.get(self.METRIC_COL, 0) or 0)))
            raw_data.append(record)
        return {"data": {"headers": headers, "rawData": raw_data}, "updatePolicy": policy}

    def _open_upload_session(self, dataset_id: str) -> str:
        """
        Create a new upload session against an existing cube.
        Required by MSTR Push Data API before any PUT upload call.
        Returns the uploadSessionId.
        """
        resp = self.c.post(
            f"/datasets/{dataset_id}/uploadSessions",
            body={"uploadSessionType": "normalUpload"},
            pid=self.project_id
        )
        session_id = resp.get("uploadSessionId", "")
        if not session_id:
            raise RuntimeError(
                f"Failed to open upload session for dataset {dataset_id}. Response: {resp}"
            )
        log.info(f"  [SESSION] Upload session opened: {session_id}")
        return session_id

    def _upload_chunks(self, dataset_id: str, session_id: str, df: pd.DataFrame):
        """
        Chunk-upload all rows into an open upload session.
        First chunk uses Replace (truncates existing data), subsequent chunks use Add.
        """
        chunks = [df.iloc[i:i+self.CHUNK_SIZE] for i in range(0, len(df), self.CHUNK_SIZE)]
        for idx, chunk in enumerate(chunks):
            policy = "Replace" if idx == 0 else "Add"
            log.info(f"  [UPLOAD] Chunk {idx+1}/{len(chunks)} ({len(chunk):,} rows)  policy={policy}")
            self.c.put(
                f"/datasets/{dataset_id}/uploadSessions/{session_id}/tables/{self.table_name}",
                body=self._serialize(chunk, policy),
                pid=self.project_id
            )

    def _publish_session(self, dataset_id: str, session_id: str):
        """Commit the upload session -- makes data visible in the cube."""
        pub = self.c.post(
            f"/datasets/{dataset_id}/uploadSessions/{session_id}/publish",
            body={},
            pid=self.project_id
        )
        log.info(f"  [PUBLISH] Committed session {session_id}  response: {pub}")

    def publish(self, df: pd.DataFrame) -> str:
        """
        Update-or-create -- same fixed cube name every run, zero duplicates.

        Run 1  (cube does not exist yet):
          POST /api/datasets                                          create definition
          -> returns datasetId + uploadSessionId
          PUT  /api/datasets/{id}/uploadSessions/{sid}/tables/{t}    upload rows
          POST /api/datasets/{id}/uploadSessions/{sid}/publish        publish

        Run 2+ (cube already exists):
          POST /api/datasets/{id}/uploadSessions                      open NEW session
          -> returns fresh uploadSessionId
          PUT  /api/datasets/{id}/uploadSessions/{sid}/tables/{t}    upload rows (Replace then Add)
          POST /api/datasets/{id}/uploadSessions/{sid}/publish        publish / commit

        The Replace policy on the first chunk truncates all previous data before
        loading the new full set -- so old projects drop out, new projects appear,
        and you always get a clean current snapshot.
        """
        if df.empty:
            raise ValueError("No lineage rows to publish.")

        log.info(f"\n[PHASE 4] Update-or-create: '{self.cube_name}'")
        log.info(f"  Target : {self.c.base_url}")
        log.info(f"  Project: {self.project_id}")
        log.info(f"  Rows   : {len(df):,}")

        existing_id = self._find_existing_cube()

        if existing_id:
            # -- UPDATE path ---------------------------------------------------
            # Cube exists: open a new upload session, upload, publish.
            # The Replace policy on chunk 0 clears the old data first.
            log.info(f"  [MODE] UPDATE -- opening upload session on existing cube {existing_id}")
            session_id = self._open_upload_session(existing_id)
            self._upload_chunks(existing_id, session_id, df)
            self._publish_session(existing_id, session_id)
            log.info(f"\n  Cube UPDATED -> ID: {existing_id} | Name: {self.cube_name}")
            return existing_id

        else:
            # -- CREATE path ---------------------------------------------------
            # First run: POST /api/datasets returns both datasetId and uploadSessionId
            # in one call -- no separate session open needed.
            log.info("  [MODE] CREATE -- posting dataset definition")
            resp = self.c.post(
                "/datasets",
                body=self._definition_body(df),
                pid=self.project_id
            )
            dataset_id = resp.get("datasetId") or resp.get("id", "")
            session_id = resp.get("uploadSessionId", "")
            if not dataset_id:
                raise RuntimeError(f"Dataset creation failed. Response: {resp}")
            if not session_id:
                # Some MSTR versions require a separate session open even on create
                log.info("  [SESSION] uploadSessionId not in create response -- opening session explicitly")
                session_id = self._open_upload_session(dataset_id)
            log.info(f"  [CREATE] Dataset ID: {dataset_id}  Session: {session_id}")
            self._upload_chunks(dataset_id, session_id, df)
            self._publish_session(dataset_id, session_id)
            log.info(f"\n  Cube CREATED -> ID: {dataset_id} | Name: {self.cube_name}")
            return dataset_id

# -----------------------------------------------------------------------------
# MAIN
# -----------------------------------------------------------------------------

def main():
    run_ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    tracker = ProgressTracker(total_steps=4, label="MSTR Lineage Harvester")

    print("=" * 70)
    print("  MSTR FULL-CHAIN LINEAGE HARVESTER  v2.0.0")
    print(f"  Run        : {run_ts}")
    print(f"  Source     : {SOURCE_BASE_URL}")
    print(f"  Target     : {TARGET_BASE_URL}")
    print(f"  Project(s) : {RUN_ONLY_PROJECT_IDS if RUN_ONLY_PROJECT_IDS else 'ALL'}")
    print(f"  Cube       : {CUBE_NAME}  (update-or-create)")
    print("=" * 70)

    src = MSTRClient(SOURCE_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD)
    src.login()

    df_lineage = pd.DataFrame()

    try:
        # -- Phase 1: Project discovery -------------------------------------
        tracker.step("Phase 1 of 4 -- Discovering projects")
        df_projects = discover_projects(src, RUN_ONLY_PROJECT_IDS)
        if df_projects.empty:
            log.error("No projects found. Exiting.")
            return
        tracker.item(f"{len(df_projects)} project(s) ready to harvest")

        # -- Phase 2: Harvest all projects into normalized DFs --------------
        tracker.step("Phase 2 of 4 -- Harvesting metadata from production")
        engine = HarvestEngine(src)

        tracker.item("Loading datasources...")
        engine.harvest_datasources()

        total_projects = len(df_projects)
        for idx, (_, proj) in enumerate(df_projects.iterrows(), 1):
            pid   = proj["project_id"]
            pname = proj["project_name"]
            tracker.item(f"Project {idx}/{total_projects}: {pname}")
            engine.harvest_project(pid, pname)

        tracker.item("Fetching datasource table topology...")
        engine.harvest_datasource_topology()

        tracker.item("Building normalized DataFrames...")
        dfs = engine.build_dataframes()

        # -- Phase 3: Join chain -> final lineage DataFrame -----------------
        tracker.step("Phase 3 of 4 -- Joining DataFrames into lineage edges")
        joiner     = LineageJoiner(dfs, df_projects)
        df_lineage = joiner.build()
        tracker.item(f"{len(df_lineage):,} lineage edges built")
        tracker.item(f"Edge types: {df_lineage['edge_type'].nunique()} distinct types")

        # Sample preview
        print("\n  Sample lineage edges:")
        print(df_lineage[["edge_type","parent_node_name","child_node_name","node_level"]]
              .head(10).to_string(index=False))

    finally:
        src.logout()
        log.info("[AUTH] Production server disconnected")

    # -- Phase 4: Publish cube to dev server -------------------------------
    tracker.step("Phase 4 of 4 -- Publishing lineage cube to dev server")
    tgt = MSTRClient(TARGET_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD)
    tgt.login()
    try:
        publisher = CubePublisher(
            client=tgt,
            project_id=TARGET_PROJECT_ID,
            cube_name=CUBE_NAME,
            table_name=TABLE_NAME,
            folder_id=TARGET_FOLDER_ID
        )
        dataset_id = publisher.publish(df_lineage)
        tracker.item(f"Cube ID: {dataset_id}")
    finally:
        tgt.logout()
        log.info("[AUTH] Dev server disconnected")

    tracker.done(total_rows=len(df_lineage))


if __name__ == "__main__":
    main()
