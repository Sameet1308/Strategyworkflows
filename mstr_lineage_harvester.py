#!/usr/bin/env python3
"""
mstr_lineage_harvester.py
Version: 3.0.0  -- Full rewrite with correct API endpoints

CONFIRMED API ENDPOINTS (from official MSTR REST API docs):
  Auth      : POST /api/auth/login
  Projects  : GET  /api/projects
  Search    : GET  /api/searches/results?type={t}&limit={n}&offset={n}
  Report    : GET  /api/model/reports/{id}          -> grid.rows/columns[].units[]
  Attribute : GET  /api/model/attributes/{id}?showExpressionAs=tokens
                   -> forms[].expressions[].tables[].objectId  (NOT .id)
                   -> forms[].expressions[].expression.tokens[].value
  Metric    : GET  /api/model/metrics/{id}?showExpressionAs=tokens
                   -> expression.text / expression.tokens[]
  Fact      : GET  /api/model/facts/{id}?showExpressionAs=tokens
                   -> expressions[].expression.tree.columnName
                   -> expressions[].tables[].objectId
  Table     : GET  /api/model/tables/{id}
                   -> information.name
                   -> physicalTable.columns[].name
                   -> physicalTable.columns[].dataType.type  (nested obj)
  Datasrc   : GET  /api/datasources
  Dossier   : GET  /api/dossiers/{id}/definition     -> datasets[]
  Document  : GET  /api/documents/{id}/definition    -> datasets[]

MASTER OBJECTS TABLE:
  df_objects -- one row per MSTR object (universal join backbone)
  Columns: object_id | object_name | object_type | object_subtype |
           project_id | owner | date_modified | folder_id | folder_name

LINEAGE CHAIN:
  Project -> Dossier/Document -> Report/Cube
          -> Metric -> formula text (inline)
          -> Attribute -> Form -> Table -> Column -> Datasource
          -> Fact -> Column reference -> Table -> Datasource
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

# =============================================================================
# CONFIGURATION  -- Replace YOUR_* placeholders before running
# =============================================================================

# SOURCE = PRODUCTION  (harvest FROM here)
SOURCE_BASE_URL = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"

# Shared credentials (same for both servers)
MSTR_USERNAME = "YOUR_USERNAME"
MSTR_PASSWORD = "YOUR_PASSWORD"

# Project scope:
#   Add project GUIDs to harvest only those projects.
#   Leave [] to harvest ALL projects automatically.
RUN_ONLY_PROJECT_IDS = [
    # "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
]

# TARGET = DEV  (publish cube HERE)
TARGET_BASE_URL   = "https://YOUR_DEV_SERVER/MicroStrategyLibrarySTD"
TARGET_PROJECT_ID = "YOUR_DEV_PROJECT_ID"
TARGET_FOLDER_ID  = ""   # "" = project root

CUBE_NAME  = "MSTR_Lineage_Harvest"
TABLE_NAME = "LineageEdges"

# Tuning
PAGE_SIZE     = 200
REQUEST_DELAY = 0.1
SQL_MAX       = 100000   # no truncation -- full SQL captured regardless of length
EXPR_MAX      = 100000   # no truncation -- full expression captured regardless of length
CHUNK_SIZE    = 50000

# =============================================================================
# MSTR OBJECT TYPE CONSTANTS  (EnumDSSObjectType)
# =============================================================================
TYPE_FILTER    = 1
TYPE_REPORT    = 3
TYPE_METRIC    = 4
TYPE_FACT      = 13
TYPE_ATTRIBUTE = 12
TYPE_TABLE     = 53
TYPE_CUBE      = 776
TYPE_DOCUMENT  = 55

# Report subtypes
SUBTYPE_GRID       = 768
SUBTYPE_GRAPH      = 769
SUBTYPE_FREEFORM   = 772
SUBTYPE_CUBE_RPT   = 774

# =============================================================================
# LOGGING + PROGRESS
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("lineage")


def step(phase: str, msg: str):
    print(f"\n  [{phase}] {msg}")
    log.info(f"[{phase}] {msg}")


# =============================================================================
# HELPERS
# =============================================================================
def safe(v) -> str:
    return str(v).strip() if v is not None else ""


def extract_tables_from_sql(sql: str) -> list:
    """
    Parse a freeform SQL statement and return all table names referenced.
    Handles FROM, JOIN variants. Returns deduplicated list in order of appearance.
    Used to generate one lineage row per table for freeform reports and cubes.
    """
    import re
    if not sql:
        return []
    sql_upper = re.sub(r"--[^\n]*", "", sql.upper())   # strip line comments
    sql_upper = re.sub(r"/\*.*?\*/", "", sql_upper, flags=re.DOTALL)  # block comments
    pattern = (r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN"
               r"|FULL\s+JOIN|CROSS\s+JOIN|LEFT\s+OUTER\s+JOIN"
               r"|RIGHT\s+OUTER\s+JOIN|FULL\s+OUTER\s+JOIN)\s+"
               r"([A-Z0-9_#@\.]+)")
    matches = re.findall(pattern, sql_upper)
    # Filter out SQL keywords mistakenly caught
    skip = {"WHERE","SELECT","ON","SET","WITH","AS","AND","OR","NOT",
            "IN","NULL","CASE","WHEN","THEN","ELSE","END","GROUP","ORDER",
            "HAVING","UNION","EXCEPT","INTERSECT","LATERAL","VALUES"}
    tables = [m.split(".")[-1] for m in matches   # strip schema prefix e.g. dbo.FACT
              if m not in skip and not m.startswith("(")]
    return list(dict.fromkeys(tables))   # deduplicated, order preserved


def trunc(v, n: int) -> str:
    s = str(v).strip() if v else ""
    return s[:n] + "..." if len(s) > n else s


def tokens_to_str(tokens: list) -> str:
    if not isinstance(tokens, list):
        return ""
    return "".join(t.get("value", "") for t in tokens if isinstance(t, dict))


# =============================================================================
# MSTR REST CLIENT
# =============================================================================
class MSTRClient:
    """
    Base URL stops at /MicroStrategyLibrarySTD.
    /api/ appended in code. loginMode=1. verify=False everywhere.
    """

    def __init__(self, base_url: str, username: str, password: str):
        self.base_url = base_url
        self.api      = base_url + "/api"
        self.username = username
        self.password = password
        self.token: Optional[str] = None
        self._s = requests.Session()
        self._s.verify = False

    def login(self):
        r = self._s.post(
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
                self._s.post(f"{self.api}/auth/logout",
                             headers=self._h(), verify=False)
            except Exception:
                pass
            self.token = None
            log.info("[AUTH] Disconnected")

    def _h(self, pid: str = "") -> dict:
        h = {"X-MSTR-AuthToken": self.token,
             "Content-Type": "application/json",
             "Accept": "application/json"}
        if pid:
            h["X-MSTR-ProjectID"] = pid
        return h

    def _req(self, method: str, path: str, pid: str = "",
             params: dict = None, body: dict = None) -> dict:
        time.sleep(REQUEST_DELAY)
        try:
            r = self._s.request(
                method,
                f"{self.api}{path}",
                headers=self._h(pid),
                params=params,
                json=body,
                verify=False,
                timeout=60
            )
            if r.status_code in (400, 403, 404, 500):
                log.debug(f"  [{r.status_code}] {method} {path}")
                return {}
            r.raise_for_status()
            return r.json() if r.text.strip() else {}
        except Exception as e:
            log.warning(f"  [ERR] {method} {path} -> {e}")
            return {}

    def get(self, path: str, pid: str = "", params: dict = None) -> dict:
        return self._req("GET", path, pid=pid, params=params)

    def post(self, path: str, body: dict = None, pid: str = "") -> dict:
        return self._req("POST", path, pid=pid, body=body)

    def put(self, path: str, body: dict = None, pid: str = "") -> dict:
        return self._req("PUT", path, pid=pid, body=body)

    def search_all(self, obj_type: int, pid: str) -> list:
        """Paginated search for all objects of given type in a project."""
        results, offset = [], 0
        while True:
            data  = self.get("/searches/results", pid=pid,
                             params={"type": obj_type,
                                     "limit": PAGE_SIZE,
                                     "offset": offset})
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
        return self.get("/datasources").get("datasources", [])


# =============================================================================
# PHASE 1 -- PROJECT DISCOVERY
# =============================================================================
def discover_projects(client: MSTRClient,
                      run_only: list) -> pd.DataFrame:
    step("PHASE 1", "Discovering projects...")
    rows = []
    for p in client.get_projects():
        pid  = safe(p.get("id"))
        name = safe(p.get("name"))
        if run_only and pid not in run_only:
            continue
        rows.append({"project_id": pid,
                     "project_name": name,
                     "project_status": safe(p.get("status", ""))})
        log.info(f"  [QUEUED] {name} ({pid})")

    df = pd.DataFrame(rows) if rows else pd.DataFrame(
        columns=["project_id", "project_name", "project_status"])
    step("PHASE 1", f"{len(df)} project(s) queued")
    return df


# =============================================================================
# PHASE 2 -- HARVEST ENGINE
# =============================================================================
class HarvestEngine:
    """
    Builds the following normalized DataFrames:

    df_objects       -- MASTER: one row per MSTR object (universal join key)
    df_reports       -- reports with subtype and sql preview
    df_cubes         -- cubes with sql preview
    df_documents     -- dossiers and documents
    df_metrics       -- metrics with formula text
    df_attributes    -- attributes
    df_attr_forms    -- attribute form expressions + table mapping
    df_facts         -- facts with column + table mapping
    df_br_rpt_metric -- bridge report <-> metric
    df_br_rpt_attr   -- bridge report <-> attribute
    df_br_cube_metric-- bridge cube <-> metric
    df_br_cube_attr  -- bridge cube <-> attribute
    df_br_doc_ds     -- bridge document <-> dataset (report or cube)
    df_datasources   -- DB instances / DSN info
    df_tables        -- logical tables
    df_columns       -- physical columns per table
    """

    def __init__(self, client: MSTRClient):
        self.c = client

        # -- caches
        self._mcache: dict = {}
        self._acache: dict = {}
        self._tcache: dict = {}
        self._fcache: dict = {}

        # -- accumulators
        self._objects:     list = []
        self._reports:     list = []
        self._cubes:       list = []
        self._documents:   list = []
        self._metrics:     list = []
        self._attributes:  list = []
        self._attr_forms:  list = []
        self._facts:       list = []
        self._br_rm:       list = []   # report-metric bridge
        self._br_ra:       list = []   # report-attribute bridge
        self._br_cm:       list = []   # cube-metric bridge
        self._br_ca:       list = []   # cube-attribute bridge
        self._br_dd:       list = []   # document-dataset bridge
        self._filters:     list = []   # filter objects
        self._prompts:     list = []   # prompt objects
        self._br_rf:       list = []   # report-filter bridge
        self._br_cf:       list = []   # cube-filter bridge
        self._br_rp:       list = []   # report-prompt bridge
        self._br_cp:       list = []   # cube-prompt bridge
        self._datasources: list = []
        self._tables:      list = []
        self._columns:     list = []

    # -------------------------------------------------------------------------
    # Master objects table
    # -------------------------------------------------------------------------
    def _register_object(self, obj: dict, pid: str):
        """Add any MSTR object to df_objects from a search result row."""
        oid   = safe(obj.get("id"))
        if not oid:
            return
        # Avoid duplicates
        if any(r["object_id"] == oid and r["project_id"] == pid
               for r in self._objects):
            return
        owner = safe(obj.get("owner", {}).get("name", "")
                     if isinstance(obj.get("owner"), dict) else obj.get("owner", ""))
        self._objects.append({
            "object_id":       oid,
            "object_name":     safe(obj.get("name", "")),
            "object_type":     safe(obj.get("type", "")),
            "object_subtype":  safe(obj.get("subtype", "")),
            "project_id":      pid,
            "owner":           owner,
            "date_modified":   safe(obj.get("modificationTime",
                                   obj.get("dateModified", ""))),
            "folder_id":       safe(obj.get("ancestors", [{}])[-1].get("objectId", "")
                                    if obj.get("ancestors") else ""),
            "folder_name":     safe(obj.get("ancestors", [{}])[-1].get("name", "")
                                    if obj.get("ancestors") else ""),
        })

    def harvest_objects(self, pid: str, pname: str):
        """
        Build the master objects table for a project.
        Searches all object types we care about.
        """
        log.info(f"  [OBJECTS] {pname}...")
        for otype in [TYPE_REPORT, TYPE_CUBE, TYPE_DOCUMENT,
                      TYPE_METRIC, TYPE_ATTRIBUTE, TYPE_FACT,
                      TYPE_TABLE, TYPE_FILTER]:
            for obj in self.c.search_all(otype, pid):
                self._register_object(obj, pid)

    # -------------------------------------------------------------------------
    # Datasources
    # -------------------------------------------------------------------------
    def harvest_datasources(self):
        log.info("  [DATASOURCES] Loading...")
        seen = set()
        for ds in self.c.get_datasources():
            did = safe(ds.get("id"))
            if did in seen:
                continue
            seen.add(did)
            conn = ds.get("datasourceConnection", {}) or {}
            self._datasources.append({
                "datasource_id":    did,
                "db_instance_name": safe(ds.get("name", "")),
                "dsn_name":         safe(conn.get("name", "")),
                "db_type":          safe(ds.get("dbType", "")),
            })
        log.info(f"    -> {len(self._datasources)} datasources")

    # -------------------------------------------------------------------------
    # Table + Column  (GET /api/model/tables/{id})
    # Response: information.name | physicalTable.columns[].name + .dataType.type
    # -------------------------------------------------------------------------
    def _register_table(self, tbl_id: str, tbl_name: str, pid: str):
        key = f"{pid}:{tbl_id}"
        if any(f"{r['project_id']}:{r['table_id']}" == key for r in self._tables):
            return

        detail   = self.c.get(f"/model/tables/{tbl_id}", pid=pid)
        tbl_name = tbl_name or safe(
            detail.get("information", {}).get("name", ""))
        ds_id    = safe(detail.get("physicalTable", {})
                        .get("information", {})
                        .get("dataSourceId", ""))
        if not ds_id:
            ds_id = safe(detail.get("dataSource", {}).get("id", ""))

        self._tables.append({
            "project_id":    pid,
            "table_id":      tbl_id,
            "table_name":    tbl_name,
            "datasource_id": ds_id,
        })

        # physicalTable.columns[].name + .dataType.type (nested obj)
        for col in detail.get("physicalTable", {}).get("columns", []):
            col_name = safe(col.get("name", ""))
            dtype    = col.get("dataType", {})
            col_type = safe(dtype.get("type", "") if isinstance(dtype, dict) else dtype)
            if col_name:
                self._columns.append({
                    "project_id":       pid,
                    "table_id":         tbl_id,
                    "table_name":       tbl_name,
                    "column_name":      col_name,
                    "column_data_type": col_type,
                })

    # -------------------------------------------------------------------------
    # Attribute  (GET /api/model/attributes/{id}?showExpressionAs=tokens)
    # Confirmed response structure:
    #   forms[].name
    #   forms[].expressions[].expression.tokens[].value   -> column name
    #   forms[].expressions[].tables[].objectId           -> table GUID (NOT .id)
    #   forms[].expressions[].tables[].name               -> table name
    # -------------------------------------------------------------------------
    def _register_attribute(self, aid: str, aname: str, pid: str):
        key = f"{pid}:{aid}"
        if any(f"{r['project_id']}:{r['attribute_id']}" == key
               for r in self._attributes):
            return

        if key not in self._acache:
            self._acache[key] = self.c.get(
                f"/model/attributes/{aid}", pid=pid,
                params={"showExpressionAs": "tokens"})
        detail = self._acache[key] or {}

        aname = aname or safe(detail.get("information", {}).get("name", ""))
        self._attributes.append({
            "project_id":    pid,
            "attribute_id":  aid,
            "attribute_name": aname,
        })

        for form in detail.get("forms", []):
            form_name = safe(form.get("name", ""))
            for expr in form.get("expressions", []):
                # Column expression from tokens
                expr_obj = expr.get("expression", {})
                expr_str = tokens_to_str(expr_obj.get("tokens", []))
                if not expr_str:
                    expr_str = safe(expr_obj.get("text", ""))

                # tables[].objectId  (confirmed field name from MSTR docs)
                for tbl_info in expr.get("tables", []):
                    tbl_id   = safe(tbl_info.get("objectId", ""))
                    tbl_name = safe(tbl_info.get("name", ""))

                    self._attr_forms.append({
                        "project_id":      pid,
                        "attribute_id":    aid,
                        "attribute_name":  aname,
                        "form_name":       form_name,
                        "form_expression": expr_str,
                        "table_id":        tbl_id,
                        "table_name":      tbl_name,
                    })
                    if tbl_id:
                        self._register_table(tbl_id, tbl_name, pid)

    # -------------------------------------------------------------------------
    # Metric  (GET /api/model/metrics/{id}?showExpressionAs=tokens)
    # Confirmed response: expression.text | expression.tokens[]
    # -------------------------------------------------------------------------
    def _register_metric(self, mid: str, mname: str, pid: str):
        key = f"{pid}:{mid}"
        if any(f"{r['project_id']}:{r['metric_id']}" == key
               for r in self._metrics):
            return

        if key not in self._mcache:
            self._mcache[key] = self.c.get(
                f"/model/metrics/{mid}", pid=pid,
                params={"showExpressionAs": "tokens"})
        detail = self._mcache[key] or {}

        mname   = mname or safe(detail.get("information", {}).get("name", ""))
        expr    = detail.get("expression", {}) or {}
        formula = tokens_to_str(expr.get("tokens", []))
        if not formula:
            formula = safe(expr.get("text", ""))

        self._metrics.append({
            "project_id":        pid,
            "metric_id":         mid,
            "metric_name":       mname,
            "metric_formula":    formula,
        })

    # -------------------------------------------------------------------------
    # Fact  (GET /api/model/facts/{id}?showExpressionAs=tokens)
    # Confirmed response:
    #   information.objectId, information.name
    #   expressions[].expression.tree.columnName -> physical column
    #   expressions[].tables[].objectId          -> table GUID
    #   expressions[].tables[].name              -> table name
    # -------------------------------------------------------------------------
    def _register_fact(self, fid: str, fname: str, pid: str):
        key = f"{pid}:{fid}"
        if any(f"{r['project_id']}:{r['fact_id']}" == key
               for r in self._facts):
            return

        if key not in self._fcache:
            self._fcache[key] = self.c.get(
                f"/model/facts/{fid}", pid=pid,
                params={"showExpressionAs": "tokens"})
        detail = self._fcache[key] or {}

        fname = fname or safe(detail.get("information", {}).get("name", ""))

        for expr in detail.get("expressions", []):
            tree       = expr.get("expression", {}).get("tree", {})
            col_name   = safe(tree.get("columnName", ""))

            for tbl_info in expr.get("tables", []):
                tbl_id   = safe(tbl_info.get("objectId", ""))
                tbl_name = safe(tbl_info.get("name", ""))

                self._facts.append({
                    "project_id":  pid,
                    "fact_id":     fid,
                    "fact_name":   fname,
                    "column_name": col_name,
                    "table_id":    tbl_id,
                    "table_name":  tbl_name,
                })
                if tbl_id:
                    self._register_table(tbl_id, tbl_name, pid)

    # -------------------------------------------------------------------------
    # Filter  (GET /api/model/filters/{id}?showFilterTokens=true&showExpressionAs=tokens)
    # Confirmed response: qualification.text  (human-readable filter expression)
    # -------------------------------------------------------------------------
    def _register_filter(self, fid: str, fname: str, pid: str) -> str:
        """Register a filter and return its expression text."""
        key = f"{pid}:{fid}"
        if any(f"{r['project_id']}:{r['filter_id']}" == key for r in self._filters):
            # Already registered -- return cached expression
            cached = [r for r in self._filters if f"{r['project_id']}:{r['filter_id']}" == key]
            return cached[0].get("filter_expression", "") if cached else ""

        detail = self.c.get(
            f"/model/filters/{fid}", pid=pid,
            params={"showFilterTokens": "true", "showExpressionAs": "tokens"})
        fname = fname or safe(detail.get("information", {}).get("name", ""))

        # qualification.text is the human-readable filter expression
        qual     = detail.get("qualification", {}) or {}
        expr_txt = safe(qual.get("text", ""))
        if not expr_txt:
            # Fallback: build from tokens
            expr_txt = tokens_to_str(qual.get("tokens", []))

        self._filters.append({
            "project_id":        pid,
            "filter_id":         fid,
            "filter_name":       fname,
            "filter_expression": expr_txt,
        })
        return expr_txt

    # -------------------------------------------------------------------------
    # Prompt  (GET /api/model/prompts/{id})
    # Also pulled inline from /api/reports/{id}/prompts
    # -------------------------------------------------------------------------
    def _register_prompt(self, prid: str, prname: str, prtype: str, pid: str):
        key = f"{pid}:{prid}"
        if any(f"{r['project_id']}:{r['prompt_id']}" == key for r in self._prompts):
            return
        # Try to get full definition
        detail = self.c.get(f"/model/prompts/{prid}", pid=pid) or {}
        prname = prname or safe(detail.get("information", {}).get("name", ""))
        prtype = prtype or safe(detail.get("information", {}).get("subType", ""))
        self._prompts.append({
            "project_id":  pid,
            "prompt_id":   prid,
            "prompt_name": prname,
            "prompt_type": prtype,
        })

    # -------------------------------------------------------------------------
    # Reports  (GET /api/model/reports/{id})
    # Confirmed structure:
    #   grid.rows[]/columns[]: list of sections
    #   Each section has units[] or is itself a unit: {id, name, type}
    #   type "attribute" or "metrics"
    # -------------------------------------------------------------------------
    def _extract_units(self, detail: dict) -> tuple:
        """Return (metrics_list, attrs_list) from /model/reports response."""
        grid  = detail.get("grid", {})
        avail = detail.get("availableObjects", {})

        units = []
        for section in grid.get("rows", []) + grid.get("columns", []):
            for u in section.get("units", []):
                units.append(u)
            if "type" in section and "id" in section:
                units.append(section)

        metrics = avail.get("metrics", []) + \
                  [u for u in units if u.get("type") in ("metric", "metrics")]
        attrs   = avail.get("attributes", []) + \
                  [u for u in units if u.get("type") == "attribute"]

        # Deduplicate by id
        def dedup(lst):
            seen, out = set(), []
            for x in lst:
                oid = safe(x.get("id"))
                if oid and oid not in seen:
                    seen.add(oid)
                    out.append(x)
            return out

        return dedup(metrics), dedup(attrs)

    def harvest_reports(self, pid: str, pname: str):
        log.info(f"  [REPORTS] {pname}...")
        for obj in self.c.search_all(TYPE_REPORT, pid):
            rid     = safe(obj.get("id"))
            rname   = safe(obj.get("name"))
            subtype = int(obj.get("subtype", SUBTYPE_GRID) or SUBTYPE_GRID)

            self._reports.append({
                "project_id":     pid,
                "report_id":      rid,
                "report_name":    rname,
                "report_subtype": str(subtype),
                "sql_preview":    "",
            })

            # Freeform SQL -- capture SQL, skip schema traversal
            if subtype == SUBTYPE_FREEFORM:
                sql_data = self.c.get(f"/reports/{rid}/sqlView", pid=pid)
                sql_text = ""
                if sql_data:
                    passes = sql_data.get("sqlStatements", [])
                    sql_text = " | ".join(
                        p.get("sql", "") for p in passes
                        if isinstance(p, dict))
                self._reports[-1]["sql_preview"] = sql_text
                continue

            # GET /api/model/reports/{id}
            detail = self.c.get(f"/model/reports/{rid}", pid=pid)
            if not detail:
                continue

            # Cube-sourced report -- just record the source cube
            if subtype == SUBTYPE_CUBE_RPT:
                ds = (detail.get("dataSource", {}) or
                      detail.get("dataPartition", {}))
                src_cube_id   = safe(ds.get("objectId", ds.get("id", "")))
                src_cube_name = safe(ds.get("name", ""))
                self._reports[-1]["source_cube_id"]   = src_cube_id
                self._reports[-1]["source_cube_name"] = src_cube_name
                continue

            # Standard grid/graph report
            metrics, attrs = self._extract_units(detail)
            for m in metrics:
                mid = safe(m.get("id"))
                if not mid:
                    continue
                self._br_rm.append({"project_id": pid,
                                    "report_id": rid, "metric_id": mid})
                self._register_metric(mid, safe(m.get("name", "")), pid)

            for a in attrs:
                aid = safe(a.get("id"))
                if not aid:
                    continue
                self._br_ra.append({"project_id": pid,
                                    "report_id": rid, "attribute_id": aid})
                self._register_attribute(aid, safe(a.get("name", "")), pid)

            # Report filter  -- in /model/reports response under "filter"
            # qualification.text gives the full human-readable filter expression
            rpt_filter = detail.get("filter", {}) or {}
            fid_inline  = safe(rpt_filter.get("objectId", rpt_filter.get("id", "")))
            if fid_inline:
                self._register_filter(fid_inline,
                                      safe(rpt_filter.get("name", "")), pid)
                self._br_rf.append({"project_id": pid,
                                    "report_id": rid, "filter_id": fid_inline})
            else:
                # Inline filter expression (no separate filter object)
                qual = rpt_filter.get("qualification", {}) or {}
                expr = safe(qual.get("text", ""))
                if expr:
                    synthetic_id = f"inline_{rid}"
                    self._filters.append({
                        "project_id":        pid,
                        "filter_id":         synthetic_id,
                        "filter_name":       f"{rname} [Report Filter]",
                        "filter_expression": expr,
                    })
                    self._br_rf.append({"project_id": pid,
                                        "report_id": rid,
                                        "filter_id": synthetic_id})

            # Prompts  -- GET /api/reports/{id}/prompts
            prompts_resp = self.c.get(f"/reports/{rid}/prompts", pid=pid)
            for pr in (prompts_resp if isinstance(prompts_resp, list) else []):
                prid   = safe(pr.get("id", pr.get("key", "")))
                prname = safe(pr.get("name", ""))
                prtype = safe(pr.get("type", pr.get("promptType", "")))
                if prid:
                    self._register_prompt(prid, prname, prtype, pid)
                    self._br_rp.append({"project_id": pid,
                                        "report_id": rid, "prompt_id": prid})

    # -------------------------------------------------------------------------
    # Cubes  (GET /api/model/reports/{id} -- same endpoint works for cubes)
    # -------------------------------------------------------------------------
    def harvest_cubes(self, pid: str, pname: str):
        log.info(f"  [CUBES] {pname}...")
        for obj in self.c.search_all(TYPE_CUBE, pid):
            cid   = safe(obj.get("id"))
            cname = safe(obj.get("name"))

            # SQL preview via data API
            sql_data = self.c.get(f"/cubes/{cid}/sqlView", pid=pid)
            sql_text = ""
            if sql_data:
                passes   = sql_data.get("sqlStatements", [])
                sql_text = " | ".join(
                    p.get("sql", "") for p in passes if isinstance(p, dict))

            self._cubes.append({
                "project_id":      pid,
                "cube_id":         cid,
                "cube_name":       cname,
                "sql_preview":     sql_text,
                "cube_source_type": "",   # filled in below from /model/reports
            })

            # Schema definition via /model/reports (same endpoint for cubes)
            # sourceType field tells us whether this is schema-based or freeform SQL:
            #   "normal"               -> schema-based: traverse metrics, attributes
            #   "custom_sql_free_form" -> freeform SQL cube: SQL IS the lineage
            detail = self.c.get(f"/model/reports/{cid}", pid=pid)
            if not detail:
                continue

            source_type = safe(detail.get("sourceType", "normal"))
            self._cubes[-1]["cube_source_type"] = source_type

            # Harvest filter + prompts regardless of cube type
            self._harvest_cube_filter_prompt(cid, cname, pid, detail)

            if source_type == "custom_sql_free_form":
                # Freeform SQL cube -- SQL already captured above via sqlView.
                # Also try to get SQL directly from definition if sqlView was empty.
                if not self._cubes[-1]["sql_preview"]:
                    data_src = detail.get("dataSource", {}) or {}
                    tbl      = data_src.get("table", {}) or {}
                    phys     = tbl.get("physicalTable", {}) or {}
                    sql_expr = phys.get("sqlExpression", {}) or {}
                    sql_tree = sql_expr.get("tree", {}) or {}
                    sql_val  = ""
                    for child in sql_tree.get("children", []):
                        sql_val += safe(child.get("variant", {}).get("value", ""))
                    if sql_val:
                        self._cubes[-1]["sql_preview"] = sql_val
                log.debug(f"    [FREEFORM CUBE] {cname} -- SQL captured, skipping schema traversal")
                continue   # no attributes/metrics to traverse

            # Schema-based cube -- traverse metrics and attributes normally
            metrics, attrs = self._extract_units(detail)
            for m in metrics:
                mid = safe(m.get("id"))
                if not mid:
                    continue
                self._br_cm.append({"project_id": pid,
                                    "cube_id": cid, "metric_id": mid})
                self._register_metric(mid, safe(m.get("name", "")), pid)

            for a in attrs:
                aid = safe(a.get("id"))
                if not aid:
                    continue
                self._br_ca.append({"project_id": pid,
                                    "cube_id": cid, "attribute_id": aid})
                self._register_attribute(aid, safe(a.get("name", "")), pid)

    # -------------------------------------------------------------------------
    # Cube filters and prompts
    # -------------------------------------------------------------------------
    def _harvest_cube_filter_prompt(self, cid: str, cname: str,
                                    pid: str, detail: dict):
        """Extract filter and prompts from a cube definition."""
        # Cube filter (same structure as report filter)
        cube_filter = detail.get("filter", {}) or {}
        fid_inline  = safe(cube_filter.get("objectId", cube_filter.get("id", "")))
        if fid_inline:
            self._register_filter(fid_inline,
                                  safe(cube_filter.get("name", "")), pid)
            self._br_cf.append({"project_id": pid,
                                 "cube_id": cid, "filter_id": fid_inline})
        else:
            qual = cube_filter.get("qualification", {}) or {}
            expr = safe(qual.get("text", ""))
            if expr:
                synthetic_id = f"inline_{cid}"
                self._filters.append({
                    "project_id":        pid,
                    "filter_id":         synthetic_id,
                    "filter_name":       f"{cname} [Cube Filter]",
                    "filter_expression": expr,
                })
                self._br_cf.append({"project_id": pid,
                                     "cube_id": cid, "filter_id": synthetic_id})

        # Cube prompts -- GET /api/cubes/{id}/prompts
        prompts_resp = self.c.get(f"/cubes/{cid}/prompts", pid=pid)
        for pr in (prompts_resp if isinstance(prompts_resp, list) else []):
            prid   = safe(pr.get("id", pr.get("key", "")))
            prname = safe(pr.get("name", ""))
            prtype = safe(pr.get("type", pr.get("promptType", "")))
            if prid:
                self._register_prompt(prid, prname, prtype, pid)
                self._br_cp.append({"project_id": pid,
                                     "cube_id": cid, "prompt_id": prid})

    # -------------------------------------------------------------------------
    # Documents / Dossiers
    # -------------------------------------------------------------------------
    def harvest_documents(self, pid: str, pname: str):
        log.info(f"  [DOCUMENTS] {pname}...")
        for obj in self.c.search_all(TYPE_DOCUMENT, pid):
            did     = safe(obj.get("id"))
            dname   = safe(obj.get("name"))
            subtype = "Dossier" if obj.get("subtype") == 14081 else "Document"
            self._documents.append({
                "project_id": pid,
                "doc_id":     did,
                "doc_name":   dname,
                "doc_subtype": subtype,
            })
            detail = (self.c.get(f"/dossiers/{did}/definition", pid=pid) or
                      self.c.get(f"/documents/{did}/definition", pid=pid))
            if not detail:
                continue
            for ds in detail.get("datasets", []):
                ds_id = safe(ds.get("id"))
                if ds_id:
                    self._br_dd.append({
                        "project_id":   pid,
                        "doc_id":       did,
                        "dataset_id":   ds_id,
                        "dataset_name": safe(ds.get("name", "")),
                        "dataset_type": safe(ds.get("type", "")),
                    })

    # -------------------------------------------------------------------------
    # Facts (standalone pass)
    # -------------------------------------------------------------------------
    def harvest_facts(self, pid: str, pname: str):
        log.info(f"  [FACTS] {pname}...")
        for obj in self.c.search_all(TYPE_FACT, pid):
            self._register_fact(safe(obj.get("id")),
                                safe(obj.get("name", "")), pid)

    # -------------------------------------------------------------------------
    # Standalone metric + attribute passes (catch anything missed by reports)
    # -------------------------------------------------------------------------
    def harvest_standalone_metrics(self, pid: str, pname: str):
        for obj in self.c.search_all(TYPE_METRIC, pid):
            self._register_metric(safe(obj.get("id")),
                                  safe(obj.get("name", "")), pid)

    def harvest_standalone_attributes(self, pid: str, pname: str):
        for obj in self.c.search_all(TYPE_ATTRIBUTE, pid):
            self._register_attribute(safe(obj.get("id")),
                                     safe(obj.get("name", "")), pid)

    # -------------------------------------------------------------------------
    # Per-project orchestrator
    # -------------------------------------------------------------------------
    def harvest_project(self, pid: str, pname: str):
        print(f"\n    Project : {pname}")
        print(f"    ID      : {pid}")
        print(f"    {'--' * 28}")
        log.info(f"  == Project: {pname} ({pid}) ==")
        self.harvest_objects(pid, pname)
        self.harvest_reports(pid, pname)
        self.harvest_cubes(pid, pname)
        self.harvest_documents(pid, pname)
        self.harvest_facts(pid, pname)
        self.harvest_standalone_metrics(pid, pname)
        self.harvest_standalone_attributes(pid, pname)

        # Per-project summary
        r_count = sum(1 for r in self._reports    if r["project_id"] == pid)
        c_count = sum(1 for c in self._cubes       if c["project_id"] == pid)
        d_count = sum(1 for d in self._documents   if d["project_id"] == pid)
        m_count = sum(1 for m in self._metrics     if m["project_id"] == pid)
        a_count = sum(1 for a in self._attributes  if a["project_id"] == pid)
        f_count  = sum(1 for f in self._facts    if f["project_id"] == pid)
        fl_count = sum(1 for f in self._filters  if f["project_id"] == pid)
        pr_count = sum(1 for p in self._prompts  if p["project_id"] == pid)
        print(f"    Reports: {r_count}  Cubes: {c_count}  Docs: {d_count}")
        print(f"    Metrics: {m_count}  Attrs: {a_count}  Facts: {f_count}"
              f"  Filters: {fl_count}  Prompts: {pr_count}")

    # -------------------------------------------------------------------------
    # Build DataFrames
    # -------------------------------------------------------------------------
    def build_dataframes(self) -> dict:
        step("PHASE 2", "Building normalized DataFrames...")

        def to_df(rows, dedup=None):
            if not rows:
                return pd.DataFrame()
            df = pd.DataFrame(rows)
            return df.drop_duplicates(subset=dedup) if dedup else df.drop_duplicates()

        dfs = {
            "df_objects":      to_df(self._objects,    ["project_id", "object_id"]),
            "df_reports":      to_df(self._reports,    ["project_id", "report_id"]),
            "df_cubes":        to_df(self._cubes,      ["project_id", "cube_id"]),
            "df_documents":    to_df(self._documents,  ["project_id", "doc_id"]),
            "df_metrics":      to_df(self._metrics,    ["project_id", "metric_id"]),
            "df_attributes":   to_df(self._attributes, ["project_id", "attribute_id"]),
            "df_attr_forms":   to_df(self._attr_forms),
            "df_facts":        to_df(self._facts),
            "df_br_rpt_metric":to_df(self._br_rm),
            "df_br_rpt_attr":  to_df(self._br_ra),
            "df_br_cube_metric":to_df(self._br_cm),
            "df_br_cube_attr": to_df(self._br_ca),
            "df_br_doc_ds":    to_df(self._br_dd),
            "df_filters":      to_df(self._filters,   ["project_id","filter_id"]),
            "df_prompts":      to_df(self._prompts,   ["project_id","prompt_id"]),
            "df_br_rpt_filter":to_df(self._br_rf),
            "df_br_cube_filter":to_df(self._br_cf),
            "df_br_rpt_prompt":to_df(self._br_rp),
            "df_br_cube_prompt":to_df(self._br_cp),
            "df_datasources":  to_df(self._datasources, ["datasource_id"]),
            "df_tables":       to_df(self._tables,     ["project_id", "table_id"]),
            "df_columns":      to_df(self._columns),
        }

        for name, df in dfs.items():
            log.info(f"  {name:<22}: {len(df):>7,} rows")

        return dfs


# =============================================================================
# PHASE 3 -- LINEAGE JOINER
# =============================================================================
class LineageJoiner:
    """
    Joins all DataFrames into one edge-grain lineage DataFrame.

    df_objects is the universal backbone -- every parent_node_id and
    child_node_id in df_lineage joins back to df_objects.object_id
    to get object_type, object_name, folder, owner for any node.

    Edge grain: one row = one directional relationship.
    """

    # -------------------------------------------------------------------------
    # FINAL FLAT TABLE STRUCTURE
    # One row = one lineage path.  Same columns for ALL object types.
    #
    # SCHEMA rows  (attribute or metric):
    #   object_type       = "Attribute" or "Metric"
    #   object_name       = attribute or metric name
    #   attribute_column  = physical column from attribute form expression
    #   metric_formula    = metric formula text
    #   sql_preview       = blank
    #   table_name        = physical table
    #   column_name       = physical column
    #   db_instance_name  = always filled
    #
    # FREEFORM rows  (no attributes/metrics):
    #   object_type       = "FreeformSQL"
    #   object_name       = blank
    #   attribute_column  = blank
    #   metric_formula    = blank
    #   sql_preview       = full SQL
    #   table_name        = one of the tables extracted from the SQL
    #   column_name       = blank (freeform has no column mapping)
    #   db_instance_name  = always filled
    #
    # FILTER / PROMPT rows:
    #   object_type       = "Filter" or "Prompt"
    #   object_name       = filter/prompt name
    #   filter_expression = filter expression text
    #   prompt_type       = prompt type
    #   table/column/db   = blank (filters don't map to physical layer directly)
    # -------------------------------------------------------------------------
    FINAL_COLS = [
        "lineage_row_id",
        # -- Who owns this lineage path --------------------------------------
        "project_id",
        "project_name",
        "app_name",           # Dossier / Document name  (the application)
        "app_folder",         # folder the dossier lives in
        "app_owner",          # owner of the dossier
        # -- The dataset (cube or report) ------------------------------------
        "dataset_name",       # cube or report name
        "dataset_type",       # Schema Cube / Freeform Cube / Grid Report / Freeform Report
        "dataset_folder",     # folder of the cube/report
        "dataset_owner",      # owner of the cube/report
        # -- The object on that dataset --------------------------------------
        "object_type",        # Attribute / Metric / Filter / Prompt / FreeformSQL
        "object_name",        # attribute/metric/filter/prompt name  (blank for freeform)
        # -- Schema enrichment -----------------------------------------------
        "attribute_column",   # column used in attribute form expression  (schema only)
        "metric_formula",     # metric formula text                       (schema only)
        "filter_expression",  # filter qualification text                 (filter rows)
        "prompt_type",        # prompt type                               (prompt rows)
        # -- Freeform enrichment ---------------------------------------------
        "sql_preview",        # full SQL                                  (freeform only)
        # -- Physical layer -- populated for EVERYONE ------------------------
        "table_name",         # physical table name
        "column_name",        # physical column  (blank for freeform)
        "column_data_type",   # column data type (blank for freeform)
        "db_instance_name",   # database instance name
        "dsn_name",           # DSN connection name
        "db_type",            # SQL Server / Oracle / Redshift etc
        # -- Metadata --------------------------------------------------------
        "report_subtype",     # 768=Grid 772=Freeform 774=CubeSourced
        "cube_source_type",   # normal / custom_sql_free_form
        "harvested_at",
    ]

    def __init__(self, dfs: dict, df_projects: pd.DataFrame):
        self.dfs      = dfs
        self.projects = df_projects
        self.ts       = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self._edges:  list = []

    # -- Lookups ---------------------------------------------------------------

    def _pname(self, pid: str) -> str:
        row = self.projects[self.projects["project_id"] == pid]
        return row["project_name"].iloc[0] if not row.empty else pid

    def _obj(self, oid: str, pid: str) -> dict:
        """Look up an object in df_objects."""
        df = self.dfs["df_objects"]
        if df.empty or "object_id" not in df.columns:
            return {}
        row = df[(df["object_id"] == oid) & (df["project_id"] == pid)]
        if row.empty:
            row = df[df["object_id"] == oid]
        return row.iloc[0].to_dict() if not row.empty else {}

    def _ds(self, ds_id: str) -> dict:
        df = self.dfs["df_datasources"]
        if df.empty or "datasource_id" not in df.columns:
            return {}
        row = df[df["datasource_id"] == ds_id]
        return row.iloc[0].to_dict() if not row.empty else {}

    def _tbl_ds(self, tbl_id: str, pid: str) -> str:
        df = self.dfs["df_tables"]
        if df.empty:
            return ""
        row = df[(df["table_id"] == tbl_id) & (df["project_id"] == pid)]
        if row.empty:
            row = df[df["table_id"] == tbl_id]
        return row["datasource_id"].iloc[0] if not row.empty else ""

    def _cols(self, tbl_id: str, pid: str) -> pd.DataFrame:
        df = self.dfs["df_columns"]
        if df.empty:
            return pd.DataFrame()
        res = df[(df["table_id"] == tbl_id) & (df["project_id"] == pid)]
        return res if not res.empty else df[df["table_id"] == tbl_id]

    # -- Edge builder ----------------------------------------------------------

    def _e(self, **kw) -> dict:
        row = {c: "" for c in self.FINAL_COLS}
        row["node_level"]   = 0
        row["harvested_at"] = self.ts
        row.update(kw)
        return row

    def _enrich_node(self, oid: str, pid: str) -> dict:
        """Get folder and owner for a node from df_objects."""
        info = self._obj(oid, pid)
        return {
            "folder": info.get("folder_name", ""),
            "owner":  info.get("owner", ""),
        }

    # -- Table -> Column edges -------------------------------------------------

    def _table_column_edges(self, tbl_id: str, tbl_name: str,
                            top_type: str, top_id: str, top_name: str,
                            pid: str, pname: str, level: int,
                            extra: dict = None):
        ds_id  = self._tbl_ds(tbl_id, pid)
        ds_inf = self._ds(ds_id) if ds_id else {}
        extra  = extra or {}

        for _, col in self._cols(tbl_id, pid).iterrows():
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type=top_type, top_object_id=top_id,
                top_object_name=top_name,
                edge_type="Table->Column",
                parent_node_type="Table", parent_node_id=tbl_id,
                parent_node_name=tbl_name,
                child_node_type="Column", child_node_id="",
                child_node_name=col.get("column_name", ""),
                node_level=level,
                table_id=tbl_id, table_name=tbl_name,
                column_name=col.get("column_name", ""),
                column_data_type=col.get("column_data_type", ""),
                datasource_id=ds_id,
                db_instance_name=ds_inf.get("db_instance_name", ""),
                dsn_name=ds_inf.get("dsn_name", ""),
                db_type=ds_inf.get("db_type", ""),
                **extra
            ))

    # -- Attribute -> Form -> Table -> Column ----------------------------------

    def _attr_chain(self, aid: str, aname: str,
                    top_type: str, top_id: str, top_name: str,
                    pid: str, pname: str, base_level: int):
        forms = self.dfs["df_attr_forms"]
        if forms.empty:
            return

        af = forms[(forms["project_id"] == pid) & (forms["attribute_id"] == aid)]
        for _, f in af.iterrows():
            tbl_id   = safe(f.get("table_id", ""))
            tbl_name = safe(f.get("table_name", ""))
            ds_id    = self._tbl_ds(tbl_id, pid) if tbl_id else ""
            ds_inf   = self._ds(ds_id) if ds_id else {}

            # Attribute -> Form edge
            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type=top_type, top_object_id=top_id,
                top_object_name=top_name,
                edge_type="Attribute->Form",
                parent_node_type="Attribute", parent_node_id=aid,
                parent_node_name=aname,
                child_node_type="AttributeForm", child_node_id=aid,
                child_node_name=f"{aname} [{f.get('form_name','')}]",
                node_level=base_level,
                form_name=safe(f.get("form_name", "")),
                form_expression=safe(f.get("form_expression", "")),
                table_id=tbl_id, table_name=tbl_name,
                datasource_id=ds_id,
                db_instance_name=ds_inf.get("db_instance_name", ""),
                dsn_name=ds_inf.get("dsn_name", ""),
                db_type=ds_inf.get("db_type", ""),
            ))

            # Table -> Column edges
            if tbl_id:
                self._table_column_edges(
                    tbl_id, tbl_name,
                    top_type, top_id, top_name,
                    pid, pname, base_level + 1)

    # -- Report edges ----------------------------------------------------------

    def _report_edges(self):
        step("PHASE 3", "Building report lineage edges...")
        rpts  = self.dfs["df_reports"]
        br_m  = self.dfs["df_br_rpt_metric"]
        br_a  = self.dfs["df_br_rpt_attr"]
        mets  = self.dfs["df_metrics"]
        attrs = self.dfs["df_attributes"]

        if rpts.empty:
            return

        for _, rpt in rpts.iterrows():
            pid     = rpt["project_id"]
            rid     = rpt["report_id"]
            rname   = rpt.get("report_name", "")
            subtype = safe(rpt.get("report_subtype", ""))
            pname   = self._pname(pid)
            obj_inf = self._enrich_node(rid, pid)

            # Freeform SQL
            sql_prev = safe(rpt.get("sql_preview", ""))
            if subtype == str(SUBTYPE_FREEFORM):
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Report", top_object_id=rid,
                    top_object_name=rname,
                    edge_type="Report->FreeformSQL",
                    parent_node_type="Report", parent_node_id=rid,
                    parent_node_name=rname,
                    child_node_type="FreeformSQL", child_node_id=rid,
                    child_node_name=f"{rname} [SQL]",
                    node_level=2, report_subtype=subtype,
                    sql_preview=sql_prev,
                    parent_folder=obj_inf.get("folder", ""),
                    parent_owner=obj_inf.get("owner", ""),
                ))
                continue

            # Cube-sourced report
            if subtype == str(SUBTYPE_CUBE_RPT):
                src_cube_id   = safe(rpt.get("source_cube_id", ""))
                src_cube_name = safe(rpt.get("source_cube_name", ""))
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Report", top_object_id=rid,
                    top_object_name=rname,
                    edge_type="Report->Cube(OLAP)",
                    parent_node_type="Report", parent_node_id=rid,
                    parent_node_name=rname,
                    child_node_type="Cube", child_node_id=src_cube_id,
                    child_node_name=src_cube_name,
                    node_level=2, report_subtype=subtype,
                    parent_folder=obj_inf.get("folder", ""),
                    parent_owner=obj_inf.get("owner", ""),
                ))
                continue

            # Guard empty bridges
            if br_m.empty or "report_id" not in br_m.columns:
                continue

            # Report -> Metric
            r_mets = br_m[br_m["report_id"] == rid]
            r_mets = r_mets.merge(mets, on=["project_id", "metric_id"],
                                  how="left") if not r_mets.empty else pd.DataFrame()
            for _, m in r_mets.iterrows():
                mid    = safe(m.get("metric_id", ""))
                mname  = safe(m.get("metric_name", ""))
                m_inf  = self._enrich_node(mid, pid)
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Report", top_object_id=rid,
                    top_object_name=rname,
                    edge_type="Report->Metric",
                    parent_node_type="Report", parent_node_id=rid,
                    parent_node_name=rname,
                    child_node_type="Metric", child_node_id=mid,
                    child_node_name=mname,
                    node_level=2, report_subtype=subtype,
                    metric_formula=safe(m.get("metric_formula", "")),
                    parent_folder=obj_inf.get("folder", ""),
                    parent_owner=obj_inf.get("owner", ""),
                    child_folder=m_inf.get("folder", ""),
                    child_owner=m_inf.get("owner", ""),
                ))

            # Report -> Attribute -> Form -> Table -> Column
            if br_a.empty or "report_id" not in br_a.columns:
                continue
            r_attrs = br_a[br_a["report_id"] == rid]
            r_attrs = r_attrs.merge(attrs, on=["project_id", "attribute_id"],
                                    how="left") if not r_attrs.empty else pd.DataFrame()
            for _, a in r_attrs.iterrows():
                aid   = safe(a.get("attribute_id", ""))
                aname = safe(a.get("attribute_name", ""))
                a_inf = self._enrich_node(aid, pid)
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Report", top_object_id=rid,
                    top_object_name=rname,
                    edge_type="Report->Attribute",
                    parent_node_type="Report", parent_node_id=rid,
                    parent_node_name=rname,
                    child_node_type="Attribute", child_node_id=aid,
                    child_node_name=aname,
                    node_level=2, report_subtype=subtype,
                    parent_folder=obj_inf.get("folder", ""),
                    parent_owner=obj_inf.get("owner", ""),
                    child_folder=a_inf.get("folder", ""),
                    child_owner=a_inf.get("owner", ""),
                ))
                self._attr_chain(aid, aname, "Report", rid, rname,
                                 pid, pname, 3)

    # -- Cube edges ------------------------------------------------------------

    def _cube_edges(self):
        step("PHASE 3", "Building cube lineage edges...")
        cubes = self.dfs["df_cubes"]
        br_m  = self.dfs["df_br_cube_metric"]
        br_a  = self.dfs["df_br_cube_attr"]
        mets  = self.dfs["df_metrics"]
        attrs = self.dfs["df_attributes"]

        if cubes.empty:
            return

        for _, cube in cubes.iterrows():
            pid   = cube["project_id"]
            cid   = cube["cube_id"]
            cname = cube.get("cube_name", "")
            pname = self._pname(pid)
            c_inf = self._enrich_node(cid, pid)
            sql   = safe(cube.get("sql_preview", ""))

            src_type = safe(cube.get("cube_source_type", ""))
            if sql:
                # Freeform SQL cube -- edge type makes it explicit
                edge_lbl = "Cube->FreeformSQL" if src_type == "custom_sql_free_form" else "Cube->SQL"
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Cube", top_object_id=cid,
                    top_object_name=cname,
                    edge_type=edge_lbl,
                    parent_node_type="Cube", parent_node_id=cid,
                    parent_node_name=cname,
                    child_node_type="CubeSQL", child_node_id=cid,
                    child_node_name=f"{cname} [{'Freeform SQL' if src_type == 'custom_sql_free_form' else 'SQL'}]",
                    node_level=2, sql_preview=sql,
                    cube_source_type=src_type,
                    parent_folder=c_inf.get("folder", ""),
                    parent_owner=c_inf.get("owner", ""),
                ))

            if br_m.empty or "cube_id" not in br_m.columns:
                continue

            c_mets = br_m[br_m["cube_id"] == cid]
            c_mets = c_mets.merge(mets, on=["project_id", "metric_id"],
                                  how="left") if not c_mets.empty else pd.DataFrame()
            for _, m in c_mets.iterrows():
                mid    = safe(m.get("metric_id", ""))
                mname  = safe(m.get("metric_name", ""))
                m_inf  = self._enrich_node(mid, pid)
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Cube", top_object_id=cid,
                    top_object_name=cname,
                    edge_type="Cube->Metric",
                    parent_node_type="Cube", parent_node_id=cid,
                    parent_node_name=cname,
                    child_node_type="Metric", child_node_id=mid,
                    child_node_name=mname,
                    node_level=2,
                    metric_formula=safe(m.get("metric_formula", "")),
                    parent_folder=c_inf.get("folder", ""),
                    parent_owner=c_inf.get("owner", ""),
                    child_folder=m_inf.get("folder", ""),
                    child_owner=m_inf.get("owner", ""),
                ))

            if br_a.empty or "cube_id" not in br_a.columns:
                continue
            c_attrs = br_a[br_a["cube_id"] == cid]
            c_attrs = c_attrs.merge(attrs, on=["project_id", "attribute_id"],
                                    how="left") if not c_attrs.empty else pd.DataFrame()
            for _, a in c_attrs.iterrows():
                aid   = safe(a.get("attribute_id", ""))
                aname = safe(a.get("attribute_name", ""))
                a_inf = self._enrich_node(aid, pid)
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type="Cube", top_object_id=cid,
                    top_object_name=cname,
                    edge_type="Cube->Attribute",
                    parent_node_type="Cube", parent_node_id=cid,
                    parent_node_name=cname,
                    child_node_type="Attribute", child_node_id=aid,
                    child_node_name=aname,
                    node_level=2,
                    parent_folder=c_inf.get("folder", ""),
                    parent_owner=c_inf.get("owner", ""),
                    child_folder=a_inf.get("folder", ""),
                    child_owner=a_inf.get("owner", ""),
                ))
                self._attr_chain(aid, aname, "Cube", cid, cname,
                                 pid, pname, 3)

    # -- Document edges --------------------------------------------------------

    def _document_edges(self):
        step("PHASE 3", "Building document lineage edges...")
        docs = self.dfs["df_documents"]
        br   = self.dfs["df_br_doc_ds"]

        if docs.empty or br.empty:
            return

        merged = br.merge(docs, on=["project_id", "doc_id"], how="left")
        for _, r in merged.iterrows():
            pid      = r["project_id"]
            pname    = self._pname(pid)
            did      = r["doc_id"]
            dname    = safe(r.get("doc_name", ""))
            subtype  = safe(r.get("doc_subtype", "Document"))
            ds_id    = r["dataset_id"]
            ds_name  = safe(r.get("dataset_name", ""))
            ds_type  = safe(r.get("dataset_type", ""))
            d_inf    = self._enrich_node(did, pid)
            ds_inf   = self._enrich_node(ds_id, pid)

            child_type = "Cube" if ds_type == str(TYPE_CUBE) else "Report"
            # Resolve name from objects if blank
            if not ds_name:
                obj = self._obj(ds_id, pid)
                ds_name = obj.get("object_name", "")

            self._edges.append(self._e(
                project_id=pid, project_name=pname,
                top_object_type=subtype, top_object_id=did,
                top_object_name=dname,
                edge_type=f"{subtype}->{child_type}",
                parent_node_type=subtype, parent_node_id=did,
                parent_node_name=dname,
                child_node_type=child_type, child_node_id=ds_id,
                child_node_name=ds_name,
                node_level=2, doc_subtype=subtype,
                parent_folder=d_inf.get("folder", ""),
                parent_owner=d_inf.get("owner", ""),
                child_folder=ds_inf.get("folder", ""),
                child_owner=ds_inf.get("owner", ""),
            ))

    # -- Filter edges (Report/Cube -> Filter with expression) -----------------

    def _filter_edges(self):
        for obj_type, id_col, br_key in [
            ("Report", "report_id", "df_br_rpt_filter"),
            ("Cube",   "cube_id",   "df_br_cube_filter"),
        ]:
            obj_df = self.dfs.get(f"df_{obj_type.lower()}s", pd.DataFrame())
            br_df  = self.dfs.get(br_key, pd.DataFrame())
            flt_df = self.dfs.get("df_filters", pd.DataFrame())

            if br_df.empty or flt_df.empty:
                continue

            merged = br_df.merge(flt_df, on=["project_id","filter_id"], how="left")
            if not obj_df.empty and id_col in obj_df.columns:
                name_col = "report_name" if obj_type == "Report" else "cube_name"
                merged = merged.merge(
                    obj_df[["project_id", id_col, name_col]],
                    on=["project_id", id_col], how="left")

            for _, r in merged.iterrows():
                pid    = r["project_id"]
                pname  = self._pname(pid)
                oid    = safe(r.get(id_col, ""))
                oname  = safe(r.get("report_name" if obj_type == "Report"
                                    else "cube_name", ""))
                fid    = safe(r.get("filter_id", ""))
                fname  = safe(r.get("filter_name", ""))
                fexpr  = safe(r.get("filter_expression", ""))
                o_inf  = self._enrich_node(oid, pid)
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type=obj_type, top_object_id=oid,
                    top_object_name=oname,
                    edge_type=f"{obj_type}->Filter",
                    parent_node_type=obj_type, parent_node_id=oid,
                    parent_node_name=oname,
                    child_node_type="Filter", child_node_id=fid,
                    child_node_name=fname,
                    node_level=2,
                    filter_expression=fexpr,
                    parent_folder=o_inf.get("folder",""),
                    parent_owner=o_inf.get("owner",""),
                ))

    # -- Prompt edges (Report/Cube -> Prompt) ----------------------------------

    def _prompt_edges(self):
        for obj_type, id_col, br_key in [
            ("Report", "report_id", "df_br_rpt_prompt"),
            ("Cube",   "cube_id",   "df_br_cube_prompt"),
        ]:
            obj_df = self.dfs.get(f"df_{obj_type.lower()}s", pd.DataFrame())
            br_df  = self.dfs.get(br_key, pd.DataFrame())
            prm_df = self.dfs.get("df_prompts", pd.DataFrame())

            if br_df.empty or prm_df.empty:
                continue

            merged = br_df.merge(prm_df, on=["project_id","prompt_id"], how="left")
            if not obj_df.empty and id_col in obj_df.columns:
                name_col = "report_name" if obj_type == "Report" else "cube_name"
                merged = merged.merge(
                    obj_df[["project_id", id_col, name_col]],
                    on=["project_id", id_col], how="left")

            for _, r in merged.iterrows():
                pid   = r["project_id"]
                pname = self._pname(pid)
                oid   = safe(r.get(id_col, ""))
                oname = safe(r.get("report_name" if obj_type == "Report"
                                   else "cube_name", ""))
                prid  = safe(r.get("prompt_id", ""))
                prnam = safe(r.get("prompt_name", ""))
                prtyp = safe(r.get("prompt_type", ""))
                o_inf = self._enrich_node(oid, pid)
                self._edges.append(self._e(
                    project_id=pid, project_name=pname,
                    top_object_type=obj_type, top_object_id=oid,
                    top_object_name=oname,
                    edge_type=f"{obj_type}->Prompt",
                    parent_node_type=obj_type, parent_node_id=oid,
                    parent_node_name=oname,
                    child_node_type="Prompt", child_node_id=prid,
                    child_node_name=prnam,
                    node_level=2,
                    prompt_type=prtyp,
                    parent_folder=o_inf.get("folder",""),
                    parent_owner=o_inf.get("owner",""),
                ))

    # -- Fact edges (Fact -> Column -> Table -> Datasource) --------------------

    def _fact_edges(self):
        step("PHASE 3", "Building fact lineage edges...")
        facts = self.dfs["df_facts"]
        if facts.empty:
            return

        for pid in facts["project_id"].unique():
            pname = self._pname(pid)
            pf    = facts[facts["project_id"] == pid]

            for fid in pf["fact_id"].unique():
                ff    = pf[pf["fact_id"] == fid]
                fname = safe(ff["fact_name"].iloc[0])
                f_inf = self._enrich_node(fid, pid)

                for _, row in ff.iterrows():
                    tbl_id   = safe(row.get("table_id", ""))
                    tbl_name = safe(row.get("table_name", ""))
                    col_name = safe(row.get("column_name", ""))
                    ds_id    = self._tbl_ds(tbl_id, pid) if tbl_id else ""
                    ds_inf   = self._ds(ds_id) if ds_id else {}

                    self._edges.append(self._e(
                        project_id=pid, project_name=pname,
                        top_object_type="Fact", top_object_id=fid,
                        top_object_name=fname,
                        edge_type="Fact->Column",
                        parent_node_type="Fact", parent_node_id=fid,
                        parent_node_name=fname,
                        child_node_type="Column", child_node_id="",
                        child_node_name=col_name,
                        node_level=1,
                        fact_name=fname, fact_column=col_name,
                        table_id=tbl_id, table_name=tbl_name,
                        datasource_id=ds_id,
                        db_instance_name=ds_inf.get("db_instance_name", ""),
                        dsn_name=ds_inf.get("dsn_name", ""),
                        db_type=ds_inf.get("db_type", ""),
                        parent_folder=f_inf.get("folder", ""),
                        parent_owner=f_inf.get("owner", ""),
                    ))

    # -- Datasource topology (DB Instance -> DSN -> Table -> Column) -----------

    def _datasource_edges(self):
        step("PHASE 3", "Building datasource topology edges...")
        ds_df  = self.dfs["df_datasources"]
        tbl_df = self.dfs["df_tables"]

        if ds_df.empty:
            return

        for _, ds in ds_df.iterrows():
            did   = ds["datasource_id"]
            dname = ds.get("db_instance_name", "")
            dsn   = ds.get("dsn_name", "")
            dbtyp = ds.get("db_type", "")

            self._edges.append(self._e(
                project_id="__global__",
                top_object_type="DBInstance", top_object_id=did,
                top_object_name=dname,
                edge_type="DBInstance->DSN",
                parent_node_type="DBInstance", parent_node_id=did,
                parent_node_name=dname,
                child_node_type="DSN", child_node_id=did,
                child_node_name=dsn,
                node_level=1,
                datasource_id=did, db_instance_name=dname,
                dsn_name=dsn, db_type=dbtyp,
            ))

            if tbl_df.empty:
                continue

            for _, t in tbl_df[tbl_df["datasource_id"] == did].iterrows():
                tid   = t["table_id"]
                tname = t["table_name"]
                pid   = t["project_id"]

                self._edges.append(self._e(
                    project_id=pid,
                    top_object_type="DBInstance", top_object_id=did,
                    top_object_name=dname,
                    edge_type="DSN->Table",
                    parent_node_type="DSN", parent_node_id=did,
                    parent_node_name=dsn,
                    child_node_type="Table", child_node_id=tid,
                    child_node_name=tname,
                    node_level=2,
                    table_id=tid, table_name=tname,
                    datasource_id=did, db_instance_name=dname,
                    dsn_name=dsn, db_type=dbtyp,
                ))

                for _, col in self._cols(tid, pid).iterrows():
                    self._edges.append(self._e(
                        project_id=pid,
                        top_object_type="DBInstance", top_object_id=did,
                        top_object_name=dname,
                        edge_type="Table->Column",
                        parent_node_type="Table", parent_node_id=tid,
                        parent_node_name=tname,
                        child_node_type="Column", child_node_id="",
                        child_node_name=col.get("column_name", ""),
                        node_level=3,
                        table_id=tid, table_name=tname,
                        column_name=col.get("column_name", ""),
                        column_data_type=col.get("column_data_type", ""),
                        datasource_id=did, db_instance_name=dname,
                        dsn_name=dsn, db_type=dbtyp,
                    ))

    def _cols(self, tbl_id: str, pid: str) -> pd.DataFrame:
        df = self.dfs["df_columns"]
        if df.empty:
            return pd.DataFrame()
        res = df[(df["table_id"] == tbl_id) & (df["project_id"] == pid)]
        return res if not res.empty else df[df["table_id"] == tbl_id]

    # -- Build final DataFrame -------------------------------------------------

    def _resolve_ds(self, ds_id: str) -> tuple:
        """Return (db_instance_name, dsn_name, db_type) for a datasource id."""
        info = self._ds(ds_id)
        return (info.get("db_instance_name",""),
                info.get("dsn_name",""),
                info.get("db_type",""))

    def _flat_row(self, **kw) -> dict:
        """Build one flat lineage row with all FINAL_COLS defaulted to blank."""
        row = {c: "" for c in self.FINAL_COLS}
        row["harvested_at"] = self.ts
        row.update(kw)
        return row

    def build(self) -> pd.DataFrame:
        """
        Build the final FLAT lineage DataFrame.
        One row = one lineage path from app to physical layer.
        Same columns for schema and freeform -- freeform columns blank for schema,
        attribute/metric columns blank for freeform.
        """
        rows = []
        dfs  = self.dfs

        # -- helpers -----------------------------------------------------------
        def pname(pid):
            r = self.projects[self.projects["project_id"] == pid]
            return r["project_name"].iloc[0] if not r.empty else pid

        def obj_info(oid, pid):
            info = self._obj(oid, pid)
            return info.get("folder_name",""), info.get("owner","")

        def tbl_physical(tbl_id, pid):
            """Return (table_name, datasource_id) for a table_id."""
            tbls = dfs.get("df_tables", pd.DataFrame())
            if tbls.empty:
                return "", ""
            r = tbls[(tbls["table_id"]==tbl_id)&(tbls["project_id"]==pid)]
            if r.empty:
                r = tbls[tbls["table_id"]==tbl_id]
            if r.empty:
                return "", ""
            return safe(r["table_name"].iloc[0]), safe(r["datasource_id"].iloc[0])

        def cols_for_table(tbl_id, pid):
            return self._cols(tbl_id, pid)

        # -- document -> dataset map -------------------------------------------
        docs   = dfs.get("df_documents",  pd.DataFrame())
        br_dd  = dfs.get("df_br_doc_ds",  pd.DataFrame())
        # Build lookup: dataset_id -> (app_name, app_folder, app_owner, doc_subtype)
        ds_to_app = {}
        if not docs.empty and not br_dd.empty:
            doc_map = docs.set_index(["project_id","doc_id"]).to_dict("index")
            for _, br in br_dd.iterrows():
                pid    = br["project_id"]
                doc_id = br["doc_id"]
                ds_id2 = br["dataset_id"]
                key    = (pid, doc_id)
                dinfo  = doc_map.get(key, {})
                ds_to_app[(pid, ds_id2)] = {
                    "app_name":    dinfo.get("doc_name",""),
                    "app_folder":  "",
                    "app_owner":   "",
                    "doc_subtype": dinfo.get("doc_subtype",""),
                }

        def get_app(pid, dataset_id):
            return ds_to_app.get((pid, dataset_id),
                   {"app_name":"(No App)","app_folder":"","app_owner":"","doc_subtype":""})

        # ====================================================================
        # SCHEMA CUBES
        # ====================================================================
        cubes     = dfs.get("df_cubes",          pd.DataFrame())
        br_cm     = dfs.get("df_br_cube_metric",  pd.DataFrame())
        br_ca     = dfs.get("df_br_cube_attr",    pd.DataFrame())
        metrics   = dfs.get("df_metrics",         pd.DataFrame())
        attrs     = dfs.get("df_attributes",      pd.DataFrame())
        attr_forms= dfs.get("df_attr_forms",      pd.DataFrame())

        if not cubes.empty:
            for _, cube in cubes.iterrows():
                pid        = cube["project_id"]
                cid        = cube["cube_id"]
                cname      = safe(cube.get("cube_name",""))
                src_type   = safe(cube.get("cube_source_type",""))
                sql_prev   = safe(cube.get("sql_preview",""))
                c_fold, c_own = obj_info(cid, pid)
                app        = get_app(pid, cid)
                pn         = pname(pid)

                if src_type == "custom_sql_free_form":
                    # -- FREEFORM CUBE -- one row per table in SQL
                    tables_in_sql = extract_tables_from_sql(sql_prev)
                    if not tables_in_sql:
                        tables_in_sql = ["(tables not parsed)"]
                    for tbl in tables_in_sql:
                        # Try to find datasource from df_tables
                        tbl_rows = dfs.get("df_tables", pd.DataFrame())
                        ds_id3 = ""
                        if not tbl_rows.empty:
                            tr = tbl_rows[tbl_rows["table_name"].str.upper() == tbl.upper()]
                            if not tr.empty:
                                ds_id3 = tr["datasource_id"].iloc[0]
                        db_inst, dsn, db_tp = self._resolve_ds(ds_id3)
                        rows.append(self._flat_row(
                            project_id=pid, project_name=pn,
                            app_name=app["app_name"],
                            app_folder=app["app_folder"],
                            app_owner=app["app_owner"],
                            dataset_name=cname,
                            dataset_type="Freeform Cube",
                            dataset_folder=c_fold,
                            dataset_owner=c_own,
                            object_type="FreeformSQL",
                            sql_preview=sql_prev,
                            table_name=tbl,
                            db_instance_name=db_inst,
                            dsn_name=dsn,
                            db_type=db_tp,
                            cube_source_type=src_type,
                        ))
                    continue

                # -- SCHEMA CUBE -- metrics ------------------------------------
                dataset_type = "Schema Cube"
                if not br_cm.empty and "cube_id" in br_cm.columns:
                    cm = br_cm[(br_cm["project_id"]==pid)&(br_cm["cube_id"]==cid)]
                    cm = cm.merge(metrics, on=["project_id","metric_id"], how="left") if not cm.empty else pd.DataFrame()
                    for _, m in cm.iterrows():
                        mid   = safe(m.get("metric_id",""))
                        mname = safe(m.get("metric_name",""))
                        form  = safe(m.get("metric_formula",""))
                        m_fold, m_own = obj_info(mid, pid)
                        rows.append(self._flat_row(
                            project_id=pid, project_name=pn,
                            app_name=app["app_name"],
                            app_folder=app["app_folder"],
                            app_owner=app["app_owner"],
                            dataset_name=cname,
                            dataset_type=dataset_type,
                            dataset_folder=c_fold,
                            dataset_owner=c_own,
                            object_type="Metric",
                            object_name=mname,
                            metric_formula=form,
                            cube_source_type=src_type,
                        ))

                # -- SCHEMA CUBE -- attributes -> forms -> table -> columns ----
                if not br_ca.empty and "cube_id" in br_ca.columns:
                    ca = br_ca[(br_ca["project_id"]==pid)&(br_ca["cube_id"]==cid)]
                    ca = ca.merge(attrs, on=["project_id","attribute_id"], how="left") if not ca.empty else pd.DataFrame()
                    for _, a in ca.iterrows():
                        aid   = safe(a.get("attribute_id",""))
                        aname = safe(a.get("attribute_name",""))
                        a_fold, a_own = obj_info(aid, pid)
                        if not attr_forms.empty:
                            af = attr_forms[(attr_forms["project_id"]==pid)&(attr_forms["attribute_id"]==aid)]
                            for _, f in af.iterrows():
                                tbl_id   = safe(f.get("table_id",""))
                                tbl_name = safe(f.get("table_name",""))
                                attr_col = safe(f.get("form_expression",""))
                                ds_id3   = self._tbl_ds(tbl_id, pid) if tbl_id else ""
                                db_inst, dsn, db_tp = self._resolve_ds(ds_id3)
                                # One row per column in that table
                                tcols = cols_for_table(tbl_id, pid)
                                if not tcols.empty:
                                    for _, col in tcols.iterrows():
                                        rows.append(self._flat_row(
                                            project_id=pid, project_name=pn,
                                            app_name=app["app_name"],
                                            app_folder=app["app_folder"],
                                            app_owner=app["app_owner"],
                                            dataset_name=cname,
                                            dataset_type=dataset_type,
                                            dataset_folder=c_fold,
                                            dataset_owner=c_own,
                                            object_type="Attribute",
                                            object_name=aname,
                                            attribute_column=attr_col,
                                            table_name=tbl_name,
                                            column_name=safe(col.get("column_name","")),
                                            column_data_type=safe(col.get("column_data_type","")),
                                            db_instance_name=db_inst,
                                            dsn_name=dsn,
                                            db_type=db_tp,
                                            cube_source_type=src_type,
                                        ))
                                else:
                                    rows.append(self._flat_row(
                                        project_id=pid, project_name=pn,
                                        app_name=app["app_name"],
                                        app_folder=app["app_folder"],
                                        app_owner=app["app_owner"],
                                        dataset_name=cname,
                                        dataset_type=dataset_type,
                                        dataset_folder=c_fold,
                                        dataset_owner=c_own,
                                        object_type="Attribute",
                                        object_name=aname,
                                        attribute_column=attr_col,
                                        table_name=tbl_name,
                                        db_instance_name=db_inst,
                                        dsn_name=dsn,
                                        db_type=db_tp,
                                        cube_source_type=src_type,
                                    ))

        # ====================================================================
        # REPORTS (Grid + Freeform)
        # ====================================================================
        rpts   = dfs.get("df_reports",       pd.DataFrame())
        br_rm  = dfs.get("df_br_rpt_metric", pd.DataFrame())
        br_ra  = dfs.get("df_br_rpt_attr",   pd.DataFrame())
        br_rf  = dfs.get("df_br_rpt_filter", pd.DataFrame())
        br_rp  = dfs.get("df_br_rpt_prompt", pd.DataFrame())
        filters= dfs.get("df_filters",       pd.DataFrame())
        prompts= dfs.get("df_prompts",       pd.DataFrame())

        if not rpts.empty:
            for _, rpt in rpts.iterrows():
                pid      = rpt["project_id"]
                rid      = rpt["report_id"]
                rname    = safe(rpt.get("report_name",""))
                subtype  = safe(rpt.get("report_subtype",""))
                sql_prev = safe(rpt.get("sql_preview",""))
                r_fold, r_own = obj_info(rid, pid)
                app      = get_app(pid, rid)
                pn       = pname(pid)

                # -- FREEFORM REPORT ------------------------------------------
                if subtype == str(SUBTYPE_FREEFORM):
                    tables_in_sql = extract_tables_from_sql(sql_prev)
                    if not tables_in_sql:
                        tables_in_sql = ["(tables not parsed)"]
                    for tbl in tables_in_sql:
                        tbl_rows = dfs.get("df_tables", pd.DataFrame())
                        ds_id3 = ""
                        if not tbl_rows.empty:
                            tr = tbl_rows[tbl_rows["table_name"].str.upper() == tbl.upper()]
                            if not tr.empty:
                                ds_id3 = tr["datasource_id"].iloc[0]
                        db_inst, dsn, db_tp = self._resolve_ds(ds_id3)
                        rows.append(self._flat_row(
                            project_id=pid, project_name=pn,
                            app_name=app["app_name"],
                            app_folder=app["app_folder"],
                            app_owner=app["app_owner"],
                            dataset_name=rname,
                            dataset_type="Freeform Report",
                            dataset_folder=r_fold,
                            dataset_owner=r_own,
                            object_type="FreeformSQL",
                            sql_preview=sql_prev,
                            table_name=tbl,
                            db_instance_name=db_inst,
                            dsn_name=dsn,
                            db_type=db_tp,
                            report_subtype=subtype,
                        ))
                    continue

                dataset_type = "Grid Report"

                # -- GRID REPORT -- metrics ------------------------------------
                if not br_rm.empty and "report_id" in br_rm.columns:
                    rm = br_rm[(br_rm["project_id"]==pid)&(br_rm["report_id"]==rid)]
                    rm = rm.merge(metrics, on=["project_id","metric_id"], how="left") if not rm.empty else pd.DataFrame()
                    for _, m in rm.iterrows():
                        rows.append(self._flat_row(
                            project_id=pid, project_name=pn,
                            app_name=app["app_name"],
                            app_folder=app["app_folder"],
                            app_owner=app["app_owner"],
                            dataset_name=rname,
                            dataset_type=dataset_type,
                            dataset_folder=r_fold,
                            dataset_owner=r_own,
                            object_type="Metric",
                            object_name=safe(m.get("metric_name","")),
                            metric_formula=safe(m.get("metric_formula","")),
                            report_subtype=subtype,
                        ))

                # -- GRID REPORT -- attributes -> forms -> table -> columns ---
                if not br_ra.empty and "report_id" in br_ra.columns:
                    ra = br_ra[(br_ra["project_id"]==pid)&(br_ra["report_id"]==rid)]
                    ra = ra.merge(attrs, on=["project_id","attribute_id"], how="left") if not ra.empty else pd.DataFrame()
                    for _, a in ra.iterrows():
                        aid   = safe(a.get("attribute_id",""))
                        aname = safe(a.get("attribute_name",""))
                        if not attr_forms.empty:
                            af = attr_forms[(attr_forms["project_id"]==pid)&(attr_forms["attribute_id"]==aid)]
                            for _, f in af.iterrows():
                                tbl_id   = safe(f.get("table_id",""))
                                tbl_name = safe(f.get("table_name",""))
                                attr_col = safe(f.get("form_expression",""))
                                ds_id3   = self._tbl_ds(tbl_id, pid) if tbl_id else ""
                                db_inst, dsn, db_tp = self._resolve_ds(ds_id3)
                                tcols = cols_for_table(tbl_id, pid)
                                if not tcols.empty:
                                    for _, col in tcols.iterrows():
                                        rows.append(self._flat_row(
                                            project_id=pid, project_name=pn,
                                            app_name=app["app_name"],
                                            app_folder=app["app_folder"],
                                            app_owner=app["app_owner"],
                                            dataset_name=rname,
                                            dataset_type=dataset_type,
                                            dataset_folder=r_fold,
                                            dataset_owner=r_own,
                                            object_type="Attribute",
                                            object_name=aname,
                                            attribute_column=attr_col,
                                            table_name=tbl_name,
                                            column_name=safe(col.get("column_name","")),
                                            column_data_type=safe(col.get("column_data_type","")),
                                            db_instance_name=db_inst,
                                            dsn_name=dsn,
                                            db_type=db_tp,
                                            report_subtype=subtype,
                                        ))
                                else:
                                    rows.append(self._flat_row(
                                        project_id=pid, project_name=pn,
                                        app_name=app["app_name"],
                                        app_folder=app["app_folder"],
                                        app_owner=app["app_owner"],
                                        dataset_name=rname,
                                        dataset_type=dataset_type,
                                        dataset_folder=r_fold,
                                        dataset_owner=r_own,
                                        object_type="Attribute",
                                        object_name=aname,
                                        attribute_column=attr_col,
                                        table_name=tbl_name,
                                        db_instance_name=db_inst,
                                        dsn_name=dsn,
                                        db_type=db_tp,
                                        report_subtype=subtype,
                                    ))

                # -- GRID REPORT -- filters ------------------------------------
                if not br_rf.empty and "report_id" in br_rf.columns and not filters.empty:
                    rf = br_rf[(br_rf["project_id"]==pid)&(br_rf["report_id"]==rid)]
                    rf = rf.merge(filters, on=["project_id","filter_id"], how="left") if not rf.empty else pd.DataFrame()
                    for _, flt in rf.iterrows():
                        rows.append(self._flat_row(
                            project_id=pid, project_name=pn,
                            app_name=app["app_name"],
                            app_folder=app["app_folder"],
                            app_owner=app["app_owner"],
                            dataset_name=rname,
                            dataset_type=dataset_type,
                            dataset_folder=r_fold,
                            dataset_owner=r_own,
                            object_type="Filter",
                            object_name=safe(flt.get("filter_name","")),
                            filter_expression=safe(flt.get("filter_expression","")),
                            report_subtype=subtype,
                        ))

                # -- GRID REPORT -- prompts ------------------------------------
                if not br_rp.empty and "report_id" in br_rp.columns and not prompts.empty:
                    rp = br_rp[(br_rp["project_id"]==pid)&(br_rp["report_id"]==rid)]
                    rp = rp.merge(prompts, on=["project_id","prompt_id"], how="left") if not rp.empty else pd.DataFrame()
                    for _, prm in rp.iterrows():
                        rows.append(self._flat_row(
                            project_id=pid, project_name=pn,
                            app_name=app["app_name"],
                            app_folder=app["app_folder"],
                            app_owner=app["app_owner"],
                            dataset_name=rname,
                            dataset_type=dataset_type,
                            dataset_folder=r_fold,
                            dataset_owner=r_own,
                            object_type="Prompt",
                            object_name=safe(prm.get("prompt_name","")),
                            prompt_type=safe(prm.get("prompt_type","")),
                            report_subtype=subtype,
                        ))

        # ====================================================================
        # BUILD FINAL DATAFRAME
        # ====================================================================
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=self.FINAL_COLS)

        for col in self.FINAL_COLS:
            if col not in df.columns:
                df[col] = ""

        df = df[self.FINAL_COLS].copy()
        df["lineage_row_id"] = [str(i+1).zfill(8) for i in range(len(df))]
        df["harvested_at"]   = self.ts

        str_cols = [c for c in df.columns]
        df[str_cols] = df[str_cols].fillna("").astype(str).apply(lambda s: s.str.strip())
        df = df.drop_duplicates().reset_index(drop=True)

        step("PHASE 3", f"Final flat lineage DataFrame: {len(df):,} rows")
        if not df.empty and "dataset_type" in df.columns:
            for dt, cnt in df["dataset_type"].value_counts().items():
                log.info(f"    {dt:<30}: {cnt:>6,} rows")
        return df


# =============================================================================
# PHASE 4 -- CUBE PUBLISHER (Push Data API)
# =============================================================================
class CubePublisher:
    """
    Publishes df_lineage as an Intelligent Cube on the dev server.

    Update-or-create:
      Run 1 (cube does not exist):
        POST /api/datasets                               create definition
        PUT  /api/datasets/{id}/uploadSessions/{sid}/tables/{t}
        POST /api/datasets/{id}/uploadSessions/{sid}/publish

      Run 2+ (cube exists):
        POST /api/datasets/{id}/uploadSessions           open new session
        PUT  ...uploadSessions/{new_sid}/tables/{t}      upload (Replace+Add)
        POST ...uploadSessions/{new_sid}/publish         commit
    """

    METRIC_COL = "node_level"

    def __init__(self, client: MSTRClient, project_id: str,
                 cube_name: str, table_name: str, folder_id: str = ""):
        self.c    = client
        self.pid  = project_id
        self.name = cube_name
        self.tbl  = table_name
        self.fid  = folder_id

    def _attr_cols(self, df: pd.DataFrame) -> list:
        return [c for c in df.columns if c != self.METRIC_COL]

    def _find_existing(self) -> str:
        data = self.c.get("/searches/results", pid=self.pid,
                          params={"type": TYPE_CUBE, "name": self.name,
                                  "limit": 50})
        for obj in data.get("result", []):
            if safe(obj.get("name")) == self.name:
                eid = safe(obj.get("id"))
                log.info(f"  [FOUND] Existing cube {eid} -> will UPDATE")
                return eid
        log.info("  [NOT FOUND] Will CREATE new cube")
        return ""

    def _definition(self, df: pd.DataFrame) -> dict:
        ac  = self._attr_cols(df)
        all = ac + [self.METRIC_COL]
        body = {
            "name":   self.name,
            "tables": [{"name": self.tbl, "columnHeaders": all}],
            "attributes": [
                {"name": c, "attributeForms": [{
                    "category": "ID",
                    "expressions": [{"formula": f"{self.tbl}.{c}"}]
                }]}
                for c in ac
            ],
            "metrics": [{
                "name": "Node Level",
                "dataType": "integer",
                "expressions": [{"formula": f"{self.tbl}.{self.METRIC_COL}"}]
            }]
        }
        if self.fid:
            body["folderId"] = self.fid
        return body

    def _serialize(self, chunk: pd.DataFrame, policy: str) -> dict:
        ac   = self._attr_cols(chunk)
        all  = ac + [self.METRIC_COL]
        hdrs = {c: i for i, c in enumerate(all)}
        rows = []
        for _, r in chunk.iterrows():
            rec = [str(r.get(c, "") or "") for c in ac]
            rec.append(str(int(r.get(self.METRIC_COL, 0) or 0)))
            rows.append(rec)
        return {"data": {"headers": hdrs, "rawData": rows},
                "updatePolicy": policy}

    def _open_session(self, dataset_id: str) -> str:
        resp = self.c.post(f"/datasets/{dataset_id}/uploadSessions",
                           body={"uploadSessionType": "normalUpload"},
                           pid=self.pid)
        sid = resp.get("uploadSessionId", "")
        if not sid:
            raise RuntimeError(f"Failed to open upload session: {resp}")
        log.info(f"  [SESSION] {sid}")
        return sid

    def _upload(self, dataset_id: str, sid: str, df: pd.DataFrame):
        chunks = [df.iloc[i:i + CHUNK_SIZE]
                  for i in range(0, len(df), CHUNK_SIZE)]
        for idx, chunk in enumerate(chunks):
            policy = "Replace" if idx == 0 else "Add"
            log.info(f"  [UPLOAD] Chunk {idx+1}/{len(chunks)} "
                     f"({len(chunk):,} rows) policy={policy}")
            self.c.put(
                f"/datasets/{dataset_id}/uploadSessions/{sid}/tables/{self.tbl}",
                body=self._serialize(chunk, policy),
                pid=self.pid)

    def _publish(self, dataset_id: str, sid: str):
        resp = self.c.post(
            f"/datasets/{dataset_id}/uploadSessions/{sid}/publish",
            body={}, pid=self.pid)
        log.info(f"  [PUBLISH] {resp}")

    def publish(self, df: pd.DataFrame) -> str:
        if df.empty:
            raise ValueError("No lineage rows to publish.")

        step("PHASE 4", f"Update-or-create cube '{self.name}'")
        log.info(f"  Target  : {self.c.base_url}")
        log.info(f"  Project : {self.pid}")
        log.info(f"  Rows    : {len(df):,}")

        existing = self._find_existing()

        if existing:
            sid = self._open_session(existing)
            self._upload(existing, sid, df)
            self._publish(existing, sid)
            log.info(f"  Cube UPDATED -> {existing}")
            return existing
        else:
            resp = self.c.post("/datasets", body=self._definition(df),
                               pid=self.pid)
            dataset_id = resp.get("datasetId") or resp.get("id", "")
            sid        = resp.get("uploadSessionId", "")
            if not dataset_id:
                raise RuntimeError(f"Dataset creation failed: {resp}")
            if not sid:
                sid = self._open_session(dataset_id)
            log.info(f"  [CREATE] Dataset {dataset_id} Session {sid}")
            self._upload(dataset_id, sid, df)
            self._publish(dataset_id, sid)
            log.info(f"  Cube CREATED -> {dataset_id}")
            return dataset_id


# =============================================================================
# MAIN
# =============================================================================
def main():
    run_ts  = datetime.now().strftime("%Y%m%d_%H%M%S")
    t_start = datetime.now()

    print("=" * 65)
    print("  MSTR FULL-CHAIN LINEAGE HARVESTER  v3.0.0")
    print(f"  Run        : {run_ts}")
    print(f"  Source     : {SOURCE_BASE_URL}")
    print(f"  Target     : {TARGET_BASE_URL}")
    print(f"  Projects   : {RUN_ONLY_PROJECT_IDS if RUN_ONLY_PROJECT_IDS else 'ALL'}")
    print(f"  Cube       : {CUBE_NAME}  (update-or-create)")
    print("=" * 65)

    src = MSTRClient(SOURCE_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD)
    src.login()

    df_lineage = pd.DataFrame()

    try:
        # Phase 1
        df_projects = discover_projects(src, RUN_ONLY_PROJECT_IDS)
        if df_projects.empty:
            log.error("No projects found. Check RUN_ONLY_PROJECT_IDS or credentials.")
            return

        # Phase 2
        step("PHASE 2", "Harvesting metadata from production...")
        engine = HarvestEngine(src)
        engine.harvest_datasources()

        n = len(df_projects)
        for i, (_, proj) in enumerate(df_projects.iterrows(), 1):
            pid   = proj["project_id"]
            pname = proj["project_name"]
            step("PHASE 2", f"Project {i}/{n}: {pname}")
            engine.harvest_project(pid, pname)

        dfs = engine.build_dataframes()

        # Phase 3
        step("PHASE 3", "Joining DataFrames -> lineage edges...")
        joiner     = LineageJoiner(dfs, df_projects)
        df_lineage = joiner.build()

        print("\n  Sample edges (edge_type | parent | child | level):")
        print(df_lineage[["edge_type", "parent_node_name",
                           "child_node_name", "node_level"]]
              .head(10).to_string(index=False))

    finally:
        src.logout()

    # Phase 4
    tgt = MSTRClient(TARGET_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD)
    tgt.login()
    try:
        pub = CubePublisher(tgt, TARGET_PROJECT_ID, CUBE_NAME,
                            TABLE_NAME, TARGET_FOLDER_ID)
        dataset_id = pub.publish(df_lineage)
        step("PHASE 4", f"Cube ID: {dataset_id}")
    finally:
        tgt.logout()

    elapsed = (datetime.now() - t_start).seconds
    print("\n" + "=" * 65)
    print(f"  COMPLETE  |  {len(df_lineage):,} lineage edges  |  {elapsed}s")
    print("=" * 65)


if __name__ == "__main__":
    main()
