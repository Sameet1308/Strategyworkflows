#!/usr/bin/env python3
"""
mstr_lineage_harvester.py
Version: 7.0.0  – Full lineage with Modeling Service

REQUIRES: Modeling Service configured on I-Server (KB484255)

ENDPOINTS USED:
  Auth          : POST /api/auth/login
  Projects      : GET  /api/projects
  Search        : GET  /api/searches/results?type={t}
  Report def    : GET  /api/reports/{id}
                  -> result.definition.availableObjects.attributes/metrics
  Cube def      : GET  /api/cubes/{id}
                  -> result.definition.availableObjects.attributes/metrics
  Metric detail : GET  /api/model/metrics/{id}?showExpressionAs=tokens
                  -> expression.text (formula)
  Attr detail   : GET  /api/model/attributes/{id}?showExpressionAs=tokens
                  -> forms[].expressions[].expression.text
                  -> forms[].expressions[].tables[].objectId/.name
                  -> attributeLookupTable.name
  Fact detail   : GET  /api/model/facts/{id}?showExpressionAs=tokens
                  -> expressions[].expression.tree.columnName
                  -> expressions[].tables[].objectId/.name
  Table detail  : GET  /api/model/tables/{id}
                  -> physicalTable.columns[].name/.dataType.type
                  -> information.dataSourceId or dataSource.id
  Report SQL    : POST /api/v2/reports/{id}/instances?executionStage=resolve_prompts
                  GET  /api/v2/reports/{id}/instances/{iid}/sqlView
  Datasources   : GET  /api/datasources
  Dossier def   : GET  /api/dossiers/{id}/definition -> datasets[]
  Document def  : GET  /api/documents/{id}/definition -> datasets[]
  Push Data     : POST /api/datasets (create)
                  PUT  .../uploadSessions/{sid}/tables/{t}
                  POST .../uploadSessions/{sid}/publish
"""

import re
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
# CONFIGURATION
# =============================================================================

SOURCE_BASE_URL = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"
MSTR_USERNAME   = "YOUR_USERNAME"
MSTR_PASSWORD   = "YOUR_PASSWORD"
RUN_ONLY_PROJECT_IDS = []

TARGET_BASE_URL   = "https://YOUR_DEV_SERVER/MicroStrategyLibrarySTD"
TARGET_PROJECT_ID = "YOUR_DEV_PROJECT_ID"
TARGET_FOLDER_ID  = ""

CUBE_NAME  = "MSTR_Lineage_Harvest"
TABLE_NAME = "LineageEdges"

PAGE_SIZE     = 200
REQUEST_DELAY = 0.5
CHUNK_SIZE    = 50000

# Set False to skip SQL view collection (faster, avoids instance creation)
COLLECT_SQL   = True

# =============================================================================
# CONSTANTS
# =============================================================================

TYPE_REPORT = 3;  TYPE_METRIC = 4;  TYPE_ATTRIBUTE = 12
TYPE_FACT   = 13; TYPE_TABLE  = 53; TYPE_CUBE = 776; TYPE_DOCUMENT = 55
SUBTYPE_GRID = 768; SUBTYPE_GRAPH = 769
SUBTYPE_FREEFORM = 772; SUBTYPE_CUBE_RPT = 774

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)
log = logging.getLogger("lineage")

def step(ph, msg):
    print(f"\n  [{ph}] {msg}"); log.info(f"[{ph}] {msg}")

# =============================================================================
# HELPERS
# =============================================================================

def safe(v) -> str:
    return str(v).strip() if v is not None else ""

def extract_tables_from_sql(sql: str) -> list:
    if not sql: return []
    s = re.sub(r"--[^\n]*", "", sql.upper())
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    pat = (r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN"
           r"|FULL\s+JOIN|CROSS\s+JOIN|LEFT\s+OUTER\s+JOIN"
           r"|RIGHT\s+OUTER\s+JOIN|FULL\s+OUTER\s+JOIN)\s+"
           r"([A-Z0-9_#@.]+)")
    skip = {"WHERE","SELECT","ON","SET","WITH","AS","AND","OR","NOT","IN",
            "NULL","CASE","WHEN","THEN","ELSE","END","GROUP","ORDER",
            "HAVING","UNION","EXCEPT","INTERSECT","LATERAL","VALUES"}
    matches = re.findall(pat, s)
    tables = [m.split(".")[-1] for m in matches
              if m not in skip and not m.startswith("(")]
    return list(dict.fromkeys(tables))

def tokens_to_str(tokens):
    if not isinstance(tokens, list): return ""
    return "".join(t.get("value","") for t in tokens if isinstance(t,dict))

# =============================================================================
# REST CLIENT
# =============================================================================

class MSTRClient:
    def __init__(self, base_url, username, password):
        self.base_url = base_url
        self.api = base_url + "/api"
        self.username = username
        self.password = password
        self.token: Optional[str] = None
        self._s = requests.Session()
        self._s.verify = False

    def login(self):
        r = self._s.post(f"{self.api}/auth/login",
            json={"username":self.username,"password":self.password,"loginMode":1},
            headers={"Content-Type":"application/json"}, verify=False)
        r.raise_for_status()
        self.token = r.headers["X-MSTR-AuthToken"]
        log.info(f"[AUTH] Connected -> {self.base_url}")

    def logout(self):
        if self.token:
            try: self._s.post(f"{self.api}/auth/logout",
                              headers=self._h(), verify=False)
            except: pass
            self.token = None; log.info("[AUTH] Disconnected")

    def _h(self, pid=""):
        h = {"X-MSTR-AuthToken":self.token,
             "Content-Type":"application/json","Accept":"application/json"}
        if pid: h["X-MSTR-ProjectID"] = pid
        return h

    def _req(self, method, path, pid="", params=None, body=None, retries=2):
        for attempt in range(retries+1):
            time.sleep(REQUEST_DELAY)
            try:
                r = self._s.request(method, f"{self.api}{path}",
                    headers=self._h(pid), params=params, json=body,
                    verify=False, timeout=180)
                if r.status_code == 500 and attempt < retries:
                    time.sleep((attempt+1)*3); continue
                if r.status_code in (400,403,404,500):
                    log.debug(f"  [{r.status_code}] {method} {path} -> {r.text[:300]}")
                    return {}
                r.raise_for_status()
                return r.json() if r.text.strip() else {}
            except requests.exceptions.Timeout:
                if attempt < retries: time.sleep((attempt+1)*5); continue
                log.warning(f"  [TIMEOUT] {method} {path}"); return {}
            except Exception as e:
                if attempt < retries: time.sleep((attempt+1)*3); continue
                log.warning(f"  [ERR] {method} {path} -> {e}"); return {}
        return {}

    def get(self, path, pid="", params=None):
        return self._req("GET", path, pid=pid, params=params)
    def post(self, path, body=None, pid="", params=None):
        return self._req("POST", path, pid=pid, body=body, params=params)
    def put(self, path, body=None, pid=""):
        return self._req("PUT", path, pid=pid, body=body)

    def search_all(self, obj_type, pid):
        results, offset = [], 0
        while True:
            data = self.get("/searches/results", pid=pid,
                            params={"type":obj_type,"limit":PAGE_SIZE,"offset":offset})
            items = data.get("result",[])
            results.extend(items)
            if len(items) < PAGE_SIZE: break
            offset += PAGE_SIZE
        log.info(f"    [SEARCH] type={obj_type} -> {len(results)}")
        return results

    def get_projects(self):
        d = self.get("/projects")
        return d if isinstance(d,list) else d.get("projects",[])

    def get_datasources(self):
        return self.get("/datasources").get("datasources",[])

    def get_report_sql(self, rid, pid):
        """v2 instance-based SQL view. Returns SQL string or ''."""
        resp = self.post(f"/v2/reports/{rid}/instances", pid=pid,
                         params={"executionStage": "resolve_prompts"})
        iid = resp.get("instanceId", "")
        if not iid:
            return ""
        sql_resp = self.get(f"/v2/reports/{rid}/instances/{iid}/sqlView",
                            pid=pid)
        if not sql_resp:
            return ""
        # Response: list of sql statement objects or sqlStatement field
        if isinstance(sql_resp, list):
            return " | ".join(safe(s.get("sql","")) for s in sql_resp)
        stmts = sql_resp.get("sqlStatements", sql_resp.get("sqlStatement", []))
        if isinstance(stmts, list):
            return " | ".join(safe(s.get("sql","") if isinstance(s,dict) else s)
                              for s in stmts)
        return safe(stmts)


# =============================================================================
# PHASE 1
# =============================================================================

def discover_projects(client, run_only):
    step("PHASE 1", "Discovering projects...")
    rows = []
    for p in client.get_projects():
        pid = safe(p.get("id")); name = safe(p.get("name"))
        if run_only and pid not in run_only: continue
        rows.append({"project_id":pid, "project_name":name})
        log.info(f"  [QUEUED] {name} ({pid})")
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["project_id","project_name"])
    step("PHASE 1", f"{len(df)} project(s)")
    return df


# =============================================================================
# LINEAGE HARVESTER
# =============================================================================

class LineageHarvester:

    FINAL_COLS = [
        "lineage_row_id",    "project_id",       "project_name",
        "app_name",          "app_folder",        "app_owner",
        "dataset_name",      "dataset_type",      "dataset_folder",
        "dataset_owner",     "object_type",       "object_name",
        "attribute_column",  "metric_formula",    "sql_preview",
        "table_name",        "column_name",       "column_data_type",
        "db_instance_name",  "dsn_name",          "db_type",
        "report_subtype",    "cube_source_type",  "harvested_at",
    ]

    def __init__(self, client):
        self.c = client
        self.ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        # Caches
        self._mc = {}; self._ac = {}; self._fc = {}; self._tc = {}
        self._sm = set(); self._sa = set(); self._sf = set(); self._st = set()
        self._ds_map = {}; self._obj_cache = {}
        self._rows = []

    def _r(self, **kw):
        row = {c:"" for c in self.FINAL_COLS}
        row["harvested_at"] = self.ts
        row.update({k:v for k,v in kw.items() if k in row})
        return row

    # -- Object cache from search results ------------------------------------
    def _cache_obj(self, obj, pid):
        oid = safe(obj.get("id"))
        if not oid: return
        ow = obj.get("owner",{})
        own = safe(ow.get("name","")) if isinstance(ow,dict) else safe(ow)
        anc = obj.get("ancestors",[])
        fld = safe(anc[-1].get("name","")) if anc else ""
        self._obj_cache[(pid,oid)] = {"name":safe(obj.get("name","")),"owner":own,"folder":fld}

    def _obj(self, pid, oid):
        return self._obj_cache.get((pid,oid),{"name":"","owner":"","folder":""})

    # -- Datasource ----------------------------------------------------------
    def harvest_datasources(self):
        log.info("  [DATASOURCES]...")
        for ds in self.c.get_datasources():
            did = safe(ds.get("id"))
            cn = ds.get("datasourceConnection",{}) or {}
            self._ds_map[did] = {"db_instance_name":safe(ds.get("name","")),
                                 "dsn_name":safe(cn.get("name","")),
                                 "db_type":safe(ds.get("dbType",""))}
        log.info(f"    -> {len(self._ds_map)} datasources")

    def _ds(self, ds_id):
        return self._ds_map.get(ds_id,{"db_instance_name":"","dsn_name":"","db_type":""})

    # -- Table: GET /model/tables/{id} ---------------------------------------
    def _fetch_table(self, tid, tname, pid):
        key = f"{pid}:{tid}"
        if key in self._st: return
        self._st.add(key)
        d = self.c.get(f"/model/tables/{tid}", pid=pid)
        if not d: d = {}
        nm = tname or safe(d.get("information",{}).get("name",""))
        ds_id = (safe(d.get("physicalTable",{}).get("information",{}).get("dataSourceId",""))
                 or safe(d.get("dataSource",{}).get("id","")))
        cols = []
        for c in (d.get("physicalTable",{}) or {}).get("columns",[]):
            dt = c.get("dataType",{}) or {}
            cols.append({"name":safe(c.get("name","")),"dtype":safe(dt.get("type","")) if isinstance(dt,dict) else safe(dt)})
        self._tc[key] = {"name":nm,"ds_id":ds_id,"columns":cols}

    def _tbl(self, tid, pid):
        return self._tc.get(f"{pid}:{tid}",{"name":"","ds_id":"","columns":[]})

    def _col_dt(self, tid, col, pid):
        for c in self._tbl(tid,pid).get("columns",[]):
            if safe(c.get("name","")).upper() == col.upper().strip():
                return safe(c.get("dtype",""))
        return ""

    # -- Metric: GET /model/metrics/{id}?showExpressionAs=tokens -------------
    def _fetch_metric(self, mid, pid):
        key = f"{pid}:{mid}"
        if key in self._sm: return
        self._sm.add(key)
        d = self.c.get(f"/model/metrics/{mid}", pid=pid,
                       params={"showExpressionAs":"tokens"})
        if not d: self._mc[key] = {"name":"","formula":""}; return
        nm = safe(d.get("information",{}).get("name",""))
        ex = d.get("expression",{}) or {}
        f = tokens_to_str(ex.get("tokens",[])) or safe(ex.get("text",""))
        self._mc[key] = {"name":nm,"formula":f}

    def _met(self, mid, pid):
        return self._mc.get(f"{pid}:{mid}",{"name":"","formula":""})

    # -- Attribute: GET /model/attributes/{id}?showExpressionAs=tokens -------
    def _fetch_attribute(self, aid, pid):
        key = f"{pid}:{aid}"
        if key in self._sa: return
        self._sa.add(key)
        d = self.c.get(f"/model/attributes/{aid}", pid=pid,
                       params={"showExpressionAs":"tokens"})
        if not d: self._ac[key] = {"name":"","forms":[]}; return
        nm = safe(d.get("information",{}).get("name",""))
        forms = []
        for frm in d.get("forms",[]):
            fn = safe(frm.get("name",""))
            for ex in frm.get("expressions",[]):
                eo = ex.get("expression",{})
                es = tokens_to_str(eo.get("tokens",[])) or safe(eo.get("text",""))
                for ti in ex.get("tables",[]):
                    tid = safe(ti.get("objectId",""))
                    tn  = safe(ti.get("name",""))
                    if tid: self._fetch_table(tid, tn, pid)
                    forms.append({"form_name":fn,"expression":es,"table_id":tid,"table_name":tn})
        self._ac[key] = {"name":nm,"forms":forms}

    def _attr(self, aid, pid):
        return self._ac.get(f"{pid}:{aid}",{"name":"","forms":[]})

    # -- Fact: GET /model/facts/{id}?showExpressionAs=tokens -----------------
    def _fetch_fact(self, fid, pid):
        key = f"{pid}:{fid}"
        if key in self._sf: return
        self._sf.add(key)
        d = self.c.get(f"/model/facts/{fid}", pid=pid,
                       params={"showExpressionAs":"tokens"})
        if not d: self._fc[key] = []; return
        maps = []
        for ex in d.get("expressions",[]):
            cn = safe(ex.get("expression",{}).get("tree",{}).get("columnName",""))
            for ti in ex.get("tables",[]):
                tid = safe(ti.get("objectId",""))
                tn  = safe(ti.get("name",""))
                if tid: self._fetch_table(tid, tn, pid)
                maps.append({"column_name":cn,"table_id":tid,"table_name":tn})
        self._fc[key] = maps

    def _fact(self, fid, pid):
        return self._fc.get(f"{pid}:{fid}",[])

    # -- Harvest all facts for a project -------------------------------------
    def _harvest_facts(self, pid):
        log.info("  [FACTS] Harvesting...")
        for o in self.c.search_all(TYPE_FACT, pid):
            self._cache_obj(o, pid)
            self._fetch_fact(safe(o.get("id")), pid)
        log.info(f"    -> {len([k for k in self._fc if k.startswith(pid)])} facts")

    # -- Build fact name index for formula matching --------------------------
    def _fact_index(self, pid):
        idx = {}
        for key, maps in self._fc.items():
            if not key.startswith(f"{pid}:"): continue
            fid = key.split(":",1)[1]
            fn = self._obj(pid, fid).get("name","")
            if fn: idx[fn.upper()] = fid
        return idx

    # -- Report/Cube definition (JSON Data API) ------------------------------
    def _report_def(self, rid, pid):
        d = self.c.get(f"/reports/{rid}", pid=pid)
        if not d: return {"attributes":[],"metrics":[]}
        r = d.get("result",d); df = r.get("definition",r)
        av = df.get("availableObjects",{})
        return {"attributes":av.get("attributes",[]),"metrics":av.get("metrics",[])}

    def _cube_def(self, cid, pid):
        d = self.c.get(f"/cubes/{cid}", pid=pid)
        if not d: return {"attributes":[],"metrics":[]}
        r = d.get("result",d); df = r.get("definition",r)
        av = df.get("availableObjects",{})
        return {"attributes":av.get("attributes",[]),"metrics":av.get("metrics",[])}

    # -- Row generators ------------------------------------------------------

    def _add_metrics(self, mlist, pid, pn, app, dsi, dn, dt, fi):
        for m in mlist:
            mid = safe(m.get("id")); mn = safe(m.get("name",""))
            if not mid: continue
            self._fetch_metric(mid, pid)
            mi = self._met(mid, pid)
            formula = mi["formula"]; mn = mn or mi["name"]

            # Match facts in formula
            matched = []
            fu = formula.upper()
            for fn, fid in fi.items():
                if fn in fu:
                    matched.extend(self._fact(fid, pid))

            if matched:
                seen = set()
                for fm in matched:
                    tid = fm.get("table_id",""); tn = fm.get("table_name","")
                    cn = fm.get("column_name","")
                    rk = (tn,cn)
                    if rk in seen: continue
                    seen.add(rk)
                    ti = self._tbl(tid, pid)
                    ds = self._ds(ti.get("ds_id",""))
                    cdt = self._col_dt(tid, cn, pid)
                    self._rows.append(self._r(
                        project_id=pid, project_name=pn, **app, **dsi,
                        dataset_name=dn, dataset_type=dt,
                        object_type="Metric", object_name=mn,
                        metric_formula=formula,
                        table_name=ti.get("name","") or tn,
                        column_name=cn, column_data_type=cdt, **ds))
            else:
                self._rows.append(self._r(
                    project_id=pid, project_name=pn, **app, **dsi,
                    dataset_name=dn, dataset_type=dt,
                    object_type="Metric", object_name=mn,
                    metric_formula=formula))

    def _add_attrs(self, alist, pid, pn, app, dsi, dn, dt):
        for a in alist:
            aid = safe(a.get("id")); an = safe(a.get("name",""))
            if not aid: continue
            self._fetch_attribute(aid, pid)
            ai = self._attr(aid, pid)
            an = an or ai["name"]; forms = ai.get("forms",[])

            if forms:
                for f in forms:
                    tid = f.get("table_id",""); tn = f.get("table_name","")
                    expr = f.get("expression","")
                    ti = self._tbl(tid, pid)
                    ds = self._ds(ti.get("ds_id",""))
                    cdt = self._col_dt(tid, expr, pid)
                    self._rows.append(self._r(
                        project_id=pid, project_name=pn, **app, **dsi,
                        dataset_name=dn, dataset_type=dt,
                        object_type="Attribute", object_name=an,
                        attribute_column=expr,
                        table_name=ti.get("name","") or tn,
                        column_name=expr, column_data_type=cdt, **ds))
            else:
                self._rows.append(self._r(
                    project_id=pid, project_name=pn, **app, **dsi,
                    dataset_name=dn, dataset_type=dt,
                    object_type="Attribute", object_name=an))

    def _add_freeform(self, pid, pn, app, dsi, dn, dt, sql):
        for tbl in (extract_tables_from_sql(sql) or ["(unparsed)"]):
            ds = {"db_instance_name":"","dsn_name":"","db_type":""}
            for k,ti in self._tc.items():
                if safe(ti.get("name","")).upper() == tbl.upper():
                    ds = self._ds(ti.get("ds_id","")); break
            self._rows.append(self._r(
                project_id=pid, project_name=pn, **app, **dsi,
                dataset_name=dn, dataset_type=dt,
                object_type="FreeformSQL", sql_preview=sql,
                table_name=tbl, **ds))

    # -- Main harvest --------------------------------------------------------

    def harvest_project(self, pid, pn):
        print(f"\n    Project : {pn}")
        print(f"    ID      : {pid}")
        print(f"    {'--'*28}")

        # Cache objects
        for ot in [TYPE_REPORT,TYPE_CUBE,TYPE_DOCUMENT,TYPE_METRIC,TYPE_ATTRIBUTE]:
            for o in self.c.search_all(ot, pid):
                self._cache_obj(o, pid)

        # Facts (all upfront)
        self._harvest_facts(pid)
        fi = self._fact_index(pid)
        log.info(f"    Fact index: {len(fi)} facts")

        # App mapping
        ds_to_app = {}
        for o in self.c.search_all(TYPE_DOCUMENT, pid):
            did = safe(o.get("id")); dn = safe(o.get("name"))
            di = self._obj(pid, did)
            det = (self.c.get(f"/dossiers/{did}/definition", pid=pid) or
                   self.c.get(f"/documents/{did}/definition", pid=pid))
            if not det: continue
            for ds in det.get("datasets",[]):
                dsid = safe(ds.get("id"))
                if dsid:
                    ds_to_app[dsid] = {"app_name":dn,"app_folder":di["folder"],"app_owner":di["owner"]}

        def ga(dsid):
            return ds_to_app.get(dsid,{"app_name":"","app_folder":"","app_owner":""})

        # --- REPORTS ---
        log.info(f"  [REPORTS] {pn}...")
        reports = self.c.search_all(TYPE_REPORT, pid)
        for i,o in enumerate(reports):
            rid = safe(o.get("id")); rn = safe(o.get("name"))
            sub = int(o.get("subtype", SUBTYPE_GRID) or SUBTYPE_GRID)
            ri = self._obj(pid, rid); app = ga(rid)
            dsi = {"dataset_folder":ri["folder"],"dataset_owner":ri["owner"],
                   "report_subtype":str(sub)}
            if (i+1) % 100 == 0: log.info(f"    report {i+1}/{len(reports)}...")

            if sub == SUBTYPE_FREEFORM:
                sql = ""
                if COLLECT_SQL:
                    sql = self.c.get_report_sql(rid, pid)
                if not sql:
                    sd = self.c.get(f"/reports/{rid}/sqlView", pid=pid)
                    if sd:
                        ps = sd.get("sqlStatements",[])
                        sql = " | ".join(safe(p.get("sql","")) for p in ps if isinstance(p,dict))
                self._add_freeform(pid, pn, app, dsi, rn, "Freeform Report", sql)
                continue

            defn = self._report_def(rid, pid)
            dt = "Grid Report"
            if sub == SUBTYPE_CUBE_RPT: dt = "Cube-Sourced Report"
            elif sub == SUBTYPE_GRAPH: dt = "Graph Report"

            self._add_metrics(defn.get("metrics",[]), pid, pn, app, dsi, rn, dt, fi)
            self._add_attrs(defn.get("attributes",[]), pid, pn, app, dsi, rn, dt)

        # --- CUBES ---
        log.info(f"  [CUBES] {pn}...")
        cubes = self.c.search_all(TYPE_CUBE, pid)
        for i,o in enumerate(cubes):
            cid = safe(o.get("id")); cn = safe(o.get("name"))
            ci = self._obj(pid, cid); app = ga(cid)
            if (i+1) % 50 == 0: log.info(f"    cube {i+1}/{len(cubes)}...")

            # SQL
            sd = self.c.get(f"/cubes/{cid}/sqlView", pid=pid)
            sql = ""
            if sd:
                ps = sd.get("sqlStatements",[])
                sql = " | ".join(safe(p.get("sql","")) for p in ps if isinstance(p,dict))

            defn = self._cube_def(cid, pid)
            has = bool(defn.get("attributes") or defn.get("metrics"))
            dsi = {"dataset_folder":ci["folder"],"dataset_owner":ci["owner"]}

            if sql and not has:
                dsi["cube_source_type"] = "custom_sql_free_form"
                self._add_freeform(pid, pn, app, dsi, cn, "Freeform Cube", sql)
                continue

            dsi["cube_source_type"] = "normal"
            self._add_metrics(defn.get("metrics",[]), pid, pn, app, dsi, cn, "Schema Cube", fi)
            self._add_attrs(defn.get("attributes",[]), pid, pn, app, dsi, cn, "Schema Cube")

        rc = sum(1 for r in self._rows if r.get("project_id")==pid and "Report" in r.get("dataset_type",""))
        cc = sum(1 for r in self._rows if r.get("project_id")==pid and "Cube" in r.get("dataset_type",""))
        print(f"    Rows: {rc} from reports, {cc} from cubes")

    # -- Build ---------------------------------------------------------------

    def build(self):
        df = pd.DataFrame(self._rows) if self._rows else pd.DataFrame(columns=self.FINAL_COLS)
        for c in self.FINAL_COLS:
            if c not in df.columns: df[c] = ""
        df = df[self.FINAL_COLS].copy()
        df["lineage_row_id"] = [str(i+1).zfill(8) for i in range(len(df))]
        df["harvested_at"] = self.ts
        df = df.fillna("").astype(str).apply(lambda s: s.str.strip())
        df = df.drop_duplicates().reset_index(drop=True)
        step("BUILD", f"{len(df):,} rows x {len(self.FINAL_COLS)} cols")
        if not df.empty and "dataset_type" in df.columns:
            for dt,cnt in df["dataset_type"].value_counts().items():
                log.info(f"    {dt:<25}: {cnt:>6,}")
        return df


# =============================================================================
# CUBE PUBLISHER
# =============================================================================

class CubePublisher:
    def __init__(self, client, project_id, cube_name, table_name, folder_id=""):
        self.c=client; self.pid=project_id; self.name=cube_name
        self.tbl=table_name; self.fid=folder_id

    def _find(self):
        d = self.c.get("/searches/results", pid=self.pid,
                       params={"type":TYPE_CUBE,"name":self.name,"limit":50})
        for o in d.get("result",[]):
            if safe(o.get("name"))==self.name:
                eid=safe(o.get("id")); log.info(f"  [FOUND] {eid}"); return eid
        log.info("  [NOT FOUND] Creating..."); return ""

    def _defn(self, df):
        cols = list(df.columns)
        b = {"name":self.name,"tables":[{"name":self.tbl,"columnHeaders":cols}],
             "attributes":[{"name":c,"attributeForms":[{"category":"ID",
              "expressions":[{"formula":f"{self.tbl}.{c}"}]}]} for c in cols],
             "metrics":[]}
        if self.fid: b["folderId"] = self.fid
        return b

    def _ser(self, chunk, policy):
        cols = list(chunk.columns)
        return {"data":{"headers":{c:i for i,c in enumerate(cols)},
                "rawData":[[str(r.get(c,"") or "") for c in cols] for _,r in chunk.iterrows()]},
                "updatePolicy":policy}

    def _open(self, did):
        r = self.c.post(f"/datasets/{did}/uploadSessions",
                        body={"uploadSessionType":"normalUpload"}, pid=self.pid)
        sid = r.get("uploadSessionId","")
        if not sid: raise RuntimeError(f"No session: {r}")
        return sid

    def _upload(self, did, sid, df):
        chunks = [df.iloc[i:i+CHUNK_SIZE] for i in range(0,len(df),CHUNK_SIZE)]
        for ix,ch in enumerate(chunks):
            pol = "Replace" if ix==0 else "Add"
            log.info(f"  [UPLOAD] {ix+1}/{len(chunks)} ({len(ch):,} rows) {pol}")
            self.c.put(f"/datasets/{did}/uploadSessions/{sid}/tables/{self.tbl}",
                       body=self._ser(ch,pol), pid=self.pid)

    def _pub(self, did, sid):
        r = self.c.post(f"/datasets/{did}/uploadSessions/{sid}/publish",
                        body={}, pid=self.pid)
        log.info(f"  [PUBLISH] {r}")

    def publish(self, df):
        if df.empty: raise ValueError("Empty")
        step("PUBLISH", f"'{self.name}' -> {len(df):,} rows")
        ex = self._find()
        if ex:
            sid=self._open(ex); self._upload(ex,sid,df); self._pub(ex,sid); return ex
        else:
            r = self.c.post("/datasets", body=self._defn(df), pid=self.pid)
            did = r.get("datasetId") or r.get("id","")
            sid = r.get("uploadSessionId","")
            if not did: raise RuntimeError(f"Create failed: {r}")
            if not sid: sid = self._open(did)
            self._upload(did,sid,df); self._pub(did,sid); return did


# =============================================================================
# MAIN
# =============================================================================

def main():
    t0 = datetime.now()
    print("="*65)
    print("  MSTR LINEAGE HARVESTER  v7.0.0")
    print(f"  Source  : {SOURCE_BASE_URL}")
    print(f"  Target  : {TARGET_BASE_URL}")
    print(f"  Projects: {RUN_ONLY_PROJECT_IDS or 'ALL'}")
    print(f"  Cube    : {CUBE_NAME}")
    print(f"  SQL     : {'ON' if COLLECT_SQL else 'OFF'}")
    print("="*65)

    src = MSTRClient(SOURCE_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD)
    src.login()
    df_lin = pd.DataFrame()

    try:
        dfp = discover_projects(src, RUN_ONLY_PROJECT_IDS)
        if dfp.empty: log.error("No projects"); return
        h = LineageHarvester(src)
        h.harvest_datasources()
        for i,(_,p) in enumerate(dfp.iterrows(),1):
            step("HARVEST", f"Project {i}/{len(dfp)}: {p['project_name']}")
            h.harvest_project(p["project_id"], p["project_name"])
        df_lin = h.build()

        print()
        if not df_lin.empty and "object_type" in df_lin.columns:
            for ot,cnt in df_lin["object_type"].value_counts().items():
                print(f"    {ot:<20}: {cnt:>6,}")
        print(f"  TOTAL: {len(df_lin):,} rows")
        print()
        cols = ["dataset_name","dataset_type","object_type","object_name",
                "metric_formula","attribute_column","table_name"]
        av = [c for c in cols if c in df_lin.columns]
        if av and not df_lin.empty:
            print(df_lin[av].head(15).to_string(index=False))
    finally:
        src.logout()

    if df_lin.empty:
        print("\n  [STOP] No rows."); return

    tgt = MSTRClient(TARGET_BASE_URL, MSTR_USERNAME, MSTR_PASSWORD)
    tgt.login()
    try:
        pub = CubePublisher(tgt, TARGET_PROJECT_ID, CUBE_NAME, TABLE_NAME, TARGET_FOLDER_ID)
        did = pub.publish(df_lin)
        step("DONE", f"Cube ID: {did}")
    finally:
        tgt.logout()

    print(f"\n{'='*65}")
    print(f"  COMPLETE | {len(df_lin):,} edges | {(datetime.now()-t0).seconds}s")
    print("="*65)

if __name__ == "__main__":
    main()
