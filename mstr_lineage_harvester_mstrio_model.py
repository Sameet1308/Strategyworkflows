#!/usr/bin/env python3
"""
mstr_lineage_harvester_mstrio_model.py
Version: 7m  – Full lineage using mstrio-py + Modeling Service

REQUIRES:
  pip install mstrio-py pandas
  Modeling Service configured on I-Server (KB484255)

Uses mstrio-py classes:
  Connection            – auth + session management
  Metric                – expression.text for formula
  Attribute             – forms[].expressions for column/table mapping
  Fact, list_facts      – column + table mapping
  LogicalTable          – physical columns + datasource
  SuperCube             – push data to I-Server
  full_search           – find all objects by type
  list_reports          – enumerate reports
"""

import logging
import warnings
from datetime import datetime

import pandas as pd
from mstrio.connection import Connection
from mstrio import config as mstrio_config
from mstrio.object_management import full_search
from mstrio.project_objects.report import list_reports
from mstrio.project_objects.datasets.super_cube import SuperCube
from mstrio.modeling.metric import Metric
from mstrio.modeling.schema.attribute import Attribute
from mstrio.modeling.schema.fact import Fact, list_facts
from mstrio.modeling.schema.table import LogicalTable, list_logical_tables
from mstrio.modeling.expression.enums import ExpressionFormat
from mstrio.server import Project
from mstrio.types import ObjectTypes, ObjectSubTypes

mstrio_config.verbose = False
warnings.filterwarnings("ignore")

# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_BASE_URL = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"
MSTR_USERNAME   = "YOUR_USERNAME"
MSTR_PASSWORD   = "YOUR_PASSWORD"
SOURCE_PROJECT  = "YOUR_PROJECT_NAME"   # or use project_id=

TARGET_BASE_URL   = "https://YOUR_DEV_SERVER/MicroStrategyLibrarySTD"
TARGET_PROJECT_ID = "YOUR_DEV_PROJECT_ID"
TARGET_FOLDER_ID  = ""

CUBE_NAME  = "MSTR_Lineage_Harvest"

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("lineage")

def step(ph, msg):
    print(f"\n  [{ph}] {msg}"); log.info(f"[{ph}] {msg}")

def safe(v):
    return str(v).strip() if v is not None else ""

# =============================================================================
# FINAL COLUMNS (24)
# =============================================================================

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

# =============================================================================
# HARVESTER
# =============================================================================

class MstrioLineageHarvester:

    def __init__(self, conn: Connection):
        self.conn = conn
        self.ts   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self._rows = []
        self._metric_cache  = {}
        self._attr_cache    = {}
        self._fact_cache    = {}
        self._table_cache   = {}
        self._ds_map        = {}

    def _r(self, **kw):
        row = {c: "" for c in FINAL_COLS}
        row["harvested_at"] = self.ts
        row.update({k: v for k, v in kw.items() if k in row})
        return row

    # -- Datasource mapping --------------------------------------------------
    def _load_datasources(self):
        """Load datasource info via REST (mstrio-py doesn't have a direct wrapper)."""
        import requests
        api = self.conn.base_url + "/api"
        headers = {"X-MSTR-AuthToken": self.conn.token,
                   "Accept": "application/json"}
        try:
            r = requests.get(f"{api}/datasources", headers=headers,
                             verify=False, timeout=60)
            if r.status_code == 200:
                for ds in r.json().get("datasources", []):
                    did = safe(ds.get("id"))
                    cn = ds.get("datasourceConnection", {}) or {}
                    self._ds_map[did] = {
                        "db_instance_name": safe(ds.get("name", "")),
                        "dsn_name": safe(cn.get("name", "")),
                        "db_type": safe(ds.get("dbType", "")),
                    }
        except Exception as e:
            log.warning(f"Datasource load failed: {e}")
        log.info(f"  Datasources: {len(self._ds_map)}")

    def _ds(self, ds_id):
        return self._ds_map.get(ds_id, {"db_instance_name":"","dsn_name":"","db_type":""})

    # -- Metric detail -------------------------------------------------------
    def _get_metric(self, mid):
        if mid in self._metric_cache:
            return self._metric_cache[mid]
        try:
            m = Metric(self.conn, id=mid,
                       show_expression_as=ExpressionFormat.TOKENS)
            formula = safe(getattr(m.expression, 'text', '')) if m.expression else ""
            self._metric_cache[mid] = {"name": safe(m.name), "formula": formula}
        except Exception as e:
            log.debug(f"  Metric {mid}: {e}")
            self._metric_cache[mid] = {"name": "", "formula": ""}
        return self._metric_cache[mid]

    # -- Attribute detail ----------------------------------------------------
    def _get_attribute(self, aid):
        if aid in self._attr_cache:
            return self._attr_cache[aid]
        try:
            a = Attribute(self.conn, id=aid,
                          show_expression_as=ExpressionFormat.TOKENS)
            forms = []
            for frm in (a.forms or []):
                fn = safe(frm.name)
                for expr in (frm.expressions or []):
                    expr_text = safe(getattr(expr.expression, 'text', '')) if expr.expression else ""
                    for tbl_ref in (expr.tables or []):
                        tid = safe(tbl_ref.object_id)
                        tn  = safe(tbl_ref.name)
                        self._load_table(tid, tn)
                        forms.append({"form_name": fn, "expression": expr_text,
                                      "table_id": tid, "table_name": tn})
            self._attr_cache[aid] = {"name": safe(a.name), "forms": forms}
        except Exception as e:
            log.debug(f"  Attr {aid}: {e}")
            self._attr_cache[aid] = {"name": "", "forms": []}
        return self._attr_cache[aid]

    # -- Fact detail ---------------------------------------------------------
    def _load_all_facts(self):
        log.info("  Loading all facts...")
        try:
            facts = list_facts(self.conn)
            for f in facts:
                fid = safe(f.id)
                mappings = []
                for expr in (f.expressions or []):
                    col = ""
                    if expr.expression and hasattr(expr.expression, 'tree'):
                        tree = expr.expression.tree
                        col = safe(getattr(tree, 'column_name', ''))
                    for tbl_ref in (expr.tables or []):
                        tid = safe(tbl_ref.object_id)
                        tn  = safe(tbl_ref.name)
                        self._load_table(tid, tn)
                        mappings.append({"column_name": col,
                                         "table_id": tid, "table_name": tn})
                self._fact_cache[fid] = {"name": safe(f.name), "mappings": mappings}
        except Exception as e:
            log.warning(f"  Fact load error: {e}")
        log.info(f"  Facts loaded: {len(self._fact_cache)}")

    # -- Table detail --------------------------------------------------------
    def _load_table(self, tid, tname=""):
        if tid in self._table_cache:
            return
        try:
            t = LogicalTable(self.conn, id=tid)
            phys = t.physical_table if hasattr(t, 'physical_table') else None
            ds_id = ""
            columns = []
            if phys:
                ds_id = safe(getattr(getattr(phys, 'information', None), 'data_source_id', ''))
                for col in (getattr(phys, 'columns', []) or []):
                    dt = getattr(col, 'data_type', None)
                    dtype = safe(getattr(dt, 'type', '')) if dt else ""
                    columns.append({"name": safe(col.name), "dtype": dtype})
            self._table_cache[tid] = {"name": safe(t.name) or tname,
                                       "ds_id": ds_id, "columns": columns}
        except Exception as e:
            log.debug(f"  Table {tid}: {e}")
            self._table_cache[tid] = {"name": tname, "ds_id": "", "columns": []}

    def _tbl(self, tid):
        return self._table_cache.get(tid, {"name":"","ds_id":"","columns":[]})

    def _col_dt(self, tid, col_name):
        for c in self._tbl(tid).get("columns", []):
            if safe(c.get("name","")).upper() == col_name.upper().strip():
                return safe(c.get("dtype",""))
        return ""

    # -- Fact index for formula matching -------------------------------------
    def _fact_index(self):
        return {v["name"].upper(): k for k, v in self._fact_cache.items() if v["name"]}

    # -- Harvest -------------------------------------------------------------
    def harvest(self, project_name):
        step("HARVEST", f"Project: {project_name}")
        self._load_datasources()
        self._load_all_facts()
        fi = self._fact_index()

        pid = self.conn.project_id
        pn  = project_name

        # Get all reports
        log.info("  Listing reports...")
        reports = list_reports(connection=self.conn)
        log.info(f"  Reports: {len(reports)}")

        for i, rpt in enumerate(reports):
            rid   = safe(rpt.id)
            rname = safe(rpt.name)
            rtype = safe(rpt.type) if hasattr(rpt, 'type') else ""
            rsub  = str(rpt.subtype) if hasattr(rpt, 'subtype') else ""
            rfolder = safe(rpt.ancestors[1]['name']) if hasattr(rpt, 'ancestors') and len(rpt.ancestors) > 1 else ""
            rowner  = safe(rpt.owner.name) if hasattr(rpt, 'owner') and rpt.owner else ""

            if (i+1) % 100 == 0:
                log.info(f"    report {i+1}/{len(reports)}...")

            dsi = {"dataset_folder": rfolder, "dataset_owner": rowner,
                   "report_subtype": rsub}

            # Get definition via REST (mstrio Report class doesn't expose availableObjects directly)
            import requests as req
            api = self.conn.base_url + "/api"
            headers = {"X-MSTR-AuthToken": self.conn.token,
                       "X-MSTR-ProjectID": pid,
                       "Accept": "application/json"}
            try:
                resp = req.get(f"{api}/reports/{rid}", headers=headers,
                               verify=False, timeout=60)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                result = data.get("result", data)
                defn = result.get("definition", result)
                avail = defn.get("availableObjects", {})
            except:
                continue

            metrics = avail.get("metrics", [])
            attrs   = avail.get("attributes", [])
            app = {"app_name": "", "app_folder": "", "app_owner": ""}

            # Determine dataset type
            sub_int = int(rsub) if rsub.isdigit() else 768
            if sub_int == 772:
                dt = "Freeform Report"
            elif sub_int == 774:
                dt = "Cube-Sourced Report"
            elif sub_int == 769:
                dt = "Graph Report"
            else:
                dt = "Grid Report"

            # Metrics
            for m in metrics:
                mid = safe(m.get("id"))
                if not mid: continue
                mi = self._get_metric(mid)
                formula = mi["formula"]
                mn = safe(m.get("name","")) or mi["name"]

                matched = []
                fu = formula.upper()
                for fn, fid in fi.items():
                    if fn in fu:
                        matched.extend(self._fact_cache.get(fid, {}).get("mappings", []))

                if matched:
                    seen = set()
                    for fm in matched:
                        tid = fm.get("table_id",""); tn = fm.get("table_name","")
                        cn = fm.get("column_name","")
                        if (tn,cn) in seen: continue
                        seen.add((tn,cn))
                        ti = self._tbl(tid)
                        ds = self._ds(ti.get("ds_id",""))
                        self._rows.append(self._r(
                            project_id=pid, project_name=pn, **app, **dsi,
                            dataset_name=rname, dataset_type=dt,
                            object_type="Metric", object_name=mn,
                            metric_formula=formula,
                            table_name=ti.get("name","") or tn,
                            column_name=cn,
                            column_data_type=self._col_dt(tid,cn), **ds))
                else:
                    self._rows.append(self._r(
                        project_id=pid, project_name=pn, **app, **dsi,
                        dataset_name=rname, dataset_type=dt,
                        object_type="Metric", object_name=mn,
                        metric_formula=formula))

            # Attributes
            for a in attrs:
                aid = safe(a.get("id"))
                if not aid: continue
                ai = self._get_attribute(aid)
                an = safe(a.get("name","")) or ai["name"]
                forms = ai.get("forms",[])

                if forms:
                    for f in forms:
                        tid = f.get("table_id","")
                        ti = self._tbl(tid)
                        ds = self._ds(ti.get("ds_id",""))
                        expr = f.get("expression","")
                        self._rows.append(self._r(
                            project_id=pid, project_name=pn, **app, **dsi,
                            dataset_name=rname, dataset_type=dt,
                            object_type="Attribute", object_name=an,
                            attribute_column=expr,
                            table_name=ti.get("name","") or f.get("table_name",""),
                            column_name=expr,
                            column_data_type=self._col_dt(tid,expr), **ds))
                else:
                    self._rows.append(self._r(
                        project_id=pid, project_name=pn, **app, **dsi,
                        dataset_name=rname, dataset_type=dt,
                        object_type="Attribute", object_name=an))

        log.info(f"  Total rows: {len(self._rows)}")

    def build(self):
        df = pd.DataFrame(self._rows) if self._rows else pd.DataFrame(columns=FINAL_COLS)
        for c in FINAL_COLS:
            if c not in df.columns: df[c] = ""
        df = df[FINAL_COLS].copy()
        df["lineage_row_id"] = [str(i+1).zfill(8) for i in range(len(df))]
        df["harvested_at"] = self.ts
        df = df.fillna("").astype(str).apply(lambda s: s.str.strip())
        df = df.drop_duplicates().reset_index(drop=True)
        step("BUILD", f"{len(df):,} rows x {len(FINAL_COLS)} cols")
        return df

# =============================================================================
# MAIN
# =============================================================================

def main():
    t0 = datetime.now()
    print("="*65)
    print("  MSTR LINEAGE HARVESTER  v7m  (mstrio-py + Modeling Service)")
    print(f"  Source: {SOURCE_BASE_URL}")
    print(f"  Target: {TARGET_BASE_URL}")
    print("="*65)

    # Source connection
    conn = Connection(
        base_url=SOURCE_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_name=SOURCE_PROJECT,
        ssl_verify=False)

    h = MstrioLineageHarvester(conn)
    h.harvest(SOURCE_PROJECT)
    df = h.build()

    print(f"\n  TOTAL: {len(df):,} rows")
    if not df.empty:
        cols = ["dataset_name","object_type","object_name","metric_formula","table_name"]
        print(df[[c for c in cols if c in df.columns]].head(10).to_string(index=False))

    conn.close()

    if df.empty:
        print("\n  No rows. Stopping."); return

    # Target connection
    tgt = Connection(
        base_url=TARGET_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_id=TARGET_PROJECT_ID,
        ssl_verify=False)

    step("PUBLISH", f"SuperCube '{CUBE_NAME}'")
    ds = SuperCube(connection=tgt, name=CUBE_NAME, folder_id=TARGET_FOLDER_ID or None)
    ds.add_table(name="LineageEdges", data_frame=df, update_policy="replace")
    ds.create()
    step("DONE", f"Cube ID: {ds.id}")
    tgt.close()

    print(f"\n{'='*65}")
    print(f"  COMPLETE | {len(df):,} edges | {(datetime.now()-t0).seconds}s")
    print("="*65)

if __name__ == "__main__":
    main()
