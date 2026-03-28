#!/usr/bin/env python3
"""
mstr_lineage_harvester_mstrio_search.py
Version: 5m  – Lineage using mstrio-py + metadataSearches (NO Modeling Service)

REQUIRES:
  pip install mstrio-py pandas

DOES NOT REQUIRE:
  Modeling Service (KB484255)

Uses mstrio-py classes:
  Connection             – auth + session management
  full_search            – find all objects + uses_object dependency traversal
  list_reports           – enumerate reports
  SuperCube              – push data to I-Server
  ObjectTypes            – type constants

The metadataSearches approach gives:
  Metric  -> fact names (not full formula like Sum(X){~+})
  Attribute -> table and column component names
  Fact    -> table component names
This is the Semantic Graph — same engine as Workstation's "Show Components".
"""

import logging
import time
import warnings
from datetime import datetime

import pandas as pd
from mstrio.connection import Connection
from mstrio import config as mstrio_config
from mstrio.object_management import full_search
from mstrio.project_objects.report import list_reports
from mstrio.project_objects.datasets.super_cube import SuperCube
from mstrio.types import ObjectTypes, ObjectSubTypes

mstrio_config.verbose = False
warnings.filterwarnings("ignore")

# =============================================================================
# CONFIGURATION
# =============================================================================

SOURCE_BASE_URL = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"
MSTR_USERNAME   = "YOUR_USERNAME"
MSTR_PASSWORD   = "YOUR_PASSWORD"
SOURCE_PROJECT  = "YOUR_PROJECT_NAME"

TARGET_BASE_URL   = "https://YOUR_DEV_SERVER/MicroStrategyLibrarySTD"
TARGET_PROJECT_ID = "YOUR_DEV_PROJECT_ID"
TARGET_FOLDER_ID  = ""

CUBE_NAME = "MSTR_Lineage_Harvest"

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

import re
def extract_tables_from_sql(sql):
    if not sql: return []
    s = re.sub(r"--[^\n]*","",sql.upper())
    s = re.sub(r"/\*.*?\*/","",s,flags=re.DOTALL)
    pat = (r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN"
           r"|FULL\s+JOIN|CROSS\s+JOIN|LEFT\s+OUTER\s+JOIN"
           r"|RIGHT\s+OUTER\s+JOIN|FULL\s+OUTER\s+JOIN)\s+"
           r"([A-Z0-9_#@.]+)")
    skip = {"WHERE","SELECT","ON","SET","WITH","AS","AND","OR","NOT","IN",
            "NULL","CASE","WHEN","THEN","ELSE","END","GROUP","ORDER",
            "HAVING","UNION","EXCEPT","INTERSECT","LATERAL","VALUES"}
    matches = re.findall(pat, s)
    tables = [m.split(".")[-1] for m in matches if m not in skip and not m.startswith("(")]
    return list(dict.fromkeys(tables))

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

class MstrioSearchHarvester:

    def __init__(self, conn: Connection):
        self.conn = conn
        self.ts   = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
        self._rows = []
        self._comp_cache = {}
        self._ds_map = {}

    def _r(self, **kw):
        row = {c: "" for c in FINAL_COLS}
        row["harvested_at"] = self.ts
        row.update({k: v for k, v in kw.items() if k in row})
        return row

    # -- Datasource mapping --------------------------------------------------
    def _load_datasources(self):
        import requests
        api = self.conn.base_url + "/api"
        headers = {"X-MSTR-AuthToken": self.conn.token, "Accept": "application/json"}
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
            log.warning(f"Datasource load: {e}")
        log.info(f"  Datasources: {len(self._ds_map)}")

    # -- Component lookup using full_search ----------------------------------
    def _get_components(self, oid, otype):
        """
        Get direct components of an object using mstrio full_search
        with uses_object parameter (wraps POST /metadataSearches/results).
        """
        key = (oid, otype)
        if key in self._comp_cache:
            return self._comp_cache[key]
        try:
            # full_search with uses_object returns objects that the given object uses
            results = full_search(
                connection=self.conn,
                project=self.conn.project_id,
                uses_object=f"{oid};{otype}",
                uses_recursive=False)
            comps = []
            for obj in results:
                comps.append({
                    "id":      safe(obj.id),
                    "name":    safe(obj.name),
                    "type":    int(obj.type) if hasattr(obj, 'type') else 0,
                    "subtype": int(obj.subtype) if hasattr(obj, 'subtype') else 0,
                })
            self._comp_cache[key] = comps
        except Exception as e:
            log.debug(f"  Components for {oid}: {e}")
            self._comp_cache[key] = []
        return self._comp_cache[key]

    def _traverse_metric(self, mid):
        """Metric -> facts -> tables."""
        comps = self._get_components(mid, 4)  # TYPE_METRIC=4
        results = []
        for c in comps:
            if c["type"] == 13:  # TYPE_FACT
                fact_comps = self._get_components(c["id"], 13)
                for fc in fact_comps:
                    if fc["type"] in (15, 53):  # table types
                        results.append({"hint": c["name"],
                                        "table_name": fc["name"]})
                if not fact_comps:
                    results.append({"hint": c["name"], "table_name": ""})
            elif c["type"] == 12:  # TYPE_ATTRIBUTE
                attr_tables = self._traverse_attribute(c["id"])
                for at in attr_tables:
                    results.append({"hint": c["name"],
                                    "table_name": at.get("table_name","")})
        return results

    def _traverse_attribute(self, aid):
        """Attribute -> tables + columns."""
        comps = self._get_components(aid, 12)  # TYPE_ATTRIBUTE=12
        tables = [c for c in comps if c["type"] in (15, 53)]
        columns = [c for c in comps if c["type"] == 26]  # TYPE_COLUMN=26
        results = []
        if tables and columns:
            for col in columns:
                for tbl in tables:
                    results.append({"table_name": tbl["name"],
                                    "column_name": col["name"]})
        elif tables:
            for tbl in tables:
                results.append({"table_name": tbl["name"], "column_name": ""})
        elif columns:
            for col in columns:
                results.append({"table_name": "", "column_name": col["name"]})
        else:
            results.append({"table_name": "", "column_name": ""})
        return results

    # -- Harvest -------------------------------------------------------------
    def harvest(self, project_name):
        step("HARVEST", f"Project: {project_name}")
        self._load_datasources()
        pid = self.conn.project_id
        pn  = project_name

        # Get reports via list_reports
        log.info("  Listing reports...")
        reports = list_reports(connection=self.conn)
        log.info(f"  Reports: {len(reports)}")

        for i, rpt in enumerate(reports):
            rid   = safe(rpt.id)
            rname = safe(rpt.name)
            rsub  = str(rpt.subtype) if hasattr(rpt, 'subtype') else "768"
            rfolder = safe(rpt.ancestors[1]['name']) if hasattr(rpt,'ancestors') and len(rpt.ancestors)>1 else ""
            rowner = safe(rpt.owner.name) if hasattr(rpt,'owner') and rpt.owner else ""

            if (i+1) % 100 == 0:
                log.info(f"    report {i+1}/{len(reports)}...")

            dsi = {"dataset_folder": rfolder, "dataset_owner": rowner,
                   "report_subtype": rsub}
            app = {"app_name": "", "app_folder": "", "app_owner": ""}

            sub_int = int(rsub) if rsub.isdigit() else 768

            # Freeform: get SQL
            if sub_int == 772:
                import requests as req
                api = self.conn.base_url + "/api"
                headers = {"X-MSTR-AuthToken": self.conn.token,
                           "X-MSTR-ProjectID": pid, "Accept": "application/json"}
                try:
                    sr = req.get(f"{api}/reports/{rid}/sqlView",
                                 headers=headers, verify=False, timeout=60)
                    sql = ""
                    if sr.status_code == 200:
                        sd = sr.json()
                        ps = sd.get("sqlStatements", [])
                        sql = " | ".join(safe(p.get("sql","")) for p in ps if isinstance(p,dict))
                    for tbl in (extract_tables_from_sql(sql) or ["(unparsed)"]):
                        self._rows.append(self._r(
                            project_id=pid, project_name=pn, **app, **dsi,
                            dataset_name=rname, dataset_type="Freeform Report",
                            object_type="FreeformSQL", sql_preview=sql,
                            table_name=tbl))
                except:
                    pass
                continue

            # Schema report: get definition
            import requests as req
            api = self.conn.base_url + "/api"
            headers = {"X-MSTR-AuthToken": self.conn.token,
                       "X-MSTR-ProjectID": pid, "Accept": "application/json"}
            try:
                resp = req.get(f"{api}/reports/{rid}", headers=headers,
                               verify=False, timeout=60)
                if resp.status_code != 200: continue
                data = resp.json()
                result = data.get("result", data)
                defn = result.get("definition", result)
                avail = defn.get("availableObjects", {})
            except:
                continue

            dt = "Grid Report"
            if sub_int == 774: dt = "Cube-Sourced Report"
            elif sub_int == 769: dt = "Graph Report"

            # Metrics via metadataSearches
            for m in avail.get("metrics", []):
                mid = safe(m.get("id"))
                if not mid: continue
                mn = safe(m.get("name",""))
                phys = self._traverse_metric(mid)
                if phys and any(p.get("table_name") for p in phys):
                    for p in phys:
                        self._rows.append(self._r(
                            project_id=pid, project_name=pn, **app, **dsi,
                            dataset_name=rname, dataset_type=dt,
                            object_type="Metric", object_name=mn,
                            metric_formula=p.get("hint",""),
                            table_name=p.get("table_name","")))
                else:
                    hint = phys[0].get("hint","") if phys else ""
                    self._rows.append(self._r(
                        project_id=pid, project_name=pn, **app, **dsi,
                        dataset_name=rname, dataset_type=dt,
                        object_type="Metric", object_name=mn,
                        metric_formula=hint))

            # Attributes via metadataSearches
            for a in avail.get("attributes", []):
                aid = safe(a.get("id"))
                if not aid: continue
                an = safe(a.get("name",""))
                phys = self._traverse_attribute(aid)
                if phys and any(p.get("table_name") or p.get("column_name") for p in phys):
                    for p in phys:
                        self._rows.append(self._r(
                            project_id=pid, project_name=pn, **app, **dsi,
                            dataset_name=rname, dataset_type=dt,
                            object_type="Attribute", object_name=an,
                            attribute_column=p.get("column_name",""),
                            table_name=p.get("table_name",""),
                            column_name=p.get("column_name","")))
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
    print("  MSTR LINEAGE HARVESTER  v5m  (mstrio-py + metadataSearches)")
    print("  NO Modeling Service required")
    print(f"  Source: {SOURCE_BASE_URL}")
    print(f"  Target: {TARGET_BASE_URL}")
    print("="*65)

    conn = Connection(
        base_url=SOURCE_BASE_URL,
        username=MSTR_USERNAME,
        password=MSTR_PASSWORD,
        project_name=SOURCE_PROJECT,
        ssl_verify=False)

    h = MstrioSearchHarvester(conn)
    h.harvest(SOURCE_PROJECT)
    df = h.build()

    print(f"\n  TOTAL: {len(df):,} rows")
    if not df.empty:
        cols = ["dataset_name","object_type","object_name","metric_formula","table_name"]
        print(df[[c for c in cols if c in df.columns]].head(10).to_string(index=False))

    conn.close()

    if df.empty:
        print("\n  No rows. Stopping."); return

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
