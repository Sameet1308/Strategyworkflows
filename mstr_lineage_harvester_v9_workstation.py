#!/usr/bin/env python3
"""
mstr_lineage_harvester.py
Version: 9.0.0  – mstrio-py definitive version

REQUIRES:
  pip install mstrio-py pandas sql-metadata

Based on Robert Prochowicz's proven community patterns:
  - 5-level lineage script (document > dataset > report object > schema object > table)
  - Cube SQL extraction script (OlapCube.export_sql_view + sql_metadata.Parser)

# =============================================================================
# DATA FLOW AND JOINS
# =============================================================================
#
# The script collects data at 6 levels and joins them via mapping tables:
#
#   L0: Projects
#       |
#   L1: Documents / Dossiers  (apps)
#       |  joined via L12_MAPPING (doc_id -> dataset_id)
#       |  source: get_dossier_definition().datasets[] and document.list_dependencies()
#       v
#   L2: Datasets  (reports + OLAP cubes + data import cubes + dossier internal datasets)
#       |  joined via L23_MAPPING (dataset_id -> report_object_id)
#       |  source: report.list_dependencies() resolved down to metric/attr/fact types
#       v
#   L3: Report Objects  (metrics, attributes, facts on report/cube templates)
#       |  joined via L34_MAPPING (report_object_id -> schema_object_id)
#       |  source: metric.list_dependencies() resolved down to attr/fact types
#       v
#   L4: Schema Objects  (attribute forms with expressions, fact expressions)
#       |  source: list_logical_tables() -> table.attributes[].forms[].expressions
#       |          list_logical_tables() -> table.facts[].expressions
#       v
#   L5: Tables + Datasources
#       source: table.name + table.primary_data_source.name
#
# FLAT TABLE JOIN:
#   For each dataset (L2):
#     -> look up app via L12 (which document/dossier contains this dataset?)
#     -> look up report objects via L23 (which metrics/attrs are on this dataset?)
#     -> for each report object:
#         -> look up schema objects via L34 (which facts/attrs does this metric use?)
#         -> for each schema object:
#             -> get expression, table, datasource from L4
#     -> emit one row per (dataset, report_object, schema_object) combination
#
# FREEFORM / CUSTOM SQL:
#   For freeform reports and custom SQL cubes:
#     -> extract SQL via OlapCube.export_sql_view() or report sqlView
#     -> parse SQL with sql_metadata.Parser to get table names
#     -> emit one row per (dataset, table_from_sql) combination
#
# =============================================================================
"""

import csv
import itertools
import logging
import re
import warnings
from datetime import datetime

import pandas as pd
from mstrio.connection import Connection
from mstrio.server import Environment
from mstrio.object_management import full_search
from mstrio.types import ObjectSubTypes, ObjectTypes
from mstrio.project_objects.datasets.super_cube import SuperCube
from mstrio.project_objects import list_olap_cubes, OlapCube, list_all_cubes
from mstrio.project_objects.report import list_reports
from mstrio.project_objects.dossier import list_dossiers
from mstrio.project_objects.document import list_documents
from mstrio.modeling.schema import list_attributes, list_facts
from mstrio.modeling.schema.table import list_logical_tables
from mstrio.modeling import list_metrics

from mstrio import config as mstrio_config
mstrio_config.verbose = False
warnings.filterwarnings("ignore")

# Try to import sql_metadata for proper SQL parsing
try:
    from sql_metadata import Parser as SqlParser
    HAS_SQL_PARSER = True
except ImportError:
    HAS_SQL_PARSER = False
    print("  [WARN] sql-metadata not installed. Using regex fallback.")
    print("         pip install sql-metadata for better SQL parsing.")

# =============================================================================
# CONFIGURATION
# =============================================================================

# ---- MODE ----
RUN_MODE = "workstation"   # "workstation" or "standalone"

# ---- PROD SERVER (harvest FROM here) ----
# Even in Workstation mode, we need explicit PROD credentials
# because Workstation is connected to DEV, not PROD.
PROD_BASE_URL    = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"
PROD_USERNAME    = "YOUR_USERNAME"
PROD_PASSWORD    = "YOUR_PASSWORD"
PROD_PROJECT     = "YOUR_PROD_PROJECT_NAME"

# ---- DEV SERVER (publish cube HERE) ----
# In Workstation mode: uses Workstation's own DEV connection
# In standalone mode: uses these credentials
DEV_BASE_URL     = "https://YOUR_DEV_SERVER/MicroStrategyLibrarySTD"
DEV_USERNAME     = "YOUR_USERNAME"   # same creds if shared
DEV_PASSWORD     = "YOUR_PASSWORD"
DEV_PROJECT      = "YOUR_DEV_PROJECT_NAME"
TARGET_FOLDER_ID = ""   # "" = user's My Reports folder

CUBE_NAME  = "MSTR_Lineage_Harvest"
CSV_EXPORT = False   # Set True to also save a local CSV

# Cube subtypes to harvest SQL from
CUBE_SUBTYPES_SQL = [776, 779]  # 776=OLAP cube, 779=data import cube

# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S")
log = logging.getLogger("lineage")

def step(ph, msg):
    print(f"\n  [{ph}] {msg}")
    log.info(f"[{ph}] {msg}")

def safe(v):
    return str(v).strip() if v is not None else ""

# =============================================================================
# 24-COLUMN OUTPUT
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
# SQL PARSING
# =============================================================================

def parse_sql_tables(sql: str) -> list:
    """
    Extract table names from SQL using sql_metadata.Parser (preferred)
    or regex fallback.
    """
    if not sql:
        return []

    if HAS_SQL_PARSER:
        try:
            # sql_metadata handles complex SQL, CTEs, subqueries
            parser = SqlParser(sql)
            tables = [t for t in parser.tables
                      if not t.startswith(("ZZ", "*"))]
            return list(dict.fromkeys(tables))  # dedupe, preserve order
        except Exception:
            pass  # fall through to regex

    # Regex fallback
    s = re.sub(r"--[^\n]*", "", sql.upper())
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    pat = (r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN"
           r"|FULL\s+JOIN|CROSS\s+JOIN|LEFT\s+OUTER\s+JOIN"
           r"|RIGHT\s+OUTER\s+JOIN|FULL\s+OUTER\s+JOIN)\s+"
           r"([A-Z0-9_#@.]+)")
    skip = {"WHERE", "SELECT", "ON", "SET", "WITH", "AS", "AND", "OR",
            "NOT", "IN", "NULL", "CASE", "WHEN", "THEN", "ELSE", "END",
            "GROUP", "ORDER", "HAVING", "UNION", "EXCEPT", "INTERSECT",
            "LATERAL", "VALUES"}
    matches = re.findall(pat, s)
    tables = [m.split(".")[-1] for m in matches
              if m not in skip and not m.startswith("(")]
    return list(dict.fromkeys(tables))

# =============================================================================
# COMMUNITY PATTERN HELPERS
# =============================================================================

def unique_list(alist):
    alist.sort()
    return list(a for a, _ in itertools.groupby(alist))


def get_object_deps(r, project_id):
    """r.list_dependencies() -> [[pid, parent_type, parent_id, parent_name,
                                   dep_type, dep_id, dep_name], ...]"""
    try:
        deps = r.list_dependencies()
        return [[project_id, r.type.value, r.id, r.name,
                 d["type"], d["id"], d["name"]] for d in deps]
    except Exception as e:
        log.debug(f"  deps failed {getattr(r,'name','?')}: {e}")
        return []


def search_deps(conn, project_id, obj_id, obj_type):
    """full_search(used_by_object_id=...) -> [[type, id, name], ...]"""
    try:
        objects = full_search(conn, project=project_id,
                              used_by_object_id=obj_id,
                              used_by_object_type=obj_type)
        return [[d["type"], d["id"], d["name"]] for d in objects]
    except:
        return []


def resolve_down(conn, pid, object_dependants, deps_to_resolve, deps_finished):
    """Recursively resolve non-schema dependencies until we reach
    metrics (4), agg_metrics (7), attributes (12), facts (13)."""
    while deps_to_resolve:
        for dtr in deps_to_resolve[:]:
            dep_type, dep_id, dep_name = dtr[4], dtr[5], dtr[6]
            rep_type, rep_id, rep_name = dtr[1], dtr[2], dtr[3]

            # Skip already-seen or irrelevant types (functions, columns, etc.)
            if (dep_id in [o[2] for o in object_dependants]) or \
               (dep_type in [11, 61, 53, 22, 26]):
                pass
            else:
                deps = search_deps(conn, pid, dep_id, dep_type)
                if deps:
                    for nd in deps:
                        object_dependants.append(
                            [pid, dep_type, dep_id, dep_name,
                             nd[0], nd[1], nd[2]])
                        target = [pid, rep_type, rep_id, rep_name,
                                  nd[0], nd[1], nd[2]]
                        if nd[0] in [4, 7, 12, 13]:
                            deps_finished.append(target)
                        else:
                            deps_to_resolve.append(target)
                else:
                    object_dependants.append(
                        [pid, dep_type, dep_id, dep_name, 0, "NA", "NA"])
            deps_to_resolve.remove(dtr)
    return deps_finished


def get_dossier_definition(connection, dossier_id):
    """GET /api/v2/dossiers/{id}/definition"""
    url = connection.base_url + f"/api/v2/dossiers/{dossier_id}/definition"
    try:
        res = connection.get(url=url)
        return res.json() if hasattr(res, 'json') else (res if isinstance(res, dict) else {})
    except:
        return {}


# =============================================================================
# HARVESTER
# =============================================================================

class LineageHarvester:

    def __init__(self, conn, pid, pname):
        self.conn  = conn
        self.pid   = pid
        self.pname = pname
        self.ts    = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

        # L1: documents/dossiers
        self.l1_docs = []      # [{id, name, type, folder, owner}]

        # L2: datasets (reports + cubes + dossier internal)
        self.l2_datasets = []  # [{id, name, type, subtype, folder, owner}]
        self.l2_ids = set()

        # Mappings
        self.l12 = []   # [[pid, doc_id, dataset_id]]
        self.l23 = []   # [[pid, dataset_id, repobj_id]]
        self.l34 = []   # [[pid, repobj_id, schemaobj_id]]

        # L3: report objects
        self.report_objs = {}      # {oid: {type, name}}
        self.metric_formulas = {}  # {metric_id: formula_text}

        # L4: schema objects from list_logical_tables
        # Keyed by "oid:table_id" to handle same attr/fact on multiple tables
        self.schema_objs = {}  # {key: {obj_type, obj_id, obj_name, table_name,
                               #        datasource, expression, form_dtype, ...}}

        # Freeform SQL cache
        self.freeform_sql = {}  # {dataset_id: sql_text}

    # -----------------------------------------------------------------
    # L1: DOCUMENTS & DOSSIERS
    # -----------------------------------------------------------------
    def harvest_l1(self):
        step("L1", "Documents & Dossiers")
        self.conn.select_project(project_id=self.pid)

        for d in list_dossiers(connection=self.conn, project_id=self.pid):
            fld = safe(d.ancestors[1]['name']) if hasattr(d, 'ancestors') and len(getattr(d, 'ancestors', [])) > 1 else ""
            own = safe(d.owner.name) if hasattr(d, 'owner') and d.owner else ""
            self.l1_docs.append({"id": safe(d.id), "name": safe(d.name),
                                 "type": "DOSSIER", "folder": fld, "owner": own})

        for d in list_documents(connection=self.conn, project_id=self.pid):
            fld = safe(d.ancestors[1]['name']) if hasattr(d, 'ancestors') and len(getattr(d, 'ancestors', [])) > 1 else ""
            own = safe(d.owner.name) if hasattr(d, 'owner') and d.owner else ""
            self.l1_docs.append({"id": safe(d.id), "name": safe(d.name),
                                 "type": "DOCUMENT", "folder": fld, "owner": own})

        log.info(f"  L1: {len(self.l1_docs)} apps")

    # -----------------------------------------------------------------
    # L2: DATASETS (reports + cubes + dossier internal) + L12 MAPPING
    # -----------------------------------------------------------------
    def harvest_l2(self):
        step("L2", "Datasets (reports + cubes) + L12 mapping")
        self.conn.select_project(project_id=self.pid)

        # --- Reports ---
        reports = list_reports(connection=self.conn, project_id=self.pid)
        log.info(f"  Reports: {len(reports)}")
        for r in reports:
            rid = safe(r.id)
            fld = safe(r.ancestors[1]['name']) if hasattr(r, 'ancestors') and len(getattr(r, 'ancestors', [])) > 1 else ""
            own = safe(r.owner.name) if hasattr(r, 'owner') and r.owner else ""
            sub = int(r.subtype) if hasattr(r, 'subtype') else 0
            self.l2_datasets.append({
                "id": rid, "name": safe(r.name),
                "type": safe(r.type.name).upper() if hasattr(r, 'type') else "REPORT",
                "subtype": sub, "folder": fld, "owner": own,
            })
            self.l2_ids.add(rid)

        # --- Cubes (OLAP + data import) ---
        cubes_dicts = list_all_cubes(connection=self.conn, to_dictionary=True)
        cubes_dicts = [c for c in cubes_dicts if c.get('subtype') in CUBE_SUBTYPES_SQL]
        log.info(f"  Cubes (776/779): {len(cubes_dicts)}")
        for c in cubes_dicts:
            cid = safe(c.get("id", ""))
            if cid in self.l2_ids:
                continue  # already added via reports
            self.l2_datasets.append({
                "id": cid, "name": safe(c.get("name", "")),
                "type": "CUBE", "subtype": int(c.get("subtype", 776)),
                "folder": "", "owner": "",
            })
            self.l2_ids.add(cid)

        # --- Cube SQL extraction ---
        step("L2-SQL", "Extracting SQL from cubes")
        for i, c in enumerate(cubes_dicts):
            cid = safe(c.get("id", ""))
            cname = safe(c.get("name", ""))
            if (i + 1) % 20 == 0:
                log.info(f"    cube sql {i+1}/{len(cubes_dicts)}...")
            try:
                cube_obj = OlapCube(self.conn, cid)
                sql_view = cube_obj.export_sql_view()
                if sql_view:
                    # Extract individual SELECT statements
                    pattern = r"(select\s+.*?)\n\n"
                    matches = re.findall(pattern, sql_view,
                                         flags=re.DOTALL | re.IGNORECASE)
                    if matches:
                        # Combine all statements
                        self.freeform_sql[cid] = " | ".join(
                            " ".join(m.split()) for m in matches)
                    elif sql_view.strip():
                        self.freeform_sql[cid] = " ".join(sql_view.split())
            except Exception as e:
                log.debug(f"  Cube SQL failed {cname}: {e}")

        log.info(f"  Cube SQL captured: {len(self.freeform_sql)} cubes")

        # --- Dossier internal datasets + L12 mapping ---
        step("L2-DOSSIER", "Dossier datasets + L12")
        dossiers = list_dossiers(connection=self.conn, project_id=self.pid)
        for i, d in enumerate(dossiers):
            if (i + 1) % 50 == 0:
                log.info(f"    dossier {i+1}/{len(dossiers)}...")
            defn = get_dossier_definition(self.conn, d.id)
            if not defn:
                continue
            for ds in defn.get('datasets', []):
                dsid = safe(ds.get('id', ''))
                if not dsid:
                    continue
                self.l12.append([self.pid, safe(d.id), dsid])
                if dsid not in self.l2_ids:
                    self.l2_datasets.append({
                        "id": dsid, "name": safe(ds.get('name', '')),
                        "type": "DATASET", "subtype": 0,
                        "folder": "dynamic", "owner": "",
                    })
                    self.l2_ids.add(dsid)
                    for ao in ds.get('availableObjects', []):
                        self.l23.append([self.pid, dsid, safe(ao.get('id', ''))])

        # Document -> report L12
        for d in list_documents(connection=self.conn, project_id=self.pid):
            for c in get_object_deps(d, self.pid):
                if c[4] == 3:  # report type
                    self.l12.append([self.pid, safe(d.id), c[5]])

        self.l12 = unique_list(self.l12)
        log.info(f"  L2: {len(self.l2_datasets)} datasets, L12: {len(self.l12)} mappings")

    # -----------------------------------------------------------------
    # L3: REPORT OBJECTS + L23 MAPPING
    #
    # JOIN: L23 maps dataset_id -> report_object_id
    # Source: report.list_dependencies() resolved to metric/attr/fact
    # -----------------------------------------------------------------
    def harvest_l3(self):
        step("L3", "Report objects + L23 mapping")
        self.conn.select_project(project_id=self.pid)

        # Enumerate all report objects
        for a in list_attributes(connection=self.conn, project_id=self.pid):
            self.report_objs[safe(a.id)] = {"type": "ATTRIBUTE", "name": safe(a.name)}

        all_metrics = list_metrics(connection=self.conn, project_id=self.pid)
        for m in all_metrics:
            mid = safe(m.id)
            self.report_objs[mid] = {"type": "METRIC", "name": safe(m.name)}
            try:
                self.metric_formulas[mid] = safe(m.expression.text) if m.expression else ""
            except:
                self.metric_formulas[mid] = ""

        for f in list_facts(connection=self.conn, project_id=self.pid):
            self.report_objs[safe(f.id)] = {"type": "FACT", "name": safe(f.name)}

        log.info(f"  Report objects: {len(self.report_objs)}")
        log.info(f"  Metric formulas: {len(self.metric_formulas)}")

        # L23: report -> report objects via list_dependencies + resolve
        reports = list_reports(connection=self.conn, project_id=self.pid)
        object_dependants = []
        for i, r in enumerate(reports):
            if (i + 1) % 100 == 0:
                log.info(f"    report deps {i+1}/{len(reports)}...")
            object_dependants.extend(get_object_deps(r, self.pid))

        deps_finished = []
        deps_to_resolve = []
        for d in object_dependants:
            if d[4] in [4, 7, 12, 13, 1]:  # metric, agg_metric, attr, fact, filter
                deps_finished.append(d)
            else:
                deps_to_resolve.append(d)

        log.info(f"  Direct deps: {len(deps_finished)}, to resolve: {len(deps_to_resolve)}")
        deps_finished = resolve_down(self.conn, self.pid, object_dependants,
                                     deps_to_resolve, deps_finished)

        # Build L23 from results
        # deps_finished: [pid, report_type, report_id, report_name, dep_type, dep_id, dep_name]
        # L23 needs: [pid, dataset_id(=report_id), repobj_id(=dep_id)]
        map23 = [[m[0], m[2], m[5]] for m in deps_finished]
        map23.extend(self.l23)
        self.l23 = unique_list(map23)
        log.info(f"  L23: {len(self.l23)} mappings")

    # -----------------------------------------------------------------
    # L4: SCHEMA OBJECTS FROM list_logical_tables()
    #
    # This is where attribute expressions, fact expressions,
    # table names, and datasource names come from.
    # No /model/attributes/{id} or /model/metrics/{id} calls needed.
    # -----------------------------------------------------------------
    def harvest_l4(self):
        step("L4", "Schema objects from logical tables")
        self.conn.select_project(project_id=self.pid)

        tables = list_logical_tables(connection=self.conn)
        log.info(f"  Logical tables: {len(tables)}")

        for i, tbl in enumerate(tables):
            if (i + 1) % 50 == 0:
                log.info(f"    table {i+1}/{len(tables)}...")

            tn = safe(tbl.name)
            tid = safe(tbl.id)
            tds = ""
            try:
                tds = safe(tbl.primary_data_source.name) if tbl.primary_data_source else ""
            except:
                pass

            # --- Attributes on this table ---
            if tbl.attributes and (getattr(tbl, 'subtype', 3840) == 3840):
                for a in (tbl.attributes or []):
                    if not a.id:
                        continue
                    aid = safe(a.id)

                    # Check sub_type
                    if hasattr(a, 'sub_type') and safe(a.sub_type) != "attribute":
                        continue

                    # Download full attribute details
                    try:
                        a.list_properties()
                    except:
                        pass

                    # Lookup table
                    lkp = ""
                    try:
                        lkp = safe(a.attribute_lookup_table.name) if a.attribute_lookup_table else ""
                    except:
                        pass

                    for frm in (a.forms or []):
                        if getattr(frm, 'is_form_group', False):
                            continue
                        fd = ""
                        fp = ""
                        if frm.data_type:
                            fd = safe(frm.data_type.type)
                            fp = safe(frm.data_type.precision)

                        for expr in (frm.expressions or []):
                            et = ""
                            try:
                                et = safe(expr.expression.text)
                            except:
                                pass

                            key = f"{aid}:{tid}"
                            self.schema_objs[key] = {
                                "obj_type": "ATTRIBUTE", "obj_id": aid,
                                "obj_name": safe(a.name),
                                "table_name": tn, "table_id": tid,
                                "datasource": tds,
                                "expression": et,
                                "form_name": safe(frm.name),
                                "form_dtype": fd,
                                "form_precis": fp,
                                "lookup_table": lkp,
                            }

            # --- Facts on this table ---
            if tbl.facts:
                for f in (tbl.facts or []):
                    if not f.id:
                        continue
                    fid = safe(f.id)
                    fd = ""
                    fp = ""
                    try:
                        if f.data_type:
                            fd = safe(f.data_type.type)
                            fp = safe(f.data_type.precision)
                    except:
                        pass

                    for expr in (f.expressions or []):
                        et = ""
                        try:
                            et = safe(expr.expression.text)
                        except:
                            pass

                        key = f"{fid}:{tid}"
                        self.schema_objs[key] = {
                            "obj_type": "FACT", "obj_id": fid,
                            "obj_name": safe(f.name),
                            "table_name": tn, "table_id": tid,
                            "datasource": tds,
                            "expression": et,
                            "form_name": "",
                            "form_dtype": fd,
                            "form_precis": fp,
                            "lookup_table": "",
                        }

        log.info(f"  Schema objects: {len(self.schema_objs)}")

    # -----------------------------------------------------------------
    # L34 MAPPING: metric/filter -> schema objects
    #
    # JOIN: L34 maps report_object_id -> schema_object_id
    # Source: metric.list_dependencies() resolved to attr/fact
    # -----------------------------------------------------------------
    def harvest_l34(self):
        step("L34", "Metric -> schema object mapping")
        self.conn.select_project(project_id=self.pid)

        all_metrics = list_metrics(connection=self.conn, project_id=self.pid)
        object_dependants = []
        for i, m in enumerate(all_metrics):
            if (i + 1) % 100 == 0:
                log.info(f"    metric deps {i+1}/{len(all_metrics)}...")
            object_dependants.extend(get_object_deps(m, self.pid))

        deps_finished = []
        deps_to_resolve = []
        for d in object_dependants:
            if d[4] in [4, 7, 12, 13]:
                deps_finished.append(d)
            else:
                deps_to_resolve.append(d)

        log.info(f"  Direct: {len(deps_finished)}, resolve: {len(deps_to_resolve)}")
        deps_finished = resolve_down(self.conn, self.pid, object_dependants,
                                     deps_to_resolve, deps_finished)

        # Subtotals to exclude
        subtotals = {
            "00B7BFFF967F42C4B71A4B53D90FB095", "078C50834B484EE29948FA9DD5300ADF",
            "1769DBFCCF2D4392938E40418C6E065E", "36226A4048A546139BE0AF5F24737BA8",
            "54E7BFD129514717A92BC44CF1FE5A32", "7FBA414995194BBAB2CF1BB599209824",
            "83A663067F7E43B2ABF67FD38ECDC7FE", "96C487AF4D12472A910C1ACACFB56EFB",
            "B1F4AA7DE683441BA559AA6453C5113E", "B328C60462634223B2387D4ADABEEB53",
            "E1853D5A36C74F59A9F8DEFB3F9527A1", "F225147A4CA0BB97368A5689D9675E73",
        }

        # L34: [pid, repobj_id(=metric_id), schemaobj_id(=fact/attr_id)]
        self.l34 = [[m[0], m[2], m[5]] for m in deps_finished
                     if m[5] not in subtotals]

        # Identity mappings: attr->attr, fact->fact
        seen_ids = set()
        for key, so in self.schema_objs.items():
            oid = so["obj_id"]
            if oid not in seen_ids:
                self.l34.append([self.pid, oid, oid])
                seen_ids.add(oid)

        self.l34 = unique_list(self.l34)
        log.info(f"  L34: {len(self.l34)} mappings")

    # -----------------------------------------------------------------
    # FLAT TABLE JOIN
    #
    # For each dataset (L2):
    #   app_info       <- L12 join L1 (doc_id -> doc details)
    #   report_objects <- L23 (dataset_id -> repobj_ids)
    #   schema_objects <- L34 (repobj_id -> schemaobj_ids)
    #   physical_info  <- L4 (schemaobj_id -> expression, table, datasource)
    #
    # One row = one (dataset, report_object, schema_object_on_table) path
    # -----------------------------------------------------------------
    def build_flat_table(self) -> pd.DataFrame:
        step("JOIN", "Building 24-column flat table")

        # --- Build lookup dicts ---

        # doc_id -> doc info
        doc_lk = {d["id"]: d for d in self.l1_docs}

        # dataset_id -> doc info (via L12)
        ds_to_doc = {}
        for m in self.l12:
            doc = doc_lk.get(m[1], {})
            if doc.get("name"):
                ds_to_doc[m[2]] = doc

        # dataset_id -> [repobj_ids] (via L23)
        ds_to_ro = {}
        for m in self.l23:
            ds_to_ro.setdefault(m[1], []).append(m[2])

        # repobj_id -> [schemaobj_ids] (via L34)
        ro_to_so = {}
        for m in self.l34:
            ro_to_so.setdefault(m[1], []).append(m[2])

        # schemaobj_id -> [schema_obj entries] (from L4, may have multiple tables)
        so_by_id = {}
        for key, so in self.schema_objs.items():
            so_by_id.setdefault(so["obj_id"], []).append(so)

        # --- Generate rows ---
        rows = []

        for ds in self.l2_datasets:
            dsid = ds["id"]
            dsn  = ds["name"]
            dss  = ds.get("subtype", 0)

            # App info via L12 -> L1
            doc = ds_to_doc.get(dsid, {})
            base = {
                "project_id": self.pid, "project_name": self.pname,
                "app_name": doc.get("name", ""),
                "app_folder": doc.get("folder", ""),
                "app_owner": doc.get("owner", ""),
                "dataset_name": dsn,
                "dataset_folder": ds.get("folder", ""),
                "dataset_owner": ds.get("owner", ""),
                "report_subtype": str(dss),
            }

            # Dataset type classification
            if dss == 772:
                base["dataset_type"] = "Freeform Report"
            elif dss == 774:
                base["dataset_type"] = "Cube-Sourced Report"
            elif dss == 769:
                base["dataset_type"] = "Graph Report"
            elif ds.get("type") == "DATASET":
                base["dataset_type"] = "Dossier Dataset"
            elif ds.get("type") == "CUBE":
                base["dataset_type"] = "Schema Cube"
                base["cube_source_type"] = "normal"
            else:
                base["dataset_type"] = "Grid Report"

            # --- FREEFORM: cubes with SQL ---
            if dsid in self.freeform_sql:
                sql = self.freeform_sql[dsid]
                base["dataset_type"] = "Freeform Cube"
                base["cube_source_type"] = "custom_sql_free_form"
                tables = parse_sql_tables(sql)
                if not tables:
                    tables = ["(SQL tables not parsed)"]
                for tbl in tables:
                    rows.append({**base, "object_type": "FreeformSQL",
                                 "sql_preview": sql, "table_name": tbl})
                continue

            # --- FREEFORM: reports ---
            if dss == 772:
                # TODO: extract SQL from freeform reports via report sqlView
                rows.append({**base, "object_type": "FreeformSQL"})
                continue

            # --- SCHEMA: join through L23 -> L34 -> L4 ---
            ro_ids = ds_to_ro.get(dsid, [])

            if not ro_ids:
                rows.append({**base})
                continue

            for ro_id in ro_ids:
                ri = self.report_objs.get(ro_id, {"type": "", "name": ro_id})
                rt = ri["type"]
                rn = ri["name"]

                so_ids = ro_to_so.get(ro_id, [])

                if not so_ids:
                    row = {**base, "object_type": rt or "Other", "object_name": rn}
                    if rt == "METRIC":
                        row["metric_formula"] = self.metric_formulas.get(ro_id, "")
                    rows.append(row)
                    continue

                for so_id in so_ids:
                    so_entries = so_by_id.get(so_id, [{}])
                    for so in so_entries:
                        row = {**base}

                        if rt == "METRIC":
                            row.update({
                                "object_type": "Metric",
                                "object_name": rn,
                                "metric_formula": self.metric_formulas.get(ro_id, ""),
                                "table_name": so.get("table_name", ""),
                                "column_name": so.get("expression", ""),
                                "column_data_type": so.get("form_dtype", ""),
                                "db_instance_name": so.get("datasource", ""),
                            })
                        elif rt == "ATTRIBUTE":
                            row.update({
                                "object_type": "Attribute",
                                "object_name": rn,
                                "attribute_column": so.get("expression", ""),
                                "table_name": so.get("table_name", ""),
                                "column_name": so.get("expression", ""),
                                "column_data_type": so.get("form_dtype", ""),
                                "db_instance_name": so.get("datasource", ""),
                            })
                        elif rt == "FACT":
                            row.update({
                                "object_type": "Fact",
                                "object_name": rn,
                                "table_name": so.get("table_name", ""),
                                "column_name": so.get("expression", ""),
                                "column_data_type": so.get("form_dtype", ""),
                                "db_instance_name": so.get("datasource", ""),
                            })
                        else:
                            row.update({
                                "object_type": rt or "Other",
                                "object_name": rn,
                                "table_name": so.get("table_name", ""),
                                "db_instance_name": so.get("datasource", ""),
                            })

                        rows.append(row)

        # --- Build final DataFrame ---
        df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=FINAL_COLS)
        for c in FINAL_COLS:
            if c not in df.columns:
                df[c] = ""
        df = df[FINAL_COLS].copy()
        df["lineage_row_id"] = [str(i + 1).zfill(8) for i in range(len(df))]
        df["harvested_at"]   = self.ts
        df = df.fillna("").astype(str).apply(lambda s: s.str.strip())
        df = df.drop_duplicates().reset_index(drop=True)

        step("BUILD", f"{len(df):,} rows x {len(FINAL_COLS)} cols")
        if not df.empty:
            if "dataset_type" in df.columns:
                print("\n  By dataset type:")
                for dt, cnt in df["dataset_type"].value_counts().items():
                    print(f"    {dt:<25}: {cnt:>6,}")
            if "object_type" in df.columns:
                print("\n  By object type:")
                for ot, cnt in df["object_type"].value_counts().items():
                    print(f"    {ot:<25}: {cnt:>6,}")
        return df


# =============================================================================
# MAIN
# =============================================================================

def main():
    t0 = datetime.now()
    print("=" * 65)
    print("  MSTR LINEAGE HARVESTER  v9.0.0")
    print(f"  Mode      : {RUN_MODE}")
    print(f"  Harvest   : {PROD_BASE_URL}  ({PROD_PROJECT})")
    print(f"  Publish to: {'Workstation DEV session' if RUN_MODE == 'workstation' else DEV_BASE_URL}")
    print(f"  Cube      : {CUBE_NAME}")
    print(f"  SQL parser: {'sql-metadata' if HAS_SQL_PARSER else 'regex fallback'}")
    print("=" * 65)

    # =========================================================================
    # STEP 1: Connect to PROD (always explicit — Workstation is on DEV)
    # =========================================================================
    step("CONNECT", f"Connecting to PROD: {PROD_BASE_URL}")
    prod_conn = Connection(PROD_BASE_URL, PROD_USERNAME, PROD_PASSWORD,
                           project_name=PROD_PROJECT, login_mode=1,
                           ssl_verify=False)
    pid = prod_conn.project_id
    log.info(f"  PROD Project ID: {pid}")

    # =========================================================================
    # STEP 2: Harvest lineage from PROD
    # =========================================================================
    h = LineageHarvester(prod_conn, pid, PROD_PROJECT)
    h.harvest_l1()        # L1: Documents & Dossiers
    h.harvest_l2()        # L2: Datasets + L12 + Cube SQL
    h.harvest_l3()        # L3: Report objects + L23
    h.harvest_l4()        # L4: Schema objects from logical tables
    h.harvest_l34()       # L34: Metric -> schema mapping
    df = h.build_flat_table()  # JOIN: everything -> 24-column flat table

    # Summary
    print(f"\n  TOTAL: {len(df):,} rows")
    if not df.empty:
        sc = ["dataset_name", "dataset_type", "object_type", "object_name",
              "metric_formula", "attribute_column", "table_name",
              "db_instance_name"]
        avail = [c for c in sc if c in df.columns]
        print()
        print(df[avail].head(20).to_string(index=False))

    # CSV export
    if CSV_EXPORT:
        cp = f"lineage_{PROD_PROJECT}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(cp, index=False, encoding='utf-8', quoting=csv.QUOTE_ALL)
        log.info(f"  CSV: {cp}")

    # Done with PROD
    prod_conn.close()
    log.info("  PROD connection closed")

    if df.empty:
        print("\n  No rows. Stopping.")
        return

    # =========================================================================
    # STEP 3: Connect to DEV and publish cube
    # =========================================================================
    step("PUBLISH", f"SuperCube '{CUBE_NAME}' -> DEV")

    if RUN_MODE == "workstation":
        # Workstation is already connected to DEV — use that session
        from mstrio.connection import get_connection
        dev_conn = get_connection(workstationData, project_name=DEV_PROJECT)
        log.info("  Using Workstation DEV session")
    else:
        dev_conn = Connection(DEV_BASE_URL, DEV_USERNAME, DEV_PASSWORD,
                              project_name=DEV_PROJECT, login_mode=1,
                              ssl_verify=False)
        log.info(f"  Connected to DEV: {DEV_BASE_URL}")

    ds = SuperCube(connection=dev_conn, name=CUBE_NAME)
    ds.add_table(name="LineageEdges", data_frame=df,
                 update_policy="replace",
                 to_attribute=list(df.columns), to_metric=[])
    if TARGET_FOLDER_ID:
        ds.create(folder_id=TARGET_FOLDER_ID)
    else:
        ds.create()

    step("DONE", f"Cube ID: {ds.id}")

    # Cleanup
    if RUN_MODE != "workstation":
        dev_conn.close()

    print(f"\n{'=' * 65}")
    print(f"  COMPLETE | {len(df):,} edges | {(datetime.now() - t0).seconds}s")
    print(f"  Harvested from : {PROD_BASE_URL}")
    print(f"  Published to   : {'DEV (Workstation)' if RUN_MODE == 'workstation' else DEV_BASE_URL}")
    print(f"  Cube           : {CUBE_NAME} ({ds.id})")
    print("=" * 65)


# =============================================================================
# ENTRY POINT
# =============================================================================
# In Workstation: runs automatically when you click "Run"
# Standalone:     python mstr_lineage_harvester.py

main()
