#!/usr/bin/env python3
"""
mstr_lineage_harvester.py  v10.0
Direct adaptation of Robert Prochowicz's proven 5-level lineage script.
Same imports, same functions, same API patterns.
Added: flat 24-column join + SuperCube publish to DEV.

Run from Workstation "Run locally" — harvests from PROD, publishes to DEV.

pip install mstrio-py pandas sql-metadata
"""

import csv, json, pickle, itertools, re, logging, warnings
import pandas as pd
from datetime import datetime
from mstrio.connection import Connection
from mstrio.server import Environment, Project
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

from mstrio import config
config.verbose = False
warnings.filterwarnings("ignore")

try:
    from sql_metadata import Parser as SqlParser
    HAS_SQL_PARSER = True
except ImportError:
    HAS_SQL_PARSER = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("lineage")

# =============================================================================
# CONFIGURATION
# =============================================================================

RUN_MODE = "workstation"  # "workstation" or "standalone"

# PROD — harvest FROM here (always explicit connection)
PROD_URL      = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"
PROD_USERNAME = "YOUR_USERNAME"
PROD_PASSWORD = "YOUR_PASSWORD"

# DEV — publish cube HERE
DEV_PROJECT   = "YOUR_DEV_PROJECT_NAME"
FOLDER_ID     = ""  # "" = My Reports

CUBE_NAME     = "MSTR_Lineage_Harvest"
KEY_SF        = "standalone"

# Leave empty [] to harvest ALL projects on PROD
# Add project IDs to harvest only specific projects
RUN_ONLY_PROJECT_IDS = [
    # "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
]

# =============================================================================
# FUNCTIONS — identical to Robert's reference script
# =============================================================================

def export_to_csv(level_id, df):
    df.to_csv(f"{level_id}.csv", index=False, encoding='utf-8',
              sep=",", quoting=csv.QUOTE_ALL, escapechar="\\", header=True)

def unique_list(alist):
    alist.sort()
    alist = list(alist for alist, _ in itertools.groupby(alist))
    return alist

def get_object_deps(r, project_id):
    deps = r.list_dependencies()
    return [[project_id, r.type.value, r.id, r.name,
             d["type"], d["id"], d["name"]] for d in deps]

def search_deps(conn, project_id, obj_id, obj_type):
    objects = full_search(conn, project=project_id,
                          used_by_object_id=obj_id,
                          used_by_object_type=obj_type)
    return [[d["type"], d["id"], d["name"]] for d in objects]

def resolve_down(conn, pid, object_dependants, deps_to_resolve, deps_finished):
    while deps_to_resolve:
        for dtr in deps_to_resolve[:]:
            pid2, rep_type, rep_id, rep_name = dtr[0], dtr[1], dtr[2], dtr[3]
            dep_type, dep_id, dep_name = dtr[4], dtr[5], dtr[6]
            if (dep_id in [o[2] for o in object_dependants]) or \
               (dep_type in [11, 61, 53, 22, 26]):
                pass
            else:
                deps = search_deps(conn, pid, dep_id, dep_type)
                if deps:
                    for nd in deps:
                        dep_nd = [pid, dep_type, dep_id, dep_name, nd[0], nd[1], nd[2]]
                        rep_nd = [pid, rep_type, rep_id, rep_name, nd[0], nd[1], nd[2]]
                        object_dependants.append(dep_nd)
                        if nd[0] in [4, 7, 12, 13]:
                            deps_finished.append(rep_nd)
                        else:
                            deps_to_resolve.append(rep_nd)
                else:
                    object_dependants.append([pid, dep_type, dep_id, dep_name, 0, "NA", "NA"])
            deps_to_resolve.remove(dtr)
    return deps_finished

def get_dossier_definition(connection, dossier_id):
    url_add = f"/api/v2/dossiers/{dossier_id}/definition"
    res = connection.get(url=connection.base_url + url_add)
    return res

def map_standalone_obj(map_list, child_obj_list, child_obj_position):
    map_list_std = unique_list([[m[0], m[2]] for m in map_list])
    child_obj_list_std = unique_list([[m[0], m[child_obj_position]] for m in child_obj_list])
    for d in child_obj_list_std:
        if d not in map_list_std:
            map_list.append([d[0], KEY_SF, d[1]])
    return map_list

def safe(v):
    return str(v).strip() if v is not None else ""

def parse_sql_tables(sql):
    if not sql:
        return []
    if HAS_SQL_PARSER:
        try:
            return [t for t in SqlParser(sql).tables if not t.startswith(("ZZ", "*"))]
        except:
            pass
    s = re.sub(r"--[^\n]*", "", sql.upper())
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    pat = (r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN"
           r"|FULL\s+JOIN|CROSS\s+JOIN)\s+([A-Z0-9_#@.]+)")
    skip = {"WHERE","SELECT","ON","SET","WITH","AS","AND","OR","NOT","IN",
            "NULL","CASE","WHEN","THEN","ELSE","END","GROUP","ORDER","HAVING","UNION"}
    return list(dict.fromkeys(m.split(".")[-1] for m in re.findall(pat, s)
                               if m not in skip and not m.startswith("(")))

# =============================================================================
# MAIN HARVEST — follows Robert's exact sequence
# =============================================================================

def main():
    t0 = datetime.now()
    print("=" * 65)
    print("  MSTR LINEAGE HARVESTER v10  (Robert's pattern)")
    print(f"  PROD: {PROD_URL}")
    print(f"  DEV : {DEV_PROJECT}")
    print("=" * 65)

    # =========================================================================
    # CONNECT TO PROD
    # =========================================================================
    conn = Connection(PROD_URL, PROD_USERNAME, PROD_PASSWORD, login_mode=1)
    env = Environment(connection=conn)

    loaded_projects = env.list_loaded_projects()
    if RUN_ONLY_PROJECT_IDS:
        selected_projects = [[p.id, p.name] for p in loaded_projects
                             if p.id in RUN_ONLY_PROJECT_IDS]
    else:
        selected_projects = [[p.id, p.name] for p in loaded_projects]
    print(f"\n  Projects: {len(selected_projects)}")
    for p in selected_projects:
        print(f"    {p[1]} ({p[0][:16]}...)")

    # =========================================================================
    # L1: DOCUMENTS & DOSSIERS
    # =========================================================================
    print("\n  [L1] Documents & Dossiers...")
    documents_all = []
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        documents_all.append([pid, KEY_SF, KEY_SF, KEY_SF, 0, 0, ""])
        for d in list_dossiers(connection=conn, project_id=pid):
            documents_all.append([pid, "DOSSIER", d.id, d.name, d.type.name, d.subtype,
                                  d.ancestors[1]['name'] if len(d.ancestors) > 1 else ""])
        for d in list_documents(connection=conn, project_id=pid):
            documents_all.append([pid, "DOCUMENT", d.id, d.name, d.type.name, d.subtype,
                                  d.ancestors[1]['name'] if len(d.ancestors) > 1 else ""])
    print(f"    L1: {len(documents_all)} apps")

    # =========================================================================
    # L2: DATASETS (reports + cubes)
    # =========================================================================
    print("\n  [L2] Datasets...")
    datasets_all = []
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        datasets_all.append([pid, KEY_SF, 0, KEY_SF, KEY_SF, ''])
        for r in list_reports(connection=conn, project_id=pid):
            datasets_all.append([pid, r.type.name.upper(), r.subtype, r.id, r.name,
                                 r.ancestors[1]['name'] if len(r.ancestors) > 1 else ""])
    print(f"    L2 reports: {len(datasets_all)}")

    # Cubes (OLAP + data import)
    cubes_sql = {}  # cube_id -> sql_text
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        cubes_dicts = list_all_cubes(connection=conn, to_dictionary=True)
        cubes_dicts = [c for c in cubes_dicts if c.get('subtype') in [776, 779]]
        print(f"    Cubes (776/779): {len(cubes_dicts)}")
        existing_ids = {d[3] for d in datasets_all}
        for c in cubes_dicts:
            cid = c["id"]
            if cid not in existing_ids:
                datasets_all.append([pid, "CUBE", c.get("subtype", 776), cid, c["name"], ""])
            # SQL extraction
            try:
                cube_obj = OlapCube(conn, cid)
                sv = cube_obj.export_sql_view()
                if sv:
                    pattern = r"(select\s+.*?)\n\n"
                    matches = re.findall(pattern, sv, flags=re.DOTALL | re.IGNORECASE)
                    if matches:
                        cubes_sql[cid] = " | ".join(" ".join(m.split()) for m in matches)
                    elif sv.strip():
                        cubes_sql[cid] = " ".join(sv.split())
            except:
                pass
    print(f"    L2 total: {len(datasets_all)}, cube SQL: {len(cubes_sql)}")

    # =========================================================================
    # L12 MAPPING + DOSSIER INTERNAL DATASETS
    # =========================================================================
    print("\n  [L12] App -> Dataset mapping...")
    l12_mapping = []
    l23_mapping = []
    l2_non_schema = []
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        l12_mapping.append([pid, KEY_SF, KEY_SF])
        l23_mapping.append([pid, KEY_SF, KEY_SF])
        dataset_ids = [d[3] for d in datasets_all if d[0] == pid]

        for i, d in enumerate(list_dossiers(connection=conn)):
            if (i + 1) % 50 == 0: print(f"      dossier {i+1}...")
            try:
                r = get_dossier_definition(conn, d.id).json()
                for dset in r.get('datasets', []):
                    if dset['id'] not in dataset_ids:
                        l12_mapping.append([pid, d.id, dset['id']])
                        l2_non_schema.append([pid, "DATASET", 0, dset['id'], dset['name'], "dynamic"])
                        for ao in dset.get('availableObjects', []):
                            l23_mapping.append([pid, dset['id'], ao['id']])
                    else:
                        l12_mapping.append([pid, d.id, dset['id']])
            except:
                pass

        for d in list_documents(connection=conn, project_id=pid):
            try:
                for c in get_object_deps(d, pid):
                    if c[4] == 3:
                        l12_mapping.append([pid, d.id, c[5]])
            except:
                pass

    l12_mapping = unique_list(l12_mapping)
    l23_mapping = unique_list(l23_mapping)
    l2_non_schema = unique_list(l2_non_schema)
    datasets_all += l2_non_schema
    l12_mapping = map_standalone_obj(l12_mapping, datasets_all, 3)
    print(f"    L12: {len(l12_mapping)}, internal datasets: {len(l2_non_schema)}")

    # =========================================================================
    # L3: REPORT OBJECTS
    # =========================================================================
    print("\n  [L3] Report objects (attrs, metrics, facts)...")
    report_obj_all = []
    metric_formulas = {}  # metric_id -> formula
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        report_obj_all.append([pid, KEY_SF, 0, KEY_SF, KEY_SF])
        for a in list_attributes(connection=conn, project_id=pid):
            report_obj_all.append([pid, a.type.name.upper(), a.subtype, a.id, a.name])
        all_m = list_metrics(connection=conn, project_id=pid)
        for m in all_m:
            report_obj_all.append([pid, m.type.name.upper(), m.subtype, m.id, m.name])
            try:
                metric_formulas[m.id] = m.expression.text if m.expression else ""
            except:
                metric_formulas[m.id] = ""
        for f in list_facts(connection=conn, project_id=pid):
            report_obj_all.append([pid, f.type.name.upper(), f.subtype, f.id, f.name])
    print(f"    L3: {len(report_obj_all)} objects, {len(metric_formulas)} formulas")

    # =========================================================================
    # L23 MAPPING: report -> report objects via list_dependencies + resolve
    # =========================================================================
    print("\n  [L23] Dataset -> Report Object mapping...")
    object_dependants = []
    deps_completed_all = []
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        reports_list = list_reports(connection=conn, project_id=pid)
        print(f"    Reports: {len(reports_list)} — getting dependencies...")
        for i, r in enumerate(reports_list):
            if (i + 1) % 100 == 0: print(f"      {i+1}/{len(reports_list)}...")
            try:
                object_dependants.extend(get_object_deps(r, pid))
            except:
                pass
        deps_finished = []
        deps_to_resolve = []
        for d in object_dependants:
            if d[4] in [4, 7, 12, 13, 1]:
                deps_finished.append(d)
            else:
                deps_to_resolve.append(d)
        print(f"    Direct: {len(deps_finished)}, resolving: {len(deps_to_resolve)}")
        deps_finished = resolve_down(conn, pid, object_dependants,
                                     deps_to_resolve, deps_finished)
        deps_completed_all.extend(deps_finished)

    map23 = [[m[0], m[2], m[5]] for m in deps_completed_all]
    map23.extend(l23_mapping)
    map23 = unique_list(map23)
    map23 = map_standalone_obj(map23, report_obj_all, 3)
    l23_mapping = unique_list(map23)
    print(f"    L23: {len(l23_mapping)}")

    # =========================================================================
    # L4: SCHEMA OBJECTS from list_logical_tables
    # =========================================================================
    print("\n  [L4] Schema objects from logical tables...")
    schema_data = []  # [pid, tbl_name, tbl_id, tbl_datasource, obj_type,
                      #  obj_id, obj_name, lookup_tbl, form_name,
                      #  form_dtype, form_precis, expression]
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        tables_list = list_logical_tables(connection=conn)
        print(f"    Tables: {len(tables_list)}")

        for i, table in enumerate(tables_list):
            if (i + 1) % 50 == 0: print(f"      table {i+1}/{len(tables_list)}...")
            tbl_name = table.name
            tbl_id = table.id
            try:
                tbl_ds = table.primary_data_source.name
            except:
                tbl_ds = ""

            if table.attributes and table.subtype == 3840:
                for a in table.attributes:
                    if a.id and a.sub_type == "attribute":
                        try:
                            a.list_properties()
                        except:
                            pass
                        try:
                            altn = a.attribute_lookup_table.name
                        except:
                            altn = ""
                        for form in (a.forms or []):
                            if not form.is_form_group:
                                fn = form.name
                                if form.data_type:
                                    fd = form.data_type.type
                                    fp = form.data_type.precision
                                else:
                                    fd, fp = "NA", "NA"
                                for expr in (form.expressions or []):
                                    try:
                                        et = expr.expression.text
                                    except:
                                        et = "NA"
                                    schema_data.append([pid, tbl_name, tbl_id, tbl_ds,
                                                        a.type.name, a.id, a.name,
                                                        altn, fn, fd, fp, et])

            if table.facts:
                for f in table.facts:
                    if f.id:
                        try:
                            fd = f.data_type.type
                            fp = f.data_type.precision
                        except:
                            fd, fp = "NA", "NA"
                        for expr in (f.expressions or []):
                            try:
                                et = expr.expression.text
                            except:
                                et = "NA"
                            schema_data.append([pid, tbl_name, tbl_id, tbl_ds,
                                                f.type.name, f.id, f.name,
                                                "NA", "NA", fd, fp, et])

    print(f"    L4: {len(schema_data)} schema entries")

    # =========================================================================
    # L34 MAPPING: metric -> schema objects
    # =========================================================================
    print("\n  [L34] Metric -> Schema mapping...")
    object_dependants2 = []
    deps_completed_all2 = []
    for project in selected_projects:
        pid = project[0]
        conn.select_project(project_id=pid)
        metrics_list = list_metrics(connection=conn, project_id=pid)
        print(f"    Metrics: {len(metrics_list)}")
        for i, m in enumerate(metrics_list):
            if (i + 1) % 100 == 0: print(f"      {i+1}/{len(metrics_list)}...")
            try:
                object_dependants2.extend(get_object_deps(m, pid))
            except:
                pass
        df2 = []
        dr2 = []
        for d in object_dependants2:
            if d[4] in [4, 7, 12, 13]:
                df2.append(d)
            else:
                dr2.append(d)
        df2 = resolve_down(conn, pid, object_dependants2, dr2, df2)
        deps_completed_all2.extend(df2)

    subtotals = {"00B7BFFF967F42C4B71A4B53D90FB095","078C50834B484EE29948FA9DD5300ADF",
                 "1769DBFCCF2D4392938E40418C6E065E","36226A4048A546139BE0AF5F24737BA8",
                 "54E7BFD129514717A92BC44CF1FE5A32","7FBA414995194BBAB2CF1BB599209824",
                 "83A663067F7E43B2ABF67FD38ECDC7FE","96C487AF4D12472A910C1ACACFB56EFB",
                 "B1F4AA7DE683441BA559AA6453C5113E","B328C60462634223B2387D4ADABEEB53",
                 "E1853D5A36C74F59A9F8DEFB3F9527A1","F225147A4CA0BB97368A5689D9675E73"}
    l34_mapping = [[m[0], m[2], m[5]] for m in deps_completed_all2
                    if m[5] not in subtotals]
    # Add identity for attr/fact
    for s in schema_data:
        l34_mapping.append([s[0], s[5], s[5]])
    l34_mapping = unique_list(l34_mapping)
    print(f"    L34: {len(l34_mapping)}")

    # =========================================================================
    # DONE WITH PROD
    # =========================================================================
    conn.close()
    print("\n  [PROD] Connection closed")

    # =========================================================================
    # JOIN INTO 24-COLUMN FLAT TABLE
    # =========================================================================
    print("\n  [JOIN] Building flat lineage table...")
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # Lookups
    doc_lk = {}  # doc_id -> {name, folder}
    for d in documents_all:
        if d[2] != KEY_SF:
            doc_lk[d[2]] = {"name": d[3], "type": d[1], "folder": safe(d[6])}

    ds_lk = {}  # dataset_id -> {name, type, subtype, folder}
    for d in datasets_all:
        if d[3] != KEY_SF:
            ds_lk[d[3]] = {"name": d[4], "type": d[1], "subtype": d[2], "folder": safe(d[5])}

    ro_lk = {}  # repobj_id -> {type, name}
    for r in report_obj_all:
        if r[3] != KEY_SF:
            ro_lk[r[3]] = {"type": r[1], "name": r[4]}

    # schema_obj_id -> [{tbl_name, tbl_ds, expression, form_dtype, ...}]
    so_lk = {}
    for s in schema_data:
        so_lk.setdefault(s[5], []).append({
            "tbl_name": s[1], "tbl_ds": safe(s[3]),
            "obj_type": s[4], "obj_name": s[6],
            "form_name": safe(s[8]), "form_dtype": safe(s[9]),
            "expression": safe(s[11]),
        })

    ds_to_doc = {}  # dataset_id -> doc info
    for m in l12_mapping:
        if m[1] != KEY_SF and m[1] in doc_lk:
            ds_to_doc[m[2]] = doc_lk[m[1]]

    ds_to_ro = {}  # dataset_id -> [repobj_ids]
    for m in l23_mapping:
        if m[2] != KEY_SF:
            ds_to_ro.setdefault(m[1], []).append(m[2])

    ro_to_so = {}  # repobj_id -> [schemaobj_ids]
    for m in l34_mapping:
        ro_to_so.setdefault(m[1], []).append(m[2])

    rows = []
    for ds_id, ds_info in ds_lk.items():
        dsn = ds_info["name"]
        dss = ds_info.get("subtype", 0)
        dst = ds_info.get("type", "")
        dsf = ds_info.get("folder", "")

        doc = ds_to_doc.get(ds_id, {})

        # Dataset type
        if dss == 772:  dtype = "Freeform Report"
        elif dss == 774: dtype = "Cube-Sourced Report"
        elif dss == 769: dtype = "Graph Report"
        elif dst == "DATASET": dtype = "Dossier Dataset"
        elif dst == "CUBE": dtype = "Schema Cube"
        else: dtype = "Grid Report"

        base = {
            "project_name": next((p[1] for p in selected_projects), ""),
            "project_id": next((p[0] for p in selected_projects), ""),
            "app_name": doc.get("name", ""), "app_folder": doc.get("folder", ""),
            "dataset_name": dsn, "dataset_type": dtype,
            "dataset_folder": dsf, "report_subtype": str(dss),
            "harvested_at": ts,
        }

        # Freeform cube SQL
        if ds_id in cubes_sql:
            sql = cubes_sql[ds_id]
            base["cube_source_type"] = "custom_sql_free_form"
            base["dataset_type"] = "Freeform Cube"
            for tbl in (parse_sql_tables(sql) or ["(unparsed)"]):
                rows.append({**base, "object_type": "FreeformSQL",
                             "sql_preview": sql, "table_name": tbl})
            continue

        # Freeform report
        if dss == 772:
            rows.append({**base, "object_type": "FreeformSQL"})
            continue

        # Schema-based: join L23 -> L34 -> L4
        ro_ids = ds_to_ro.get(ds_id, [])
        if not ro_ids:
            rows.append({**base})
            continue

        for ro_id in ro_ids:
            ri = ro_lk.get(ro_id, {"type": "", "name": ro_id})
            rt = ri["type"]
            rn = ri["name"]
            so_ids = ro_to_so.get(ro_id, [])

            if not so_ids:
                row = {**base, "object_type": rt or "Other", "object_name": rn}
                if rt == "METRIC":
                    row["metric_formula"] = metric_formulas.get(ro_id, "")
                rows.append(row)
                continue

            for so_id in so_ids:
                so_entries = so_lk.get(so_id, [{}])
                for so in so_entries:
                    row = {**base}
                    if rt == "METRIC":
                        row.update({
                            "object_type": "Metric", "object_name": rn,
                            "metric_formula": metric_formulas.get(ro_id, ""),
                            "table_name": so.get("tbl_name", ""),
                            "column_name": so.get("expression", ""),
                            "column_data_type": so.get("form_dtype", ""),
                            "db_instance_name": so.get("tbl_ds", ""),
                        })
                    elif rt in ("ATTRIBUTE", "Attribute"):
                        row.update({
                            "object_type": "Attribute", "object_name": rn,
                            "attribute_column": so.get("expression", ""),
                            "table_name": so.get("tbl_name", ""),
                            "column_name": so.get("expression", ""),
                            "column_data_type": so.get("form_dtype", ""),
                            "db_instance_name": so.get("tbl_ds", ""),
                        })
                    elif rt in ("FACT", "Fact"):
                        row.update({
                            "object_type": "Fact", "object_name": rn,
                            "table_name": so.get("tbl_name", ""),
                            "column_name": so.get("expression", ""),
                            "column_data_type": so.get("form_dtype", ""),
                            "db_instance_name": so.get("tbl_ds", ""),
                        })
                    else:
                        row.update({
                            "object_type": rt or "Other", "object_name": rn,
                            "table_name": so.get("tbl_name", ""),
                            "db_instance_name": so.get("tbl_ds", ""),
                        })
                    rows.append(row)

    # Build DataFrame
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
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=FINAL_COLS)
    for c in FINAL_COLS:
        if c not in df.columns:
            df[c] = ""
    df = df[FINAL_COLS].copy()
    df["lineage_row_id"] = [str(i + 1).zfill(8) for i in range(len(df))]
    df["harvested_at"] = ts
    df = df.fillna("").astype(str).apply(lambda s: s.str.strip())
    df = df.drop_duplicates().reset_index(drop=True)

    print(f"\n  LINEAGE TABLE: {len(df):,} rows x {len(FINAL_COLS)} cols")
    if not df.empty:
        for ot, cnt in df["object_type"].value_counts().items():
            print(f"    {ot:<20}: {cnt:>6,}")
        print()
        sc = ["dataset_name","object_type","object_name","metric_formula",
              "attribute_column","table_name","db_instance_name"]
        print(df[[c for c in sc if c in df.columns]].head(15).to_string(index=False))

    if df.empty and not rows:
        print("\n  No rows. Done.")
        return

    # =========================================================================
    # PUBLISH TO DEV — 8 level cubes + 1 flat cube
    # =========================================================================
    print(f"\n  [PUBLISH] Connecting to DEV...")
    if RUN_MODE == "workstation":
        from mstrio.connection import get_connection
        dev_conn = get_connection(workstationData, project_name=DEV_PROJECT)
    else:
        dev_conn = Connection(DEV_URL, DEV_USERNAME, DEV_PASSWORD,
                              project_name=DEV_PROJECT, login_mode=1)

    def publish_cube(conn, name, data_list, headers, folder_id):
        """Create or update a SuperCube."""
        if not data_list:
            print(f"    SKIP {name} (empty)")
            return None
        cube_df = pd.DataFrame(data_list, columns=headers)
        cube_df = cube_df.fillna("").astype(str)
        try:
            ds = SuperCube(connection=conn, name=name)
            ds.add_table(name=name, data_frame=cube_df,
                         update_policy="replace",
                         to_attribute=headers, to_metric=[])
            if folder_id:
                ds.create(folder_id=folder_id)
            else:
                ds.create()
            print(f"    OK   {name}: {len(cube_df):,} rows -> {ds.id}")
            return ds.id
        except Exception as e:
            print(f"    FAIL {name}: {e}")
            return None

    # --- 8 LEVEL CUBES (Robert's pattern) ---
    print(f"\n  [PUBLISH] 8 level cubes...")

    publish_cube(dev_conn, "Lineage_L0_Projects",
                 selected_projects,
                 ["project_id", "project_name"],
                 FOLDER_ID)

    publish_cube(dev_conn, "Lineage_L1_Documents",
                 documents_all,
                 ["project_id", "doc_type", "doc_id", "doc_name",
                  "doc_enum_type", "doc_enum_subtype", "doc_folder"],
                 FOLDER_ID)

    publish_cube(dev_conn, "Lineage_L2_Datasets",
                 datasets_all,
                 ["project_id", "dataset_type", "dataset_subtype",
                  "dataset_id", "dataset_name", "dataset_folder"],
                 FOLDER_ID)

    # L3: add metric formula column
    l3_with_formula = []
    for r in report_obj_all:
        formula = metric_formulas.get(r[3], "") if r[1] in ("METRIC", "AGG_METRIC") else ""
        l3_with_formula.append(r + [formula])
    publish_cube(dev_conn, "Lineage_L3_ReportObjects",
                 l3_with_formula,
                 ["project_id", "repobj_type", "repobj_subtype",
                  "repobj_id", "repobj_name", "metric_formula"],
                 FOLDER_ID)

    publish_cube(dev_conn, "Lineage_L4_SchemaObjects",
                 schema_data,
                 ["project_id", "tbl_name", "tbl_id", "tbl_datasource",
                  "schemaobj_type", "schemaobj_id", "schemaobj_name",
                  "attr_lu_table", "form_name", "form_datatype",
                  "form_precision", "expression"],
                 FOLDER_ID)

    publish_cube(dev_conn, "Lineage_L12_Mapping",
                 l12_mapping,
                 ["project_id", "doc_id", "dataset_id"],
                 FOLDER_ID)

    publish_cube(dev_conn, "Lineage_L23_Mapping",
                 l23_mapping,
                 ["project_id", "dataset_id", "repobj_id"],
                 FOLDER_ID)

    publish_cube(dev_conn, "Lineage_L34_Mapping",
                 l34_mapping,
                 ["project_id", "repobj_id", "schemaobj_id"],
                 FOLDER_ID)

    # --- 1 FLAT CUBE (joined) ---
    if not df.empty:
        print(f"\n  [PUBLISH] Flat joined cube...")
        publish_cube(dev_conn, CUBE_NAME,
                     df.values.tolist(),
                     list(df.columns),
                     FOLDER_ID)

    # --- Cleanup ---
    if RUN_MODE != "workstation":
        dev_conn.close()

    print(f"\n{'='*65}")
    print(f"  COMPLETE | {(datetime.now()-t0).seconds}s")
    print(f"  9 cubes published to DEV")
    print(f"  8 level cubes (Lineage_L0 through L34)")
    print(f"  1 flat cube   ({CUBE_NAME})")
    print("=" * 65)

main()
