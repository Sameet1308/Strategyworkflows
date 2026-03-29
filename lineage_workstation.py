#!/usr/bin/env python3
"""
mstr_lineage_harvester_workstation.py  v11
For: Workstation "Run locally" (connected to DEV)
Harvests from PROD via explicit Connection.
Publishes each level cube IMMEDIATELY after harvest — partial results
available on DEV even if later steps fail.

pip install mstrio-py pandas sql-metadata

Fix for newer mstrio-py: uses list_dashboards instead of list_dossiers.
Replace list_dashboards back to list_dossiers if your version needs it.
"""

import csv, json, itertools, re, logging, warnings
import pandas as pd
from datetime import datetime
from mstrio.connection import Connection, get_connection
from mstrio.server import Environment
from mstrio.object_management import full_search
from mstrio.types import ObjectSubTypes, ObjectTypes
from mstrio.project_objects.datasets.super_cube import SuperCube
from mstrio.project_objects import OlapCube, list_all_cubes
from mstrio.project_objects.report import list_reports
try:
    from mstrio.project_objects.dossier import list_dossiers as list_dashboards
except ImportError:
    from mstrio.project_objects.dashboard import list_dashboards
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

PROD_URL      = "https://YOUR_PROD_SERVER/MicroStrategyLibrarySTD"
PROD_USERNAME = "YOUR_USERNAME"
PROD_PASSWORD = "YOUR_PASSWORD"

DEV_PROJECT   = "YOUR_DEV_PROJECT_NAME"
FOLDER_ID     = ""

CUBE_NAME     = "MSTR_Lineage_Harvest"
KEY_SF        = "standalone"

RUN_ONLY_PROJECT_IDS = [
    # "XXXXXXXX-XXXX-XXXX-XXXX-XXXXXXXXXXXX",
]

# =============================================================================
# FUNCTIONS
# =============================================================================

def unique_list(alist):
    alist.sort()
    return list(alist for alist, _ in itertools.groupby(alist))

def get_object_deps(r, project_id):
    deps = r.list_dependencies()
    return [[project_id, r.type.value, r.id, r.name, d["type"], d["id"], d["name"]] for d in deps]

def search_deps(conn, project_id, obj_id, obj_type):
    objects = full_search(conn, project=project_id, used_by_object_id=obj_id, used_by_object_type=obj_type)
    return [[d["type"], d["id"], d["name"]] for d in objects]

def resolve_down(conn, pid, object_dependants, deps_to_resolve, deps_finished):
    while deps_to_resolve:
        for dtr in deps_to_resolve[:]:
            dep_type, dep_id, dep_name = dtr[4], dtr[5], dtr[6]
            rep_type, rep_id, rep_name = dtr[1], dtr[2], dtr[3]
            if (dep_id in [o[2] for o in object_dependants]) or (dep_type in [11, 61, 53, 22, 26]):
                pass
            else:
                deps = search_deps(conn, pid, dep_id, dep_type)
                if deps:
                    for nd in deps:
                        object_dependants.append([pid, dep_type, dep_id, dep_name, nd[0], nd[1], nd[2]])
                        rep_nd = [pid, rep_type, rep_id, rep_name, nd[0], nd[1], nd[2]]
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
    if not sql: return []
    if HAS_SQL_PARSER:
        try: return [t for t in SqlParser(sql).tables if not t.startswith(("ZZ","*"))]
        except: pass
    s = re.sub(r"--[^\n]*", "", sql.upper())
    s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    pat = r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+([A-Z0-9_#@.]+)"
    skip = {"WHERE","SELECT","ON","SET","WITH","AS","AND","OR","NOT","IN","NULL","CASE","WHEN","THEN","ELSE","END","GROUP","ORDER","HAVING","UNION"}
    return list(dict.fromkeys(m.split(".")[-1] for m in re.findall(pat, s) if m not in skip and not m.startswith("(")))

def publish_cube(conn, name, data_list, headers, folder_id):
    if not data_list:
        print(f"    SKIP {name} (empty)"); return None
    cube_df = pd.DataFrame(data_list, columns=headers).fillna("").astype(str)
    try:
        ds = SuperCube(connection=conn, name=name)
        ds.add_table(name=name, data_frame=cube_df, update_policy="replace", to_attribute=headers, to_metric=[])
        ds.create(folder_id=folder_id) if folder_id else ds.create()
        print(f"    OK   {name}: {len(cube_df):,} rows -> {ds.id}"); return ds.id
    except Exception as e:
        print(f"    FAIL {name}: {e}"); return None

# =============================================================================
# MAIN
# =============================================================================

def main():
    t0 = datetime.now()
    print("=" * 65)
    print("  MSTR LINEAGE HARVESTER v11 (Workstation)")
    print(f"  PROD: {PROD_URL}")
    print(f"  DEV : {DEV_PROJECT} (Workstation session)")
    print("=" * 65)

    # --- CONNECT TO BOTH PROD AND DEV UP FRONT ---
    conn = Connection(PROD_URL, PROD_USERNAME, PROD_PASSWORD, login_mode=1)
    dev_conn = get_connection(workstationData, project_name=DEV_PROJECT)
    env = Environment(connection=conn)
    loaded_projects = env.list_loaded_projects()
    if RUN_ONLY_PROJECT_IDS:
        selected_projects = [[p.id, p.name] for p in loaded_projects if p.id in RUN_ONLY_PROJECT_IDS]
    else:
        selected_projects = [[p.id, p.name] for p in loaded_projects]
    print(f"\n  Projects: {len(selected_projects)}")
    for p in selected_projects: print(f"    {p[1]}")

    # --- L0: PUBLISH IMMEDIATELY ---
    publish_cube(dev_conn, "Lineage_L0_Projects", selected_projects, ["project_id","project_name"], FOLDER_ID)

    # --- L1: HARVEST THEN PUBLISH ---
    print("\n  [L1] Documents & Dossiers...")
    documents_all = []
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        documents_all.append([pid, KEY_SF, KEY_SF, KEY_SF, 0, 0, ""])
        for d in list_dashboards(connection=conn, project_id=pid):
            documents_all.append([pid, "DOSSIER", d.id, d.name, d.type.name, d.subtype, d.ancestors[1]['name'] if len(d.ancestors) > 1 else ""])
        for d in list_documents(connection=conn, project_id=pid):
            documents_all.append([pid, "DOCUMENT", d.id, d.name, d.type.name, d.subtype, d.ancestors[1]['name'] if len(d.ancestors) > 1 else ""])
    print(f"    {len(documents_all)} apps")
    publish_cube(dev_conn, "Lineage_L1_Documents", documents_all, ["project_id","doc_type","doc_id","doc_name","doc_enum_type","doc_enum_subtype","doc_folder"], FOLDER_ID)

    # --- L2: HARVEST THEN PUBLISH ---
    print("\n  [L2] Datasets...")
    datasets_all = []
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        datasets_all.append([pid, KEY_SF, 0, KEY_SF, KEY_SF, ''])
        for r in list_reports(connection=conn, project_id=pid):
            datasets_all.append([pid, r.type.name.upper(), r.subtype, r.id, r.name, r.ancestors[1]['name'] if len(r.ancestors) > 1 else ""])
    cubes_sql = {}
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        cubes_dicts = [c for c in list_all_cubes(connection=conn, to_dictionary=True) if c.get('subtype') in [776, 779]]
        existing_ids = {d[3] for d in datasets_all}
        for c in cubes_dicts:
            if c["id"] not in existing_ids:
                datasets_all.append([pid, "CUBE", c.get("subtype", 776), c["id"], c["name"], ""])
            try:
                sv = OlapCube(conn, c["id"]).export_sql_view()
                if sv:
                    matches = re.findall(r"(select\s+.*?)\n\n", sv, flags=re.DOTALL | re.IGNORECASE)
                    cubes_sql[c["id"]] = " | ".join(" ".join(m.split()) for m in matches) if matches else " ".join(sv.split())
            except: pass
    print(f"    {len(datasets_all)} datasets, {len(cubes_sql)} cube SQL")
    publish_cube(dev_conn, "Lineage_L2_Datasets", datasets_all, ["project_id","dataset_type","dataset_subtype","dataset_id","dataset_name","dataset_folder"], FOLDER_ID)

    # --- L12: HARVEST THEN PUBLISH ---
    print("\n  [L12] App -> Dataset...")
    l12_mapping, l23_mapping, l2_non_schema = [], [], []
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        l12_mapping.append([pid, KEY_SF, KEY_SF]); l23_mapping.append([pid, KEY_SF, KEY_SF])
        dataset_ids = [d[3] for d in datasets_all if d[0] == pid]
        for d in list_dashboards(connection=conn):
            try:
                r = get_dossier_definition(conn, d.id).json()
                for dset in r.get('datasets', []):
                    l12_mapping.append([pid, d.id, dset['id']])
                    if dset['id'] not in dataset_ids:
                        l2_non_schema.append([pid, "DATASET", 0, dset['id'], dset['name'], "dynamic"])
                        for ao in dset.get('availableObjects', []): l23_mapping.append([pid, dset['id'], ao['id']])
            except: pass
        for d in list_documents(connection=conn, project_id=pid):
            try:
                for c in get_object_deps(d, pid):
                    if c[4] == 3: l12_mapping.append([pid, d.id, c[5]])
            except: pass
    l12_mapping = unique_list(l12_mapping); l23_mapping = unique_list(l23_mapping)
    datasets_all += unique_list(l2_non_schema)
    l12_mapping = map_standalone_obj(l12_mapping, datasets_all, 3)
    print(f"    L12: {len(l12_mapping)}")
    publish_cube(dev_conn, "Lineage_L12_Mapping", l12_mapping, ["project_id","doc_id","dataset_id"], FOLDER_ID)

    # --- L3: HARVEST THEN PUBLISH ---
    print("\n  [L3] Report objects...")
    report_obj_all = []; metric_formulas = {}
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        report_obj_all.append([pid, KEY_SF, 0, KEY_SF, KEY_SF])
        for a in list_attributes(connection=conn, project_id=pid): report_obj_all.append([pid, a.type.name.upper(), a.subtype, a.id, a.name])
        for m in list_metrics(connection=conn, project_id=pid):
            report_obj_all.append([pid, m.type.name.upper(), m.subtype, m.id, m.name])
            try: metric_formulas[m.id] = m.expression.text if m.expression else ""
            except: metric_formulas[m.id] = ""
        for f in list_facts(connection=conn, project_id=pid): report_obj_all.append([pid, f.type.name.upper(), f.subtype, f.id, f.name])
    print(f"    {len(report_obj_all)} objects, {len(metric_formulas)} formulas")
    l3f = [r + [metric_formulas.get(r[3],"") if r[1] in ("METRIC","AGG_METRIC") else ""] for r in report_obj_all]
    publish_cube(dev_conn, "Lineage_L3_ReportObjects", l3f, ["project_id","repobj_type","repobj_subtype","repobj_id","repobj_name","metric_formula"], FOLDER_ID)

    # --- L23: HARVEST THEN PUBLISH ---
    print("\n  [L23] Dataset -> Report Object...")
    object_dependants, deps_completed_all = [], []
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        for i, r in enumerate(list_reports(connection=conn, project_id=pid)):
            if (i+1)%100==0: print(f"      {i+1}...")
            try: object_dependants.extend(get_object_deps(r, pid))
            except: pass
        df_, dr_ = [], []
        for d in object_dependants: (df_ if d[4] in [4,7,12,13,1] else dr_).append(d)
        deps_completed_all.extend(resolve_down(conn, pid, object_dependants, dr_, df_))
    map23 = [[m[0],m[2],m[5]] for m in deps_completed_all]; map23.extend(l23_mapping)
    l23_mapping = unique_list(map_standalone_obj(unique_list(map23), report_obj_all, 3))
    print(f"    L23: {len(l23_mapping)}")
    publish_cube(dev_conn, "Lineage_L23_Mapping", l23_mapping, ["project_id","dataset_id","repobj_id"], FOLDER_ID)

    # --- L4: HARVEST THEN PUBLISH ---
    print("\n  [L4] Schema objects from tables...")
    schema_data = []
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        for i, table in enumerate(list_logical_tables(connection=conn)):
            if (i+1)%50==0: print(f"      table {i+1}...")
            tn, tid = table.name, table.id
            try: tds = table.primary_data_source.name
            except: tds = ""
            if table.attributes and table.subtype == 3840:
                for a in table.attributes:
                    if a.id and a.sub_type == "attribute":
                        try: a.list_properties()
                        except: pass
                        try: altn = a.attribute_lookup_table.name
                        except: altn = ""
                        for form in (a.forms or []):
                            if not form.is_form_group:
                                fd = form.data_type.type if form.data_type else "NA"
                                fp = form.data_type.precision if form.data_type else "NA"
                                for expr in (form.expressions or []):
                                    try: et = expr.expression.text
                                    except: et = "NA"
                                    schema_data.append([pid,tn,tid,tds,a.type.name,a.id,a.name,altn,form.name,fd,fp,et])
            if table.facts:
                for f in table.facts:
                    if f.id:
                        try: fd,fp = f.data_type.type, f.data_type.precision
                        except: fd,fp = "NA","NA"
                        for expr in (f.expressions or []):
                            try: et = expr.expression.text
                            except: et = "NA"
                            schema_data.append([pid,tn,tid,tds,f.type.name,f.id,f.name,"NA","NA",fd,fp,et])
    print(f"    L4: {len(schema_data)} entries")
    publish_cube(dev_conn, "Lineage_L4_SchemaObjects", schema_data, ["project_id","tbl_name","tbl_id","tbl_datasource","schemaobj_type","schemaobj_id","schemaobj_name","attr_lu_table","form_name","form_datatype","form_precision","expression"], FOLDER_ID)

    # --- L34: HARVEST THEN PUBLISH ---
    print("\n  [L34] Metric -> Schema...")
    od2, dc2 = [], []
    for project in selected_projects:
        pid = project[0]; conn.select_project(project_id=pid)
        for i, m in enumerate(list_metrics(connection=conn, project_id=pid)):
            if (i+1)%100==0: print(f"      {i+1}...")
            try: od2.extend(get_object_deps(m, pid))
            except: pass
        df2, dr2 = [], []
        for d in od2: (df2 if d[4] in [4,7,12,13] else dr2).append(d)
        dc2.extend(resolve_down(conn, pid, od2, dr2, df2))
    subtotals = {"00B7BFFF967F42C4B71A4B53D90FB095","078C50834B484EE29948FA9DD5300ADF","1769DBFCCF2D4392938E40418C6E065E","36226A4048A546139BE0AF5F24737BA8","54E7BFD129514717A92BC44CF1FE5A32","7FBA414995194BBAB2CF1BB599209824","83A663067F7E43B2ABF67FD38ECDC7FE","96C487AF4D12472A910C1ACACFB56EFB","B1F4AA7DE683441BA559AA6453C5113E","B328C60462634223B2387D4ADABEEB53","E1853D5A36C74F59A9F8DEFB3F9527A1","F225147A4CA0BB97368A5689D9675E73"}
    l34_mapping = [[m[0],m[2],m[5]] for m in dc2 if m[5] not in subtotals]
    for s in schema_data: l34_mapping.append([s[0], s[5], s[5]])
    l34_mapping = unique_list(l34_mapping)
    print(f"    L34: {len(l34_mapping)}")
    publish_cube(dev_conn, "Lineage_L34_Mapping", l34_mapping, ["project_id","repobj_id","schemaobj_id"], FOLDER_ID)

    # --- DONE WITH PROD ---
    conn.close(); print("\n  PROD closed")

    # --- FLAT JOIN -> PUBLISH ---
    print("\n  [JOIN] Flat table...")
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    doc_lk = {d[2]: {"name":d[3],"folder":safe(d[6])} for d in documents_all if d[2]!=KEY_SF}
    ds_lk = {d[3]: {"name":d[4],"type":d[1],"subtype":d[2],"folder":safe(d[5])} for d in datasets_all if d[3]!=KEY_SF}
    ro_lk = {r[3]: {"type":r[1],"name":r[4]} for r in report_obj_all if r[3]!=KEY_SF}
    so_lk = {}
    for s in schema_data: so_lk.setdefault(s[5],[]).append({"tbl_name":s[1],"tbl_ds":safe(s[3]),"expression":safe(s[11]),"form_dtype":safe(s[9])})
    ds_to_doc = {}
    for m in l12_mapping:
        if m[1]!=KEY_SF and m[1] in doc_lk: ds_to_doc[m[2]] = doc_lk[m[1]]
    ds_to_ro = {}
    for m in l23_mapping:
        if m[2]!=KEY_SF: ds_to_ro.setdefault(m[1],[]).append(m[2])
    ro_to_so = {}
    for m in l34_mapping: ro_to_so.setdefault(m[1],[]).append(m[2])
    rows = []
    for ds_id, di in ds_lk.items():
        doc = ds_to_doc.get(ds_id, {}); dss = di.get("subtype",0); dst = di.get("type","")
        if dss==772: dtype="Freeform Report"
        elif dss==774: dtype="Cube-Sourced Report"
        elif dss==769: dtype="Graph Report"
        elif dst=="DATASET": dtype="Dossier Dataset"
        elif dst=="CUBE": dtype="Schema Cube"
        else: dtype="Grid Report"
        base = {"project_name":selected_projects[0][1] if selected_projects else "","project_id":selected_projects[0][0] if selected_projects else "",
                "app_name":doc.get("name",""),"app_folder":doc.get("folder",""),"dataset_name":di["name"],"dataset_type":dtype,
                "dataset_folder":di.get("folder",""),"report_subtype":str(dss),"harvested_at":ts}
        if ds_id in cubes_sql:
            sql=cubes_sql[ds_id]; base["cube_source_type"]="custom_sql_free_form"; base["dataset_type"]="Freeform Cube"
            for tbl in (parse_sql_tables(sql) or ["(unparsed)"]): rows.append({**base,"object_type":"FreeformSQL","sql_preview":sql,"table_name":tbl})
            continue
        if dss==772: rows.append({**base,"object_type":"FreeformSQL"}); continue
        ro_ids = ds_to_ro.get(ds_id,[])
        if not ro_ids: rows.append({**base}); continue
        for ro_id in ro_ids:
            ri=ro_lk.get(ro_id,{"type":"","name":ro_id}); rt=ri["type"]; rn=ri["name"]; so_ids=ro_to_so.get(ro_id,[])
            if not so_ids:
                row={**base,"object_type":rt or "Other","object_name":rn}
                if rt=="METRIC": row["metric_formula"]=metric_formulas.get(ro_id,"")
                rows.append(row); continue
            for so_id in so_ids:
                for so in so_lk.get(so_id,[{}]):
                    row={**base}
                    if rt=="METRIC": row.update({"object_type":"Metric","object_name":rn,"metric_formula":metric_formulas.get(ro_id,""),"table_name":so.get("tbl_name",""),"column_name":so.get("expression",""),"column_data_type":so.get("form_dtype",""),"db_instance_name":so.get("tbl_ds","")})
                    elif rt in ("ATTRIBUTE","Attribute"): row.update({"object_type":"Attribute","object_name":rn,"attribute_column":so.get("expression",""),"table_name":so.get("tbl_name",""),"column_name":so.get("expression",""),"column_data_type":so.get("form_dtype",""),"db_instance_name":so.get("tbl_ds","")})
                    elif rt in ("FACT","Fact"): row.update({"object_type":"Fact","object_name":rn,"table_name":so.get("tbl_name",""),"column_name":so.get("expression",""),"column_data_type":so.get("form_dtype",""),"db_instance_name":so.get("tbl_ds","")})
                    else: row.update({"object_type":rt or "Other","object_name":rn,"table_name":so.get("tbl_name",""),"db_instance_name":so.get("tbl_ds","")})
                    rows.append(row)
    FINAL_COLS=["lineage_row_id","project_id","project_name","app_name","app_folder","app_owner","dataset_name","dataset_type","dataset_folder","dataset_owner","object_type","object_name","attribute_column","metric_formula","sql_preview","table_name","column_name","column_data_type","db_instance_name","dsn_name","db_type","report_subtype","cube_source_type","harvested_at"]
    df=pd.DataFrame(rows) if rows else pd.DataFrame(columns=FINAL_COLS)
    for c in FINAL_COLS:
        if c not in df.columns: df[c]=""
    df=df[FINAL_COLS].copy(); df["lineage_row_id"]=[str(i+1).zfill(8) for i in range(len(df))]; df["harvested_at"]=ts
    df=df.fillna("").astype(str).apply(lambda s: s.str.strip()).drop_duplicates().reset_index(drop=True)
    print(f"\n  FLAT: {len(df):,} rows x {len(FINAL_COLS)} cols")
    if not df.empty:
        publish_cube(dev_conn, CUBE_NAME, df.values.tolist(), list(df.columns), FOLDER_ID)

    print(f"\n{'='*65}")
    print(f"  COMPLETE | {(datetime.now()-t0).seconds}s | 9 cubes -> DEV")
    print("="*65)

main()
