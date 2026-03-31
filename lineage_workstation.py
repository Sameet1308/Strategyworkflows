#!/usr/bin/env python3
"""
mstr_lineage_harvester_workstation.py  v11-clean
Workstation "Run locally" — harvests from PROD, publishes to DEV.
Publishes each level cube IMMEDIATELY after harvest.

pip install mstrio-py pandas sql-metadata
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

RUN_ONLY_PROJECT_IDS = []

# =============================================================================
# FUNCTIONS
# =============================================================================

prod = None
dev = None

def keep_alive_prod():
    """Renew PROD session only. Reconnect if renew fails."""
    if prod:
        try: prod.renew()
        except:
            try: prod.connect()
            except: pass

def reconnect_prod():
    """Force fresh PROD session."""
    print("    Forcing fresh PROD session...")
    try:
        prod.connect()
        print("    PROD reconnected OK")
    except Exception as e:
        print(f"    PROD reconnect warning: {e}")

def unique_list(alist):
    alist.sort()
    return list(alist for alist, _ in itertools.groupby(alist))

def get_object_deps(r, project_id):
    try:
        deps = r.list_dependencies()
        return [[project_id, r.type.value, r.id, r.name, d["type"], d["id"], d["name"]] for d in deps]
    except:
        return []

def search_deps(conn, project_id, obj_id, obj_type):
    try:
        objects = full_search(conn, project=project_id, used_by_object_id=obj_id, used_by_object_type=obj_type)
        return [[d["type"], d["id"], d["name"]] for d in objects]
    except:
        return []

def resolve_down(conn, pid, object_dependants, deps_to_resolve, deps_finished):
    count = 0
    while deps_to_resolve:
        for dtr in deps_to_resolve[:]:
            count += 1
            if count % 100 == 0: keep_alive_prod()
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
                        if nd[0] in [4, 7, 12, 13]: deps_finished.append(rep_nd)
                        else: deps_to_resolve.append(rep_nd)
                else:
                    object_dependants.append([pid, dep_type, dep_id, dep_name, 0, "NA", "NA"])
            deps_to_resolve.remove(dtr)
    return deps_finished

def get_dossier_definition(connection, dossier_id):
    try:
        res = connection.get(url=connection.base_url + f"/api/v2/dossiers/{dossier_id}/definition")
        return res
    except:
        return None

def map_standalone_obj(map_list, child_obj_list, child_obj_position):
    map_list_std = unique_list([[m[0], m[2]] for m in map_list])
    child_obj_list_std = unique_list([[m[0], m[child_obj_position]] for m in child_obj_list])
    for d in child_obj_list_std:
        if d not in map_list_std: map_list.append([d[0], KEY_SF, d[1]])
    return map_list

def safe(v): return str(v).strip() if v is not None else ""

def full_path(obj):
    try:
        anc = getattr(obj, 'ancestors', [])
        if anc and len(anc) > 1:
            return "/".join(a.get('name','') if isinstance(a,dict) else getattr(a,'name','') for a in anc[1:])
        return ""
    except:
        return ""

def get_owner(obj):
    try:
        ow = getattr(obj, 'owner', None)
        if ow:
            return safe(ow.get('name','') if isinstance(ow, dict) else getattr(ow, 'name', ''))
    except: pass
    return ""

def parse_sql_tables(sql):
    if not sql: return []
    if HAS_SQL_PARSER:
        try: return [t for t in SqlParser(sql).tables if not t.startswith(("ZZ","*"))]
        except: pass
    s = re.sub(r"--[^\n]*", "", sql.upper()); s = re.sub(r"/\*.*?\*/", "", s, flags=re.DOTALL)
    pat = r"(?:FROM|JOIN|INNER\s+JOIN|LEFT\s+JOIN|RIGHT\s+JOIN|FULL\s+JOIN|CROSS\s+JOIN)\s+([A-Z0-9_#@.]+)"
    skip = {"WHERE","SELECT","ON","SET","WITH","AS","AND","OR","NOT","IN","NULL","CASE","WHEN","THEN","ELSE","END","GROUP","ORDER","HAVING","UNION"}
    return list(dict.fromkeys(m.split(".")[-1] for m in re.findall(pat, s) if m not in skip and not m.startswith("(")))

def publish_cube(conn, name, data_list, headers, folder_id):
    if not data_list:
        print(f"    [SKIP] {name} (empty)")
        return None
    try: conn.renew()
    except:
        try: conn.connect()
        except: pass
    cube_df = pd.DataFrame(data_list, columns=headers).fillna("").astype(str)
    try:
        ds = SuperCube(connection=conn, name=name)
        ds.add_table(name=name, data_frame=cube_df, update_policy="replace", to_attribute=headers, to_metric=[])
        try:
            # Try create first (new cube)
            ds.create(folder_id=folder_id, force=True) if folder_id else ds.create(force=True)
        except Exception as ce:
            # If create fails (cube exists without force support), try update
            try:
                ds.update()
            except Exception as ue:
                print(f"    [WARN] {name}: create/update both failed, retrying with new SuperCube...")
                # Last resort: find existing cube by name and update it
                try:
                    from mstrio.project_objects import list_all_cubes
                    existing = [c for c in list_all_cubes(connection=conn, to_dictionary=True) if c.get("name") == name]
                    if existing:
                        ds2 = SuperCube(connection=conn, id=existing[0]["id"])
                        ds2.add_table(name=name, data_frame=cube_df, update_policy="replace", to_attribute=headers, to_metric=[])
                        ds2.update()
                        print(f"    [OK] {name}: {len(cube_df):,} rows -> {ds2.id} (updated existing)")
                        return ds2.id
                except: pass
                print(f"    [FAIL] {name}: {ue}")
                return None
        print(f"    [OK] {name}: {len(cube_df):,} rows -> {ds.id}")
        return ds.id
    except Exception as e:
        print(f"    [FAIL] {name}: {e}")
        return None

def elapsed(t0):
    return f"{(datetime.now()-t0).seconds}s"

# =============================================================================
# MAIN
# =============================================================================

def main():
    global prod, dev
    t0 = datetime.now()
    print("=" * 65)
    print("  MSTR LINEAGE HARVESTER v11-clean (Workstation)")
    print(f"  PROD: {PROD_URL}")
    print(f"  DEV : {DEV_PROJECT} (Workstation session)")
    print("=" * 65)

    # === CONNECT ===
    prod = Connection(PROD_URL, PROD_USERNAME, PROD_PASSWORD, login_mode=1, timeout=600)
    env = Environment(connection=prod)
    dev = get_connection(workstationData, project_name=DEV_PROJECT)
    print(f"  Both connections established ({elapsed(t0)})")

    loaded_projects = env.list_loaded_projects()
    if RUN_ONLY_PROJECT_IDS:
        selected_projects = [[p.id, p.name] for p in loaded_projects if p.id in RUN_ONLY_PROJECT_IDS]
    else:
        selected_projects = [[p.id, p.name] for p in loaded_projects]
    print(f"  Projects: {len(selected_projects)}")
    for p in selected_projects: print(f"    {p[1]}")

    # Dynamic cube prefix from project name
    if len(selected_projects) == 1:
        proj_tag = re.sub(r"[^A-Za-z0-9]", "", selected_projects[0][1])
    else:
        proj_tag = "Multi"
    cn = lambda suffix: f"Lineage_{proj_tag}_{suffix}"
    harvest_name = f"MSTR_Lineage_Harvest_{proj_tag}"
    print(f"  Cube prefix: Lineage_{proj_tag}_*")

    # === L0 ===
    publish_cube(dev, cn("L0_Projects"), selected_projects, ["project_id","project_name"], FOLDER_ID)

    # === L4: SCHEMA OBJECTS FROM LOGICAL TABLES ===
    # RUN FIRST — Modeling Service needs a fresh I-Server session.
    # After L23 (10K+ API calls), the Modeling Service session dies.
    print(f"\n  [L4] Schema objects from logical tables (FIRST — fresh session)... ({elapsed(t0)})")
    schema_data = []
    for project in selected_projects:
        pid = project[0]
        for attempt in range(1, 3):
            try:
                if attempt > 1: reconnect_prod()
                prod.select_project(project_id=pid)
                tables = list_logical_tables(connection=prod)
                print(f"    Tables: {len(tables)} (attempt {attempt})")
                for i, table in enumerate(tables):
                    if (i+1)%25==0: keep_alive_prod()
                    if (i+1)%50==0: print(f"      table {i+1}/{len(tables)}...")
                    tn, tid = table.name, table.id
                    try: tds = table.primary_data_source.name
                    except: tds = ""
                    try:
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
                    except Exception as e:
                        print(f"      [WARN] table {tn}: {e}")
                break
            except Exception as e:
                print(f"    [WARN] L4 attempt {attempt} failed for project {pid}: {e}")
                if attempt == 2:
                    print(f"    [SKIP] L4 failed for project {pid} after 2 attempts — continuing without schema data")
    print(f"    TOTAL L4: {len(schema_data)} entries ({elapsed(t0)})")
    publish_cube(dev, cn("L4_SchemaObjects"), schema_data, ["project_id","tbl_name","tbl_id","tbl_datasource","schemaobj_type","schemaobj_id","schemaobj_name","attr_lu_table","form_name","form_datatype","form_precision","expression"], FOLDER_ID)

    # === L1: DOCUMENTS & DOSSIERS ===
    print(f"\n  [L1] Documents & Dossiers... ({elapsed(t0)})")
    documents_all = []
    for project in selected_projects:
        pid = project[0]; prod.select_project(project_id=pid)
        documents_all.append([pid, KEY_SF, KEY_SF, KEY_SF, 0, 0, "", ""])
        dossiers = list_dashboards(connection=prod, project_id=pid)
        print(f"    Dossiers: {len(dossiers)}")
        for d in dossiers:
            try: documents_all.append([pid, "DOSSIER", d.id, d.name, d.type.name, d.subtype, full_path(d), get_owner(d)])
            except: pass
        docs = list_documents(connection=prod, project_id=pid)
        print(f"    Documents: {len(docs)}")
        for d in docs:
            try: documents_all.append([pid, "DOCUMENT", d.id, d.name, d.type.name, d.subtype, full_path(d), get_owner(d)])
            except: pass
    print(f"    TOTAL: {len(documents_all)} apps ({elapsed(t0)})")
    publish_cube(dev, cn("L1_Documents"), documents_all, ["project_id","doc_type","doc_id","doc_name","doc_enum_type","doc_enum_subtype","doc_folder","doc_owner"], FOLDER_ID)

    # === L2: DATASETS ===
    print(f"\n  [L2] Datasets... ({elapsed(t0)})")
    datasets_all = []
    for project in selected_projects:
        pid = project[0]; prod.select_project(project_id=pid)
        datasets_all.append([pid, KEY_SF, 0, KEY_SF, KEY_SF, '', '', ''])
        reports = list_reports(connection=prod, project_id=pid)
        print(f"    Reports: {len(reports)}")
        for r in reports:
            try: datasets_all.append([pid, r.type.name.upper(), r.subtype, r.id, r.name, full_path(r), get_owner(r), ''])
            except: pass
    cubes_sql = {}
    for project in selected_projects:
        pid = project[0]; prod.select_project(project_id=pid)
        cubes_dicts = [c for c in list_all_cubes(connection=prod, to_dictionary=True) if c.get('subtype') in [776, 779]]
        print(f"    Cubes (776/779): {len(cubes_dicts)}")
        existing_ids = {d[3] for d in datasets_all}
        # Suppress mstrio error logging during SQL extraction (very noisy)
        _mstrio_log = logging.getLogger("mstrio"); _prev_level = _mstrio_log.level; _mstrio_log.setLevel(logging.CRITICAL)
        sql_tried = 0; sql_ok = 0
        for c in cubes_dicts:
            sql_text = ""
            # Only attempt SQL on custom SQL cubes (779), not schema OLAP cubes (776)
            if c.get("subtype") == 779:
                sql_tried += 1
                try:
                    sv = OlapCube(prod, c["id"]).export_sql_view()
                    if sv:
                        matches = re.findall(r"(select\s+.*?)\n\n", sv, flags=re.DOTALL | re.IGNORECASE)
                        sql_text = " | ".join(" ".join(m.split()) for m in matches) if matches else " ".join(sv.split())
                        cubes_sql[c["id"]] = sql_text
                        sql_ok += 1
                except: pass
            if c["id"] not in existing_ids:
                datasets_all.append([pid, "CUBE", c.get("subtype", 776), c["id"], c["name"], "", "", sql_text])
            else:
                for d in datasets_all:
                    if d[3] == c["id"]: d[7] = sql_text; break
        _mstrio_log.setLevel(_prev_level)
        print(f"    Cube SQL: tried {sql_tried}, captured {sql_ok}")
    print(f"    TOTAL: {len(datasets_all)} datasets, {len(cubes_sql)} cube SQL ({elapsed(t0)})")
    publish_cube(dev, cn("L2_Datasets"), datasets_all, ["project_id","dataset_type","dataset_subtype","dataset_id","dataset_name","dataset_folder","dataset_owner","cube_sql"], FOLDER_ID)

    # === L12: APP -> DATASET MAPPING ===
    print(f"\n  [L12] App -> Dataset mapping... ({elapsed(t0)})")
    keep_alive_prod()
    l12_mapping, l23_mapping, l2_non_schema = [], [], []
    for project in selected_projects:
        pid = project[0]; prod.select_project(project_id=pid)
        l12_mapping.append([pid, KEY_SF, KEY_SF]); l23_mapping.append([pid, KEY_SF, KEY_SF])
        dataset_ids = [d[3] for d in datasets_all if d[0] == pid]
        dossiers = list_dashboards(connection=prod)
        for i, d in enumerate(dossiers):
            if (i+1)%50==0: keep_alive_prod()
            try:
                res = get_dossier_definition(prod, d.id)
                if res is None: continue
                r = res.json()
                for dset in r.get('datasets', []):
                    l12_mapping.append([pid, d.id, dset['id']])
                    if dset['id'] not in dataset_ids:
                        l2_non_schema.append([pid, "DATASET", 0, dset['id'], dset['name'], "dynamic", "", ""])
                        for ao in dset.get('availableObjects', []): l23_mapping.append([pid, dset['id'], ao['id']])
            except: pass
        docs = list_documents(connection=prod, project_id=pid)
        for d in docs:
            try:
                for c in get_object_deps(d, pid):
                    if c[4] == 3: l12_mapping.append([pid, d.id, c[5]])
            except: pass
    l12_mapping = unique_list(l12_mapping); l23_mapping = unique_list(l23_mapping)
    datasets_all += unique_list(l2_non_schema)
    l12_mapping = map_standalone_obj(l12_mapping, datasets_all, 3)
    print(f"    TOTAL L12: {len(l12_mapping)} mappings ({elapsed(t0)})")
    publish_cube(dev, cn("L12_Mapping"), l12_mapping, ["project_id","doc_id","dataset_id"], FOLDER_ID)

    # === L3: REPORT OBJECTS ===
    print(f"\n  [L3] Report objects... ({elapsed(t0)})")
    keep_alive_prod()
    report_obj_all = []; metric_ids = []; metric_name_lookup = {}; pid_lookup = {}; metric_objects = []
    for project in selected_projects:
        pid = project[0]; prod.select_project(project_id=pid)
        report_obj_all.append([pid, KEY_SF, 0, KEY_SF, KEY_SF])

        attrs = list_attributes(connection=prod, project_id=pid)
        print(f"    Attributes: {len(attrs)}")
        for a in attrs:
            try: report_obj_all.append([pid, a.type.name.upper(), a.subtype, a.id, a.name])
            except: pass

        metrics = list_metrics(connection=prod, project_id=pid)
        print(f"    Metrics: {len(metrics)}")
        for m in metrics:
            try:
                report_obj_all.append([pid, m.type.name.upper(), m.subtype, m.id, m.name])
                metric_ids.append(m.id)
                metric_name_lookup[m.id] = m.name
                pid_lookup[m.id] = pid
                metric_objects.append(m)
            except: pass

        facts = list_facts(connection=prod, project_id=pid)
        print(f"    Facts: {len(facts)}")
        for f in facts:
            try: report_obj_all.append([pid, f.type.name.upper(), f.subtype, f.id, f.name])
            except: pass

    print(f"    TOTAL: {len(report_obj_all)} objects ({elapsed(t0)})")
    publish_cube(dev, cn("L3_ReportObjects"), report_obj_all, ["project_id","repobj_type","repobj_subtype","repobj_id","repobj_name"], FOLDER_ID)

    # === L3b: METRIC FORMULAS via mstrio SDK ===
    # Uses m.expression.text — relies on timeout=600 for Modeling Service warm-up.
    # 10 consecutive failures = bail out.
    print(f"\n  [L3b] Metric formulas via mstrio SDK... ({elapsed(t0)})")
    metric_formulas = {}
    try:
        ok_count = 0; fail_count = 0; consec_fail = 0
        for i, m in enumerate(metric_objects):
            if consec_fail >= 10:
                print(f"    [SKIP] 10 consecutive failures — skipping remaining {len(metric_objects)-i} metrics.")
                break
            if (i+1)%25==0:
                keep_alive_prod()
                print(f"      formula {i+1}/{len(metric_objects)}... ({ok_count} ok, {fail_count} skip)")
            try:
                formula = m.expression.text if m.expression else ""
                if formula:
                    metric_formulas[m.id] = formula.strip()
                    ok_count += 1; consec_fail = 0
                else:
                    fail_count += 1; consec_fail += 1
            except Exception as e:
                fail_count += 1; consec_fail += 1
                if fail_count <= 3: print(f"      [WARN] metric {m.name}: {e}")
        print(f"    Metric formulas: {ok_count} captured, {fail_count} blank ({elapsed(t0)})")
    except Exception as e:
        print(f"    [WARN] L3b failed entirely: {e} — continuing without formulas")

    # Publish metric formulas as separate cube
    if metric_formulas:
        mf_data = [[pid_lookup.get(mid, selected_projects[0][0]), mid, metric_name_lookup.get(mid, ""), formula]
                    for mid, formula in metric_formulas.items()]
        publish_cube(dev, cn("L3b_MetricFormulas"), mf_data, ["project_id","metric_id","metric_name","metric_formula"], FOLDER_ID)
    else:
        print(f"    [SKIP] {cn('L3b_MetricFormulas')} (no formulas captured)")

    # === L23: DATASET -> REPORT OBJECT MAPPING ===
    print(f"\n  [L23] Dataset -> Report Object mapping... ({elapsed(t0)})")
    keep_alive_prod()
    object_dependants, deps_completed_all = [], []
    for project in selected_projects:
        pid = project[0]; prod.select_project(project_id=pid)
        reports = list_reports(connection=prod, project_id=pid)
        print(f"    Reports: {len(reports)} — getting dependencies...")
        for i, r in enumerate(reports):
            if (i+1)%50==0: keep_alive_prod()
            try: object_dependants.extend(get_object_deps(r, pid))
            except: pass
        df_, dr_ = [], []
        for d in object_dependants: (df_ if d[4] in [4,7,12,13,1] else dr_).append(d)
        print(f"    Direct: {len(df_)}, resolving: {len(dr_)}...")
        deps_completed_all.extend(resolve_down(prod, pid, object_dependants, dr_, df_))
    map23 = [[m[0],m[2],m[5]] for m in deps_completed_all]; map23.extend(l23_mapping)
    l23_mapping = unique_list(map_standalone_obj(unique_list(map23), report_obj_all, 3))
    print(f"    TOTAL L23: {len(l23_mapping)} mappings ({elapsed(t0)})")
    publish_cube(dev, cn("L23_Mapping"), l23_mapping, ["project_id","dataset_id","repobj_id"], FOLDER_ID)

    # === L34: METRIC -> SCHEMA MAPPING ===
    print(f"\n  [L34] Metric -> Schema mapping... ({elapsed(t0)})")
    od2, dc2 = [], []
    try:
        reconnect_prod()
        for project in selected_projects:
            pid = project[0]; prod.select_project(project_id=pid)
            metrics = list_metrics(connection=prod, project_id=pid)
            print(f"    Metrics: {len(metrics)} — getting dependencies...")
            for i, m in enumerate(metrics):
                if (i+1)%50==0: keep_alive_prod()
                try: od2.extend(get_object_deps(m, pid))
                except: pass
            df2, dr2 = [], []
            for d in od2: (df2 if d[4] in [4,7,12,13] else dr2).append(d)
            print(f"    Direct: {len(df2)}, resolving: {len(dr2)}...")
            dc2.extend(resolve_down(prod, pid, od2, dr2, df2))
    except Exception as e:
        print(f"    [WARN] L34 failed: {e} — continuing with partial data")
    subtotals = {"00B7BFFF967F42C4B71A4B53D90FB095","078C50834B484EE29948FA9DD5300ADF","1769DBFCCF2D4392938E40418C6E065E","36226A4048A546139BE0AF5F24737BA8","54E7BFD129514717A92BC44CF1FE5A32","7FBA414995194BBAB2CF1BB599209824","83A663067F7E43B2ABF67FD38ECDC7FE","96C487AF4D12472A910C1ACACFB56EFB","B1F4AA7DE683441BA559AA6453C5113E","B328C60462634223B2387D4ADABEEB53","E1853D5A36C74F59A9F8DEFB3F9527A1","F225147A4CA0BB97368A5689D9675E73"}
    l34_mapping = [[m[0],m[2],m[5]] for m in dc2 if m[5] not in subtotals]
    for s in schema_data: l34_mapping.append([s[0], s[5], s[5]])
    l34_mapping = unique_list(l34_mapping)
    print(f"    TOTAL L34: {len(l34_mapping)} mappings ({elapsed(t0)})")
    publish_cube(dev, cn("L34_Mapping"), l34_mapping, ["project_id","repobj_id","schemaobj_id"], FOLDER_ID)

    # === DONE WITH PROD ===
    prod.close()
    print(f"\n  [PROD] Connection closed ({elapsed(t0)})")

    # === FLAT JOIN ===
    print(f"\n  [JOIN] Building flat 24-column table... ({elapsed(t0)})")
    ts = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    doc_lk = {d[2]:{"name":d[3],"folder":safe(d[6]),"owner":safe(d[7])} for d in documents_all if d[2]!=KEY_SF}
    ds_lk = {d[3]:{"name":d[4],"type":d[1],"subtype":d[2],"folder":safe(d[5]),"owner":safe(d[6])} for d in datasets_all if d[3]!=KEY_SF}
    ro_lk = {r[3]:{"type":r[1],"name":r[4]} for r in report_obj_all if r[3]!=KEY_SF}
    so_lk = {}
    for s in schema_data: so_lk.setdefault(s[5],[]).append({"tbl_name":s[1],"tbl_ds":safe(s[3]),"expression":safe(s[11]),"form_dtype":safe(s[9])})
    ds_to_doc = {}
    for m in l12_mapping:
        if m[1]!=KEY_SF and m[1] in doc_lk: ds_to_doc[m[2]]=doc_lk[m[1]]
    ds_to_ro = {}
    for m in l23_mapping:
        if m[2]!=KEY_SF: ds_to_ro.setdefault(m[1],[]).append(m[2])
    ro_to_so = {}
    for m in l34_mapping: ro_to_so.setdefault(m[1],[]).append(m[2])

    rows = []
    for ds_id, di in ds_lk.items():
        doc=ds_to_doc.get(ds_id,{}); dss=di.get("subtype",0); dst=di.get("type","")
        if dss==772: dtype="Freeform Report"
        elif dss==774: dtype="Cube-Sourced Report"
        elif dss==769: dtype="Graph Report"
        elif dst=="DATASET": dtype="Dossier Dataset"
        elif dst=="CUBE": dtype="Schema Cube"
        else: dtype="Grid Report"
        base={"project_name":selected_projects[0][1] if selected_projects else "","project_id":selected_projects[0][0] if selected_projects else "","app_name":doc.get("name",""),"app_folder":doc.get("folder",""),"app_owner":doc.get("owner",""),"dataset_name":di["name"],"dataset_type":dtype,"dataset_folder":di.get("folder",""),"dataset_owner":di.get("owner",""),"report_subtype":str(dss),"harvested_at":ts}
        if ds_id in cubes_sql:
            sql=cubes_sql[ds_id]; base["cube_source_type"]="custom_sql_free_form"; base["dataset_type"]="Freeform Cube"
            for tbl in (parse_sql_tables(sql) or ["(unparsed)"]): rows.append({**base,"object_type":"FreeformSQL","sql_preview":sql,"table_name":tbl})
            continue
        if dss==772: rows.append({**base,"object_type":"FreeformSQL"}); continue
        ro_ids=ds_to_ro.get(ds_id,[])
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
    print(f"    FLAT TABLE: {len(df):,} rows x {len(FINAL_COLS)} cols ({elapsed(t0)})")

    if not df.empty:
        publish_cube(dev, harvest_name, df.values.tolist(), list(df.columns), FOLDER_ID)

    # === SUMMARY ===
    print(f"\n{'='*65}")
    print(f"  COMPLETE | {elapsed(t0)} | Project: {proj_tag}")
    print(f"  10 cubes published to DEV:")
    print(f"    {cn('L0_Projects')}")
    print(f"    {cn('L4_SchemaObjects')}    ({len(schema_data)} rows)")
    print(f"    {cn('L1_Documents')}        ({len(documents_all)} rows)")
    print(f"    {cn('L2_Datasets')}         ({len(datasets_all)} rows)")
    print(f"    {cn('L12_Mapping')}         ({len(l12_mapping)} rows)")
    print(f"    {cn('L3_ReportObjects')}    ({len(report_obj_all)} rows)")
    print(f"    {cn('L3b_MetricFormulas')}  ({len(metric_formulas)} rows)")
    print(f"    {cn('L23_Mapping')}         ({len(l23_mapping)} rows)")
    print(f"    {cn('L34_Mapping')}         ({len(l34_mapping)} rows)")
    print(f"    {harvest_name}  ({len(df)} rows)")
    print("=" * 65)

main()
