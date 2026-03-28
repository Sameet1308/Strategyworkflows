#!/usr/bin/env python3
"""
mstr_api_diagnostic.py
Standalone test script for MSTR REST API endpoint availability.

Tests every endpoint the lineage harvester needs and reports pass/fail.
Run against BOTH your Cloud and On-Prem environments to find what works where.

Usage:
  1. Fill in ENVIRONMENTS below with your server URLs and credentials
  2. Run: python mstr_api_diagnostic.py
  3. Review the pass/fail table at the end
"""

import json
import time
import warnings
import requests
import urllib3
from datetime import datetime

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.filterwarnings("ignore")

# =============================================================================
# CONFIGURATION  —  Fill in BOTH environments
# =============================================================================

ENVIRONMENTS = [
    {
        "name": "CLOUD",
        "base_url": "https://YOUR_CLOUD_SERVER/MicroStrategyLibrarySTD",
        "username": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",
        "project_id": "",   # leave blank to auto-detect first project
    },
    {
        "name": "ON-PREM",
        "base_url": "https://YOUR_ONPREM_SERVER/MicroStrategyLibrarySTD",
        "username": "YOUR_USERNAME",
        "password": "YOUR_PASSWORD",
        "project_id": "",
    },
]

# =============================================================================
# TEST RUNNER
# =============================================================================

class MSTRDiagnostic:

    def __init__(self, env: dict):
        self.env_name = env["name"]
        self.base_url = env["base_url"]
        self.api      = env["base_url"] + "/api"
        self.username = env["username"]
        self.password = env["password"]
        self.project_id = env.get("project_id", "")
        self.token    = None
        self._s       = requests.Session()
        self._s.verify = False
        self.results  = []

        # Discovered IDs for testing
        self._report_id    = ""
        self._cube_id      = ""
        self._metric_id    = ""
        self._attribute_id = ""
        self._fact_id      = ""
        self._table_id     = ""
        self._document_id  = ""

    def _h(self, pid=""):
        h = {"X-MSTR-AuthToken": self.token,
             "Content-Type": "application/json",
             "Accept": "application/json"}
        if pid:
            h["X-MSTR-ProjectID"] = pid
        return h

    def _get(self, path, pid="", params=None, timeout=60):
        try:
            r = self._s.get(f"{self.api}{path}", headers=self._h(pid),
                            params=params, verify=False, timeout=timeout)
            return r.status_code, r.text[:500], r
        except Exception as e:
            return 0, str(e)[:500], None

    def _post(self, path, pid="", params=None, body=None, timeout=60):
        try:
            r = self._s.post(f"{self.api}{path}", headers=self._h(pid),
                             params=params, json=body, verify=False,
                             timeout=timeout)
            return r.status_code, r.text[:500], r
        except Exception as e:
            return 0, str(e)[:500], None

    def _record(self, test_name, endpoint, status, ok, detail=""):
        self.results.append({
            "env":      self.env_name,
            "test":     test_name,
            "endpoint": endpoint,
            "status":   status,
            "result":   "PASS" if ok else "FAIL",
            "detail":   detail[:200],
        })
        icon = "✓" if ok else "✗"
        print(f"    {icon} [{status}] {test_name}")
        if not ok and detail:
            print(f"           {detail[:150]}")

    # -------------------------------------------------------------------------
    # TESTS
    # -------------------------------------------------------------------------

    def test_auth(self):
        """POST /api/auth/login"""
        try:
            r = self._s.post(
                f"{self.api}/auth/login",
                json={"username": self.username, "password": self.password,
                      "loginMode": 1},
                headers={"Content-Type": "application/json"},
                verify=False, timeout=30)
            if r.status_code == 200 and "X-MSTR-AuthToken" in r.headers:
                self.token = r.headers["X-MSTR-AuthToken"]
                self._record("Auth Login", "POST /auth/login", 200, True)
                return True
            else:
                self._record("Auth Login", "POST /auth/login",
                             r.status_code, False, r.text[:200])
                return False
        except Exception as e:
            self._record("Auth Login", "POST /auth/login", 0, False, str(e))
            return False

    def test_projects(self):
        """GET /api/projects"""
        code, body, r = self._get("/projects")
        ok = code == 200
        if ok and r:
            data = r.json()
            projects = data if isinstance(data, list) else data.get("projects", [])
            if projects:
                if not self.project_id:
                    self.project_id = projects[0].get("id", "")
                self._record("List Projects", "GET /projects", code, True,
                             f"{len(projects)} project(s). Using: {self.project_id[:16]}...")
            else:
                self._record("List Projects", "GET /projects", code, False,
                             "No projects returned")
        else:
            self._record("List Projects", "GET /projects", code, False, body)

    def test_search(self, obj_type, type_name):
        """GET /api/searches/results?type={t}&limit=1"""
        code, body, r = self._get("/searches/results",
                                   pid=self.project_id,
                                   params={"type": obj_type, "limit": 3})
        ok = code == 200
        oid = ""
        if ok and r:
            items = r.json().get("result", [])
            if items:
                oid = items[0].get("id", "")
                self._record(f"Search {type_name}",
                             f"GET /searches/results?type={obj_type}",
                             code, True,
                             f"{len(items)} found. Sample: {items[0].get('name','')[:40]}")
            else:
                self._record(f"Search {type_name}",
                             f"GET /searches/results?type={obj_type}",
                             code, True, "0 objects found (empty project?)")
        else:
            self._record(f"Search {type_name}",
                         f"GET /searches/results?type={obj_type}",
                         code, False, body)
        return oid

    def test_report_def(self):
        """GET /api/reports/{id} (JSON Data API)"""
        if not self._report_id:
            self._record("Report Definition", "GET /reports/{id}", 0, False,
                         "No report ID found from search")
            return
        code, body, r = self._get(f"/reports/{self._report_id}",
                                   pid=self.project_id)
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            result = data.get("result", data)
            defn = result.get("definition", {})
            avail = defn.get("availableObjects", {})
            na = len(avail.get("attributes", []))
            nm = len(avail.get("metrics", []))
            detail = f"{na} attrs, {nm} metrics in availableObjects"
        else:
            detail = body
        self._record("Report Definition (JSON Data API)",
                     "GET /reports/{id}", code, ok, detail)

    def test_cube_def(self):
        """GET /api/cubes/{id} (JSON Data API)"""
        if not self._cube_id:
            self._record("Cube Definition", "GET /cubes/{id}", 0, False,
                         "No cube ID found from search")
            return
        code, body, r = self._get(f"/cubes/{self._cube_id}",
                                   pid=self.project_id)
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            result = data.get("result", data)
            defn = result.get("definition", {})
            avail = defn.get("availableObjects", {})
            na = len(avail.get("attributes", []))
            nm = len(avail.get("metrics", []))
            detail = f"{na} attrs, {nm} metrics in availableObjects"
        else:
            detail = body
        self._record("Cube Definition (JSON Data API)",
                     "GET /cubes/{id}", code, ok, detail)

    def test_model_metric(self):
        """GET /api/model/metrics/{id}?showExpressionAs=tokens"""
        if not self._metric_id:
            self._record("Model Metric", "GET /model/metrics/{id}", 0, False,
                         "No metric ID found")
            return
        code, body, r = self._get(
            f"/model/metrics/{self._metric_id}",
            pid=self.project_id,
            params={"showExpressionAs": "tokens"})
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            expr = data.get("expression", {})
            formula = expr.get("text", "")
            name = data.get("information", {}).get("name", "")
            detail = f"name={name}, formula={formula[:80]}"
        else:
            detail = body
        self._record("Model Metric (formula)",
                     "GET /model/metrics/{id}?showExpressionAs=tokens",
                     code, ok, detail)

    def test_model_attribute(self):
        """GET /api/model/attributes/{id}?showExpressionAs=tokens"""
        if not self._attribute_id:
            self._record("Model Attribute", "GET /model/attributes/{id}",
                         0, False, "No attribute ID found")
            return
        code, body, r = self._get(
            f"/model/attributes/{self._attribute_id}",
            pid=self.project_id,
            params={"showExpressionAs": "tokens"})
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            name = data.get("information", {}).get("name", "")
            forms = data.get("forms", [])
            nf = len(forms)
            expr_sample = ""
            if forms:
                exprs = forms[0].get("expressions", [])
                if exprs:
                    expr_sample = exprs[0].get("expression", {}).get("text", "")
            detail = f"name={name}, {nf} forms, expr={expr_sample[:60]}"
        else:
            detail = body
        self._record("Model Attribute (expressions)",
                     "GET /model/attributes/{id}?showExpressionAs=tokens",
                     code, ok, detail)

    def test_model_fact(self):
        """GET /api/model/facts/{id}?showExpressionAs=tokens"""
        if not self._fact_id:
            self._record("Model Fact", "GET /model/facts/{id}", 0, False,
                         "No fact ID found")
            return
        code, body, r = self._get(
            f"/model/facts/{self._fact_id}",
            pid=self.project_id,
            params={"showExpressionAs": "tokens"})
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            name = data.get("information", {}).get("name", "")
            exprs = data.get("expressions", [])
            ne = len(exprs)
            col = ""
            if exprs:
                col = exprs[0].get("expression", {}).get("tree", {}).get("columnName", "")
            detail = f"name={name}, {ne} expressions, col={col}"
        else:
            detail = body
        self._record("Model Fact (column mapping)",
                     "GET /model/facts/{id}?showExpressionAs=tokens",
                     code, ok, detail)

    def test_model_table(self):
        """GET /api/model/tables/{id}"""
        if not self._table_id:
            self._record("Model Table", "GET /model/tables/{id}", 0, False,
                         "No table ID found")
            return
        code, body, r = self._get(f"/model/tables/{self._table_id}",
                                   pid=self.project_id)
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            name = data.get("information", {}).get("name", "")
            phys = data.get("physicalTable", {}) or {}
            ncols = len(phys.get("columns", []))
            ds_id = phys.get("information", {}).get("dataSourceId", "")
            detail = f"name={name}, {ncols} columns, dsId={ds_id[:16]}"
        else:
            detail = body
        self._record("Model Table (columns + datasource)",
                     "GET /model/tables/{id}", code, ok, detail)

    def test_model_report(self):
        """GET /api/model/reports/{id} -- the heavy call that may fail"""
        if not self._report_id:
            self._record("Model Report (HEAVY)", "GET /model/reports/{id}",
                         0, False, "No report ID")
            return
        code, body, r = self._get(f"/model/reports/{self._report_id}",
                                   pid=self.project_id, timeout=30)
        ok = code == 200
        detail = body if not ok else "Response received"
        self._record("Model Report Definition (HEAVY - may fail)",
                     "GET /model/reports/{id}", code, ok, detail)

    def test_report_sql_v2(self):
        """POST /v2/reports/{id}/instances?executionStage=resolve_prompts
           GET  /v2/reports/{id}/instances/{iid}/sqlView"""
        if not self._report_id:
            self._record("Report SQL (v2)", "POST /v2/reports/{id}/instances",
                         0, False, "No report ID")
            return
        code, body, r = self._post(
            f"/v2/reports/{self._report_id}/instances",
            pid=self.project_id,
            params={"executionStage": "resolve_prompts"})
        if code != 200 or not r:
            self._record("Report SQL v2 (create instance)",
                         "POST /v2/reports/{id}/instances?executionStage=resolve_prompts",
                         code, False, body)
            return
        data = r.json()
        iid = data.get("instanceId", "")
        status = data.get("status", "")
        if not iid:
            self._record("Report SQL v2 (create instance)",
                         "POST /v2/reports/{id}/instances",
                         code, False, f"No instanceId. status={status}")
            return
        self._record("Report SQL v2 (create instance)",
                     "POST /v2/reports/{id}/instances?executionStage=resolve_prompts",
                     code, True, f"instanceId={iid[:16]}, status={status}")

        # Now get SQL view
        time.sleep(1)
        code2, body2, r2 = self._get(
            f"/v2/reports/{self._report_id}/instances/{iid}/sqlView",
            pid=self.project_id)
        ok2 = code2 == 200
        detail2 = ""
        if ok2 and r2:
            sql_data = r2.json()
            stmts = sql_data.get("sqlStatements", [])
            detail2 = f"{len(stmts)} SQL statement(s)"
            if stmts and isinstance(stmts[0], dict):
                detail2 += f": {stmts[0].get('sql','')[:60]}"
        else:
            detail2 = body2
        self._record("Report SQL v2 (get sqlView)",
                     "GET /v2/reports/{id}/instances/{iid}/sqlView",
                     code2, ok2, detail2)

    def test_report_sql_direct(self):
        """GET /api/reports/{id}/sqlView (direct, simpler)"""
        if not self._report_id:
            return
        code, body, r = self._get(f"/reports/{self._report_id}/sqlView",
                                   pid=self.project_id)
        ok = code == 200
        self._record("Report SQL (direct fallback)",
                     "GET /reports/{id}/sqlView", code, ok,
                     body[:100] if not ok else "OK")

    def test_datasources(self):
        """GET /api/datasources"""
        code, body, r = self._get("/datasources")
        ok = code == 200
        detail = ""
        if ok and r:
            ds = r.json().get("datasources", [])
            detail = f"{len(ds)} datasource(s)"
        else:
            detail = body
        self._record("Datasources", "GET /datasources", code, ok, detail)

    def test_dossier_def(self):
        """GET /api/dossiers/{id}/definition"""
        if not self._document_id:
            self._record("Dossier Definition", "GET /dossiers/{id}/definition",
                         0, False, "No document/dossier ID found")
            return
        code, body, r = self._get(
            f"/dossiers/{self._document_id}/definition",
            pid=self.project_id)
        ok = code == 200
        detail = ""
        if ok and r:
            data = r.json()
            ds = data.get("datasets", [])
            detail = f"{len(ds)} dataset(s)"
        else:
            # Try /documents/ fallback
            code2, body2, r2 = self._get(
                f"/documents/{self._document_id}/definition",
                pid=self.project_id)
            if code2 == 200:
                ok = True
                detail = "OK via /documents/ fallback"
            else:
                detail = body
        self._record("Dossier/Document Definition",
                     "GET /dossiers/{id}/definition", code, ok, detail)

    def test_metadata_search(self):
        """POST /api/metadataSearches/results?usesObject={id};{type}"""
        test_id = self._metric_id or self._attribute_id or self._report_id
        test_type = 4 if self._metric_id else (12 if self._attribute_id else 3)
        if not test_id:
            self._record("Metadata Search (lineage)",
                         "POST /metadataSearches/results", 0, False,
                         "No object ID to test with")
            return
        code, body, r = self._post(
            "/metadataSearches/results",
            pid=self.project_id,
            params={"domain": 2,
                    "usesObject": f"{test_id};{test_type}",
                    "usesRecursive": "false"})
        ok = code == 200 or code == 204
        detail = ""
        if r and r.status_code == 200:
            data = r.json()
            if isinstance(data, list):
                detail = f"{len(data)} components found"
            else:
                items = data.get("result", [])
                detail = f"{len(items)} components found"
        else:
            detail = body
        self._record("Metadata Search (lineage/components)",
                     "POST /metadataSearches/results?usesObject=...",
                     code, ok, detail)

    def test_push_data_search(self):
        """GET /api/searches/results?type=776&name=... (find existing cube)"""
        code, body, r = self._get("/searches/results",
                                   pid=self.project_id,
                                   params={"type": 776,
                                           "name": "MSTR_Lineage_Harvest",
                                           "limit": 5})
        ok = code == 200
        detail = ""
        if ok and r:
            items = r.json().get("result", [])
            detail = f"{len(items)} cube(s) named MSTR_Lineage_Harvest"
        else:
            detail = body
        self._record("Push Data (search existing cube)",
                     "GET /searches/results?type=776&name=...",
                     code, ok, detail)

    # -------------------------------------------------------------------------
    # RUN ALL
    # -------------------------------------------------------------------------

    def run(self):
        print(f"\n{'='*65}")
        print(f"  MSTR API DIAGNOSTIC  —  {self.env_name}")
        print(f"  Server: {self.base_url}")
        print(f"  Time  : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*65}\n")

        # 1. Auth
        if not self.test_auth():
            print("\n  AUTH FAILED — cannot continue.\n")
            return

        # 2. Projects
        self.test_projects()
        if not self.project_id:
            print("\n  NO PROJECT — cannot continue.\n")
            return

        # 3. Search for test objects
        print(f"\n  Searching for sample objects in project {self.project_id[:16]}...\n")
        self._report_id    = self.test_search(3,   "Reports")
        self._cube_id      = self.test_search(776, "Cubes")
        self._metric_id    = self.test_search(4,   "Metrics")
        self._attribute_id = self.test_search(12,  "Attributes")
        self._fact_id      = self.test_search(13,  "Facts")
        self._table_id     = self.test_search(53,  "Tables")
        self._document_id  = self.test_search(55,  "Documents/Dossiers")

        # 4. JSON Data API endpoints (should always work)
        print(f"\n  --- JSON Data API (no Modeling Service) ---\n")
        self.test_report_def()
        self.test_cube_def()
        self.test_report_sql_direct()
        self.test_report_sql_v2()
        self.test_datasources()
        self.test_dossier_def()

        # 5. Modeling Service endpoints (may fail)
        print(f"\n  --- Modeling Service endpoints ---\n")
        self.test_model_metric()
        self.test_model_attribute()
        self.test_model_fact()
        self.test_model_table()
        self.test_model_report()

        # 6. Metadata Search (lineage)
        print(f"\n  --- Lineage API ---\n")
        self.test_metadata_search()

        # 7. Push Data readiness
        print(f"\n  --- Push Data API ---\n")
        self.test_push_data_search()

        # Cleanup
        if self.token:
            try:
                self._s.post(f"{self.api}/auth/logout",
                             headers=self._h(), verify=False)
            except:
                pass

    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------

    def print_summary(self):
        print(f"\n{'='*65}")
        print(f"  SUMMARY  —  {self.env_name}")
        print(f"{'='*65}")
        passed = sum(1 for r in self.results if r["result"] == "PASS")
        failed = sum(1 for r in self.results if r["result"] == "FAIL")
        print(f"  PASS: {passed}   FAIL: {failed}   TOTAL: {len(self.results)}\n")

        for r in self.results:
            icon = "✓" if r["result"] == "PASS" else "✗"
            print(f"  {icon} {r['result']:<4}  {r['test']:<45} [{r['status']}]")
            if r["result"] == "FAIL" and r["detail"]:
                print(f"          {r['detail'][:120]}")
        print()


# =============================================================================
# MAIN
# =============================================================================

def main():
    print("\n" + "="*65)
    print("  MSTR REST API ENDPOINT DIAGNOSTIC")
    print("  Tests all endpoints needed by the lineage harvester")
    print("  Run against Cloud AND On-Prem to compare")
    print("="*65)

    all_results = []

    for env in ENVIRONMENTS:
        if "YOUR_" in env["base_url"]:
            print(f"\n  SKIPPING {env['name']} — placeholder URL detected")
            continue

        diag = MSTRDiagnostic(env)
        diag.run()
        diag.print_summary()
        all_results.extend(diag.results)

    # Cross-environment comparison
    if len(ENVIRONMENTS) > 1 and all_results:
        envs = list(set(r["env"] for r in all_results))
        if len(envs) == 2:
            print(f"\n{'='*65}")
            print(f"  COMPARISON: {envs[0]} vs {envs[1]}")
            print(f"{'='*65}\n")

            tests_a = {r["test"]: r["result"] for r in all_results if r["env"] == envs[0]}
            tests_b = {r["test"]: r["result"] for r in all_results if r["env"] == envs[1]}

            all_tests = sorted(set(list(tests_a.keys()) + list(tests_b.keys())))
            print(f"  {'Test':<45} {envs[0]:<10} {envs[1]:<10}")
            print(f"  {'-'*45} {'-'*10} {'-'*10}")
            for t in all_tests:
                ra = tests_a.get(t, "N/A")
                rb = tests_b.get(t, "N/A")
                flag = " <-- DIFFERS" if ra != rb else ""
                print(f"  {t:<45} {ra:<10} {rb:<10}{flag}")

            print()
            print("  RECOMMENDATION:")
            # Check modeling service
            model_tests = ["Model Metric (formula)",
                           "Model Attribute (expressions)",
                           "Model Fact (column mapping)",
                           "Model Table (columns + datasource)"]
            cloud_model = all(tests_a.get(t) == "PASS" or tests_b.get(t) == "PASS"
                              for t in model_tests
                              if t in tests_a or t in tests_b)

            for env_name in envs:
                tests = {r["test"]: r["result"] for r in all_results if r["env"] == env_name}
                model_ok = all(tests.get(t) == "PASS" for t in model_tests if t in tests)
                if model_ok:
                    print(f"    {env_name}: Modeling Service WORKS -> use v7 (full harvester)")
                else:
                    lineage_ok = tests.get("Metadata Search (lineage/components)") == "PASS"
                    if lineage_ok:
                        print(f"    {env_name}: Modeling Service FAILS, metadataSearches WORKS -> use v5 (Semantic Graph)")
                    else:
                        print(f"    {env_name}: Both Modeling Service and metadataSearches FAIL -> check permissions/config")

    print(f"\n{'='*65}")
    print("  DIAGNOSTIC COMPLETE")
    print(f"{'='*65}\n")


if __name__ == "__main__":
    main()
