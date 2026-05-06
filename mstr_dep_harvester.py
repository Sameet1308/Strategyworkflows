"""
MSTR Application Dependency Harvester + Cross-Environment Diff
==============================================================

Verified against Strategy ONE (March 2026) REST API docs:
  https://microstrategy.github.io/rest-api-docs/

Key API endpoints used (all verified):
  POST  /api/auth/login                          - returns X-MSTR-AuthToken header
  GET   /api/projects                            - list projects, find projectId by name
  GET   /api/searches/results                    - quick search (name, type, pattern)
  POST  /api/metadataSearches/results            - CREATE recursive lineage search
  GET   /api/metadataSearches/results            - RETRIEVE stored lineage results
  GET   /api/objects/{id}?type={type}            - object metadata (id, version, dateModified, owner, acl)
  POST  /api/auth/logout

Critical correction from prior attempt
--------------------------------------
The dependency walk does NOT use per-type modeling endpoints
(/api/model/metrics/.../dependencies). Strategy uses a UNIFIED metadata
lineage search:

    POST /api/metadataSearches/results
       ?domain=2
       &usedByObject={objectId};{objectType}    <-- semicolon-separated; requests urlencodes
       &usedByRecursive=true                     <-- this is the magic flag
       &type={types-to-include}                  <-- optional filter, repeatable

    "usedByObject" semantics: returns objects USED BY the given object
    (i.e., its components / what it depends on). With usedByRecursive=true,
    it walks the entire transitive closure server-side in ONE call.

    "usesObject" is the inverse: returns objects that USE the given object
    (i.e., what depends on it). For migration we want usedByObject.

The doc URL note: the path is "/api/metadataSearches/results" (plural),
even though some doc text incorrectly writes "/api/metadataSearch/results".

Pattern values (EnumDSSXMLSearchTypes):
  1 = BeginsWith,  2 = Exactly,  3 = EndsWith,  4 = Contains

Object types used (EnumDSSXMLObjectTypes):
  3=Report, 4=Metric, 8=Folder/LogicalTable, 10=Prompt, 12=Attribute,
  13=Fact, 14=Function, 15=Table, 18=Schema, 21=DrillMap,
  27=Consolidation, 39=ScheduleTrigger, 55=Document/Dossier,
  58=SecurityFilter

Configuration via env vars:
  MSTR_DEV_URL  e.g. https://your-dev-server.com/MicroStrategyLibrarySTD
  MSTR_DEV_USER, MSTR_DEV_PASS
  MSTR_TGT_URL, MSTR_TGT_USER, MSTR_TGT_PASS  (only needed for --compare-target)

Base URL must stop at /MicroStrategyLibrarySTD; the /api path is appended in code.

Usage
-----
  # 1. Probe one endpoint, print raw JSON, exit
  python mstr_dep_harvester.py --probe --root "Regional Sales Dashboard" --root-type 55 --project "Enterprise DW"

  # 2. Full recursive harvest from a dossier
  python mstr_dep_harvester.py --root "Regional Sales Dashboard" --root-type 55 --project "Enterprise DW"

  # 3. Harvest from a report
  python mstr_dep_harvester.py --root "Customer Summary" --root-type 3 --project "Enterprise DW"

  # 4. Harvest + diff against UAT
  python mstr_dep_harvester.py --root "Regional Sales Dashboard" --root-type 55 --project "Enterprise DW" --compare-target
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Optional

import requests


# ---------------------------------------------------------------------------
# EnumDSSXMLObjectTypes (subset relevant to lineage)
# ---------------------------------------------------------------------------
TYPE_NAMES = {
    1:  "Filter",
    2:  "Template",
    3:  "Report",
    4:  "Metric",
    8:  "Folder/LogicalTable",
    10: "Prompt",
    11: "Function",
    12: "Attribute",
    13: "Fact",
    14: "AttributeForm",
    15: "Table",
    18: "Schema",
    21: "DrillMap",
    27: "Consolidation",
    34: "User/Group",
    39: "ScheduleTrigger",
    44: "Configuration",
    55: "Document/Dossier",
    58: "SecurityFilter",
}

# Default type filter for recursive lineage; empty list = "return all types"
DEFAULT_INCLUDE_TYPES: list[int] = []


# ---------------------------------------------------------------------------
# ObjRec: one row in the manifest
# ---------------------------------------------------------------------------
@dataclass
class ObjRec:
    id: str
    type: int
    subtype: Optional[int] = None
    name: str = ""
    path: str = ""
    date_created: str = ""
    date_modified: str = ""
    version: str = ""
    owner_name: str = ""
    owner_id: str = ""
    acg: Optional[int] = None
    description: str = ""


# ---------------------------------------------------------------------------
# MSTR REST client (verified against March 2026 docs)
# ---------------------------------------------------------------------------
class MSTR:
    def __init__(self, base_url: str, username: str, password: str, label: str = "env"):
        self.base = base_url.rstrip("/")
        if self.base.endswith("/api"):
            raise ValueError("Base URL must stop at /MicroStrategyLibrarySTD, not /api")
        self.label = label
        self.s = requests.Session()
        self.s.headers["Accept"] = "application/json"
        self.s.headers["Content-Type"] = "application/json"
        self.token: Optional[str] = None
        self.project_id: Optional[str] = None
        self._username = username
        self._password = password

    # ---- HTTP core --------------------------------------------------------
    def _req(self, method: str, path: str, **kw) -> requests.Response:
        url = f"{self.base}/api{path}"
        try:
            r = self.s.request(method, url, timeout=120, **kw)
        except requests.RequestException as e:
            raise RuntimeError(f"[{self.label}] {method} {url} network error: {e}") from e
        if not r.ok:
            body = r.text[:1000] if r.text else "(no body)"
            raise RuntimeError(
                f"[{self.label}] {method} {url} -> HTTP {r.status_code}\n"
                f"Response: {body}"
            )
        return r

    # ---- Auth (verified) --------------------------------------------------
    def login(self) -> None:
        # POST /api/auth/login  body: {loginMode, username, password}
        # Returns 204 + X-MSTR-AuthToken header
        r = self._req(
            "POST", "/auth/login",
            data=json.dumps({
                "loginMode": 1,
                "username": self._username,
                "password": self._password,
            }),
        )
        token = r.headers.get("X-MSTR-AuthToken") or r.headers.get("x-mstr-authtoken")
        if not token:
            raise RuntimeError(f"[{self.label}] login: no X-MSTR-AuthToken header")
        self.token = token
        self.s.headers["X-MSTR-AuthToken"] = token

    def logout(self) -> None:
        if self.token:
            try:
                self._req("POST", "/auth/logout")
            except Exception:
                pass

    def set_project(self, project_name: str) -> None:
        # GET /api/projects -> [{id, name, alias, description, status}]
        r = self._req("GET", "/projects")
        for p in r.json():
            if p.get("name") == project_name:
                self.project_id = p["id"]
                self.s.headers["X-MSTR-ProjectID"] = p["id"]
                return
        raise RuntimeError(f"[{self.label}] project not found: {project_name}")

    # ---- Quick search (verified) -----------------------------------------
    def search(self, name: str, types: list[int], pattern: int = 2) -> list[dict]:
        """
        GET /api/searches/results?name=X&type=N&pattern=P&getAncestors=false&limit=-1&certifiedStatus=ALL

        pattern: 1=BeginsWith, 2=Exactly, 3=EndsWith, 4=Contains
        """
        params: list[tuple[str, Any]] = [
            ("name", name),
            ("pattern", pattern),
            ("getAncestors", "false"),
            ("limit", -1),
            ("certifiedStatus", "ALL"),
        ]
        for t in types:
            params.append(("type", t))
        r = self._req("GET", "/searches/results", params=params)
        return r.json().get("result", [])

    # ---- Object metadata (verified) --------------------------------------
    def get_object(self, obj_id: str, obj_type: int) -> dict:
        """
        GET /api/objects/{id}?type={t}
        Returns: {id, name, type, subtype, dateCreated, dateModified, version,
                  acg, owner:{id,name}, acl:[...]}
        """
        r = self._req("GET", f"/objects/{obj_id}", params={"type": obj_type})
        return r.json()

    # ---- Recursive lineage (THIS is the key one) -------------------------
    def lineage_components(
        self,
        obj_id: str,
        obj_type: int,
        recursive: bool = True,
        type_filter: Optional[list[int]] = None,
    ) -> dict:
        """
        Step 1: POST /api/metadataSearches/results to CREATE a metadata search.
        Step 2: GET  /api/metadataSearches/results to RETRIEVE stored results.

        usedByObject = "{id};{type}" returns objects USED BY the given object
        (i.e., its components/dependencies). With usedByRecursive=true, the
        Intelligence Server walks the whole transitive closure server-side.

        domain=2 means project domain (use with X-MSTR-ProjectID).
        """
        params: list[tuple[str, Any]] = [
            ("domain", 2),
            ("usedByObject", f"{obj_id};{obj_type}"),
            ("usedByRecursive", "true" if recursive else "false"),
        ]
        if type_filter:
            for t in type_filter:
                params.append(("type", t))

        # Step 1: POST creates the search instance and may return results directly
        r_post = self._req("POST", "/metadataSearches/results", params=params)
        post_body: dict = {}
        try:
            post_body = r_post.json()
        except ValueError:
            post_body = {}

        if isinstance(post_body, dict) and "result" in post_body:
            return post_body

        # Step 2: GET retrieves stored results if POST didn't include them
        time.sleep(0.2)  # tiny breather; some IServer builds finalize async
        r_get = self._req("GET", "/metadataSearches/results")
        return r_get.json()


# ---------------------------------------------------------------------------
# Harvester
# ---------------------------------------------------------------------------
class Harvester:
    def __init__(self, client: MSTR, verbose: bool = True):
        self.c = client
        self.verbose = verbose
        self.objects: dict[str, ObjRec] = {}
        self.errors: list[str] = []
        self.root_id: Optional[str] = None
        self.root_type: Optional[int] = None

    def _log(self, msg: str) -> None:
        if self.verbose:
            print(f"  {msg}", flush=True)

    def harvest(self, root_name: str, root_type: int, pattern: int = 2) -> None:
        # 1. Find the root object by name
        hits = self.c.search(root_name, [root_type], pattern=pattern)
        if not hits:
            raise RuntimeError(f"Root not found: name={root_name!r} type={root_type}")
        if len(hits) > 1:
            self._log(f"WARNING: {len(hits)} matches for {root_name!r}; using first")
        root = hits[0]
        self.root_id = root["id"]
        self.root_type = int(root["type"])
        self._log(f"Root: {root['name']} (id={root['id']} type={root['type']})")

        # Record the root itself
        self._record_from_search(root)

        # 2. ONE recursive lineage call gets the entire dependency closure
        self._log("Calling recursive lineage (usedByRecursive=true)...")
        try:
            lineage = self.c.lineage_components(
                self.root_id, self.root_type,
                recursive=True,
                type_filter=DEFAULT_INCLUDE_TYPES or None,
            )
        except RuntimeError as e:
            raise RuntimeError(f"Lineage call failed: {e}") from e

        results = lineage.get("result", [])
        total = lineage.get("totalItems", len(results))
        self._log(f"Lineage returned {total} dependency objects")

        for obj in results:
            self._record_from_search(obj)

        # 3. Enrich each unique object with full metadata (version, ACL, path)
        self._log(f"Enriching {len(self.objects)} objects with full metadata...")
        for rec in list(self.objects.values()):
            try:
                meta = self.c.get_object(rec.id, rec.type)
            except RuntimeError as e:
                self.errors.append(f"meta {rec.id} type={rec.type}: {e}")
                continue
            rec.subtype = meta.get("subtype", rec.subtype)
            rec.date_created = meta.get("dateCreated", rec.date_created)
            rec.date_modified = meta.get("dateModified", rec.date_modified)
            rec.version = meta.get("version", rec.version)
            rec.acg = meta.get("acg", rec.acg)
            rec.description = meta.get("description", "") or rec.description
            owner = meta.get("owner") or {}
            if isinstance(owner, dict):
                rec.owner_name = owner.get("name", rec.owner_name)
                rec.owner_id = owner.get("id", rec.owner_id)

    def _record_from_search(self, obj: dict) -> Optional[ObjRec]:
        oid = obj.get("id")
        if not oid:
            return None
        if oid in self.objects:
            return self.objects[oid]
        rec = ObjRec(
            id=oid,
            type=int(obj.get("type", 0)),
            subtype=obj.get("subtype"),
            name=obj.get("name", ""),
            date_created=obj.get("dateCreated", ""),
            date_modified=obj.get("dateModified", ""),
            version=obj.get("version", ""),
            acg=obj.get("acg"),
            description=obj.get("description", ""),
        )
        owner = obj.get("owner") or {}
        if isinstance(owner, dict):
            rec.owner_name = owner.get("name", "")
            rec.owner_id = owner.get("id", "")
        self.objects[oid] = rec
        return rec


# ---------------------------------------------------------------------------
# Cross-environment diff
# ---------------------------------------------------------------------------
def diff_environments(dev_objects: dict[str, ObjRec], target: MSTR) -> dict:
    missing: list[ObjRec] = []
    version_mismatch: list[tuple[ObjRec, str, str]] = []
    matched: list[ObjRec] = []
    errors: list[str] = []

    for rec in dev_objects.values():
        try:
            tgt = target.get_object(rec.id, rec.type)
        except RuntimeError as e:
            err_str = str(e)
            if "404" in err_str or "-2147216959" in err_str:  # MSTR "not found"
                missing.append(rec)
            else:
                errors.append(f"{rec.id} type={rec.type}: {err_str[:200]}")
            continue
        tgt_version = tgt.get("version", "")
        if tgt_version and rec.version and tgt_version != rec.version:
            version_mismatch.append((rec, rec.version, tgt_version))
        else:
            matched.append(rec)

    return {
        "matched": matched,
        "missing": missing,
        "version_mismatch": version_mismatch,
        "errors": errors,
    }


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------
def write_manifest(harvester: Harvester, out_path: str) -> None:
    payload = {
        "harvest_timestamp": datetime.utcnow().isoformat() + "Z",
        "root_id": harvester.root_id,
        "root_type": harvester.root_type,
        "object_count": len(harvester.objects),
        "objects": [asdict(o) for o in harvester.objects.values()],
    }
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2, default=str)


def print_summary(objects: dict[str, ObjRec], diff: Optional[dict] = None) -> None:
    by_type: dict[int, int] = {}
    for o in objects.values():
        by_type[o.type] = by_type.get(o.type, 0) + 1

    print("\n" + "=" * 72)
    print(f"HARVEST SUMMARY  ({len(objects)} unique objects)")
    print("=" * 72)
    for t, n in sorted(by_type.items(), key=lambda x: -x[1]):
        print(f"  type {t:>3} ({TYPE_NAMES.get(t, '?'):<22}): {n}")

    if diff:
        print("\n" + "=" * 72)
        print("CROSS-ENV DIFF")
        print("=" * 72)
        print(f"  Matched               : {len(diff['matched'])}")
        print(f"  Missing on target     : {len(diff['missing'])}  <-- WILL FAIL MIGRATION")
        print(f"  Version mismatch      : {len(diff['version_mismatch'])}  <-- TARGET HAS DIFFERENT COPY")
        if diff.get("errors"):
            print(f"  Errors during compare : {len(diff['errors'])}")

        if diff["missing"]:
            print("\n  MISSING (must include in migration package):")
            for rec in diff["missing"][:50]:
                print(f"    {rec.id}  type={rec.type:<3}  {rec.name!r}")
            if len(diff["missing"]) > 50:
                print(f"    ... and {len(diff['missing']) - 50} more")
        if diff["version_mismatch"]:
            print("\n  VERSION MISMATCH (target copy is different):")
            for rec, dv, tv in diff["version_mismatch"][:20]:
                print(f"    {rec.id}  type={rec.type:<3}  {rec.name!r}")
                print(f"        dev: {dv}")
                print(f"        tgt: {tv}")


# ---------------------------------------------------------------------------
# Probe
# ---------------------------------------------------------------------------
def probe(client: MSTR, root_name: str, root_type: int, pattern: int) -> None:
    print("=" * 72)
    print("PROBE — verify endpoint shapes against your March 2026 server")
    print("=" * 72)

    print("\n[1] GET /api/searches/results — find root object")
    hits = client.search(root_name, [root_type], pattern=pattern)
    print(f"    Found {len(hits)} hit(s)")
    if not hits:
        print("ABORT: root not found, cannot probe further")
        return
    print(json.dumps(hits[0], indent=2, default=str)[:1500])
    root = hits[0]

    print("\n[2] GET /api/objects/{id}?type=N — fetch metadata")
    meta = client.get_object(root["id"], root["type"])
    keep_keys = ("id", "name", "type", "subtype", "dateCreated", "dateModified",
                 "version", "acg", "owner", "description")
    print(json.dumps({k: meta.get(k) for k in keep_keys}, indent=2, default=str))

    print("\n[3] POST /api/metadataSearches/results — recursive lineage")
    print(f"    usedByObject={root['id']};{root['type']}")
    print(f"    usedByRecursive=true")
    lineage = client.lineage_components(root["id"], int(root["type"]), recursive=True)
    results = lineage.get("result", [])
    total = lineage.get("totalItems", len(results))
    print(f"    totalItems = {total}")
    print(f"    result count = {len(results)}")
    if results:
        print("    first 3 entries:")
        print(json.dumps(results[:3], indent=2, default=str)[:2000])

    print("\n" + "=" * 72)
    print("If [3] returned a non-empty result with id/name/type/subtype,")
    print("the harvester is good to run. If shape differs, paste output back.")
    print("=" * 72)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    p = argparse.ArgumentParser(
        description="Strategy ONE recursive dependency harvester (March 2026 verified)"
    )
    p.add_argument("--root", required=True,
                   help="Root object name (dossier, report, document, etc.)")
    p.add_argument("--root-type", type=int, required=True,
                   help="Root object type (3=Report, 55=Dossier/Document, etc.)")
    p.add_argument("--project", required=True, help="Project name")
    p.add_argument("--pattern", type=int, default=2,
                   help="Search pattern: 1=BeginsWith 2=Exactly 3=EndsWith 4=Contains")
    p.add_argument("--probe", action="store_true",
                   help="Probe one of each endpoint and exit (no harvest)")
    p.add_argument("--compare-target", action="store_true",
                   help="After harvest, diff against MSTR_TGT_* environment")
    p.add_argument("--out", default="manifest.json", help="Manifest output path")
    args = p.parse_args()

    dev_url  = os.environ.get("MSTR_DEV_URL", "https://your-dev-server.com/MicroStrategyLibrarySTD")
    dev_user = os.environ.get("MSTR_DEV_USER", "")
    dev_pass = os.environ.get("MSTR_DEV_PASS", "")
    if not dev_user or not dev_pass:
        print("ERROR: set MSTR_DEV_USER and MSTR_DEV_PASS env vars", file=sys.stderr)
        return 2

    dev = MSTR(dev_url, dev_user, dev_pass, label="DEV")
    try:
        dev.login()
        dev.set_project(args.project)

        if args.probe:
            probe(dev, args.root, args.root_type, args.pattern)
            return 0

        h = Harvester(dev)
        h.harvest(args.root, args.root_type, pattern=args.pattern)
        write_manifest(h, args.out)

        diff = None
        if args.compare_target:
            tgt_url  = os.environ.get("MSTR_TGT_URL", "")
            tgt_user = os.environ.get("MSTR_TGT_USER", "")
            tgt_pass = os.environ.get("MSTR_TGT_PASS", "")
            if not (tgt_url and tgt_user and tgt_pass):
                print("ERROR: --compare-target needs MSTR_TGT_URL/USER/PASS", file=sys.stderr)
                return 2
            tgt = MSTR(tgt_url, tgt_user, tgt_pass, label="TGT")
            try:
                tgt.login()
                tgt.set_project(args.project)
                diff = diff_environments(h.objects, tgt)
            finally:
                tgt.logout()

        print_summary(h.objects, diff)
        if h.errors:
            print(f"\n{len(h.errors)} non-fatal errors during enrichment (first 5):")
            for e in h.errors[:5]:
                print(f"  ! {e}")
        print(f"\nManifest written: {args.out}")
        return 0
    finally:
        dev.logout()


if __name__ == "__main__":
    sys.exit(main())
