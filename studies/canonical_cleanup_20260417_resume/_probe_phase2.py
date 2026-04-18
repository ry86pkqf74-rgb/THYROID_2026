"""Probe schemas for Phase 2: provenance + lab orphan tables + 5 cancer-evidence tables."""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))
from _md_connect import connect_locked  # type: ignore

con = connect_locked()
out: dict = {}
CAT = "thyroid_canonical_publication_v1_0"


def cols(s, t):
    return [
        list(r)
        for r in con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_catalog=? AND table_schema=? AND table_name=? "
            "ORDER BY ordinal_position",
            [CAT, s, t],
        ).fetchall()
    ]


for s, t in [
    ("manuscript_workspace", "cpm_reconciliation_provenance_v1"),
    ("manuscript_workspace", "lab_orphan_audit_v1"),
    ("manuscript_workspace", "lab_orphan_cohort_review_v1"),
    ("main", "thyroglobulin_lab_canonical_v1"),
    ("main", "longitudinal_lab_canonical_v1"),
    ("main", "fna_episode_master_v2"),
    ("main", "tumor_episode_master_v2"),
    ("main", "synoptic_tumor_long_v1"),
    ("main", "path_synoptics"),
    ("main", "imaging_nodule_master_v1"),
]:
    out[f"{s}.{t}.columns"] = cols(s, t)

prov_rows = con.execute(
    "SELECT * FROM manuscript_workspace.cpm_reconciliation_provenance_v1"
).fetchall()
prov_keys = [d[0] for d in con.description]
out["provenance_existing_rows"] = [dict(zip(prov_keys, r)) for r in prov_rows]

# Sample first 5 rows of orphan audit to understand structure
oa = con.execute(
    "SELECT * FROM manuscript_workspace.lab_orphan_audit_v1 LIMIT 5"
).fetchall()
ok = [d[0] for d in con.description]
out["lab_orphan_audit_sample"] = [dict(zip(ok, r)) for r in oa]

ocr = con.execute(
    "SELECT * FROM manuscript_workspace.lab_orphan_cohort_review_v1 LIMIT 5"
).fetchall()
ock = [d[0] for d in con.description]
out["lab_orphan_cohort_review_sample"] = [dict(zip(ock, r)) for r in ocr]

# CPM cpm_built_at max
out["cpm_built_at_minmax"] = con.execute(
    "SELECT MIN(cpm_built_at), MAX(cpm_built_at) FROM main.canonical_patient_master"
).fetchone()

print(json.dumps(out, indent=2, default=str))
