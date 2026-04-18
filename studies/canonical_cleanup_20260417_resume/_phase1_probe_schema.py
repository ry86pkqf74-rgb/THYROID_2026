"""Probe relevant schemas for Phase 1 hypopara adjudication.

Read-only. Lists columns of:
  - manuscript_workspace.cpm_hypopara_adjudication_queue_v1
  - main.complication_phenotype_v1
  - main.extracted_postop_labs_expanded_v1
  - main.canonical_patient_master (subset: hypopara + surgery date cols)
  - any medications/NLP tables that mention calcium / calcitriol / replacement

Also dumps the 4 queued rids' contents from the queue and CPM hypopara cols.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1] / "scripts"))

from _md_connect import connect_locked  # type: ignore  # noqa: E402

con = connect_locked()
out: dict = {}

CAT = "thyroid_canonical_publication_v1_0"

def cols_of(schema: str, table: str) -> list[tuple[str, str]]:
    return con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema=? AND table_name=? "
        "ORDER BY ordinal_position",
        [CAT, schema, table],
    ).fetchall()

for s, t in [
    ("manuscript_workspace", "cpm_hypopara_adjudication_queue_v1"),
    ("main", "complication_phenotype_v1"),
    ("main", "extracted_postop_labs_expanded_v1"),
]:
    out[f"{s}.{t}.columns"] = [list(r) for r in cols_of(s, t)]

# CPM hypopara-related cols + surgery-date cols
cpm_cols = [
    r[0]
    for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema='main' "
        "AND table_name='canonical_patient_master' "
        "AND ("
        " column_name ILIKE '%hypopara%' OR "
        " column_name ILIKE '%pth%' OR "
        " column_name ILIKE '%calcium%' OR "
        " column_name ILIKE '%calcitriol%' OR "
        " column_name ILIKE '%first_surg%' OR "
        " column_name ILIKE '%initial_surg%' OR "
        " column_name ILIKE '%surgery_date%' OR "
        " column_name ILIKE '%first_thyroid%' OR "
        " column_name ILIKE '%total_thyroidectomy%date%' "
        ")",
        [CAT],
    ).fetchall()
]
out["cpm.hypopara_pth_calcium_surg_cols"] = sorted(cpm_cols)

# Tables that look like NLP medications / replacement / calcitriol
med_tables = [
    r[0]
    for r in con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_catalog=? AND table_schema='main' "
        "AND ("
        " table_name ILIKE '%medication%' OR "
        " table_name ILIKE '%calcitriol%' OR "
        " table_name ILIKE '%calcium%' OR "
        " table_name ILIKE '%replacement%' OR "
        " table_name ILIKE '%postop_med%' OR "
        " table_name ILIKE '%nlp%med%' OR "
        " table_name ILIKE '%pth_treat%' OR "
        " table_name ILIKE '%hypopara%'"
        ") ORDER BY table_name",
        [CAT],
    ).fetchall()
]
out["candidate_medication_tables"] = med_tables

# Queue contents for the 4 rids
queue_rows = con.execute(
    'SELECT * FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1 '
    'ORDER BY research_id'
).fetchall()
queue_cols = [d[0] for d in con.description]
out["queue_rows"] = [dict(zip(queue_cols, r)) for r in queue_rows]

# CPM relevant fields for the 4 rids
rids = [6447, 7487, 9765, 10743]
sel = ["research_id"] + sorted(cpm_cols)
sel_quoted = ", ".join(f'"{c}"' for c in sel)
cpm_rows = con.execute(
    f"SELECT {sel_quoted} FROM main.canonical_patient_master "
    f"WHERE research_id IN ({','.join(map(str, rids))}) "
    f"ORDER BY research_id"
).fetchall()
cpm_keys = [d[0] for d in con.description]
out["cpm_rows"] = [dict(zip(cpm_keys, r)) for r in cpm_rows]

# Also surface counts of phenotype + lab rows per rid (no filtering yet)
out["phenotype_row_counts"] = {
    str(rid): con.execute(
        "SELECT COUNT(*) FROM main.complication_phenotype_v1 WHERE research_id=?",
        [rid],
    ).fetchone()[0]
    for rid in rids
}
out["postop_lab_row_counts"] = {
    str(rid): con.execute(
        "SELECT COUNT(*) FROM main.extracted_postop_labs_expanded_v1 WHERE research_id=?",
        [rid],
    ).fetchone()[0]
    for rid in rids
}

# Find a surgery-date column candidate (operative_episode_detail_v2) for joining
oed_cols = [
    r[0]
    for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_catalog=? AND table_schema='main' "
        "AND table_name='operative_episode_detail_v2'",
        [CAT],
    ).fetchall()
]
out["operative_episode_detail_v2.columns"] = sorted(oed_cols)

print(json.dumps(out, indent=2, default=str))
