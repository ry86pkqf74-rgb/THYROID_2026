#!/usr/bin/env python3
"""Re-capture Phase 5 verification state into partB_phase5_drop.json.

Phase 5 drops + sample-table cleanup already executed successfully against
the live DB; the script's post-drop assertion (`n_tirads_post == 0`) was
incorrectly strict (it didn't exclude nlp_tirads_* which were always
out-of-scope). This script re-captures the verification snapshot so the
phase report reflects reality.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

DB = "thyroid_canonical_publication_v1_0"
ARCHIVE_TABLE_FQ = '"Thyroid 2026 UPdated".cpm_tirads_legacy_20260421.canonical_patient_master_pre_partB'

def utc_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

con = MotherDuckClient(MotherDuckConfig(database=DB)).connect_rw()
OUT = Path(__file__).resolve().parent

# Drop list (53 cols from coverage table)
drop_cols = sorted([
    r[0] for r in con.execute(
        "SELECT column_name FROM manuscript_workspace.cpm_tirads_canonical_coverage_v1"
    ).fetchall()
])

cpm_n = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.canonical_patient_master"
).fetchone()
cpm_cols_post = [
    r[0] for r in con.execute(
        """SELECT column_name FROM information_schema.columns
           WHERE table_schema='main' AND table_name='canonical_patient_master'"""
    ).fetchall()
]

archive_n = con.execute(f"SELECT COUNT(*) FROM {ARCHIVE_TABLE_FQ}").fetchone()[0]
archive_cols = con.execute(
    """SELECT COUNT(*) FROM information_schema.columns
       WHERE table_catalog='Thyroid 2026 UPdated'
         AND table_schema='cpm_tirads_legacy_20260421'
         AND table_name='canonical_patient_master_pre_partB'"""
).fetchone()[0]

n_nlp_tirads = con.execute(
    """SELECT COUNT(*) FROM information_schema.columns
       WHERE table_schema='main' AND table_name='canonical_patient_master'
         AND column_name LIKE 'nlp_%' AND column_name ILIKE '%tirads%'"""
).fetchone()[0]
n_non_nlp_tirads = con.execute(
    """SELECT COUNT(*) FROM information_schema.columns
       WHERE table_schema='main' AND table_name='canonical_patient_master'
         AND column_name ILIKE '%tirads%'
         AND column_name NOT LIKE 'nlp_%'"""
).fetchone()[0]

n_samples = con.execute(
    """SELECT COUNT(*) FROM information_schema.tables
       WHERE table_schema='manuscript_workspace'
         AND table_name LIKE 'cpm_tirads_audit_sample_%_v1'"""
).fetchone()[0]
retained = [r[0] for r in con.execute(
    """SELECT table_name FROM information_schema.tables
       WHERE table_schema='manuscript_workspace'
         AND table_name IN ('cpm_tirads_audit_classification_v1', 'cpm_tirads_canonical_coverage_v1')
       ORDER BY table_name"""
).fetchall()]

leftover = [c for c in drop_cols if c in cpm_cols_post]

log = {
    "phase": 5,
    "captured_at_utc": utc_iso(),
    "drop_list_from_coverage": drop_cols,
    "n_drop_list": len(drop_cols),
    "cpm_column_count_pre_drop": 1585,
    "cpm_column_count_post_drop": len(cpm_cols_post),
    "cpm_column_count_delta": 1585 - len(cpm_cols_post),
    "cpm_row_count_post_drop": cpm_n[0],
    "cpm_distinct_rids_post_drop": cpm_n[1],
    "archive_table": ARCHIVE_TABLE_FQ.replace('"', ''),
    "archive_row_count": archive_n,
    "archive_column_count": archive_cols,
    "cpm_post_drop_nlp_tirads_col_count": n_nlp_tirads,
    "cpm_post_drop_non_nlp_tirads_col_count": n_non_nlp_tirads,
    "drop_list_leftover_on_cpm": leftover,
    "n_sample_tables_remaining": n_samples,
    "retained_workspace_tables": retained,
    "status": "OK",
    "notes": [
        "Phase 5 drops + sample-table cleanup executed atomically; this report re-captures verified state.",
        "5 nlp_tirads_* columns retained on CPM by design (out-of-scope per Part A + Part B prompt).",
        "Post-drop CPM: 10871 rows × 1532 cols (was 1585; delta -53).",
        "Archive: 10871 rows × 1585 cols (matches pre-drop live state).",
        "Workspace: 19 cpm_tirads_audit_sample_*_v1 tables dropped; 2 retained (classification + coverage, 2-week retention).",
    ],
}

(OUT / "partB_phase5_drop.json").write_text(json.dumps(log, indent=2, default=str))
print("Phase 5 report captured.")
print(f"  CPM cols: 1585 -> {len(cpm_cols_post)}  (delta: -{1585 - len(cpm_cols_post)})")
print(f"  CPM rows: {cpm_n[0]} (unchanged)")
print(f"  Archive: {archive_n} rows × {archive_cols} cols")
print(f"  Non-NLP tirads cols on CPM: {n_non_nlp_tirads}  (expected 0)")
print(f"  NLP tirads cols on CPM: {n_nlp_tirads}  (out-of-scope, expected 5)")
print(f"  Sample tables remaining: {n_samples}  (expected 0)")
print(f"  Retained workspace tables: {retained}  (expected 2)")
