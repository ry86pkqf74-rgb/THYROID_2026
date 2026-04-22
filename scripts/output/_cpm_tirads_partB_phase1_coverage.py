#!/usr/bin/env python3
"""
Part B / Phase 1: Canonical coverage audit.

Reads Part A artifacts fresh (no cache):
  - manuscript_workspace.cpm_tirads_audit_classification_v1
  - scripts/output/_cpm_tirads_audit_inventory.json (for tirads_v2_* on CPM not in classification)

Builds manuscript_workspace.cpm_tirads_canonical_coverage_v1 mapping every CPM
TIRADS-related column (legacy + v2-suffixed) to a canonical column on
main.canonical_us_patient_master_VIEW_v2 (cupm_v2) when one exists. Falls back to
'gap_other_v2_table' when the mapping is on canonical_us_exam_master_VIEW_v2 or
canonical_us_nodule_v2 (per Option C the prompt names cupm_v2 only).

coverage_status ∈ {
    'mapped_cupm_v2'          # direct map on canonical_us_patient_master_VIEW_v2
    'mapped_unit_convert'     # cupm_v2 in different unit
    'mapped_5valued'          # type/value-set widened
    'mapped_category'         # category form on cupm_v2
    'mapped_points'           # points form on cupm_v2
    'retired_redesign'        # Q1 list — cohort_m025/m075 redesign
    'gap_other_v2_table'      # canonical lives on cuem_v2 / cunc_v2 (NOT cupm_v2)
    'gap_ABORT'               # no canonical equivalent at all
}

STOP gate: any 'gap_ABORT' or any 'gap_other_v2_table' (the latter is a soft gate
under Option C as written; the prompt explicitly limits canonical surface to
cupm_v2). Do not proceed to Phase 2 without Logan's adjudication.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

OUT = Path(__file__).resolve().parent
INV = json.loads((OUT / "_cpm_tirads_audit_inventory.json").read_text())

con = MotherDuckClient(
    MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
).connect_rw()

# ── 1) Pull fresh CPM TIRADS columns (audit + v2-suffixed analytic ones) ──
cpm_cols = {c["name"]: c["type"] for c in INV["audit_columns"]}
# nlp_* columns are explicitly out of scope (see Part A note)
# add tirads_v2_* analytic cols on CPM that weren't legacy-paired
extra_v2 = [
    c for c in INV["audit_columns"]
    if c["name"].startswith("tirads_v2_") or c["name"].endswith("_v2")
]
# (already covered by the audit_columns iteration above; this is defensive)

# ── 2) Pull fresh canonical inventories ──
def cols_for(table: str) -> dict[str, str]:
    rows = con.execute(f"""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_catalog='thyroid_canonical_publication_v1_0'
          AND table_schema='main' AND table_name='{table}'
        ORDER BY ordinal_position
    """).fetchall()
    return {r[0]: r[1] for r in rows}


cupm_v2 = cols_for("canonical_us_patient_master_VIEW_v2")
cuem_v2 = cols_for("canonical_us_exam_master_VIEW_v2")
cunc_v2 = cols_for("canonical_us_nodule_v2")  # canonical_us_nodule_v2

# ── 3) Hard mapping table built from Part A findings + column comments ──
# (legacy_col, canonical_table, canonical_col, status, notes)
# canonical_table='-' means retired or gap.
MAPPING: list[tuple[str, str, str, str, str]] = [
    # Patient-level rollups already on cupm_v2
    ("max_tirads_ever",                          "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "BIGINT 1-5 → VARCHAR TR1-TR5; verify rank concordance"),
    ("max_tirads_ever_v2",                       "canonical_us_patient_master_VIEW_v2", "max_tirads_points_ever",     "mapped_cupm_v2",      "DOUBLE points (0-13+); already exists on cupm_v2"),
    ("worst_tirads_category",                    "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Patient worst TR rank rollup"),
    ("imaging_tirads_best",                      "canonical_us_patient_master_VIEW_v2", "tirads_category_at_first_exam","mapped_cupm_v2",     "Per-exam first-exam category rollup"),
    ("imaging_tirads_worst",                     "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Patient worst TR rank rollup"),
    ("preop_tirads_best",                        "canonical_us_patient_master_VIEW_v2", "tirads_category_at_last_preop_exam", "mapped_cupm_v2","Last-preop-exam category"),
    ("preop_tirads_worst",                       "canonical_us_patient_master_VIEW_v2", "tirads_category_at_last_preop_exam", "mapped_cupm_v2","Best+worst flatten to last preop value on cupm_v2; verify"),
    ("preop_tirads_category",                    "canonical_us_patient_master_VIEW_v2", "tirads_category_at_last_preop_exam", "mapped_cupm_v2","Likely same as preop_best/worst on cupm_v2"),
    ("imaging_updated_tirads_category_cpm_v1",   "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Same patient-rollup; v1 is older, _v2 newer"),
    ("imaging_updated_tirads_category_cpm_v2",   "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Same patient-rollup"),
    ("imaging_tirads_best_v2",                   "canonical_us_patient_master_VIEW_v2", "tirads_category_at_first_exam","mapped_cupm_v2",     "Same as imaging_tirads_best — both legacy and v2 form drop"),
    ("imaging_tirads_worst_v2",                  "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Same as imaging_tirads_worst"),
    ("preop_tirads_best_v2",                     "canonical_us_patient_master_VIEW_v2", "tirads_category_at_last_preop_exam","mapped_cupm_v2","Same as preop_tirads_best"),
    ("preop_tirads_category_v2",                 "canonical_us_patient_master_VIEW_v2", "tirads_category_at_last_preop_exam","mapped_cupm_v2","Same as preop_tirads_category"),

    # 'Combined' (pre-v12 era) — same patient-rollups
    ("tirads_best_combined",                     "canonical_us_patient_master_VIEW_v2", "tirads_category_at_first_exam","mapped_cupm_v2",     "Pre-v12 INTEGER form; map to first-exam category"),
    ("tirads_worst_combined",                    "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Pre-v12 INTEGER worst-ever"),

    # _v12 family — partial coverage
    ("tirads_best_category_v12",                 "canonical_us_patient_master_VIEW_v2", "tirads_category_at_first_exam","mapped_cupm_v2",     "VARCHAR labels collapse to TR rank"),
    ("tirads_worst_category_v12",                "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "VARCHAR labels collapse to TR rank"),
    ("tirads_best_score_v12",                    "canonical_us_patient_master_VIEW_v2", "tirads_category_at_first_exam","mapped_category",    "BIGINT category 1-5 → VARCHAR TR rank"),
    ("tirads_worst_score_v12",                   "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_category",    "BIGINT category 1-5 → VARCHAR TR rank"),
    ("tirads_n_nodule_records_v12",              "-",                              "-",                          "gap_other_v2_table", "Available as agg on canonical_us_nodule_v2 (COUNT(*) PER RID); not on cupm_v2 directly"),
    ("tirads_nodules_scored_combined",           "-",                              "-",                          "gap_other_v2_table", "Same as above; cupm_v2 has n_nodules_total_across_exams (HUGEINT) — verify semantic match"),
    ("tirads_nodule_size_max_mm_v12",            "-",                              "-",                          "gap_other_v2_table", "Per-nodule size lives on canonical_us_nodule_v2; no patient-level rollup on cupm_v2 (Part A: 2% agreement vs tirads_v2_largest_nodule_cm — different pipelines)"),
    ("tirads_concordant_count_v12",              "-",                              "-",                          "retired_redesign",   "Q1 decision: redesign cohort_m025/m075; no port"),
    ("tirads_mismatch_count_v12",                "-",                              "-",                          "retired_redesign",   "Q1 decision: redesign cohort_m025/m075; no port"),
    ("tirads_n_sources_v12",                     "-",                              "-",                          "retired_redesign",   "Q1 decision: redesign cohort_m025"),
    ("tirads_reliability_v12",                   "-",                              "-",                          "retired_redesign",   "Q1 decision: redesign cohort_m075"),
    ("tirads_has_acr_recalc_v12",                "-",                              "-",                          "retired_redesign",   "Concept retired in v2 pipeline (acr2017_vs_updated_concordant lives per-nodule)"),
    ("tirads_source_v12",                        "-",                              "-",                          "retired_redesign",   "Pipeline-source label (excel_complete_structured) — no canonical equivalent; metadata-only"),

    # _v271 family — points
    ("tirads_worst_points_v271",                 "canonical_us_patient_master_VIEW_v2", "max_tirads_points_ever",     "mapped_points",      "DOUBLE points; cupm_v2 has max-only — Part A: 27.7% agreement, different pipelines"),
    ("tirads_best_points_v271",                  "-",                              "-",                          "gap_other_v2_table", "MIN points; needs aggregation from canonical_us_nodule_v2.acr2017_tirads_points (no cupm_v2 column)"),
    ("tirads_source_system_v271",                "-",                              "-",                          "retired_redesign",   "Pipeline-source label (cunc_v1_points_acr2017) — metadata-only"),

    # Source label (un-suffixed) — also metadata
    ("imaging_tirads_source",                    "-",                              "-",                          "retired_redesign",   "Pipeline-source label — metadata-only; no canonical equivalent"),

    # Laterality columns — NO canonical equivalent on cupm_v2
    ("imaging_laterality_rollup",                "-",                              "-",                          "gap_ABORT",          "5+ valued patient laterality; cupm_v2 has only bilateral_disease_flag_ever (BOOL) — value-loss"),
    ("imaging_laterality_rollup_v271b",          "-",                              "-",                          "gap_ABORT",          "Same as above; cupm_v2 has no laterality VARCHAR rollup"),
    ("imaging_laterality_rollup_v2",             "-",                              "-",                          "gap_ABORT",          "Same as above"),
    ("tumor_pathology_laterality_v271b",         "-",                              "-",                          "gap_ABORT",          "Patient-level pathology laterality rollup; no canonical equivalent on cupm_v2"),
    ("pathology_vs_imaging_laterality_concordant","-",                             "-",                          "gap_ABORT",          "Concordance metric; depends on both laterality cols above; no canonical equivalent"),
    ("pathology_vs_imaging_laterality_concordant_v271b","-",                       "-",                          "gap_ABORT",          "Same as above; 5-valued VARCHAR"),

    # tirads_v2_* analytic columns currently on CPM — NOT on cupm_v2
    ("tirads_v2_n_nodules_scored",               "-",                              "-",                          "gap_other_v2_table", "Aggregable from canonical_us_nodule_v2 (COUNT WHERE tirads scored)"),
    ("tirads_v2_n_reports",                      "-",                              "-",                          "gap_ABORT",          "Per-patient TIRADS-report count; no equivalent on cupm_v2 nor cunc_v2 — would need new aggregation"),
    ("tirads_v2_worst_category",                 "canonical_us_patient_master_VIEW_v2", "max_tirads_category_ever",   "mapped_cupm_v2",      "Same column"),
    ("tirads_v2_worst_rank",                     "-",                              "-",                          "gap_ABORT",          "Numeric rank of worst TIRADS; cupm_v2 has VARCHAR only"),
    ("tirads_v2_worst_rank_source",              "-",                              "-",                          "gap_ABORT",          "Source label for worst-rank derivation; no equivalent"),
    ("tirads_v2_max_points",                     "canonical_us_patient_master_VIEW_v2", "max_tirads_points_ever",     "mapped_cupm_v2",      "Same column"),
    ("tirads_v2_largest_nodule_cm",              "-",                              "-",                          "gap_other_v2_table", "Largest nodule size; aggregable from canonical_us_nodule_v2 (no cupm_v2 column)"),
    ("tirads_v2_any_ete_on_us",                  "-",                              "-",                          "gap_ABORT",          "Patient-level ETE-on-US flag; no equivalent on cupm_v2 nor cunc_v2"),
    ("tirads_v2_any_interval_growth",            "-",                              "-",                          "gap_other_v2_table", "Aggregable from canonical_us_nodule_v2.interval_growth_flag (BOOL OR per RID); not on cupm_v2"),
    ("tirads_v2_any_fna_recommended",            "-",                              "-",                          "gap_other_v2_table", "Aggregable from canonical_us_nodule_v2.fna_recommended_this_nodule"),
    ("tirads_v2_any_fna_recommended_report",     "-",                              "-",                          "gap_ABORT",          "Report-level FNA recommendation rollup; no canonical equivalent (different denominator from per-nodule)"),
    ("tirads_v2_any_fna_recommended_report_source","-",                           "-",                          "gap_ABORT",          "Source label for the report-level rollup; no equivalent"),
    ("tirads_v2_any_suspicious_ln_on_us",        "canonical_us_patient_master_VIEW_v2", "any_suspicious_us_ln_ever", "mapped_cupm_v2",      "Direct match"),
    ("tirads_v2_shortest_followup_months",       "-",                              "-",                          "gap_ABORT",          "Patient followup-window metric; no canonical equivalent"),
]

# ── 4) Cross-check: every legacy/v2 col on CPM must appear in MAPPING ──
mapped_set = {m[0] for m in MAPPING}
inventory_cols = set(cpm_cols.keys())  # 53 cols from Part A inventory
missing_in_mapping = inventory_cols - mapped_set
extra_in_mapping = mapped_set - inventory_cols

# ── 5) Verify mapped canonical columns actually exist ──
errors: list[str] = []
for legacy, ctab, ccol, status, notes in MAPPING:
    if status.startswith("mapped"):
        if ctab == "canonical_us_patient_master_VIEW_v2" and ccol not in cupm_v2:
            errors.append(f"BAD MAP: {legacy} → {ctab}.{ccol} — column NOT FOUND on cupm_v2")
        elif ctab == "canonical_us_exam_master_VIEW_v2" and ccol not in cuem_v2:
            errors.append(f"BAD MAP: {legacy} → {ctab}.{ccol} — column NOT FOUND on cuem_v2")
        elif ctab == "canonical_us_nodule_v2" and ccol not in cunc_v2:
            errors.append(f"BAD MAP: {legacy} → {ctab}.{ccol} — column NOT FOUND on cunc_v2")

# ── 6) Build the coverage table ──
con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")
con.execute("DROP TABLE IF EXISTS manuscript_workspace.cpm_tirads_canonical_coverage_v1")
con.execute("""
    CREATE TABLE manuscript_workspace.cpm_tirads_canonical_coverage_v1 (
        column_name VARCHAR,
        cpm_dtype   VARCHAR,
        canonical_table VARCHAR,
        canonical_column VARCHAR,
        canonical_dtype  VARCHAR,
        coverage_status  VARCHAR,
        notes VARCHAR
    )
""")

rows: list[tuple] = []
for legacy, ctab, ccol, status, notes in MAPPING:
    cpm_dt = cpm_cols.get(legacy, "<NOT_ON_CPM>")
    if ctab == "canonical_us_patient_master_VIEW_v2":
        canon_dt = cupm_v2.get(ccol, "—")
    elif ctab == "canonical_us_exam_master_VIEW_v2":
        canon_dt = cuem_v2.get(ccol, "—")
    elif ctab == "canonical_us_nodule_v2":
        canon_dt = cunc_v2.get(ccol, "—")
    else:
        canon_dt = "—"
    rows.append((legacy, cpm_dt, ctab, ccol, canon_dt, status, notes))

for r in rows:
    con.execute(
        "INSERT INTO manuscript_workspace.cpm_tirads_canonical_coverage_v1 VALUES (?,?,?,?,?,?,?)",
        list(r),
    )

# ── 7) Summary ──
import collections
status_counts = collections.Counter(s for _, _, _, _, s in [(r[0], r[1], r[2], r[3], r[5]) for r in rows])
print(f"Coverage rows: {len(rows)}")
print(f"  CPM inventory cols: {len(inventory_cols)}")
print(f"  cols in MAPPING but NOT on CPM: {sorted(extra_in_mapping)}")
print(f"  cols on CPM but NOT in MAPPING: {sorted(missing_in_mapping)}")
print()
print("Status counts:")
for s, n in sorted(status_counts.items(), key=lambda kv: -kv[1]):
    print(f"  {s:24s} {n}")
print()
if errors:
    print("MAPPING ERRORS:")
    for e in errors:
        print(f"  - {e}")
else:
    print("All mapped canonical columns verified to exist.")
