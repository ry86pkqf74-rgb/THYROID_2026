#!/usr/bin/env python3
"""
Phases 1 + 2 + 3:
- Phase 1: Pairing map (legacy_col, v2_col, type_coercion_rule, semantic_pair?)
- Phase 2: Cell-level comparison per pair (n_both_populated, n_both_agree, n_both_disagree,
           n_legacy_only, n_v2_only, n_both_null) plus 10-row sample of disagreements
           written to manuscript_workspace.cpm_tirads_audit_sample_<col>_v1
- Phase 3: Classification table at manuscript_workspace.cpm_tirads_audit_classification_v1

Read-only against main.canonical_patient_master. Writes only to manuscript_workspace.*.
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
WR = json.loads((OUT / "_cpm_tirads_audit_writers_readers.json").read_text())
VR = json.loads((OUT / "_cpm_tirads_audit_view_readers.json").read_text())

con = MotherDuckClient(
    MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
).connect_rw()


col_types = {c["name"]: c["type"] for c in INV["audit_columns"] + INV["nlp_columns"]}
pop = {c["name"]: c["n_populated"] for c in INV["audit_columns"] + INV["nlp_columns"]}

# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: Pairing map
#
# Each entry: (legacy_col, v2_col_or_None, equality_sql_expr_template, note)
# equality_sql_expr_template is a SQL fragment producing a BOOLEAN where TRUE=agree;
# uses {L} and {V} placeholders for the legacy and v2 column names.
# If v2_col is None, this is an unpaired legacy column.
# If equality is None, semantic mismatch — skip agreement comparison.
# ─────────────────────────────────────────────────────────────────────────────

# Extract TR1..TR5 rank substring from a free-form string like "TR4_Moderately_Suspicious"
# Falls back to NULL if no match.
def _tr_rank(expr: str) -> str:
    return f"REGEXP_EXTRACT({expr}, 'TR[1-5]', 0)"


EQ_INT_VS_TR_VARCHAR = f"'TR' || CAST({{L}} AS VARCHAR) = {_tr_rank('{V}')}"
# Both VARCHARs but possibly with longer labels — compare the TR rank substring.
EQ_TR_VARCHAR_BOTH = f"{_tr_rank('{L}')} = {_tr_rank('{V}')}"
EQ_VARCHAR_VARCHAR = "TRIM(LOWER({L})) = TRIM(LOWER({V}))"
EQ_DOUBLE_DOUBLE = "ABS({L} - {V}) < 0.01"
EQ_MM_VS_CM = "ABS(({L}/10.0) - {V}) < 0.05"
EQ_BIGINT_VS_VARCHAR_TR = EQ_INT_VS_TR_VARCHAR
# L is the v271b VARCHAR (5-valued), V is legacy BOOLEAN
EQ_VARCHAR_CONCORD_VS_BOOL = (
    "((TRIM(LOWER({L})) = 'concordant'   AND {V} = TRUE) "
    "OR (TRIM(LOWER({L})) = 'discordant' AND {V} = FALSE))"
)

PAIRINGS: list[tuple[str, str | None, str | None, str]] = [
    # ── Obvious pairs (BIGINT 1-5 ↔ VARCHAR "TR1"-"TR5") ──
    ("imaging_tirads_best",  "imaging_tirads_best_v2",  EQ_INT_VS_TR_VARCHAR, "BIGINT(1-5) vs VARCHAR(TR1-TR5)"),
    ("imaging_tirads_worst", "imaging_tirads_worst_v2", EQ_INT_VS_TR_VARCHAR, "BIGINT(1-5) vs VARCHAR(TR1-TR5)"),
    ("preop_tirads_best",    "preop_tirads_best_v2",    EQ_INT_VS_TR_VARCHAR, "BIGINT(1-5) vs VARCHAR(TR1-TR5)"),
    ("preop_tirads_category","preop_tirads_category_v2",EQ_TR_VARCHAR_BOTH,    "VARCHAR vs VARCHAR (TR-rank category, label format may differ)"),
    ("imaging_updated_tirads_category_cpm_v1", "imaging_updated_tirads_category_cpm_v2", EQ_TR_VARCHAR_BOTH, "VARCHAR vs VARCHAR (TR-rank category)"),
    # ── No v2 counterpart for preop_tirads_worst ──
    ("preop_tirads_worst", None, None, "no v2 counterpart (only preop_tirads_best_v2 exists)"),

    # ── _v12 family (semantic categories, not points) ──
    ("tirads_best_category_v12",   "imaging_tirads_best_v2",  EQ_TR_VARCHAR_BOTH, "VARCHAR(TR4_Moderately_Suspicious style) vs VARCHAR(TR2 style) — compare TR rank substring"),
    ("tirads_worst_category_v12",  "imaging_tirads_worst_v2", EQ_TR_VARCHAR_BOTH, "VARCHAR(TR1-TR5) — category vs category, label format may differ"),
    ("tirads_best_score_v12",      "tirads_v2_worst_category", None, "v12.score = MIN tirads_acr_recalculated (category code 1-5); v2 has only worst_category not best — semantic mismatch"),
    ("tirads_worst_score_v12",     "tirads_v2_worst_category", "'TR' || CAST({L} AS VARCHAR) = " + _tr_rank('{V}'), "v12.worst_score = MAX category code 1-5 (BIGINT); tirads_v2_worst_category VARCHAR TR1-TR5 — extract rank"),
    ("tirads_n_nodule_records_v12","tirads_v2_n_nodules_scored", "{L} = {V}", "BIGINT vs BIGINT (count of scored nodule records)"),
    ("tirads_nodule_size_max_mm_v12","tirads_v2_largest_nodule_cm", EQ_MM_VS_CM, "mm vs cm — divide by 10"),
    ("tirads_concordant_count_v12",None, None, "no v2 — concept retired (per-nodule acr2017_vs_updated_concordant on canonical_us_nodule_v2)"),
    ("tirads_mismatch_count_v12",  None, None, "no v2 — concept retired"),
    ("tirads_has_acr_recalc_v12",  None, None, "no v2 — concept retired"),
    ("tirads_n_sources_v12",       None, None, "no v2 — concept retired"),
    ("tirads_reliability_v12",     None, None, "no v2 — concept retired (no reliability score in v2)"),
    ("tirads_source_v12",          "tirads_source_system_v271", EQ_VARCHAR_VARCHAR, "VARCHAR vs VARCHAR — source system label"),

    # ── _v271 family (POINTS, not category) ──
    ("tirads_worst_points_v271",   "tirads_v2_max_points", EQ_DOUBLE_DOUBLE, "DOUBLE points (ACR 0-13+) — both MAX per RID"),
    ("tirads_best_points_v271",    None, None, "no v2 best_points — would need MIN aggregate from canonical_us_nodule_v2"),
    ("tirads_source_system_v271",  None, None, "label-only column for v271 points; superseded if v2 path becomes canonical"),

    # ── _v271b laterality (NEWER than unsuffixed; per column comments) ──
    ("tumor_pathology_laterality_v271b", None, None, "newer name; no _v2 — keep (and consider rename to drop _v271b suffix once stabilized)"),
    ("imaging_laterality_rollup_v271b",  "imaging_laterality_rollup_v2", EQ_VARCHAR_VARCHAR, "both VARCHAR; check whether v2 reproduces v271b semantics"),
    ("pathology_vs_imaging_laterality_concordant_v271b", "pathology_vs_imaging_laterality_concordant", EQ_VARCHAR_CONCORD_VS_BOOL, "v271b is 5-valued VARCHAR (concordant/discordant/partially_concordant/unknown_path/insufficient_data); legacy is BOOLEAN — VALUE-LOSS in legacy"),

    # ── Legacy unsuffixed laterality (per comments, supersedeD by _v271b) ──
    ("imaging_laterality_rollup",  "imaging_laterality_rollup_v271b", EQ_VARCHAR_VARCHAR, "legacy 'mixed' is ambiguous; v271b is the analytic canonical"),
    ("pathology_vs_imaging_laterality_concordant", "pathology_vs_imaging_laterality_concordant_v271b", None, "legacy BOOLEAN; v271b is multi-valued VARCHAR — type mismatch, see _v271b row"),

    # ── "Combined" cohort (pre-v12 era) ──
    ("tirads_best_combined",        "imaging_tirads_best_v2",  EQ_INT_VS_TR_VARCHAR, "INTEGER 1-5 vs VARCHAR TR1-TR5"),
    ("tirads_worst_combined",       "imaging_tirads_worst_v2", EQ_INT_VS_TR_VARCHAR, "INTEGER 1-5 vs VARCHAR TR1-TR5"),
    ("tirads_nodules_scored_combined","tirads_v2_n_nodules_scored", "{L} = {V}", "BIGINT vs BIGINT count"),

    # ── Semantic-mismatch flagged in starter map ──
    ("max_tirads_ever",    "max_tirads_ever_v2", None,
     "BIGINT category code 1-5 vs DOUBLE points (0-13+). Different semantics — DO NOT compare. Both should be kept under clarifying names."),

    # ── Un-suffixed "current" rollups ──
    ("imaging_tirads_source", None, None, "VARCHAR source system label; possibly redundant with tirads_source_system_v271 — flagged"),
    ("worst_tirads_category", "tirads_v2_worst_category", EQ_TR_VARCHAR_BOTH, "VARCHAR vs VARCHAR — patient worst TR-rank rollup"),

    # ── tirads_v2_* columns that have no legacy twin (so their pair side is NULL) ──
    # These are listed as the "v2" side of pairings above; we don't separately audit them
    # but we'll include the 'v2 with no legacy twin' set in the orphan section of the report.
]

# Sanity: all legacy cols listed should exist in CPM
audit_names = {c["name"] for c in INV["audit_columns"]}
missing = [p[0] for p in PAIRINGS if p[0] not in audit_names]
if missing:
    print(f"WARNING: pairings reference columns not in audit set: {missing}")

# Compute the set of v2-side cols that appear in a pairing
v2_used = {p[1] for p in PAIRINGS if p[1] is not None}
v2_orphans = sorted(
    n for n in audit_names
    if (n.endswith("_v2") or n.startswith("tirads_v2_"))
    and n not in v2_used
)
print(f"v2 columns NOT paired with any legacy: {len(v2_orphans)}")
for v in v2_orphans:
    print(f"  - {v}")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: Cell-level comparisons
# ─────────────────────────────────────────────────────────────────────────────

results: list[dict] = []
sample_tables_created: list[str] = []

con.execute("CREATE SCHEMA IF NOT EXISTS manuscript_workspace")

for legacy, v2, eq_tmpl, note in PAIRINGS:
    legacy_q = f'"{legacy}"'
    n_legacy = pop.get(legacy, 0)
    if v2 is None:
        results.append({
            "column_name": legacy,
            "data_type": col_types.get(legacy, ""),
            "proposed_v2_counterpart": None,
            "v2_data_type": None,
            "n_populated_legacy": n_legacy,
            "n_populated_v2": None,
            "n_both_populated": None,
            "n_both_agree": None,
            "n_both_disagree": None,
            "n_legacy_only": None,
            "n_v2_only": None,
            "n_both_null": None,
            "note": note,
        })
        continue

    v2_q = f'"{v2}"'
    n_v2 = pop.get(v2, 0)

    if eq_tmpl is None:
        # Semantic mismatch — count populated only, skip equality
        row = con.execute(f"""
            SELECT
              COUNT(*) FILTER (WHERE {legacy_q} IS NOT NULL AND {v2_q} IS NOT NULL) AS n_both,
              COUNT(*) FILTER (WHERE {legacy_q} IS NOT NULL AND {v2_q} IS NULL)     AS n_legacy_only,
              COUNT(*) FILTER (WHERE {legacy_q} IS NULL AND {v2_q} IS NOT NULL)     AS n_v2_only,
              COUNT(*) FILTER (WHERE {legacy_q} IS NULL AND {v2_q} IS NULL)         AS n_both_null
            FROM main.canonical_patient_master
        """).fetchone()
        results.append({
            "column_name": legacy,
            "data_type": col_types.get(legacy, ""),
            "proposed_v2_counterpart": v2,
            "v2_data_type": col_types.get(v2, ""),
            "n_populated_legacy": n_legacy,
            "n_populated_v2": n_v2,
            "n_both_populated": row[0],
            "n_both_agree": None,
            "n_both_disagree": None,
            "n_legacy_only": row[1],
            "n_v2_only": row[2],
            "n_both_null": row[3],
            "note": "SEMANTIC MISMATCH — equality not computed: " + note,
        })
        continue

    eq_expr = eq_tmpl.format(L=legacy_q, V=v2_q)
    row = con.execute(f"""
        SELECT
          COUNT(*) FILTER (WHERE {legacy_q} IS NOT NULL AND {v2_q} IS NOT NULL) AS n_both,
          COUNT(*) FILTER (WHERE {legacy_q} IS NOT NULL AND {v2_q} IS NULL)     AS n_legacy_only,
          COUNT(*) FILTER (WHERE {legacy_q} IS NULL AND {v2_q} IS NOT NULL)     AS n_v2_only,
          COUNT(*) FILTER (WHERE {legacy_q} IS NULL AND {v2_q} IS NULL)         AS n_both_null,
          COUNT(*) FILTER (WHERE {legacy_q} IS NOT NULL AND {v2_q} IS NOT NULL AND ({eq_expr})) AS n_agree,
          COUNT(*) FILTER (WHERE {legacy_q} IS NOT NULL AND {v2_q} IS NOT NULL AND NOT ({eq_expr})) AS n_disagree
        FROM main.canonical_patient_master
    """).fetchone()

    n_both, n_lo, n_vo, n_bn, n_ag, n_dis = row
    results.append({
        "column_name": legacy,
        "data_type": col_types.get(legacy, ""),
        "proposed_v2_counterpart": v2,
        "v2_data_type": col_types.get(v2, ""),
        "n_populated_legacy": n_legacy,
        "n_populated_v2": n_v2,
        "n_both_populated": n_both,
        "n_both_agree": n_ag,
        "n_both_disagree": n_dis,
        "n_legacy_only": n_lo,
        "n_v2_only": n_vo,
        "n_both_null": n_bn,
        "note": note,
    })

    # Sample disagreements
    if n_dis and n_dis > 0:
        sample_table = f"manuscript_workspace.cpm_tirads_audit_sample_{legacy}_v1"
        # DuckDB doesn't allow dots inside quoted identifiers; need to split
        # We'll just CREATE OR REPLACE TABLE
        con.execute(f"""
            CREATE OR REPLACE TABLE {sample_table} AS
            SELECT research_id,
                   {legacy_q} AS legacy_val,
                   {v2_q}     AS v2_val
            FROM main.canonical_patient_master
            WHERE {legacy_q} IS NOT NULL
              AND {v2_q} IS NOT NULL
              AND NOT ({eq_expr})
            ORDER BY research_id
            LIMIT 10
        """)
        sample_tables_created.append(sample_table)


# ─────────────────────────────────────────────────────────────────────────────
# Recommendation heuristics
# ─────────────────────────────────────────────────────────────────────────────

def writers_str(col: str) -> str:
    # Combine repo writers + view-readers (view defs aren't writers)
    files = WR.get(col, {}).get("writer_files", [])
    return "; ".join(Path(f).name for f in files) or "(none)"


def readers_str(col: str) -> str:
    files = list(WR.get(col, {}).get("reader_files", []))
    files += [f"VIEW:{v}" for v in VR.get(col, [])]
    return "; ".join(
        (Path(f).name if not f.startswith("VIEW:") else f) for f in files
    ) or "(none)"


def classify(r: dict) -> tuple[str, str]:
    col = r["column_name"]
    n_both = r["n_both_populated"]
    n_dis = r["n_both_disagree"]
    n_ag = r["n_both_agree"]
    n_leg_only = r["n_legacy_only"]
    n_v2_only = r["n_v2_only"]
    n_pop_leg = r["n_populated_legacy"]
    n_pop_v2 = r["n_populated_v2"]
    note_in = r["note"]

    n_view_readers = len(VR.get(col, []))
    n_repo_readers = WR.get(col, {}).get("n_reader_files", 0)
    n_repo_writers = WR.get(col, {}).get("n_writer_files", 0)

    # Special: explicit semantic mismatch (we set agree/disagree to None)
    if note_in.startswith("SEMANTIC MISMATCH") or "DO NOT compare" in note_in:
        return ("PRESERVE_DIFFERENT_SEMANTIC",
                f"Pair has different semantics: {note_in.split(':', 1)[-1].strip()}. "
                f"Recommend renaming pair to clarifying names.")

    # Unpaired legacy
    if r["proposed_v2_counterpart"] is None:
        if n_pop_leg == 0:
            return ("DROP", "Orphaned legacy: 0 rows populated and no v2 counterpart.")
        if n_view_readers > 0:
            return ("INVESTIGATE",
                    f"No v2 counterpart but {n_view_readers} cohort view(s) read this column. "
                    f"Need to design a replacement or rename before drop. {note_in}")
        if n_repo_readers > 0:
            return ("INVESTIGATE",
                    f"No v2 counterpart and {n_repo_readers} script reader(s). {note_in}")
        return ("INVESTIGATE", f"No v2 counterpart. {note_in}")

    # Paired
    if n_pop_leg == 0 and n_pop_v2 > 0:
        return ("DROP", "Legacy is empty (0 rows populated) — v2 holds all data; safe drop.")
    if n_pop_leg > 0 and (n_pop_v2 or 0) == 0:
        return ("RENAME_TO_V2",
                "v2 was provisioned but never backfilled. Promote legacy data into v2 or rename legacy to v2.")
    if n_both is None or n_both == 0:
        return ("INVESTIGATE", "No overlap rows to compare; cannot assess agreement.")
    agree_rate = (n_ag or 0) / n_both
    if (n_dis or 0) == 0 and n_view_readers == 0:
        return ("DROP",
                f"100% agreement on {n_both} overlapping rows; no cohort views read legacy. "
                f"Drop legacy after rewriting {n_repo_readers} script reader(s).")
    if (n_dis or 0) == 0 and n_view_readers > 0:
        return ("INVESTIGATE",
                f"100% agreement on {n_both} rows BUT {n_view_readers} cohort view(s) reference legacy. "
                f"Drop only after view rewrite.")
    if agree_rate > 0.95:
        return ("INVESTIGATE",
                f"{agree_rate:.1%} agreement on {n_both} rows ({n_dis} disagreements). "
                f"Mostly equivalent; review samples to decide DROP vs PRESERVE.")
    if agree_rate < 0.5:
        return ("PRESERVE_DIFFERENT_SEMANTIC",
                f"Only {agree_rate:.1%} agreement — pair likely measures different things despite naming.")
    return ("INVESTIGATE",
            f"Middle-zone agreement {agree_rate:.1%} on {n_both} rows — partial backfill, stale data, or logic change suspected.")


# Augment results with writer/reader metadata + recommendation
for r in results:
    col = r["column_name"]
    r["writer_scripts"] = writers_str(col)
    r["reader_scripts"] = readers_str(col)
    rec, rationale = classify(r)
    r["recommendation"] = rec
    r["rationale"] = rationale


# ─────────────────────────────────────────────────────────────────────────────
# Phase 3: Persist classification table to manuscript_workspace
# ─────────────────────────────────────────────────────────────────────────────

con.execute("DROP TABLE IF EXISTS manuscript_workspace.cpm_tirads_audit_classification_v1")
con.execute("""
    CREATE TABLE manuscript_workspace.cpm_tirads_audit_classification_v1 (
      column_name VARCHAR,
      data_type VARCHAR,
      proposed_v2_counterpart VARCHAR,
      v2_data_type VARCHAR,
      n_populated_legacy BIGINT,
      n_populated_v2 BIGINT,
      n_both_populated BIGINT,
      n_both_agree BIGINT,
      n_both_disagree BIGINT,
      n_legacy_only BIGINT,
      n_v2_only BIGINT,
      n_both_null BIGINT,
      writer_scripts VARCHAR,
      reader_scripts VARCHAR,
      recommendation VARCHAR,
      rationale VARCHAR,
      note VARCHAR
    )
""")

cols_order = [
    "column_name", "data_type", "proposed_v2_counterpart", "v2_data_type",
    "n_populated_legacy", "n_populated_v2", "n_both_populated",
    "n_both_agree", "n_both_disagree", "n_legacy_only", "n_v2_only", "n_both_null",
    "writer_scripts", "reader_scripts", "recommendation", "rationale", "note",
]

for r in results:
    con.execute(
        f"INSERT INTO manuscript_workspace.cpm_tirads_audit_classification_v1 ({','.join(cols_order)}) "
        f"VALUES ({','.join(['?'] * len(cols_order))})",
        [r.get(c) for c in cols_order],
    )

# Persist to JSON for the agent's report
(OUT / "_cpm_tirads_audit_classification.json").write_text(
    json.dumps({"pairings": results, "v2_orphans": v2_orphans, "samples": sample_tables_created}, indent=2)
)

# Verify counts
n_classified = con.execute(
    "SELECT COUNT(*) FROM manuscript_workspace.cpm_tirads_audit_classification_v1"
).fetchone()[0]
n_samples = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_schema = 'manuscript_workspace' "
    "AND table_name LIKE 'cpm_tirads_audit_sample_%_v1'"
).fetchone()[0]
print(f"\nWritten: classification table with {n_classified} rows, {n_samples} sample tables.")
print(f"Sample tables: {sample_tables_created}")
