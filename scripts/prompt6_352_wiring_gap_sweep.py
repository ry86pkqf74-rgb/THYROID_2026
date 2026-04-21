"""
Script 352 — Sweep cpm_missing_data_provenance_v1 for remaining
backfill_feasible=TRUE wiring gaps not addressed by Scripts 349-351.

Target rows from provenance table (4 candidates):

  nucmed_tgab_max
    Source: thyroglobulin_lab_canonical_v1 (analyte='TgAb', result_numeric)
    Action: MAX(result_numeric) per research_id WHERE analyte='TgAb'
    CPM before: 2602 nonnull; source ceiling: 2994 rids; max delta ~392

  biochemical_concern_first_date
    Source: tg_postop_surveillance_windows_v1 (analyte='Tg', value_max > 2.0)
    Action: MIN(window_first_date) per research_id
    CPM before: 1372 nonnull; source ceiling: 1659 rids; max delta ~287

  comp_vc_paralysis_evidence_tier
    Source: complication_phenotype_v1 (complication_entity='vocal_cord_paralysis')
    Source state: 88 rows, only 34 already tiered.  No way to NEW-tier the
                  remaining 54 without re-running the upstream tiering logic.
    Action: backfill only the 34 already-tiered rows that aren't already in CPM
            (will likely be 0 — source is already aligned with CPM).
    Defer rest to Prompt 7.

  comp_vc_paresis_evidence_tier
    Source: same — 71 rows, 22 tiered.
    Same approach as paralysis.

Output:
  - Per-column rows in manuscript_workspace.prompt6_wiring_gap_remediation_v1
  - Defer rows for cols where source ceiling already met or no fillable data
"""

from datetime import datetime, timezone
from scripts._md_connect import connect_locked

con = connect_locked()
DB = '"thyroid_canonical_publication_v1_0"'
SCRIPT_NUM = 352
SCRIPT_TAG = "352_wiring_gap_sweep"


def header(s):
    print()
    print("=" * 78)
    print(s)
    print("=" * 78)


def cpm_nonnull(col: str) -> int:
    return con.execute(
        f'SELECT COUNT("{col}") FROM {DB}.main.canonical_patient_master'
    ).fetchone()[0]


def cpm_has_col(col: str) -> bool:
    return con.execute("""
        SELECT COUNT(*) FROM duckdb_columns()
         WHERE database_name='thyroid_canonical_publication_v1_0'
           AND schema_name='main' AND table_name='canonical_patient_master'
           AND column_name=?
    """, [col]).fetchone()[0] > 0


def log_remediation(col, src, rows_staged, delta, notes):
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1
        VALUES (?, ?, ?, ?, ?, ?, NOW())
    """, [col, src, rows_staged, delta, delta, notes])


def log_defer(col, reason, where_to):
    con.execute(f"""
        INSERT INTO {DB}.manuscript_workspace.prompt6_defer_log_v1
        VALUES (?, ?, ?, ?, NOW())
    """, [SCRIPT_NUM, col, reason, where_to])


total_delta = 0


# 1. nucmed_tgab_max
header("1. nucmed_tgab_max — MAX(TgAb result_numeric)")
src_rids = con.execute(f"""
    SELECT COUNT(DISTINCT research_id) FROM {DB}.main.thyroglobulin_lab_canonical_v1
     WHERE analyte='TgAb' AND result_numeric IS NOT NULL
""").fetchone()[0]
before = cpm_nonnull("nucmed_tgab_max")
con.execute(
    f'DROP TABLE IF EXISTS {DB}.manuscript_workspace.nucmed_tgab_max_backfill_v1'
)
con.execute(f"""
    CREATE TABLE {DB}.manuscript_workspace.nucmed_tgab_max_backfill_v1 AS
    SELECT research_id,
           MAX(result_numeric) AS max_tgab
      FROM {DB}.main.thyroglobulin_lab_canonical_v1
     WHERE analyte = 'TgAb' AND result_numeric IS NOT NULL
     GROUP BY research_id
""")
if not cpm_has_col("nucmed_tgab_max_source"):
    con.execute(
        f'ALTER TABLE {DB}.main.canonical_patient_master '
        f'ADD COLUMN "nucmed_tgab_max_source" VARCHAR'
    )
con.execute(f"""
    UPDATE {DB}.main.canonical_patient_master AS c
       SET nucmed_tgab_max        = b.max_tgab,
           nucmed_tgab_max_source = 'thyroglobulin_lab_canonical_v1_TgAb_max_352'
      FROM {DB}.manuscript_workspace.nucmed_tgab_max_backfill_v1 AS b
     WHERE c.research_id = b.research_id
       AND c.nucmed_tgab_max IS NULL
""")
after = cpm_nonnull("nucmed_tgab_max")
delta = after - before
total_delta += delta
print(f"  before={before}, after={after}, delta={delta} (source ceiling {src_rids})")
log_remediation("nucmed_tgab_max",
                "thyroglobulin_lab_canonical_v1 (analyte='TgAb')",
                src_rids, delta,
                "MAX(result_numeric) per rid; only filled NULL slots")


# 2. biochemical_concern_first_date
header("2. biochemical_concern_first_date — MIN(window_first_date) where Tg value_max > 2.0")
src_rids2 = con.execute(f"""
    SELECT COUNT(DISTINCT research_id) FROM {DB}.main.tg_postop_surveillance_windows_v1
     WHERE analyte='Tg' AND value_max > 2.0
""").fetchone()[0]
before2 = cpm_nonnull("biochemical_concern_first_date")
con.execute(
    f'DROP TABLE IF EXISTS {DB}.manuscript_workspace.biochemical_concern_backfill_v1'
)
con.execute(f"""
    CREATE TABLE {DB}.manuscript_workspace.biochemical_concern_backfill_v1 AS
    SELECT research_id,
           CAST(MIN(window_first_date) AS DATE) AS first_concern_date
      FROM {DB}.main.tg_postop_surveillance_windows_v1
     WHERE analyte = 'Tg' AND value_max > 2.0
     GROUP BY research_id
""")
if not cpm_has_col("biochemical_concern_first_date_source"):
    con.execute(
        f'ALTER TABLE {DB}.main.canonical_patient_master '
        f'ADD COLUMN "biochemical_concern_first_date_source" VARCHAR'
    )

# Determine actual data type of the existing column
col_dt = con.execute("""
    SELECT data_type FROM duckdb_columns()
     WHERE database_name='thyroid_canonical_publication_v1_0'
       AND schema_name='main' AND table_name='canonical_patient_master'
       AND column_name='biochemical_concern_first_date'
""").fetchone()[0]
cast_target = "DATE" if col_dt.upper() == "DATE" else "TIMESTAMP"
print(f"  CPM target type: {col_dt} -> casting backfill to {cast_target}")

con.execute(f"""
    UPDATE {DB}.main.canonical_patient_master AS c
       SET biochemical_concern_first_date        = CAST(b.first_concern_date AS {cast_target}),
           biochemical_concern_first_date_source = 'tg_postop_surveillance_windows_v1_value_max_gt_2.0_352'
      FROM {DB}.manuscript_workspace.biochemical_concern_backfill_v1 AS b
     WHERE c.research_id = b.research_id
       AND c.biochemical_concern_first_date IS NULL
""")
after2 = cpm_nonnull("biochemical_concern_first_date")
delta2 = after2 - before2
total_delta += delta2
print(f"  before={before2}, after={after2}, delta={delta2} (source ceiling {src_rids2})")
log_remediation("biochemical_concern_first_date",
                "tg_postop_surveillance_windows_v1 (analyte='Tg', value_max > 2.0)",
                src_rids2, delta2,
                "MIN(window_first_date); only filled NULL slots; threshold 2.0 ng/mL")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.pi_review_queue_v1
    VALUES (?, ?, ?, ?, ?, NOW())
""", [SCRIPT_NUM, "biochemical_concern_threshold",
      "Tg value_max > 2.0 ng/mL (per provenance recommendation)",
      "alternative thresholds 1.0 or 5.0 per ATA risk strata",
      "ATA biochemical incomplete response threshold varies; 2.0 chosen per provenance note"])


# 3. comp_vc_paralysis_evidence_tier
header("3. comp_vc_paralysis_evidence_tier — re-pull tiered rows")
before3 = cpm_nonnull("comp_vc_paralysis_evidence_tier")
con.execute(f"""
    UPDATE {DB}.main.canonical_patient_master AS c
       SET comp_vc_paralysis_evidence_tier = p.evidence_tier
      FROM (
        SELECT research_id,
               MAX(evidence_tier) AS evidence_tier
          FROM {DB}.main.complication_phenotype_v1
         WHERE complication_entity = 'vocal_cord_paralysis'
           AND evidence_tier IS NOT NULL
         GROUP BY research_id
      ) p
     WHERE c.research_id = p.research_id
       AND c.comp_vc_paralysis_evidence_tier IS NULL
""")
after3 = cpm_nonnull("comp_vc_paralysis_evidence_tier")
delta3 = after3 - before3
total_delta += delta3
print(f"  before={before3}, after={after3}, delta={delta3}")
log_remediation("comp_vc_paralysis_evidence_tier",
                "complication_phenotype_v1 (vocal_cord_paralysis)",
                88, delta3,
                "MAX(evidence_tier) per rid; source has 88 rows but only 34 tiered")
log_defer("comp_vc_paralysis_evidence_tier__untiered",
          "54 of 88 vocal_cord_paralysis rows have evidence_tier IS NULL "
          "in upstream complication_phenotype_v1; cannot derive without "
          "re-running upstream tiering logic for vocal_cord_* entities",
          "Prompt 7 (re-run complication tiering)")


# 4. comp_vc_paresis_evidence_tier
header("4. comp_vc_paresis_evidence_tier — re-pull tiered rows")
before4 = cpm_nonnull("comp_vc_paresis_evidence_tier")
con.execute(f"""
    UPDATE {DB}.main.canonical_patient_master AS c
       SET comp_vc_paresis_evidence_tier = p.evidence_tier
      FROM (
        SELECT research_id,
               MAX(evidence_tier) AS evidence_tier
          FROM {DB}.main.complication_phenotype_v1
         WHERE complication_entity = 'vocal_cord_paresis'
           AND evidence_tier IS NOT NULL
         GROUP BY research_id
      ) p
     WHERE c.research_id = p.research_id
       AND c.comp_vc_paresis_evidence_tier IS NULL
""")
after4 = cpm_nonnull("comp_vc_paresis_evidence_tier")
delta4 = after4 - before4
total_delta += delta4
print(f"  before={before4}, after={after4}, delta={delta4}")
log_remediation("comp_vc_paresis_evidence_tier",
                "complication_phenotype_v1 (vocal_cord_paresis)",
                71, delta4,
                "MAX(evidence_tier) per rid; source has 71 rows but only 22 tiered")
log_defer("comp_vc_paresis_evidence_tier__untiered",
          "49 of 71 vocal_cord_paresis rows have evidence_tier IS NULL "
          "upstream; same as paralysis",
          "Prompt 7 (re-run complication tiering)")


# 5. Defer rows for backfill_feasible=FALSE provenance entries
header("5. Defer log for backfill_feasible=FALSE provenance entries")
notyet = con.execute(f"""
    SELECT cpm_column, classification, recommended_action
      FROM {DB}.manuscript_workspace.cpm_missing_data_provenance_v1
     WHERE backfill_feasible = FALSE
""").fetchall()
for col, cls, act in notyet:
    log_defer(col, f"provenance: classification={cls}, recommended_action={act}",
              "Prompt 7 (LLM extraction or schema decision)")
    print(f"  defer-logged {col} ({cls})")


# 6. Final
header("6. Final summary")
print(f"  TOTAL CPM cell delta this script: {total_delta}")
con.execute(f"""
    INSERT INTO {DB}.manuscript_workspace.prompt6_poststate_v1
    VALUES (?, 'main', '__cpm_wiring_gap_sweep__', ?, NULL, NOW())
""", [SCRIPT_NUM, total_delta])

# No hard floor — this is a sweep script; success means: all backfill_feasible
# wiring_gap rows were either filled or have a defer entry.
remaining_unaddressed = con.execute(f"""
    SELECT cpm_column FROM {DB}.manuscript_workspace.cpm_missing_data_provenance_v1 p
     WHERE p.backfill_feasible = TRUE
       AND p.classification = 'wiring_gap'
       AND NOT EXISTS (
         SELECT 1 FROM {DB}.manuscript_workspace.prompt6_wiring_gap_remediation_v1 r
          WHERE r.cpm_column LIKE p.cpm_column || '%' OR r.cpm_column = p.cpm_column
       )
       AND NOT EXISTS (
         SELECT 1 FROM {DB}.manuscript_workspace.prompt6_defer_log_v1 d
          WHERE d.table_name LIKE p.cpm_column || '%' OR d.table_name = p.cpm_column
       )
""").fetchall()
print(f"  unaddressed wiring_gap+feasible rows: {len(remaining_unaddressed)}")
for (c,) in remaining_unaddressed:
    print(f"    UNADDRESSED: {c}")
if remaining_unaddressed:
    raise SystemExit(
        f"{len(remaining_unaddressed)} feasible wiring-gap rows unaddressed"
    )

print()
print(f"DONE. 352 swept wiring gaps; total delta {total_delta} cells.")
