"""
Script 221b: Final Gap Resolution — NSQIP Thyroidectomy Enrichment + Parathyroid Intent Integration
Database: thyroid_ete_fix_20260413 on MotherDuck

Idempotent: safe to re-run. Skips column adds that are already present.
Fixes: nsqip_thyroidectomy_has_data was set TRUE for all 10,871 patients (bug from prior run);
       corrected to TRUE only for the 1,261 matched NSQIP patients.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from motherduck_client import get_token

import duckdb

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ── Connection ────────────────────────────────────────────────────────────────
print("=" * 70)
print("Script 221b: Final Gap Resolution")
print("=" * 70)
token = get_token()
con = duckdb.connect(f"md:thyroid_ete_fix_20260413?motherduck_token={token}")
print("✓ Connected to thyroid_ete_fix_20260413")

PHI_COLS = {"nsqip_dob", "nsqip_death_date", "nsqip_case_number", "research_id"}

# ── Helper ────────────────────────────────────────────────────────────────────
def canonical_columns():
    return {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'canonical_patient_master_v1' AND table_schema = 'main'"
        ).fetchall()
    }

def check_invariants(label=""):
    inv = con.execute("""
        SELECT COUNT(*) as total_rows,
               COUNT(DISTINCT research_id) as distinct_rids,
               COUNT(*) FILTER (WHERE research_id IS NULL) as null_rids,
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) as null_fna
        FROM canonical_patient_master_v1
    """).fetchone()
    print(f"\n{'[INVARIANTS' + (' ' + label if label else '') + ']':}")
    print(f"  total_rows={inv[0]}, distinct_rids={inv[1]}, null_rids={inv[2]}, null_fna={inv[3]}")
    assert inv == (10871, 10871, 0, 0), f"FAIL: invariants broken: {inv}"
    print("  ✓ All invariants PASS")
    return inv

# ═══════════════════════════════════════════════════════════════════════════════
# TASK 1: NSQIP THYROIDECTOMY ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TASK 1: NSQIP THYROIDECTOMY ENRICHMENT")
print("=" * 70)

# ── Step 1.1: Load NSQIP enrichment parquet ───────────────────────────────────
nsqip_path = REPO / "exports" / "nsqip" / "nsqip_patient_summary.parquet"
try:
    df_nsqip = pd.read_parquet(nsqip_path)
    print(f"Loaded NSQIP parquet: {len(df_nsqip)} rows × {len(df_nsqip.columns)} columns")
except Exception as e:
    print(f"Parquet load failed ({e}), falling back to CSV")
    df_nsqip = pd.read_csv(REPO / "exports" / "nsqip" / "nsqip_patient_summary.csv")

assert len(df_nsqip) == df_nsqip["research_id"].nunique(), "FAIL: duplicate research_ids in NSQIP parquet"
assert df_nsqip["research_id"].notna().all(), "FAIL: null research_ids in NSQIP parquet"

# Drop PHI
df_nsqip = df_nsqip.drop(columns=[c for c in PHI_COLS if c in df_nsqip.columns and c != "research_id"])
print(f"After PHI removal: {len(df_nsqip.columns)} columns (excl research_id)")

# Cast research_id to string for canonical compatibility
df_nsqip["research_id"] = df_nsqip["research_id"].astype(str)

# ── Step 1.2: Upload or verify staging table ──────────────────────────────────
staging_exists = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_name = '_nsqip_thyroidectomy_enrichment_v1' AND table_schema = 'main'"
).fetchone()[0]

if staging_exists:
    stg = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _nsqip_thyroidectomy_enrichment_v1"
    ).fetchone()
    print(f"Staging table already exists: {stg[0]} rows, {stg[1]} patients")
    if stg != (1261, 1261):
        print("  ⚠ Unexpected staging count — re-uploading")
        staging_exists = False

if not staging_exists:
    tmp_path = OUTPUT_DIR / "_nsqip_enrichment_staging_221b.parquet"
    df_nsqip.to_parquet(str(tmp_path), index=False)
    con.execute(f"""
        CREATE OR REPLACE TABLE _nsqip_thyroidectomy_enrichment_v1 AS
        SELECT * FROM read_parquet('{tmp_path}')
    """)
    try:
        tmp_path.unlink()
    except Exception:
        pass
    stg = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _nsqip_thyroidectomy_enrichment_v1"
    ).fetchone()
    print(f"✓ Uploaded _nsqip_thyroidectomy_enrichment_v1: {stg[0]} rows, {stg[1]} patients")

assert stg == (1261, 1261), f"FAIL: expected (1261, 1261), got {stg}"

# ── Step 1.3: Validate research_ids against canonical spine ───────────────────
print("\n[Step 1.3] Validate NSQIP research_ids against canonical spine")
orphans = con.execute("""
    SELECT COUNT(*) FROM _nsqip_thyroidectomy_enrichment_v1 n
    WHERE n.research_id NOT IN (SELECT research_id FROM canonical_patient_master_v1)
""").fetchone()[0]
print(f"  Orphan NSQIP research_ids (not in canonical): {orphans}")
assert orphans == 0, f"FAIL: {orphans} NSQIP patients not in canonical spine"
print("  ✓ All NSQIP research_ids validated against canonical")

# ── Step 1.4: Cross-validate overlapping columns ──────────────────────────────
print("\n[Step 1.4] Cross-validate overlapping columns")
overlap_results = con.execute("""
    SELECT
        'asa_class' AS field,
        COUNT(*) as total_overlap,
        COUNT(*) FILTER (WHERE c.nsqip_asa_class IS NOT NULL AND n.nsqip_asa_class IS NOT NULL) as both_non_null,
        COUNT(*) FILTER (WHERE c.nsqip_asa_class IS NOT NULL AND n.nsqip_asa_class IS NOT NULL
                         AND CAST(c.nsqip_asa_class AS VARCHAR) != CAST(n.nsqip_asa_class AS VARCHAR)) as discordant
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
    WHERE c.nsqip_asa_class IS NOT NULL
    UNION ALL
    SELECT
        'bmi',
        COUNT(*),
        COUNT(*) FILTER (WHERE c.nsqip_bmi IS NOT NULL AND n.nsqip_bmi IS NOT NULL),
        COUNT(*) FILTER (WHERE c.nsqip_bmi IS NOT NULL AND n.nsqip_bmi IS NOT NULL
                         AND ABS(TRY_CAST(c.nsqip_bmi AS DOUBLE) - TRY_CAST(n.nsqip_bmi AS DOUBLE)) > 0.5)
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
    WHERE c.nsqip_bmi IS NOT NULL
    UNION ALL
    SELECT
        'diabetes',
        COUNT(*),
        COUNT(*) FILTER (WHERE c.nsqip_diabetes IS NOT NULL AND n.nsqip_diabetes IS NOT NULL),
        COUNT(*) FILTER (WHERE c.nsqip_diabetes IS NOT NULL AND n.nsqip_diabetes IS NOT NULL
                         AND CAST(c.nsqip_diabetes AS VARCHAR) != CAST(n.nsqip_diabetes AS VARCHAR))
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
    WHERE c.nsqip_diabetes IS NOT NULL
    UNION ALL
    SELECT
        'hypertension',
        COUNT(*),
        COUNT(*) FILTER (WHERE c.nsqip_hypertension IS NOT NULL AND n.nsqip_hypertension IS NOT NULL),
        COUNT(*) FILTER (WHERE c.nsqip_hypertension IS NOT NULL AND n.nsqip_hypertension IS NOT NULL
                         AND CAST(c.nsqip_hypertension AS VARCHAR) != CAST(n.nsqip_hypertension AS VARCHAR))
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
    WHERE c.nsqip_hypertension IS NOT NULL
    UNION ALL
    SELECT
        'functional_status',
        COUNT(*),
        COUNT(*) FILTER (WHERE c.nsqip_functional_status IS NOT NULL AND n.nsqip_functional_status IS NOT NULL),
        COUNT(*) FILTER (WHERE c.nsqip_functional_status IS NOT NULL AND n.nsqip_functional_status IS NOT NULL
                         AND CAST(c.nsqip_functional_status AS VARCHAR) != CAST(n.nsqip_functional_status AS VARCHAR))
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
    WHERE c.nsqip_functional_status IS NOT NULL
""").fetchdf()

print(f"  {'Field':<22} {'Overlap':>8} {'Both':>8} {'Discord':>8} {'Rate%':>7}")
print("  " + "-" * 57)
for _, row in overlap_results.iterrows():
    rate = (row["discordant"] / row["both_non_null"] * 100) if row["both_non_null"] > 0 else 0.0
    flag = "⚠" if rate > 5 else "✓"
    print(f"  {flag} {row['field']:<20} {row['total_overlap']:>8} {row['both_non_null']:>8} {row['discordant']:>8} {rate:>6.1f}%")

# ── Step 1.5: Cross-validate staging vs canonical T/N/staging ─────────────────
print("\n[Step 1.5] Cross-validate NSQIP staging vs canonical pathology staging")
staging_xval = con.execute("""
    SELECT
        COUNT(*) FILTER (WHERE n.nsqip_t_classification IS NOT NULL
                         AND c.ajcc8_t_stage IS NOT NULL) AS both_have_t,
        COUNT(*) FILTER (WHERE n.nsqip_t_classification IS NOT NULL
                         AND c.ajcc8_t_stage IS NOT NULL
                         AND UPPER(CAST(n.nsqip_t_classification AS VARCHAR))
                             != UPPER(CAST(c.ajcc8_t_stage AS VARCHAR))) AS t_discordant,
        COUNT(*) FILTER (WHERE n.nsqip_n_classification IS NOT NULL
                         AND c.ajcc8_n_stage IS NOT NULL) AS both_have_n,
        COUNT(*) FILTER (WHERE n.nsqip_n_classification IS NOT NULL
                         AND c.ajcc8_n_stage IS NOT NULL
                         AND UPPER(CAST(n.nsqip_n_classification AS VARCHAR))
                             != UPPER(CAST(c.ajcc8_n_stage AS VARCHAR))) AS n_discordant,
        COUNT(*) FILTER (WHERE n.nsqip_nodes_removed IS NOT NULL
                         AND c.ln_total_examined IS NOT NULL) AS both_have_ln,
        COUNT(*) FILTER (WHERE n.nsqip_nodes_removed IS NOT NULL
                         AND c.ln_total_examined IS NOT NULL
                         AND ABS(TRY_CAST(n.nsqip_nodes_removed AS INT)
                                 - TRY_CAST(c.ln_total_examined AS INT)) > 2) AS ln_discordant
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
""").fetchone()
print(f"  T-stage  : both={staging_xval[0]}, discordant={staging_xval[1]}"
      + (f" ({staging_xval[1]/staging_xval[0]*100:.1f}%)" if staging_xval[0] else ""))
print(f"  N-stage  : both={staging_xval[2]}, discordant={staging_xval[3]}"
      + (f" ({staging_xval[3]/staging_xval[2]*100:.1f}%)" if staging_xval[2] else ""))
print(f"  LN count : both={staging_xval[4]}, discordant={staging_xval[5]}"
      + (f" ({staging_xval[5]/staging_xval[4]*100:.1f}%)" if staging_xval[4] else ""))
print("  Note: discordance expected (different coding systems / timing)")

# ── Step 1.6: Cross-validate hypocalcemia ────────────────────────────────────
print("\n[Step 1.6] Cross-validate NSQIP hypocalcemia vs canonical complications")
hypo_xval = con.execute("""
    SELECT
        COUNT(*) FILTER (WHERE n.nsqip_hypocalcemia_flag = 1) AS nsqip_hypo,
        COUNT(*) FILTER (WHERE c.comp_hypoparathyroidism_confirmed IS NOT NULL
                         OR c.comp_hypocalcemia_confirmed IS NOT NULL) AS canon_hypo,
        COUNT(*) FILTER (WHERE n.nsqip_hypocalcemia_flag = 1
                         AND (c.comp_hypoparathyroidism_confirmed IS NOT NULL
                              OR c.comp_hypocalcemia_confirmed IS NOT NULL)) AS both,
        COUNT(*) FILTER (WHERE n.nsqip_hypocalcemia_flag = 1
                         AND c.comp_hypoparathyroidism_confirmed IS NULL
                         AND c.comp_hypocalcemia_confirmed IS NULL) AS nsqip_only,
        COUNT(*) FILTER (WHERE (n.nsqip_hypocalcemia_flag IS NULL OR n.nsqip_hypocalcemia_flag = 0)
                         AND (c.comp_hypoparathyroidism_confirmed IS NOT NULL
                              OR c.comp_hypocalcemia_confirmed IS NOT NULL)) AS canon_only
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
""").fetchone()
print(f"  NSQIP hypocalcemia (30-day): {hypo_xval[0]}")
print(f"  Canonical hypocalcemia (any-time): {hypo_xval[1]}")
print(f"  Both positive: {hypo_xval[2]} | NSQIP-only: {hypo_xval[3]} | Canonical-only: {hypo_xval[4]}")
print("  Note: NSQIP=30-day window vs canonical=any-time; expected divergence")

# ── Step 1.7: Cross-validate RLN injury ─────────────────────────────────────
print("\n[Step 1.7] Cross-validate NSQIP RLN injury vs canonical")
rln_xval = con.execute("""
    SELECT
        COUNT(*) FILTER (WHERE n.nsqip_rln_injury_flag = 1) AS nsqip_rln,
        COUNT(*) FILTER (WHERE c.comp_rln_injury_confirmed IS NOT NULL) AS canon_rln,
        COUNT(*) FILTER (WHERE n.nsqip_rln_injury_flag = 1
                         AND c.comp_rln_injury_confirmed IS NOT NULL) AS both,
        COUNT(*) FILTER (WHERE n.nsqip_rln_injury_flag = 1
                         AND c.comp_rln_injury_confirmed IS NULL) AS nsqip_only,
        COUNT(*) FILTER (WHERE (n.nsqip_rln_injury_flag IS NULL OR n.nsqip_rln_injury_flag = 0)
                         AND c.comp_rln_injury_confirmed IS NOT NULL) AS canon_only
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
""").fetchone()
print(f"  NSQIP RLN injury (30-day): {rln_xval[0]}")
print(f"  Canonical RLN confirmed (any-time): {rln_xval[1]}")
print(f"  Both: {rln_xval[2]} | NSQIP-only: {rln_xval[3]} | Canonical-only: {rln_xval[4]}")

# ── Step 1.8: Cross-validate CND/LND vs canonical surgical ───────────────────
print("\n[Step 1.8] Cross-validate NSQIP surgical detail vs canonical")
surg_xval = con.execute("""
    SELECT
        COUNT(*) FILTER (WHERE UPPER(CAST(n.nsqip_central_neck_dissection AS VARCHAR)) = 'YES') AS nsqip_cnd,
        COUNT(*) FILTER (WHERE TRY_CAST(c.tp_ln_central_positive AS INT) > 0
                         OR c.ln_lateral_dissected = TRUE) AS canon_cnd,
        COUNT(*) FILTER (WHERE UPPER(CAST(n.nsqip_lateral_neck_dissection AS VARCHAR)) = 'YES') AS nsqip_lnd,
        COUNT(*) FILTER (WHERE c.ln_lateral_dissected = TRUE) AS canon_lnd,
        COUNT(*) FILTER (WHERE UPPER(CAST(n.nsqip_drain_usage AS VARCHAR)) = 'YES') AS nsqip_drain,
        COUNT(*) FILTER (WHERE LOWER(CAST(c.op_drain_placed_any AS VARCHAR)) = 'true') AS canon_drain,
        COUNT(*) FILTER (WHERE UPPER(CAST(n.nsqip_rln_monitoring AS VARCHAR)) = 'YES') AS nsqip_rln_mon,
        COUNT(*) FILTER (WHERE LOWER(CAST(c.op_rln_monitoring_any AS VARCHAR)) = 'true') AS canon_rln_mon
    FROM canonical_patient_master_v1 c
    JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
""").fetchone()
print(f"  CND   — NSQIP: {surg_xval[0]:>4}  Canonical: {surg_xval[1]:>4}")
print(f"  LND   — NSQIP: {surg_xval[2]:>4}  Canonical: {surg_xval[3]:>4}")
print(f"  Drain — NSQIP: {surg_xval[4]:>4}  Canonical: {surg_xval[5]:>4}")
print(f"  RLN-mon NSQIP: {surg_xval[6]:>4}  Canonical: {surg_xval[7]:>4}")

# ── Step 1.9: Determine new columns to add ───────────────────────────────────
print("\n[Step 1.9] Determine new NSQIP columns to add")
existing = canonical_columns()
nsqip_enrichment_cols = {
    r[0]
    for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = '_nsqip_thyroidectomy_enrichment_v1' AND table_schema = 'main'"
    ).fetchall()
}
new_nsqip_cols = sorted(nsqip_enrichment_cols - existing - PHI_COLS - {"nsqip_thyroidectomy_has_data",
                                                                         "nsqip_thyroidectomy_source_script"})
print(f"  New NSQIP columns to add: {len(new_nsqip_cols)}")
if new_nsqip_cols:
    for c in new_nsqip_cols:
        print(f"    {c}")

# ── Step 1.10: Fix nsqip_thyroidectomy_has_data BUG + integrate any new cols ──
print("\n[Step 1.10] Backup canonical + integrate NSQIP (fix has_data bug)")

# Check current has_data distribution
has_data_dist = dict(con.execute("""
    SELECT CAST(nsqip_thyroidectomy_has_data AS VARCHAR), COUNT(*)
    FROM canonical_patient_master_v1
    GROUP BY 1
""").fetchall())
print(f"  Current nsqip_thyroidectomy_has_data distribution: {has_data_dist}")

# Backup
backup_count = con.execute(
    "SELECT COUNT(*) FROM information_schema.tables "
    "WHERE table_name = 'canonical_patient_master_v1_pre221b' AND table_schema = 'main'"
).fetchone()[0]
if backup_count == 0:
    con.execute("""
        CREATE OR REPLACE TABLE canonical_patient_master_v1_pre221b AS
        SELECT * FROM canonical_patient_master_v1
    """)
    bc = con.execute("SELECT COUNT(*) FROM canonical_patient_master_v1_pre221b").fetchone()[0]
    assert bc == 10871, f"FAIL: backup has {bc} rows"
    print(f"  ✓ Backup created: canonical_patient_master_v1_pre221b ({bc} rows)")
else:
    bc = con.execute("SELECT COUNT(*) FROM canonical_patient_master_v1_pre221b").fetchone()[0]
    print(f"  Backup already exists ({bc} rows) — skipping re-create")

need_rebuild = new_nsqip_cols or has_data_dist.get("true", 0) == 10871 or has_data_dist.get("True", 0) == 10871

if need_rebuild:
    print("  Rebuilding canonical to fix has_data bug and add any new columns...")
    new_col_select = ""
    if new_nsqip_cols:
        new_col_select = ",\n    " + ",\n    ".join([f'n."{col}"' for col in new_nsqip_cols])

    rebuild_sql = f"""
CREATE OR REPLACE TABLE canonical_patient_master_v1 AS
SELECT
    c.* EXCLUDE (nsqip_thyroidectomy_has_data, nsqip_thyroidectomy_source_script){new_col_select},
    CASE WHEN n.research_id IS NOT NULL THEN TRUE ELSE FALSE END AS nsqip_thyroidectomy_has_data,
    '221b_final_gap_resolution' AS nsqip_thyroidectomy_source_script
FROM canonical_patient_master_v1 c
LEFT JOIN _nsqip_thyroidectomy_enrichment_v1 n ON c.research_id = n.research_id
"""
    con.execute(rebuild_sql)
    check_invariants("post-NSQIP")

    new_dist = dict(con.execute("""
        SELECT CAST(nsqip_thyroidectomy_has_data AS VARCHAR), COUNT(*)
        FROM canonical_patient_master_v1
        GROUP BY 1
    """).fetchall())
    print(f"  Updated nsqip_thyroidectomy_has_data distribution: {new_dist}")
    expected_true = 1261
    actual_true = new_dist.get("true", new_dist.get("True", 0))
    assert actual_true == expected_true, \
        f"FAIL: expected {expected_true} TRUE has_data, got {actual_true}"
    print(f"  ✓ nsqip_thyroidectomy_has_data fixed: {actual_true} TRUE (matched patients only)")
else:
    print("  nsqip_thyroidectomy_has_data already correct — no rebuild needed")
    check_invariants("post-NSQIP-check")

# ═══════════════════════════════════════════════════════════════════════════════
# TASK 2: PARATHYROID NOTES INTENT INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TASK 2: PARATHYROID NOTES INTENT INTEGRATION")
print("=" * 70)

# ── Step 2.1: Verify source and rollup tables ─────────────────────────────────
print("\n[Step 2.1] Verify parathyroid source and rollup tables")
pt_count = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM parathyroid_notes_intent_v1"
).fetchone()
print(f"  parathyroid_notes_intent_v1: {pt_count[0]} rows, {pt_count[1]} unique patients")

rollup_count = con.execute(
    "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _parathyroid_patient_rollup_v1"
).fetchone()
print(f"  _parathyroid_patient_rollup_v1: {rollup_count[0]} rows, {rollup_count[1]} unique patients")

# Check for orphan source patients (not in canonical spine)
orphan_para = con.execute("""
    SELECT COUNT(DISTINCT CAST(research_id AS VARCHAR)) FROM parathyroid_notes_intent_v1
    WHERE CAST(research_id AS VARCHAR) NOT IN (SELECT research_id FROM canonical_patient_master_v1)
""").fetchone()[0]
print(f"  Source patients NOT in canonical spine (expected gap): {orphan_para}")

# ── Step 2.2: Rebuild rollup if needed (includes all canonical-matched patients) ─
print("\n[Step 2.2] Verify/rebuild parathyroid patient rollup")

# Check if rollup captures all canonical-matched source patients
canonical_matched_para = con.execute("""
    SELECT COUNT(DISTINCT CAST(p.research_id AS VARCHAR))
    FROM parathyroid_notes_intent_v1 p
    WHERE CAST(p.research_id AS VARCHAR) IN (SELECT research_id FROM canonical_patient_master_v1)
""").fetchone()[0]
print(f"  Source patients matched to canonical spine: {canonical_matched_para}")
print(f"  Current rollup patients: {rollup_count[1]}")

# Identify the column that maps to research_id in the source
# We know from inspection it's 'Research ID number'
source_rid_col = '"Research ID number"'

if rollup_count[1] < canonical_matched_para:
    print(f"  ⚠ Rollup missing {canonical_matched_para - rollup_count[1]} patients — rebuilding")
    con.execute(f"""
        CREATE OR REPLACE TABLE _parathyroid_patient_rollup_v1 AS
        WITH src AS (
            SELECT
                CAST({source_rid_col} AS VARCHAR) AS research_id,
                "Parathyroid Gland &/oR tissue included in resected specimen?" AS parathyroid_included,
                incidental_status_refined,
                note_intent_inferred,
                removal_intent,
                pathologic_glands,
                parathyroid_abnormality,
                g1_location, g1_biopsy, g1_excision, g1_cellularity, g1_weight, g1_size,
                g2_location, g2_biopsy, g2_excision, g2_cellularity, g2_weight, g2_size,
                g3_location, g3_biopsy, g3_excision, g3_cellularity, g3_weight, g3_size,
                g4_location, g4_biopsy, g4_excision, g4_cellularity, g4_weight, g4_size,
                g5_location, g5_biopsy, g5_excision, g5_cellularity, g5_weight, g5_size,
                g6_location, g6_biopsy, g6_excision, g6_cellularity, g6_weight, g6_size,
                ROW_NUMBER() OVER (
                    PARTITION BY CAST({source_rid_col} AS VARCHAR)
                    ORDER BY
                        (CASE WHEN g1_location IS NOT NULL THEN 1 ELSE 0 END
                           + CASE WHEN g2_location IS NOT NULL THEN 1 ELSE 0 END
                           + CASE WHEN g3_location IS NOT NULL THEN 1 ELSE 0 END
                           + CASE WHEN g4_location IS NOT NULL THEN 1 ELSE 0 END) DESC
                ) AS rn
            FROM parathyroid_notes_intent_v1
            WHERE CAST({source_rid_col} AS VARCHAR) IN (
                SELECT research_id FROM canonical_patient_master_v1
            )
        )
        SELECT
            research_id,
            CASE WHEN LOWER(CAST(parathyroid_included AS VARCHAR)) IN ('yes', 'x') THEN TRUE
                 WHEN LOWER(CAST(parathyroid_included AS VARCHAR)) = 'no' THEN FALSE
                 ELSE NULL END AS para_specimen_included,
            CAST(COALESCE(
                NULLIF(LOWER(CAST(note_intent_inferred AS VARCHAR)), ''),
                NULLIF(LOWER(CAST(removal_intent AS VARCHAR)), '')
            ) AS VARCHAR) AS para_removal_intent,
            CAST(incidental_status_refined AS VARCHAR) AS para_incidental_status_refined,
            CASE WHEN pathologic_glands IS NOT NULL
                      AND CAST(pathologic_glands AS VARCHAR) NOT IN ('', 'nan')
                 THEN TRUE ELSE FALSE END AS para_has_pathologic_glands,
            CAST(parathyroid_abnormality AS VARCHAR) AS para_abnormality_type,
            (CASE WHEN g1_location IS NOT NULL AND CAST(g1_location AS VARCHAR) NOT IN ('', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g2_location IS NOT NULL AND CAST(g2_location AS VARCHAR) NOT IN ('', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g3_location IS NOT NULL AND CAST(g3_location AS VARCHAR) NOT IN ('', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g4_location IS NOT NULL AND CAST(g4_location AS VARCHAR) NOT IN ('', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g5_location IS NOT NULL AND CAST(g5_location AS VARCHAR) NOT IN ('', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g6_location IS NOT NULL AND CAST(g6_location AS VARCHAR) NOT IN ('', 'nan') THEN 1 ELSE 0 END
            ) AS para_n_glands_identified,
            (CASE WHEN g1_biopsy IS NOT NULL AND LOWER(CAST(g1_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g2_biopsy IS NOT NULL AND LOWER(CAST(g2_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g3_biopsy IS NOT NULL AND LOWER(CAST(g3_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g4_biopsy IS NOT NULL AND LOWER(CAST(g4_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g5_biopsy IS NOT NULL AND LOWER(CAST(g5_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g6_biopsy IS NOT NULL AND LOWER(CAST(g6_biopsy AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
            ) AS para_n_glands_biopsied,
            (CASE WHEN g1_excision IS NOT NULL AND LOWER(CAST(g1_excision AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g2_excision IS NOT NULL AND LOWER(CAST(g2_excision AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g3_excision IS NOT NULL AND LOWER(CAST(g3_excision AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g4_excision IS NOT NULL AND LOWER(CAST(g4_excision AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g5_excision IS NOT NULL AND LOWER(CAST(g5_excision AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
           + CASE WHEN g6_excision IS NOT NULL AND LOWER(CAST(g6_excision AS VARCHAR)) NOT IN ('', 'no', 'nan') THEN 1 ELSE 0 END
            ) AS para_n_glands_excised,
            GREATEST(
                TRY_CAST(g1_cellularity AS DOUBLE),
                TRY_CAST(g2_cellularity AS DOUBLE),
                TRY_CAST(g3_cellularity AS DOUBLE),
                TRY_CAST(g4_cellularity AS DOUBLE),
                TRY_CAST(g5_cellularity AS DOUBLE),
                TRY_CAST(g6_cellularity AS DOUBLE)
            ) AS para_max_cellularity_pct,
            LEAST(
                TRY_CAST(NULLIF(CAST(g1_cellularity AS VARCHAR), '') AS DOUBLE),
                TRY_CAST(NULLIF(CAST(g2_cellularity AS VARCHAR), '') AS DOUBLE),
                TRY_CAST(NULLIF(CAST(g3_cellularity AS VARCHAR), '') AS DOUBLE),
                TRY_CAST(NULLIF(CAST(g4_cellularity AS VARCHAR), '') AS DOUBLE),
                TRY_CAST(NULLIF(CAST(g5_cellularity AS VARCHAR), '') AS DOUBLE),
                TRY_CAST(NULLIF(CAST(g6_cellularity AS VARCHAR), '') AS DOUBLE)
            ) AS para_min_cellularity_pct,
            GREATEST(
                TRY_CAST(g1_weight AS DOUBLE),
                TRY_CAST(g2_weight AS DOUBLE),
                TRY_CAST(g3_weight AS DOUBLE),
                TRY_CAST(g4_weight AS DOUBLE),
                TRY_CAST(g5_weight AS DOUBLE),
                TRY_CAST(g6_weight AS DOUBLE)
            ) AS para_max_gland_weight_g,
            'parathyroid_notes_intent.xlsx' AS para_source_workbook,
            '221b_final_gap_resolution' AS para_source_script
        FROM src
        WHERE rn = 1
    """)
    rollup_count = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _parathyroid_patient_rollup_v1"
    ).fetchone()
    print(f"  ✓ Rollup rebuilt: {rollup_count[0]} rows, {rollup_count[1]} unique patients")
else:
    print(f"  ✓ Rollup already complete ({rollup_count[1]} patients)")

# ── Step 2.3: Validate rollup ─────────────────────────────────────────────────
print("\n[Step 2.3] Validate parathyroid rollup")
para_orphans = con.execute("""
    SELECT COUNT(*) FROM _parathyroid_patient_rollup_v1 p
    WHERE p.research_id NOT IN (SELECT research_id FROM canonical_patient_master_v1)
""").fetchone()[0]
assert para_orphans == 0, f"FAIL: {para_orphans} parathyroid rollup patients not in canonical"
print("  ✓ No orphan patients in rollup")

para_dupes = con.execute("""
    SELECT COUNT(*) - COUNT(DISTINCT research_id) FROM _parathyroid_patient_rollup_v1
""").fetchone()[0]
assert para_dupes == 0, f"FAIL: {para_dupes} duplicate research_ids in rollup"
print("  ✓ No duplicates in rollup")

cov = con.execute("""
    SELECT
        COUNT(*) AS total_patients,
        COUNT(*) FILTER (WHERE para_specimen_included IS NOT NULL) AS has_specimen_flag,
        COUNT(*) FILTER (WHERE para_removal_intent IS NOT NULL
                         AND para_removal_intent NOT IN ('unsure', 'unknown')) AS has_clear_intent,
        COUNT(*) FILTER (WHERE para_removal_intent = 'intentional') AS intentional,
        COUNT(*) FILTER (WHERE para_removal_intent = 'incidental') AS incidental,
        COUNT(*) FILTER (WHERE para_removal_intent = 'mixed') AS mixed,
        COUNT(*) FILTER (WHERE para_has_pathologic_glands = TRUE) AS has_pathologic,
        COUNT(*) FILTER (WHERE para_n_glands_identified > 0) AS has_gland_data,
        COUNT(*) FILTER (WHERE para_max_cellularity_pct IS NOT NULL) AS has_cellularity,
        COUNT(*) FILTER (WHERE para_max_gland_weight_g IS NOT NULL) AS has_weight
    FROM _parathyroid_patient_rollup_v1
""").fetchone()
print(f"\n  Parathyroid rollup coverage ({cov[0]} patients):")
print(f"    specimen_flag: {cov[1]} | clear_intent: {cov[2]}")
print(f"    intentional: {cov[3]} | incidental: {cov[4]} | mixed: {cov[5]}")
print(f"    pathologic: {cov[6]} | has_gland_data: {cov[7]}")
print(f"    cellularity: {cov[8]} | weight: {cov[9]}")

# ── Step 2.4: Cross-validate parathyroid rollup vs canonical ──────────────────
print("\n[Step 2.4] Cross-validate parathyroid intent vs canonical complications")
para_xval = con.execute("""
    SELECT
        p.para_removal_intent,
        COUNT(*) AS n,
        COUNT(*) FILTER (WHERE LOWER(CAST(c.op_nlp_parathyroid_managed AS VARCHAR)) = 'true') AS nlp_managed,
        COUNT(*) FILTER (WHERE LOWER(CAST(c.op_parathyroid_autograft_any AS VARCHAR)) = 'true') AS autograft,
        COUNT(*) FILTER (WHERE c.comp_hypoparathyroidism_confirmed IS NOT NULL) AS hypoparathyroidism
    FROM _parathyroid_patient_rollup_v1 p
    JOIN canonical_patient_master_v1 c ON p.research_id = c.research_id
    GROUP BY p.para_removal_intent
    ORDER BY n DESC
""").fetchdf()
print(f"\n  {'Intent':<22} {'N':>6} {'NLP-managed':>12} {'Autograft':>10} {'Hypoparathyroid':>16}")
print("  " + "-" * 68)
for _, row in para_xval.iterrows():
    print(f"  {str(row['para_removal_intent']):<22} {int(row['n']):>6} {int(row['nlp_managed']):>12} "
          f"{int(row['autograft']):>10} {int(row['hypoparathyroidism']):>16}")
print("  Note: intentional removal should show higher autograft/hypoparathyroidism rates")

# ── Step 2.5: Integrate parathyroid into canonical (if needed) ────────────────
print("\n[Step 2.5] Integrate parathyroid rollup into canonical")
existing = canonical_columns()
rollup_cols = [
    r[0] for r in con.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = '_parathyroid_patient_rollup_v1' AND table_schema = 'main'
          AND column_name != 'research_id'
    """).fetchall()
]
new_para_cols = [c for c in rollup_cols if c not in existing]
print(f"  New parathyroid columns to add: {len(new_para_cols)}")
if new_para_cols:
    for c in new_para_cols:
        print(f"    {c}")
    select_para = ",\n    ".join([f'p."{col}"' for col in new_para_cols])
    con.execute(f"""
        CREATE OR REPLACE TABLE canonical_patient_master_v1 AS
        SELECT
            c.*,
            {select_para}
        FROM canonical_patient_master_v1 c
        LEFT JOIN _parathyroid_patient_rollup_v1 p ON c.research_id = p.research_id
    """)
    check_invariants("post-parathyroid")
    print(f"  ✓ Added {len(new_para_cols)} parathyroid columns to canonical")
else:
    print("  All parathyroid columns already in canonical")
    # Still need to refresh para columns from corrected rollup if rollup was rebuilt
    if rollup_count[1] > 3660:  # rollup was rebuilt with more patients
        print("  Rollup was rebuilt — refreshing para columns in canonical")
        refresh_cols = [c for c in rollup_cols if c in existing]
        if refresh_cols:
            # Drop old para cols and re-add from new rollup
            exclude_para = ", ".join([f'"{c}"' for c in refresh_cols])
            select_para = ",\n    ".join([f'p."{c}"' for c in refresh_cols])
            con.execute(f"""
                CREATE OR REPLACE TABLE canonical_patient_master_v1 AS
                SELECT
                    c.* EXCLUDE ({exclude_para}),
                    {select_para}
                FROM canonical_patient_master_v1 c
                LEFT JOIN _parathyroid_patient_rollup_v1 p ON c.research_id = p.research_id
            """)
            check_invariants("post-parathyroid-refresh")
            print(f"  ✓ Refreshed {len(refresh_cols)} parathyroid columns from rebuilt rollup")
    else:
        check_invariants("post-parathyroid-check")

# ═══════════════════════════════════════════════════════════════════════════════
# TASK 3: COVERAGE REPORT + ETE VARIABLE VERIFICATION
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TASK 3: FINAL COVERAGE REPORT")
print("=" * 70)

total_cols = con.execute("""
    SELECT COUNT(*) FROM information_schema.columns
    WHERE table_name = 'canonical_patient_master_v1' AND table_schema = 'main'
""").fetchone()[0]
print(f"\n  canonical_patient_master_v1: 10,871 patients × {total_cols} columns")

# NSQIP coverage report
print("\n  NSQIP Thyroidectomy Enrichment Coverage (1,261 matched patients):")
nsqip_key_cols = [
    "nsqip_thyroidectomy_has_data", "nsqip_operation_date", "nsqip_primary_indication",
    "nsqip_hypocalcemia_flag", "nsqip_rln_injury_flag", "nsqip_hematoma_flag",
    "nsqip_central_neck_dissection", "nsqip_lateral_neck_dissection",
    "nsqip_rln_monitoring", "nsqip_drain_usage", "nsqip_t_classification",
    "nsqip_n_classification", "nsqip_nodes_removed", "nsqip_nodes_positive",
    "nsqip_hospital_los_days", "nsqip_operative_duration_min",
    "nsqip_albumin", "nsqip_creatinine", "nsqip_hba1c",
    "nsqip_sodium", "nsqip_calcium_vitd_replacement",
    "nsqip_same_day_discharge_flag", "nsqip_readmission_30d_flag",
]
print(f"  {'Column':<45} {'Non-null':>8} {'Pct':>6}")
print("  " + "-" * 61)
for col in nsqip_key_cols:
    try:
        count = con.execute(f"""
            SELECT COUNT(*) FROM canonical_patient_master_v1
            WHERE "{col}" IS NOT NULL AND CAST("{col}" AS VARCHAR) NOT IN ('', 'nan', 'None')
        """).fetchone()[0]
        pct = count / 10871 * 100
        print(f"  {col:<45} {count:>8} {pct:>5.1f}%")
    except Exception as e:
        print(f"  {col:<45} {'ERROR':>8}  {str(e)[:20]}")

# Parathyroid coverage report
print("\n  Parathyroid Intent Coverage:")
para_key_cols = [
    "para_specimen_included", "para_removal_intent", "para_incidental_status_refined",
    "para_has_pathologic_glands", "para_abnormality_type", "para_n_glands_identified",
    "para_n_glands_biopsied", "para_n_glands_excised",
    "para_max_cellularity_pct", "para_min_cellularity_pct", "para_max_gland_weight_g",
]
print(f"  {'Column':<45} {'Non-null':>8} {'Pct':>6}")
print("  " + "-" * 61)
for col in para_key_cols:
    try:
        count = con.execute(f"""
            SELECT COUNT(*) FROM canonical_patient_master_v1
            WHERE "{col}" IS NOT NULL AND CAST("{col}" AS VARCHAR) NOT IN ('', 'nan', 'None', 'False', 'false')
        """).fetchone()[0]
        pct = count / 10871 * 100
        print(f"  {col:<45} {count:>8} {pct:>5.1f}%")
    except Exception as e:
        print(f"  {col:<45} {'ERROR':>8}  {str(e)[:20]}")

# Domain summary
print("\n  Domain column counts:")
all_cols = [
    r[0] for r in con.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'canonical_patient_master_v1' AND table_schema = 'main'"
    ).fetchall()
]
domains = [
    ("nsqip_", "NSQIP"), ("para_", "Parathyroid"), ("ct_", "CT Imaging"),
    ("mri_", "MRI Imaging"), ("pet_", "PET/CT"), ("nucmed_", "Nuclear Med"),
    ("lnus_", "LN Ultrasound"), ("cnln_", "Clinical Note LN"),
    ("lab_", "Labs"), ("nlp_", "NLP Entities"), ("op_nlp_", "Operative NLP"),
    ("comp_", "Complications"), ("ops_", "OP Sheet"), ("tg_", "Thyroglobulin"),
    ("rai_", "RAI Treatment"), ("ln_", "Lymph Node Path"), ("ete_", "ETE"),
]
accounted = set()
print(f"  {'Domain':<25} {'Cols':>6}")
print("  " + "-" * 33)
for prefix, label in domains:
    cols = [c for c in all_cols if c.startswith(prefix)]
    accounted.update(cols)
    print(f"  {label:<25} {len(cols):>6}")
remaining = len(set(all_cols) - accounted)
print(f"  {'Other/core':<25} {remaining:>6}")
print(f"  {'TOTAL':<25} {len(all_cols):>6}")

# ETE paper key variable verification
print("\n  ETE Paper Key Variables:")
ete_vars = [
    ("research_id", "IS NOT NULL"),
    ("fna_path_outcome", "= 'malignant'"),
    ("ete_grade", "IS NOT NULL"),
    ("ete_refined_grade", "IS NOT NULL"),
    ("gross_ete_flag", "IS NOT NULL"),
    ("vascular_invasion_grade", "IS NOT NULL"),
    ("capsular_invasion_refined", "IS NOT NULL"),
    ("perineural_invasion", "IS NOT NULL"),
    ("ajcc8_t_stage", "IS NOT NULL"),
    ("ajcc8_n_stage", "IS NOT NULL"),
    ("ln_total_examined", "IS NOT NULL"),
    ("followup_years", "> 0"),
    ("age_at_surgery", "IS NOT NULL"),
    ("diagnosis_primary", "IS NOT NULL"),
    ("nsqip_thyroidectomy_has_data", "= TRUE"),
    ("para_removal_intent", "IS NOT NULL"),
    ("para_n_glands_identified", "> 0"),
]
print(f"  {'Variable':<40} {'Count':>8} {'Pct':>7}")
print("  " + "-" * 57)
for var, condition in ete_vars:
    try:
        count = con.execute(
            f"SELECT COUNT(*) FROM canonical_patient_master_v1 WHERE {var} {condition}"
        ).fetchone()[0]
        pct = count / 10871 * 100
        print(f"  {var:<40} {count:>8} {pct:>6.1f}%")
    except Exception as e:
        print(f"  {var:<40} {'ERROR':>8}  {str(e)[:25]}")

# ═══════════════════════════════════════════════════════════════════════════════
# TASK 4: DATA DICTIONARY REFRESH
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("TASK 4: DATA DICTIONARY REFRESH")
print("=" * 70)

col_info = con.execute("""
    SELECT
        column_name,
        data_type,
        ordinal_position,
        CASE
            WHEN column_name LIKE 'nsqip_%' THEN 'NSQIP'
            WHEN column_name LIKE 'para_%' THEN 'Parathyroid'
            WHEN column_name LIKE 'ct_%' THEN 'CT Imaging'
            WHEN column_name LIKE 'mri_%' THEN 'MRI'
            WHEN column_name LIKE 'pet_%' THEN 'PET/CT'
            WHEN column_name LIKE 'nucmed_%' THEN 'Nuclear Med'
            WHEN column_name LIKE 'cnln_%' THEN 'Clinical Note LN'
            WHEN column_name LIKE 'lnus_%' THEN 'LN Ultrasound'
            WHEN column_name LIKE 'lab_%' THEN 'Labs'
            WHEN column_name LIKE 'nlp_%' THEN 'NLP Entities'
            WHEN column_name LIKE 'op_nlp_%' THEN 'Operative NLP'
            WHEN column_name LIKE 'comp_%' THEN 'Complications'
            WHEN column_name LIKE 'ops_%' THEN 'OP Sheet'
            WHEN column_name LIKE 'tg_%' THEN 'Thyroglobulin'
            WHEN column_name LIKE 'rai_%' THEN 'RAI Treatment'
            WHEN column_name LIKE 'ln_%' THEN 'Lymph Node Path'
            WHEN column_name LIKE 'ete_%' THEN 'ETE'
            ELSE 'Core/Other'
        END AS domain
    FROM information_schema.columns
    WHERE table_name = 'canonical_patient_master_v1' AND table_schema = 'main'
    ORDER BY ordinal_position
""").fetchdf()

dd_path = REPO / "data_dictionary.csv"
col_info.to_csv(str(dd_path), index=False)
print(f"\n  ✓ Data dictionary exported: {len(col_info)} columns → {dd_path}")

# Also export to scripts/output/
out_dd_path = OUTPUT_DIR / "data_dictionary.csv"
col_info.to_csv(str(out_dd_path), index=False)
print(f"  ✓ Data dictionary copy: {out_dd_path}")

# Domain summary for data dictionary
domain_counts = col_info.groupby("domain").size().sort_values(ascending=False)
print("\n  Domain breakdown in data dictionary:")
for domain, count in domain_counts.items():
    print(f"    {domain:<25} {count:>6} columns")

# ── Final invariant check ─────────────────────────────────────────────────────
print()
check_invariants("FINAL")

print("\n" + "=" * 70)
print("Script 221b COMPLETE")
print(f"canonical_patient_master_v1: 10,871 patients × {total_cols} columns")
print("All invariants PASS")
print("=" * 70)
