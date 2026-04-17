"""
Script 114: RAI Dose Recovery from Nuclear Medicine Text
=========================================================
Extracts therapeutic RAI (I-131) doses from nuclear_med free-text fields
via regex, matches to rai_treatment_episode_v2 episodes with missing doses,
and propagates recovered doses to all canonical/manuscript tables.

Scope
-----
- Source  : nuclear_med.{findings_text, impression_text, indication_text}
- Pattern : NUMBER + mCi/millicuries
- Filter  : therapeutic scantypes only; exclude MIBG; exclude I-123 text
- Match   : closest nuclear_med scan per episode (no date-window cap);
            tie-break = minimum |day_gap|
- Confidence scoring by proximity:
    0d → 0.95, 1-30d → 0.90, 31-90d → 0.85, 91-180d → 0.80,
    181-365d → 0.70, >365d → 0.60

Tables written
--------------
  rai_dose_recovery_v1                  ← new audit/recovery table
  rai_treatment_episode_v2_backup_20260415 ← backup (idempotent)
  rai_treatment_episode_v2              ← UPDATE dose_mci (4 episodes)
  patient_refined_master_clinical_v12   ← UPDATE rai_dose_v9 / max_dose_mci
  patient_analysis_resolved_v1          ← UPDATE rai_max_dose_mci
  manuscript_cohort_v1                  ← UPDATE rai_max_dose_mci

Usage
-----
  .venv/bin/python scripts/114_rai_dose_recovery_nuclear_med.py [--dry-run] [--md]
"""

import sys, toml, duckdb, pandas as pd
from datetime import datetime

DRY_RUN = "--dry-run" in sys.argv
USE_MD  = "--md" in sys.argv or True          # always uses MD (local token)

print(f"=== Script 114: RAI Dose Recovery ({'DRY-RUN' if DRY_RUN else 'LIVE'}) ===")
print(f"Started: {datetime.now().isoformat()}\n")

cfg = toml.load("motherduck.local.toml")
tok = cfg.get("MD_SA_TOKEN") or cfg.get("MOTHERDUCK_TOKEN") or cfg.get("motherduck_token")
assert tok, "MotherDuck token not found in motherduck.local.toml"
print(f"Token: SET (len={len(tok)})")

con = duckdb.connect(f"md:Thyroid 2026?motherduck_token={tok}")

# ── PHASE A: Extract doses from nuclear_med (read-only) ─────────────────────
print("\n" + "─" * 60)
print("PHASE A  Extract doses from nuclear_med therapeutic scans")
print("─" * 60)

EXTRACT_SQL = r"""
WITH raw_extract AS (
    SELECT
        CAST(nm.research_id AS INTEGER) AS research_id,
        nm.scan_index,
        nm.scandate,
        nm.scantype,
        nm.radiotracer,
        COALESCE(
            TRY_STRPTIME(nm.scandate, '%m/%d/%y'),
            TRY_STRPTIME(nm.scandate, '%m/%d/%Y')
        ) AS scandate_parsed,
        -- Try each field in priority order
        NULLIF(regexp_extract(LOWER(COALESCE(nm.findings_text,  '')),
            '(\d+\.?\d*)\s*(?:mci|millicuri)', 1), '') AS dose_findings,
        NULLIF(regexp_extract(LOWER(COALESCE(nm.impression_text,'')),
            '(\d+\.?\d*)\s*(?:mci|millicuri)', 1), '') AS dose_impression,
        NULLIF(regexp_extract(LOWER(COALESCE(nm.indication_text,'')),
            '(\d+\.?\d*)\s*(?:mci|millicuri)', 1), '') AS dose_indication,
        -- Exclusion flags
        (LOWER(COALESCE(nm.findings_text,'') || ' ' || COALESCE(nm.impression_text,'')
               || ' ' || COALESCE(nm.indication_text,''))
         LIKE '%mibg%') AS is_mibg,
        (LOWER(COALESCE(nm.findings_text,'') || ' ' || COALESCE(nm.impression_text,'')
               || ' ' || COALESCE(nm.indication_text,'') || ' ' || COALESCE(nm.radiotracer,''))
         LIKE '%i-123%'
         OR LOWER(COALESCE(nm.radiotracer,'')) LIKE '%i123%') AS is_i123,
        LEFT(COALESCE(nm.findings_text, nm.impression_text, nm.indication_text), 500) AS text_snippet
    FROM nuclear_med nm
    WHERE nm.scantype IN ('RAI Therapy Scan', 'Post-Therapy Scan')
      AND LOWER(COALESCE(nm.findings_text,'') || ' ' || COALESCE(nm.impression_text,'')
                || ' ' || COALESCE(nm.indication_text,''))
          LIKE '%mci%'
)
SELECT
    research_id,
    scan_index,
    scandate,
    scandate_parsed,
    scantype,
    radiotracer,
    COALESCE(dose_findings, dose_impression, dose_indication) AS dose_raw,
    CASE
        WHEN dose_findings  IS NOT NULL THEN 'findings_text'
        WHEN dose_impression IS NOT NULL THEN 'impression_text'
        ELSE 'indication_text'
    END AS source_field,
    TRY_CAST(
        COALESCE(dose_findings, dose_impression, dose_indication)
        AS DOUBLE) AS extracted_dose_mci,
    text_snippet
FROM raw_extract
WHERE NOT is_mibg
  AND NOT is_i123
  AND scandate_parsed IS NOT NULL
  AND COALESCE(dose_findings, dose_impression, dose_indication) IS NOT NULL
  AND TRY_CAST(COALESCE(dose_findings, dose_impression, dose_indication) AS DOUBLE)
      BETWEEN 30 AND 500
"""

df_nm = con.execute(EXTRACT_SQL).df()
print(f"  nuclear_med therapeutic rows with dose extracted : {len(df_nm)}")
print(f"  Unique patients                                  : {df_nm['research_id'].nunique()}")
bins   = [0, 30, 50, 100, 150, 200, 300, 500, float('inf')]
labels = ['30-50','50-100','100-150','150-200','200-300','300-500','500+','error']
df_nm['bucket'] = pd.cut(df_nm['extracted_dose_mci'], bins=bins, labels=labels, right=False)
print("\n  Dose distribution:")
for lbl, cnt in df_nm['bucket'].value_counts().sort_index().items():
    print(f"    {lbl:<15}: {cnt}")

# ── PHASE B: Match to RAI episodes (no date-window cap) ─────────────────────
print("\n" + "─" * 60)
print("PHASE B  Match extracted doses to RAI episodes with missing dose")
print("─" * 60)

MATCH_SQL = r"""
WITH nm_clean AS (
    """ + EXTRACT_SQL + r"""
),
rai_missing AS (
    SELECT
        rai_episode_id,
        research_id,
        resolved_rai_date,
        CAST(resolved_rai_date AS DATE) AS rai_date_parsed
    FROM rai_treatment_episode_v2
    WHERE dose_mci IS NULL
      AND resolved_rai_date IS NOT NULL
),
matched AS (
    SELECT
        r.rai_episode_id,
        r.research_id,
        r.resolved_rai_date,
        r.rai_date_parsed,
        n.scan_index                                                    AS nuclear_med_scan_index,
        n.scandate                                                      AS nuclear_med_scandate,
        n.scandate_parsed                                               AS nuclear_med_scandate_parsed,
        n.scantype                                                      AS nuclear_med_scantype,
        n.source_field                                                  AS dose_source_field,
        n.dose_raw,
        n.extracted_dose_mci,
        n.text_snippet,
        DATEDIFF('day', r.rai_date_parsed,
                 CAST(n.scandate_parsed AS DATE))                       AS signed_day_gap,
        ABS(DATEDIFF('day', r.rai_date_parsed,
                     CAST(n.scandate_parsed AS DATE)))                  AS day_gap,
        -- Confidence by proximity
        CASE
            WHEN ABS(DATEDIFF('day', r.rai_date_parsed,
                               CAST(n.scandate_parsed AS DATE))) = 0    THEN 0.95
            WHEN ABS(DATEDIFF('day', r.rai_date_parsed,
                               CAST(n.scandate_parsed AS DATE))) <= 30  THEN 0.90
            WHEN ABS(DATEDIFF('day', r.rai_date_parsed,
                               CAST(n.scandate_parsed AS DATE))) <= 90  THEN 0.85
            WHEN ABS(DATEDIFF('day', r.rai_date_parsed,
                               CAST(n.scandate_parsed AS DATE))) <= 180 THEN 0.80
            WHEN ABS(DATEDIFF('day', r.rai_date_parsed,
                               CAST(n.scandate_parsed AS DATE))) <= 365 THEN 0.70
            ELSE 0.60
        END                                                             AS recovery_confidence,
        'regex_from_nuclear_med_text'                                   AS recovery_source,
        ROW_NUMBER() OVER (
            PARTITION BY r.rai_episode_id
            ORDER BY ABS(DATEDIFF('day', r.rai_date_parsed,
                                  CAST(n.scandate_parsed AS DATE)))
        ) AS rank_closest
    FROM rai_missing r
    JOIN nm_clean n ON r.research_id = n.research_id
)
SELECT
    rai_episode_id, research_id, resolved_rai_date,
    nuclear_med_scan_index, nuclear_med_scandate,
    nuclear_med_scandate_parsed, nuclear_med_scantype,
    extracted_dose_mci, dose_raw, dose_source_field,
    signed_day_gap, day_gap, recovery_confidence, recovery_source, text_snippet
FROM matched
WHERE rank_closest = 1
ORDER BY day_gap, research_id
"""

df_match = con.execute(MATCH_SQL).df()
print(f"  Total matched episodes       : {len(df_match)}")
print(f"  Unique patients              : {df_match['research_id'].nunique()}")
print("\n  Matched episodes:")
pd.set_option("display.max_colwidth", 100)
show = ["rai_episode_id","research_id","resolved_rai_date","nuclear_med_scandate",
        "signed_day_gap","day_gap","extracted_dose_mci","recovery_confidence","dose_source_field"]
print(df_match[show].to_string(index=False))

# ── PHASE C: Create / replace rai_dose_recovery_v1 ──────────────────────────
print("\n" + "─" * 60)
print("PHASE C  Create rai_dose_recovery_v1")
print("─" * 60)

if not DRY_RUN:
    con.execute("DROP TABLE IF EXISTS rai_dose_recovery_v1")
    con.execute(f"CREATE TABLE rai_dose_recovery_v1 AS {MATCH_SQL}")
    n = con.execute("SELECT COUNT(*) FROM rai_dose_recovery_v1").fetchone()[0]
    print(f"  ✓ rai_dose_recovery_v1 created ({n} rows)")
else:
    print("  [DRY-RUN] Would create rai_dose_recovery_v1")

# ── PHASE D: Backup ──────────────────────────────────────────────────────────
print("\n" + "─" * 60)
print("PHASE D  Backup rai_treatment_episode_v2")
print("─" * 60)

BACKUP = "rai_treatment_episode_v2_backup_20260415"
exists = con.execute(f"""
    SELECT COUNT(*) FROM information_schema.tables
    WHERE table_schema='main' AND table_name='{BACKUP}'
""").fetchone()[0]

if not DRY_RUN:
    if not exists:
        con.execute(f"CREATE TABLE {BACKUP} AS SELECT * FROM rai_treatment_episode_v2")
        bc = con.execute(f"SELECT COUNT(*) FROM {BACKUP}").fetchone()[0]
        print(f"  ✓ Backup created: {bc} rows")
    else:
        bc = con.execute(f"SELECT COUNT(*) FROM {BACKUP}").fetchone()[0]
        print(f"  ✓ Backup already exists: {bc} rows")
else:
    print("  [DRY-RUN] Would create backup")

# ── PHASE E: UPDATE rai_treatment_episode_v2 ────────────────────────────────
print("\n" + "─" * 60)
print("PHASE E  UPDATE rai_treatment_episode_v2")
print("─" * 60)

# Before counts
before_with    = con.execute("SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL").fetchone()[0]
before_missing = con.execute("SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NULL").fetchone()[0]
print(f"  BEFORE — with dose: {before_with} | missing: {before_missing}")

UPDATE_SQL = """
UPDATE rai_treatment_episode_v2 AS e
SET
    dose_mci                = r.extracted_dose_mci,
    dose_missingness_reason = 'dose_recovered_nuclear_med_regex',
    dose_source             = 'nuclear_med_text_regex',
    dose_confidence         = r.recovery_confidence
FROM rai_dose_recovery_v1 r
WHERE e.research_id    = r.research_id
  AND e.rai_episode_id = r.rai_episode_id
  AND e.dose_mci IS NULL
"""

if not DRY_RUN:
    con.execute(UPDATE_SQL)
    after_with    = con.execute("SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL").fetchone()[0]
    after_missing = con.execute("SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NULL").fetchone()[0]
    gained = after_with - before_with
    print(f"  AFTER  — with dose: {after_with} | missing: {after_missing}")
    print(f"  ✓ Gained {gained} dose values")
else:
    print(f"  [DRY-RUN] Would update {len(df_match)} episodes")

# ── PHASE F: Propagate to master canonical tables ────────────────────────────
print("\n" + "─" * 60)
print("PHASE F  Propagate to master canonical tables")
print("─" * 60)

research_ids = df_match["research_id"].tolist()
ids_str = ",".join(str(i) for i in research_ids)

def safe_update(label, sql):
    if DRY_RUN:
        print(f"  [DRY-RUN] Would UPDATE {label}")
        return
    try:
        con.execute(sql)
        print(f"  ✓ Updated {label}")
    except Exception as e:
        print(f"  ⚠ {label}: {e}")

# Aggregate: use MAX dose per patient from rai_treatment_episode_v2
# (same logic as original rai_max_dose_mci derivation)
AGG_SQL = f"""
    SELECT research_id, MAX(dose_mci) AS max_dose
    FROM rai_treatment_episode_v2
    WHERE research_id IN ({ids_str})
      AND dose_mci IS NOT NULL
    GROUP BY research_id
"""
df_agg = con.execute(AGG_SQL).df()
print("  Patient aggregates (max dose per patient):")
print(df_agg.to_string(index=False))

# patient_refined_master_clinical_v12
safe_update("patient_refined_master_clinical_v12", f"""
    UPDATE patient_refined_master_clinical_v12 AS m
    SET
        rai_dose_v9    = agg.max_dose,
        max_dose_mci   = agg.max_dose,
        max_rai_dose_mci = agg.max_dose,
        rai_dose_source = 'nuclear_med_text_regex'
    FROM ({AGG_SQL}) agg
    WHERE m.research_id = agg.research_id
      AND (m.rai_dose_v9 IS NULL OR m.max_dose_mci IS NULL)
""")

# patient_analysis_resolved_v1 — treat both NULL and 0 as missing
safe_update("patient_analysis_resolved_v1", f"""
    UPDATE patient_analysis_resolved_v1 AS m
    SET rai_max_dose_mci = agg.max_dose
    FROM ({AGG_SQL}) agg
    WHERE m.research_id = agg.research_id
      AND (m.rai_max_dose_mci IS NULL OR m.rai_max_dose_mci = 0)
""")

# manuscript_cohort_v1
safe_update("manuscript_cohort_v1", f"""
    UPDATE manuscript_cohort_v1 AS m
    SET rai_max_dose_mci = agg.max_dose
    FROM ({AGG_SQL}) agg
    WHERE m.research_id = agg.research_id
      AND (m.rai_max_dose_mci IS NULL OR m.rai_max_dose_mci = 0)
""")

# ── PHASE G: Verify ──────────────────────────────────────────────────────────
print("\n" + "─" * 60)
print("PHASE G  Verification")
print("─" * 60)

if not DRY_RUN:
    # Confirm updates
    verify = con.execute("""
        SELECT e.research_id, e.rai_episode_id, e.dose_mci, e.dose_source,
               e.dose_missingness_reason, e.dose_confidence,
               r.day_gap, r.nuclear_med_scandate
        FROM rai_treatment_episode_v2 e
        JOIN rai_dose_recovery_v1 r
          ON e.research_id = r.research_id AND e.rai_episode_id = r.rai_episode_id
        ORDER BY r.day_gap
    """).df()
    print("  Updated episodes in rai_treatment_episode_v2:")
    print(verify.to_string(index=False))

    # Canonical check
    print("\n  Canonical table — rai_max_dose_mci for recovered patients:")
    for tbl in ["patient_analysis_resolved_v1", "manuscript_cohort_v1"]:
        try:
            r = con.execute(f"""
                SELECT research_id, rai_max_dose_mci
                FROM {tbl} WHERE research_id IN ({ids_str})
                ORDER BY research_id
            """).df()
            print(f"\n    {tbl}:")
            print(r.to_string(index=False))
        except Exception as ex:
            print(f"    {tbl}: {ex}")

    print(f"\n  rai_dose_recovery_v1 row count: "
          f"{con.execute('SELECT COUNT(*) FROM rai_dose_recovery_v1').fetchone()[0]}")
    print(f"  rai_treatment_episode_v2 total with dose: "
          f"{con.execute('SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL').fetchone()[0]}")
    print(f"  rai_treatment_episode_v2 total missing  : "
          f"{con.execute('SELECT COUNT(*) FROM rai_treatment_episode_v2 WHERE dose_mci IS NULL').fetchone()[0]}")
else:
    print("  [DRY-RUN] Skipping verification.")

con.close()
print(f"\n✓ Script 114 complete at {datetime.now().isoformat()}")
