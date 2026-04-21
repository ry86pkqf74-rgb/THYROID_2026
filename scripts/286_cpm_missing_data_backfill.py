"""
Script 286 — CPM missing-data backfill (conservative).

Performs only the backfills that have CLEAN upstream data, per the
provenance audit in Script 285. Each backfill:
  1. Computes the value per research_id from upstream
  2. Prints a pre/post count and a representative sample (no PHI)
  3. UPDATEs CPM on --commit only
  4. Logs an entry to `manuscript_workspace.cpm_backfill_log_v1`

Targets (3 columns):
  1. `nucmed_tgab_max`             <- MAX(result_numeric) from
                                     thyroglobulin_lab_VIEW_v1 where analyte='TgAb'
  2. `tsh_suppressed_ever`         <- BOOL: any TSH < 0.1 from
                                     longitudinal_lab_VIEW_v1 where analyte='TSH'
  3. `biochemical_concern_first_date` <- MIN(window_first_date) from
                                     tg_postop_surveillance_windows_v1 where value_max > 2.0

NOT included (documented in Script 285 audit as needing new extraction,
DROP, or legitimately-empty):
  - recurrence_histology, recurrence_site_primary, gm_recurrence_site_primary
  - op_esophageal_inv_any
  - third_surgery_date, third_surgery_days_from_surg  (only 0 patients qualify)
  - path_stage_raw, gm_path_stage_raw
  - comp_vc_paralysis_evidence_tier, comp_vc_paresis_evidence_tier  (upstream also NULL)
  - rai_scan_findings_v9

Safety invariants:
  - CPM row count = 10871 (pre AND post)
  - distinct research_id = 10871
  - fna_path_outcome never NULL (before AND after)
  - For each target: if the pre-value is non-NULL for a row, we do NOT overwrite.

Usage:
    python 286_cpm_missing_data_backfill.py            # dry-run
    python 286_cpm_missing_data_backfill.py --commit   # execute backfill
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

TSH_SUPPRESSED_THRESHOLD = 0.1   # ng/mL — standard endocrine suppression threshold
TG_CONCERN_THRESHOLD = 2.0        # ng/mL — ATA intermediate-risk concern threshold

SCRIPT = "286_cpm_missing_data_backfill"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_backfill_log_v1 (
            backfilled_at TIMESTAMP,
            cpm_column VARCHAR,
            source_description VARCHAR,
            threshold VARCHAR,
            n_rows_updated BIGINT,
            n_distinct_rid BIGINT,
            sample_values VARCHAR,
            script VARCHAR
        )
    """)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def backfill_simple(con, commit, target_col, source_sql, desc, threshold, dtype):
    """Run `source_sql` (must return research_id, value). UPDATE CPM where target IS NULL."""
    log(f"--- Backfilling CPM.{target_col} ---")
    log(f"    source: {desc}")
    log(f"    threshold: {threshold}")

    # Materialize the source into a temp table
    tmp = f"tmp_backfill_{target_col}"
    con.execute(f"DROP TABLE IF EXISTS {tmp}")
    con.execute(f"CREATE TEMP TABLE {tmp} AS {source_sql}")
    n_src = con.execute(f"SELECT COUNT(*) FROM {tmp}").fetchone()[0]
    n_rid = con.execute(f"SELECT COUNT(DISTINCT research_id) FROM {tmp}").fetchone()[0]
    log(f"    source produced: {n_src} rows across {n_rid} distinct research_id")

    # Sample (no PHI — just values)
    sample = con.execute(f"SELECT value FROM {tmp} LIMIT 5").fetchall()
    sample_str = ", ".join(str(s[0]) for s in sample)
    log(f"    sample values: {sample_str}")

    # How many CPM rows currently NULL (sanity)
    cur_null = con.execute(
        f'SELECT COUNT(*) FROM main.canonical_patient_master '
        f'WHERE "{target_col}" IS NULL').fetchone()[0]
    log(f"    CPM.{target_col} currently NULL: {cur_null} / 10871")

    # Plan: how many would be set?
    would_set = con.execute(f"""
        SELECT COUNT(*)
          FROM main.canonical_patient_master c
          JOIN {tmp} s USING (research_id)
         WHERE c."{target_col}" IS NULL
           AND s.value IS NOT NULL
    """).fetchone()[0]
    log(f"    Planned UPDATE: {would_set} rows")

    if not commit:
        log("    (dry-run — no UPDATE)")
        con.execute(f"DROP TABLE IF EXISTS {tmp}")
        return 0

    # Execute update. DuckDB UPDATE-FROM syntax:
    con.execute(f"""
        UPDATE main.canonical_patient_master AS c
           SET "{target_col}" = s.value
          FROM {tmp} AS s
         WHERE c.research_id = s.research_id
           AND c."{target_col}" IS NULL
           AND s.value IS NOT NULL
    """)

    # Verify
    new_populated = con.execute(
        f'SELECT COUNT(*) FROM main.canonical_patient_master '
        f'WHERE "{target_col}" IS NOT NULL').fetchone()[0]
    log(f"    CPM.{target_col} populated after: {new_populated}")

    # Log
    con.execute("""
        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), target_col, desc, threshold,
          new_populated, n_rid, sample_str, SCRIPT])

    con.execute(f"DROP TABLE IF EXISTS {tmp}")
    cpm_invariants(con, "post-backfill")
    return new_populated


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 286 — CPM missing-data backfill "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    cpm_invariants(con, "pre")

    # 1. nucmed_tgab_max
    src_tgab = """
        SELECT research_id,
               MAX(result_numeric) AS value
          FROM main.thyroglobulin_lab_VIEW_v1
         WHERE analyte = 'TgAb'
           AND result_numeric IS NOT NULL
         GROUP BY research_id
    """
    backfill_simple(
        con, args.commit, "nucmed_tgab_max",
        src_tgab,
        "MAX(result_numeric) from thyroglobulin_lab_VIEW_v1 where analyte='TgAb'",
        "no threshold (raw max)", "DOUBLE"
    )

    # 2. tsh_suppressed_ever
    src_tsh = f"""
        SELECT research_id,
               MAX(CASE WHEN value_numeric < {TSH_SUPPRESSED_THRESHOLD} THEN TRUE
                        ELSE FALSE END) AS value
          FROM main.longitudinal_lab_VIEW_v1
         WHERE lab_name_standardized = 'tsh'
           AND value_numeric IS NOT NULL
         GROUP BY research_id
    """
    backfill_simple(
        con, args.commit, "tsh_suppressed_ever",
        src_tsh,
        "MAX(TSH<0.1) from longitudinal_lab_VIEW_v1 where lab_name_standardized='tsh'",
        f"TSH < {TSH_SUPPRESSED_THRESHOLD} ng/mL", "BOOLEAN"
    )

    # 3. biochemical_concern_first_date
    src_concern = f"""
        SELECT research_id,
               MIN(window_first_date) AS value
          FROM main.tg_postop_surveillance_windows_v1
         WHERE analyte = 'Tg'
           AND value_max > {TG_CONCERN_THRESHOLD}
           AND window_first_date IS NOT NULL
         GROUP BY research_id
    """
    backfill_simple(
        con, args.commit, "biochemical_concern_first_date",
        src_concern,
        "MIN(window_first_date) from tg_postop_surveillance_windows_v1 where value_max > 2.0",
        f"Tg > {TG_CONCERN_THRESHOLD} ng/mL", "TIMESTAMP"
    )

    log("=" * 72)
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
