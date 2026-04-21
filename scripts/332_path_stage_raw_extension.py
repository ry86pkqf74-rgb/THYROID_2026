"""
Script 332 — Extend path_stage_raw beyond current 4,070.

Script 294 derived path_stage_raw from path_synoptics (stage_group and
T-stage concatenation).  This script extends coverage by deriving from
CPM T_stage/N_stage/M_stage columns directly, reaching ~8,000 patients.

Derivation:
  - Full: 'T'||T_stage||'N'||N_stage||'M'||M_stage (all three present)
  - Partial: 'T'||T_stage || COALESCE('N'||N_stage,'Nx') || COALESCE('M'||M_stage,'Mx')

Usage:
    python 332_path_stage_raw_extension.py            # dry-run
    python 332_path_stage_raw_extension.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "332_path_stage_raw_extension"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def ensure_log_table(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.cpm_backfill_log_v1 (
            backfilled_at TIMESTAMP, cpm_column VARCHAR,
            source_description VARCHAR, threshold VARCHAR,
            n_rows_updated BIGINT, n_distinct_rid BIGINT,
            sample_values VARCHAR, script VARCHAR
        )
    """)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 332 — path_stage_raw extension "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Check which stage columns exist in CPM
    stage_cols = {}
    for col in ["path_stage_raw", "gm_path_stage_raw",
                 "T_stage", "N_stage", "M_stage",
                 "t_stage", "n_stage", "m_stage",
                 "ajcc8_t", "ajcc8_n", "ajcc8_m",
                 "path_t_stage", "path_n_stage", "path_m_stage"]:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND LOWER(column_name) = LOWER('{col}')
        """).fetchone()[0]
        if exists:
            actual_name = con.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'canonical_patient_master'
                  AND LOWER(column_name) = LOWER('{col}')
                LIMIT 1
            """).fetchone()[0]
            stage_cols[col.lower()] = actual_name

    log(f"  Stage columns found: {list(stage_cols.values())}")

    # Determine T/N/M column names
    t_col = stage_cols.get("t_stage") or stage_cols.get("path_t_stage") or stage_cols.get("ajcc8_t")
    n_col = stage_cols.get("n_stage") or stage_cols.get("path_n_stage") or stage_cols.get("ajcc8_n")
    m_col = stage_cols.get("m_stage") or stage_cols.get("path_m_stage") or stage_cols.get("ajcc8_m")

    if not t_col:
        log("  No T_stage column found in CPM — cannot derive")
        log("  Falling back to synoptic_tumor_long_v1 for derivation...")

    for target_col in ["path_stage_raw", "gm_path_stage_raw"]:
        actual_target = stage_cols.get(target_col.lower())
        if not actual_target:
            log(f"  CPM.{target_col}: not found — skipping")
            continue

        pre_pop = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE "{actual_target}" IS NOT NULL
        """).fetchone()[0]
        log(f"  {actual_target}: pre-pop={pre_pop}")

        if t_col:
            # Derive from CPM T/N/M columns
            plan = con.execute(f"""
                SELECT COUNT(*)
                FROM main.canonical_patient_master
                WHERE "{actual_target}" IS NULL
                  AND "{t_col}" IS NOT NULL
                  AND TRIM(CAST("{t_col}" AS VARCHAR)) != ''
            """).fetchone()[0]
            log(f"  {actual_target}: {plan} derivable from CPM T/N/M")

            if plan > 0 and args.commit:
                con.execute(f"""
                    UPDATE main.canonical_patient_master
                       SET "{actual_target}" =
                           'T' || CAST("{t_col}" AS VARCHAR)
                           || COALESCE('N' || NULLIF(TRIM(CAST("{n_col}" AS VARCHAR)), ''),
                                       'Nx')
                           || COALESCE('M' || NULLIF(TRIM(CAST("{m_col}" AS VARCHAR)), ''),
                                       'Mx')
                     WHERE "{actual_target}" IS NULL
                       AND "{t_col}" IS NOT NULL
                       AND TRIM(CAST("{t_col}" AS VARCHAR)) != ''
                """)
                post_pop = con.execute(f"""
                    SELECT COUNT(*) FROM main.canonical_patient_master
                    WHERE "{actual_target}" IS NOT NULL
                """).fetchone()[0]
                log(f"  {actual_target}: post-pop={post_pop} (delta +{post_pop - pre_pop})")

                con.execute("""
                    INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                    (?, ?, ?, ?, ?, ?, ?, ?)
                """, [dt.datetime.utcnow(), actual_target,
                      "Derived from CPM T/N/M columns + synoptics (332)",
                      "v1 NULL only", post_pop - pre_pop, None, None, SCRIPT])
        else:
            # Try synoptic_tumor_long_v1
            try:
                plan_syn = con.execute(f"""
                    SELECT COUNT(*)
                    FROM main.canonical_patient_master c
                    JOIN main.synoptic_tumor_long_v1 s
                        ON CAST(c.research_id AS VARCHAR) = CAST(s.research_id AS VARCHAR)
                    WHERE c."{actual_target}" IS NULL
                      AND s.t_stage IS NOT NULL
                """).fetchone()[0]
                log(f"  {actual_target}: {plan_syn} derivable from synoptic_tumor_long_v1")

                if plan_syn > 0 and args.commit:
                    con.execute(f"""
                        UPDATE main.canonical_patient_master AS c
                           SET "{actual_target}" =
                               'T' || CAST(s.t_stage AS VARCHAR)
                               || COALESCE('N' || NULLIF(TRIM(CAST(s.n_stage AS VARCHAR)), ''),
                                           'Nx')
                               || COALESCE('M' || NULLIF(TRIM(CAST(s.m_stage AS VARCHAR)), ''),
                                           'Mx')
                          FROM (
                              SELECT CAST(research_id AS VARCHAR) AS research_id,
                                     FIRST(t_stage ORDER BY t_stage) AS t_stage,
                                     FIRST(n_stage ORDER BY n_stage) AS n_stage,
                                     FIRST(m_stage ORDER BY m_stage) AS m_stage
                              FROM main.synoptic_tumor_long_v1
                              WHERE t_stage IS NOT NULL
                              GROUP BY CAST(research_id AS VARCHAR)
                          ) AS s
                         WHERE CAST(c.research_id AS VARCHAR) = s.research_id
                           AND c."{actual_target}" IS NULL
                    """)
                    post_pop = con.execute(f"""
                        SELECT COUNT(*) FROM main.canonical_patient_master
                        WHERE "{actual_target}" IS NOT NULL
                    """).fetchone()[0]
                    log(f"  {actual_target}: post-pop={post_pop}")

                    con.execute("""
                        INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?)
                    """, [dt.datetime.utcnow(), actual_target,
                          "Derived from synoptic_tumor_long_v1 T/N/M",
                          "v1 NULL only", post_pop - pre_pop, None, None, SCRIPT])
            except Exception as e:
                log(f"  synoptic_tumor_long_v1 fallback failed: {e}")

    if not args.commit:
        log("  (dry-run — no UPDATE)")

    # Final check
    for col_name in ["path_stage_raw", "gm_path_stage_raw"]:
        actual = stage_cols.get(col_name.lower())
        if actual:
            final = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master
                WHERE "{actual}" IS NOT NULL
            """).fetchone()[0]
            log(f"  Final {actual}: {final} nonnull")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 332 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
