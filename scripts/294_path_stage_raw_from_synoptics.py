"""
Script 294 — Derive path_stage_raw / gm_path_stage_raw from path_synoptics.

path_synoptics has tumor_1..5 x {t,n,m,stage_group} x {ajcc7, ajcc8}.
This script derives a concatenated stage string and backfills
path_stage_raw and gm_path_stage_raw on CPM where NULL.

Usage:
    python 294_path_stage_raw_from_synoptics.py            # dry-run
    python 294_path_stage_raw_from_synoptics.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "294_path_stage_raw_from_synoptics"


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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 294 — path_stage_raw from synoptics "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Build path stage derivation
    con.execute("DROP TABLE IF EXISTS _path_stage")
    con.execute("""
        CREATE TEMP TABLE _path_stage AS
        SELECT
            CAST(research_id AS VARCHAR) AS research_id,
            COALESCE(
                tumor_1_stage_group_ajcc8,
                tumor_1_stage_group_ajcc7
            ) AS stage_primary,
            CONCAT_WS(' | ',
                NULLIF(COALESCE(tumor_1_t_stage_ajcc8, tumor_1_t_stage_ajcc7), ''),
                NULLIF(COALESCE(tumor_2_t_stage_ajcc8, tumor_2_t_stage_ajcc7), ''),
                NULLIF(COALESCE(tumor_3_t_stage_ajcc8, tumor_3_t_stage_ajcc7), ''),
                NULLIF(COALESCE(tumor_4_t_stage_ajcc8, tumor_4_t_stage_ajcc7), ''),
                NULLIF(COALESCE(tumor_5_t_stage_ajcc8, tumor_5_t_stage_ajcc7), '')
            ) AS t_stages_concat
        FROM main.path_synoptics
        WHERE research_id IS NOT NULL
    """)

    n_src = con.execute("SELECT COUNT(*) FROM _path_stage").fetchone()[0]
    n_with_stage = con.execute("""
        SELECT COUNT(*) FROM _path_stage
        WHERE COALESCE(stage_primary, t_stages_concat) IS NOT NULL
          AND COALESCE(stage_primary, t_stages_concat) != ''
    """).fetchone()[0]
    log(f"  Source: {n_src} patients, {n_with_stage} with derivable stage")

    for cpm_col in ["path_stage_raw", "gm_path_stage_raw"]:
        pre_pop = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE "{cpm_col}" IS NOT NULL
        """).fetchone()[0]

        plan = con.execute(f"""
            SELECT COUNT(*)
            FROM main.canonical_patient_master c
            JOIN _path_stage p ON c.research_id = p.research_id
            WHERE c."{cpm_col}" IS NULL
              AND COALESCE(p.stage_primary, p.t_stages_concat) IS NOT NULL
              AND COALESCE(p.stage_primary, p.t_stages_concat) != ''
        """).fetchone()[0]
        log(f"  {cpm_col}: pre-pop={pre_pop}, planned={plan}")

        sample = con.execute(f"""
            SELECT COALESCE(p.stage_primary, p.t_stages_concat)
            FROM main.canonical_patient_master c
            JOIN _path_stage p ON c.research_id = p.research_id
            WHERE c."{cpm_col}" IS NULL
              AND COALESCE(p.stage_primary, p.t_stages_concat) IS NOT NULL
              AND COALESCE(p.stage_primary, p.t_stages_concat) != ''
            LIMIT 5
        """).fetchall()
        sample_str = ", ".join(str(s[0]) for s in sample)
        log(f"  Samples: {sample_str}")

        if not args.commit:
            log("  (dry-run)")
            continue

        if plan > 0:
            con.execute(f"""
                UPDATE main.canonical_patient_master AS c
                   SET "{cpm_col}" = COALESCE(p.stage_primary, p.t_stages_concat)
                  FROM _path_stage AS p
                 WHERE c.research_id = p.research_id
                   AND c."{cpm_col}" IS NULL
                   AND COALESCE(p.stage_primary, p.t_stages_concat) IS NOT NULL
                   AND COALESCE(p.stage_primary, p.t_stages_concat) != ''
            """)

            post_pop = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master
                WHERE "{cpm_col}" IS NOT NULL
            """).fetchone()[0]
            log(f"  {cpm_col}: post-pop={post_pop} (delta +{post_pop - pre_pop})")

            con.execute("""
                INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
                (?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                dt.datetime.utcnow(), cpm_col,
                "COALESCE(stage_group_ajcc8, ajcc7) or CONCAT_WS t_stages from path_synoptics",
                "v1 NULL only", post_pop - pre_pop, None, sample_str, SCRIPT
            ])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 294 complete.")
    if not args.commit:
        log("(dry-run — re-run with --commit to apply)")


if __name__ == "__main__":
    main()
