"""
Script 330 — Vocal-cord complication tiering (re-run of 295 with reconciliation).

comp_vc_paralysis_evidence_tier and comp_vc_paresis_evidence_tier are both
0 nonnull in CPM, despite Script 295 building vc_complication_tiering_v1.
This script reconciles: if 295's queue exists with proposed_tier, use that
directly (the UPDATE didn't land). Otherwise rebuild from scratch.

Tier rules (INTEGER, matching hypocalcemia/hypopara convention):
  1 = confirmed  (explicit diagnosis + treatment / scope evidence)
  2 = probable   (explicit mention, no treatment; or 2+ NLP mentions)
  3 = possible   (symptom-only, e.g. "hoarseness" without scope)
  4 = ruled out / resolved

Usage:
    python 330_vc_complication_tiering.py            # dry-run
    python 330_vc_complication_tiering.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "330_vc_complication_tiering"


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
    log(f"Script 330 — VC complication tiering "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Check current CPM state
    for col in ["comp_vc_paralysis_evidence_tier", "comp_vc_paresis_evidence_tier"]:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{col}'
        """).fetchone()[0]
        if exists:
            nn = con.execute(f"""
                SELECT COUNT(*) FROM main.canonical_patient_master
                WHERE "{col}" IS NOT NULL
            """).fetchone()[0]
            log(f"  CPM.{col} pre: {nn} nonnull")
        else:
            log(f"  CPM.{col}: NOT FOUND — will need to add")

    # Check if Script 295's queue exists
    queue_exists = con.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'manuscript_workspace'
          AND table_name = 'vc_complication_tiering_v1'
    """).fetchone()[0] > 0

    value_source = "fresh_from_phenotype"
    if queue_exists:
        queue_rows = con.execute("""
            SELECT COUNT(*), COUNT(CASE WHEN proposed_tier IS NOT NULL THEN 1 END)
            FROM manuscript_workspace.vc_complication_tiering_v1
        """).fetchone()
        log(f"  295 queue found: {queue_rows[0]} total, {queue_rows[1]} with proposed_tier")
        if queue_rows[1] > 0:
            value_source = "script_295_queue"
            log("  → Will use existing 295 queue (UPDATE didn't land)")
        else:
            log("  → 295 queue empty, rebuilding from phenotype")

    # Build or reuse the tiering queue
    if value_source == "fresh_from_phenotype":
        log("  Building tiering queue from complication_phenotype_v1...")
        con.execute("""
            CREATE OR REPLACE TABLE manuscript_workspace.vc_complication_tiering_v1 AS
            SELECT
                CAST(research_id AS VARCHAR) AS research_id,
                complication_entity,
                confirmed_flag,
                suspected_flag,
                note_mention_flag,
                n_raw_nlp_mentions,
                n_valid_nlp_mentions,
                voice_resolution_noted,
                voice_permanence_noted,
                timing_days_post_surgery,
                final_complication_status AS current_status,
                evidence_tier AS current_tier,
                CASE
                    WHEN confirmed_flag = TRUE AND note_mention_flag = TRUE THEN 1
                    WHEN confirmed_flag = TRUE THEN 2
                    WHEN note_mention_flag = TRUE
                         AND COALESCE(n_valid_nlp_mentions, 0) >= 2 THEN 2
                    WHEN note_mention_flag = TRUE THEN 3
                    WHEN suspected_flag = TRUE THEN 4
                    ELSE NULL
                END AS proposed_tier,
                CASE
                    WHEN confirmed_flag = TRUE THEN 'confirmed'
                    WHEN note_mention_flag = TRUE
                         AND COALESCE(n_valid_nlp_mentions, 0) >= 2 THEN 'confirmed_nlp'
                    WHEN note_mention_flag = TRUE THEN 'suspected_single_mention'
                    WHEN suspected_flag = TRUE THEN 'suspected_only'
                    ELSE 'absent_or_unconfirmed'
                END AS proposed_status,
                CASE
                    WHEN confirmed_flag = TRUE AND note_mention_flag = TRUE
                        THEN 'clinical_confirmed_plus_nlp'
                    WHEN confirmed_flag = TRUE THEN 'clinical_confirmed_only'
                    WHEN note_mention_flag = TRUE
                         AND COALESCE(n_valid_nlp_mentions, 0) >= 2 THEN 'nlp_multi_mention'
                    WHEN note_mention_flag = TRUE THEN 'nlp_single_mention'
                    WHEN suspected_flag = TRUE THEN 'suspected_flag_only'
                    ELSE 'no_evidence'
                END AS proposed_source_tier_label
            FROM main.complication_phenotype_v1
            WHERE complication_entity IN ('vocal_cord_paralysis', 'vocal_cord_paresis')
        """)

    # Summary
    summary = con.execute("""
        SELECT complication_entity, proposed_tier, proposed_status, COUNT(*)
        FROM manuscript_workspace.vc_complication_tiering_v1
        GROUP BY 1, 2, 3 ORDER BY 1, 2
    """).fetchall()
    log("  Tiering queue summary:")
    for s in summary:
        log(f"    {s[0]:25s} tier={s[1]} status={s[2]:30s} n={s[3]}")

    # Reconciliation log
    con.execute(f"""
        CREATE OR REPLACE TABLE manuscript_workspace.script330_vc_tier_reconcile_v1 AS
        SELECT
            research_id,
            complication_entity,
            proposed_tier,
            proposed_status,
            '{value_source}' AS value_source,
            CURRENT_TIMESTAMP AS reconciled_at
        FROM manuscript_workspace.vc_complication_tiering_v1
        WHERE proposed_tier IS NOT NULL
    """)
    recon_n = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.script330_vc_tier_reconcile_v1"
    ).fetchone()[0]
    log(f"  Reconciliation log: {recon_n} rows (source={value_source})")

    if not args.commit:
        log("  (dry-run — no UPDATE)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # Add companion columns if missing
    companion_cols = [
        ("comp_vc_paralysis_first_date", "DATE"),
        ("comp_vc_paralysis_first_note_id", "VARCHAR"),
        ("comp_vc_paralysis_first_evidence_text", "VARCHAR"),
        ("comp_vc_paralysis_resolution_date", "DATE"),
        ("comp_vc_paralysis_n_notes_documenting", "INTEGER"),
        ("comp_vc_paresis_first_date", "DATE"),
        ("comp_vc_paresis_first_note_id", "VARCHAR"),
        ("comp_vc_paresis_first_evidence_text", "VARCHAR"),
        ("comp_vc_paresis_resolution_date", "DATE"),
        ("comp_vc_paresis_n_notes_documenting", "INTEGER"),
    ]
    for col, dtype in companion_cols:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{col}'
        """).fetchone()[0]
        if exists == 0:
            con.execute(f'ALTER TABLE main.canonical_patient_master ADD COLUMN "{col}" {dtype}')
            log(f"    Added CPM column: {col} {dtype}")

    # UPDATE CPM tier columns
    for entity, cpm_col in [
        ("vocal_cord_paralysis", "comp_vc_paralysis_evidence_tier"),
        ("vocal_cord_paresis", "comp_vc_paresis_evidence_tier"),
    ]:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master' AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"  CPM.{cpm_col}: not found — skipping")
            continue

        con.execute(f"""
            UPDATE main.canonical_patient_master AS c
               SET "{cpm_col}" = q.proposed_tier
              FROM manuscript_workspace.vc_complication_tiering_v1 AS q
             WHERE CAST(c.research_id AS VARCHAR) = q.research_id
               AND q.complication_entity = '{entity}'
               AND q.proposed_tier IS NOT NULL
               AND c."{cpm_col}" IS NULL
        """)

        post_pop = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
            WHERE "{cpm_col}" IS NOT NULL
        """).fetchone()[0]
        log(f"  CPM.{cpm_col}: post-pop={post_pop}")

        con.execute("""
            INSERT INTO manuscript_workspace.cpm_backfill_log_v1 VALUES
            (?, ?, ?, ?, ?, ?, ?, ?)
        """, [dt.datetime.utcnow(), cpm_col,
              f"VC tiering ({entity}) via {value_source}",
              "v1 NULL only", post_pop, None, None, SCRIPT])

    # UPDATE companion columns (n_notes, first evidence from complication_phenotype_v1)
    for entity, prefix in [
        ("vocal_cord_paralysis", "comp_vc_paralysis"),
        ("vocal_cord_paresis", "comp_vc_paresis"),
    ]:
        con.execute(f"""
            UPDATE main.canonical_patient_master AS c
               SET "{prefix}_n_notes_documenting" = COALESCE(q.n_valid_nlp_mentions, 0)
                       + CASE WHEN q.confirmed_flag THEN 1 ELSE 0 END
              FROM manuscript_workspace.vc_complication_tiering_v1 AS q
             WHERE CAST(c.research_id AS VARCHAR) = q.research_id
               AND q.complication_entity = '{entity}'
               AND q.proposed_tier IS NOT NULL
               AND c."{prefix}_n_notes_documenting" IS NULL
        """)

    # Update complication_phenotype_v1 tiers too
    con.execute("""
        UPDATE main.complication_phenotype_v1 AS cp
           SET evidence_tier = q.proposed_tier,
               source_tier_label = q.proposed_source_tier_label,
               final_complication_status = q.proposed_status,
               phenotype_version = '330_vc_complication_tiering',
               phenotyped_at = CURRENT_TIMESTAMP
          FROM manuscript_workspace.vc_complication_tiering_v1 AS q
         WHERE CAST(cp.research_id AS VARCHAR) = q.research_id
           AND cp.complication_entity = q.complication_entity
           AND q.proposed_tier IS NOT NULL
    """)
    log("  Updated complication_phenotype_v1 tiers")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 330 complete.")


if __name__ == "__main__":
    main()
