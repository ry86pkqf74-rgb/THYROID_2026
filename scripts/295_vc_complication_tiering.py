"""
Script 295 — VC complication tiering for vocal_cord_paralysis and paresis.

complication_phenotype_v1 has 88 VC paralysis + 71 paresis rows, all stuck at
final_complication_status='absent_or_unconfirmed' even though 32 have
confirmed_flag=TRUE. The tiering logic from Script 235 was applied to
hypocalcemia/hypoparathyroidism but never VC entities.

Approach:
  1. Build a queue table manuscript_workspace.vc_complication_tiering_v1
     with the proposed tier per row BEFORE updating.
  2. Sample 5 evidence_text rows for PHI check.
  3. On --commit, UPDATE complication_phenotype_v1 and roll up to CPM.

Tier rules (adapted from 52_complication_phenotyping_v2.py):
  Tier 1: confirmed_flag=TRUE AND note_mention_flag=TRUE (clinical + NLP)
  Tier 2: confirmed_flag=TRUE OR (note_mention_flag=TRUE AND n_valid_nlp_mentions>=2)
  Tier 3: note_mention_flag=TRUE (single mention)
  Tier 4: suspected_flag=TRUE only

Usage:
    python 295_vc_complication_tiering.py            # dry-run
    python 295_vc_complication_tiering.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "295_vc_complication_tiering"
VC_ENTITIES = ("vocal_cord_paralysis", "vocal_cord_paresis")


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
    log(f"Script 295 — VC complication tiering "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # Step 1: Build queue table with proposed tiers
    log("  Building vc_complication_tiering_v1 queue table...")
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_workspace.vc_complication_tiering_v1 AS
        SELECT
            research_id,
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
                WHEN confirmed_flag = TRUE AND note_mention_flag = TRUE
                    THEN 1
                WHEN confirmed_flag = TRUE
                    THEN 2
                WHEN note_mention_flag = TRUE AND COALESCE(n_valid_nlp_mentions, 0) >= 2
                    THEN 2
                WHEN note_mention_flag = TRUE
                    THEN 3
                WHEN suspected_flag = TRUE
                    THEN 4
                ELSE NULL
            END AS proposed_tier,
            CASE
                WHEN confirmed_flag = TRUE THEN 'confirmed'
                WHEN note_mention_flag = TRUE AND COALESCE(n_valid_nlp_mentions, 0) >= 2
                    THEN 'confirmed_nlp'
                WHEN note_mention_flag = TRUE THEN 'suspected_single_mention'
                WHEN suspected_flag = TRUE THEN 'suspected_only'
                ELSE 'absent_or_unconfirmed'
            END AS proposed_status,
            CASE
                WHEN confirmed_flag = TRUE AND note_mention_flag = TRUE
                    THEN 'clinical_confirmed_plus_nlp'
                WHEN confirmed_flag = TRUE
                    THEN 'clinical_confirmed_only'
                WHEN note_mention_flag = TRUE AND COALESCE(n_valid_nlp_mentions, 0) >= 2
                    THEN 'nlp_multi_mention'
                WHEN note_mention_flag = TRUE
                    THEN 'nlp_single_mention'
                WHEN suspected_flag = TRUE
                    THEN 'suspected_flag_only'
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
    log("  Queue summary:")
    for s in summary:
        log(f"    {s[0]:25s} tier={s[1]} status={s[2]:30s} n={s[3]}")

    # Step 2: PHI safety check on evidence
    log("  PHI safety check (checking evidence_span from source entities)...")
    phi_sample = con.execute("""
        SELECT ne.evidence_span
        FROM main.note_entities_operative_detail ne
        WHERE ne.entity_type IN ('rln_finding')
          AND ne.present_or_negated = 'present'
        LIMIT 5
    """).fetchall()
    for i, s in enumerate(phi_sample):
        snippet = str(s[0])[:80] if s[0] else "NULL"
        log(f"    Sample {i+1}: {snippet}")
    log("  (Evidence text is from operative notes entity extraction — redacted per pipeline)")

    if not args.commit:
        log("  (dry-run — no UPDATE)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    # Step 3: UPDATE complication_phenotype_v1
    log("  Updating complication_phenotype_v1...")
    con.execute("""
        UPDATE main.complication_phenotype_v1 AS cp
           SET evidence_tier = q.proposed_tier,
               source_tier_label = q.proposed_source_tier_label,
               final_complication_status = q.proposed_status,
               phenotype_version = '295_vc_complication_tiering',
               phenotyped_at = CURRENT_TIMESTAMP
          FROM manuscript_workspace.vc_complication_tiering_v1 AS q
         WHERE cp.research_id = q.research_id
           AND cp.complication_entity = q.complication_entity
           AND q.proposed_tier IS NOT NULL
    """)

    post_vc = con.execute("""
        SELECT complication_entity, final_complication_status, evidence_tier, COUNT(*)
        FROM main.complication_phenotype_v1
        WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
        GROUP BY 1, 2, 3 ORDER BY 1, 2
    """).fetchall()
    log("  Post-update VC rows:")
    for v in post_vc:
        log(f"    {v[0]:25s} status={v[1]:30s} tier={v[2]} n={v[3]}")

    # Step 4: Roll up to CPM
    log("  Rolling up to CPM vc evidence tier columns...")
    for entity, cpm_col in [
        ("vocal_cord_paralysis", "comp_vc_paralysis_evidence_tier"),
        ("vocal_cord_paresis", "comp_vc_paresis_evidence_tier"),
    ]:
        col_exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
            WHERE table_name = 'canonical_patient_master'
              AND column_name = '{cpm_col}'
        """).fetchone()[0]
        if col_exists == 0:
            log(f"  CPM column {cpm_col} does not exist — skipping")
            continue

        con.execute(f"""
            UPDATE main.canonical_patient_master AS c
               SET "{cpm_col}" = q.proposed_tier
              FROM manuscript_workspace.vc_complication_tiering_v1 AS q
             WHERE c.research_id = q.research_id
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
        """, [
            dt.datetime.utcnow(), cpm_col,
            f"VC tiering from complication_phenotype_v1 ({entity})",
            "v1 NULL only", post_pop, None, None, SCRIPT
        ])

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 295 complete.")


if __name__ == "__main__":
    main()
