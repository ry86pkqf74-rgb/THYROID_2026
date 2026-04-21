"""
Script 302 — Build genetics_per_test_master_v1.

Grain: (research_id, molecular_episode_id) — one row per molecular test.

Joins five source tables into a single wide fact table at the molecular-test
level.  Writes a discordance queue for source disagreements and the master
table itself.

Source tables
  molecular_test_episode_v2       (10,650 rows, 10,026 patients)
  molecular_results               (10,861 rows)
  molecular_variant_long          (1,640 rows, 703 patients)
  thyroseq_molecular_enrichment   (10,861 rows)
  note_entities_genetics          (1,738 rows, 605 patients)
  canonical_molecular_tested_v1   (1,286 rows)

Usage:
    python 302_genetics_per_test_master_v1.py            # dry-run
    python 302_genetics_per_test_master_v1.py --commit   # apply
"""
from __future__ import annotations

import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "302_genetics_per_test_master_v1"


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
        CREATE TABLE IF NOT EXISTS manuscript_workspace.archive_move_log_v1 (
            moved_at TIMESTAMP,
            src_schema VARCHAR,
            src_table VARCHAR,
            archive_fq VARCHAR,
            n_rows BIGINT,
            reason VARCHAR,
            script VARCHAR
        )
    """)


# ── Step 1: Filter molecular_test_episode_v2 to real tests ──────────────

REAL_TESTS_CTE = """
real_episodes AS (
    SELECT *
    FROM main.molecular_test_episode_v2
    WHERE (platform IS NOT NULL AND platform != 'Other')
       OR braf_flag = TRUE
       OR ras_flag = TRUE
       OR tert_flag = TRUE
       OR tp53_flag = TRUE
       OR ret_flag = TRUE
       OR ret_fusion_flag = TRUE
       OR ntrk_flag = TRUE
       OR alk_flag = TRUE
       OR fusion_flag = TRUE
)
"""


# ── Step 3: Roll up molecular_variant_long to episode grain ─────────────

VARIANT_ROLLUP_CTE = """
variant_rollup AS (
    SELECT
        research_id,
        COUNT(*)                          AS n_variants,
        STRING_AGG(
            '{'
            || '"gene":"'        || COALESCE(gene_symbol, '')      || '"'
            || ',"hgvs":"'       || COALESCE(canonical_hgvs, '')   || '"'
            || ',"protein":"'    || COALESCE(protein_hgvs, '')     || '"'
            || ',"af":"'         || COALESCE(CAST(allele_fraction AS VARCHAR), '') || '"'
            || ',"zygosity":"'   || COALESCE(zygosity, '')         || '"'
            || ',"class":"'      || COALESCE(variant_class, '')    || '"'
            || ',"fusion":"'     || COALESCE(fusion_partner, '')   || '"'
            || ',"risk":"'       || COALESCE(risk_call, '')        || '"'
            || '}',
            ','
        ) AS variants_json
    FROM main.molecular_variant_long
    GROUP BY research_id
)
"""


# ── Step 5: Roll up note_entities_genetics by research_id ───────────────

ENTITY_ROLLUP_CTE = """
entity_rollup AS (
    SELECT
        research_id,
        STRING_AGG(
            DISTINCT entity_type || ':' || COALESCE(entity_value_norm, ''),
            '|'
        ) AS entity_summary
    FROM main.note_entities_genetics
    GROUP BY research_id
)
"""


# ── Master query ────────────────────────────────────────────────────────

MASTER_SQL = f"""
WITH
{REAL_TESTS_CTE},
{VARIANT_ROLLUP_CTE},
{ENTITY_ROLLUP_CTE}
SELECT
    ep.research_id,
    ep.molecular_episode_id,
    ep.test_date_native AS test_date,
    ep.platform,
    mr.assay_name,
    mr.panel_version,
    ep.bethesda_category,

    -- Per-gene flags: never coerce untested (NULL) to FALSE
    ep.braf_flag                                 AS braf_positive_this_test,
    ep.ras_flag                                  AS ras_positive_this_test,
    ep.tert_flag                                 AS tert_positive_this_test,
    ep.tp53_flag                                 AS tp53_positive_this_test,
    COALESCE(ep.fusion_flag,
             ep.ret_fusion_flag,
             ep.ntrk_flag,
             ep.alk_flag)                        AS any_fusion_flag,

    vr.n_variants,
    cmt.molecular_risk_tier,
    tse.cna_raw,
    tse.gep_raw,
    vr.variants_json,

    -- Source provenance booleans
    TRUE                                         AS source_episode_v2,
    (mr.research_id IS NOT NULL)                 AS source_results,
    (vr.research_id IS NOT NULL)                 AS source_variant_long,
    (tse.research_id IS NOT NULL)                AS source_enrichment,
    (ent.research_id IS NOT NULL)                AS source_entities

FROM real_episodes ep

-- Step 2: LEFT-join molecular_results (best-effort on research_id)
LEFT JOIN (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id
               ORDER BY assay_name NULLS LAST
           ) AS rn
    FROM main.molecular_results
) mr ON mr.research_id = ep.research_id AND mr.rn = 1

-- Step 3: LEFT-join rolled-up variants
LEFT JOIN variant_rollup vr ON vr.research_id = ep.research_id

-- Step 4: LEFT-join thyroseq_molecular_enrichment
LEFT JOIN (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY research_id ORDER BY imported_at NULLS LAST
           ) AS rn
    FROM main.thyroseq_molecular_enrichment
) tse ON tse.research_id = ep.research_id AND tse.rn = 1

-- Step 5: LEFT-join entity rollup
LEFT JOIN entity_rollup ent ON ent.research_id = ep.research_id

-- Step 6: LEFT-join canonical_molecular_tested for risk tier
LEFT JOIN main.canonical_molecular_tested_v1 cmt
    ON CAST(cmt.research_id AS BIGINT) = ep.research_id
"""


# ── Step 6: Discordance queue ───────────────────────────────────────────

DISCORDANCE_SQL = """
WITH ep AS (
    SELECT research_id, braf_flag, ras_flag, tert_flag
    FROM main.molecular_test_episode_v2
    WHERE platform IS NOT NULL
       OR bethesda_category IS NOT NULL
       OR braf_flag IS NOT NULL
       OR ras_flag IS NOT NULL
       OR tert_flag IS NOT NULL
       OR tp53_flag IS NOT NULL
       OR ret_flag IS NOT NULL
       OR ret_fusion_flag IS NOT NULL
       OR ntrk_flag IS NOT NULL
       OR alk_flag IS NOT NULL
       OR fusion_flag IS NOT NULL
)
SELECT
    COALESCE(ep.research_id, CAST(cmt.research_id AS BIGINT)) AS research_id,
    ep.braf_flag              AS ep_braf,
    cmt.braf_positive_canonical AS cmt_braf,
    ep.ras_flag               AS ep_ras,
    cmt.ras_positive_canonical AS cmt_ras,
    ep.tert_flag              AS ep_tert,
    cmt.tert_positive_canonical AS cmt_tert,
    CASE
        WHEN ep.braf_flag IS NOT NULL
         AND cmt.braf_positive_canonical IS NOT NULL
         AND ep.braf_flag != cmt.braf_positive_canonical THEN 'braf_mismatch'
        WHEN ep.ras_flag IS NOT NULL
         AND cmt.ras_positive_canonical IS NOT NULL
         AND ep.ras_flag != cmt.ras_positive_canonical THEN 'ras_mismatch'
        WHEN ep.tert_flag IS NOT NULL
         AND cmt.tert_positive_canonical IS NOT NULL
         AND ep.tert_flag != cmt.tert_positive_canonical THEN 'tert_mismatch'
        ELSE 'other'
    END AS discordance_type,
    'episode_v2 vs canonical_molecular_tested_v1' AS comparison
FROM ep
FULL OUTER JOIN main.canonical_molecular_tested_v1 cmt
    ON CAST(cmt.research_id AS BIGINT) = ep.research_id
WHERE (
    (ep.braf_flag IS NOT NULL AND cmt.braf_positive_canonical IS NOT NULL
     AND ep.braf_flag != cmt.braf_positive_canonical)
    OR
    (ep.ras_flag IS NOT NULL AND cmt.ras_positive_canonical IS NOT NULL
     AND ep.ras_flag != cmt.ras_positive_canonical)
    OR
    (ep.tert_flag IS NOT NULL AND cmt.tert_positive_canonical IS NOT NULL
     AND ep.tert_flag != cmt.tert_positive_canonical)
)
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_table(con)
    log("=" * 72)
    log(f"Script 302 — genetics_per_test_master_v1 "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)

    cpm_invariants(con, "pre")

    # ── Step 1: Count real episodes ─────────────────────────────────────
    log("Step 1: Filtering molecular_test_episode_v2 to real tests...")
    n_real = con.execute(f"""
        WITH {REAL_TESTS_CTE}
        SELECT COUNT(*), COUNT(DISTINCT research_id) FROM real_episodes
    """).fetchone()
    log(f"  Real episodes: {n_real[0]} rows, {n_real[1]} distinct patients")
    if not (1100 <= n_real[1] <= 1500):
        raise SystemExit(
            f"Expected ~1,286 distinct patients, got {n_real[1]}. "
            "Check filter criteria."
        )

    # ── Source counts ───────────────────────────────────────────────────
    log("  Source table counts:")
    for tbl, label in [
        ("molecular_results", "molecular_results"),
        ("molecular_variant_long", "molecular_variant_long"),
        ("thyroseq_molecular_enrichment", "thyroseq_enrichment"),
        ("note_entities_genetics", "note_entities_genetics"),
        ("canonical_molecular_tested_v1", "canonical_molecular_tested_v1"),
    ]:
        r = con.execute(f"""
            SELECT COUNT(*), COUNT(DISTINCT research_id) FROM main.{tbl}
        """).fetchone()
        log(f"    {label}: {r[0]} rows, {r[1]} patients")

    # ── Step 6: Discordance queue ───────────────────────────────────────
    log("Step 6: Building discordance queue...")
    con.execute(f"""
        CREATE OR REPLACE TABLE
            manuscript_workspace.genetics_per_test_discordance_v1
        AS {DISCORDANCE_SQL}
    """)
    n_disc = con.execute("""
        SELECT COUNT(*)
        FROM manuscript_workspace.genetics_per_test_discordance_v1
    """).fetchone()[0]
    log(f"  Discordance rows: {n_disc}")

    if n_disc > 0:
        disc_summary = con.execute("""
            SELECT discordance_type, COUNT(*)
            FROM manuscript_workspace.genetics_per_test_discordance_v1
            GROUP BY 1 ORDER BY 2 DESC
        """).fetchall()
        for d in disc_summary:
            log(f"    {d[0]}: {d[1]}")

    # ── Step 7: Build master ────────────────────────────────────────────
    log("Step 7: Building genetics_per_test_master_v1...")

    preview = con.execute(f"""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM ({MASTER_SQL}) t
    """).fetchone()
    log(f"  Master preview: {preview[0]} rows, {preview[1]} patients")

    source_coverage = con.execute(f"""
        SELECT
            SUM(CASE WHEN source_results      THEN 1 ELSE 0 END) AS n_results,
            SUM(CASE WHEN source_variant_long  THEN 1 ELSE 0 END) AS n_variants,
            SUM(CASE WHEN source_enrichment    THEN 1 ELSE 0 END) AS n_enrichment,
            SUM(CASE WHEN source_entities      THEN 1 ELSE 0 END) AS n_entities
        FROM ({MASTER_SQL}) t
    """).fetchone()
    log(f"  Source coverage: results={source_coverage[0]} "
        f"variants={source_coverage[1]} enrichment={source_coverage[2]} "
        f"entities={source_coverage[3]}")

    if not args.commit:
        log("  (dry-run — no CREATE TABLE)")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run — re-run with --commit to apply)")
        return

    con.execute(f"""
        CREATE OR REPLACE TABLE main.genetics_per_test_master_v1
        AS {MASTER_SQL}
    """)

    n_final = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id)
        FROM main.genetics_per_test_master_v1
    """).fetchone()
    log(f"  Created genetics_per_test_master_v1: "
        f"{n_final[0]} rows, {n_final[1]} patients")

    # Sanity: no untested gene coerced to FALSE
    null_check = con.execute("""
        SELECT
            SUM(CASE WHEN braf_positive_this_test IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN ras_positive_this_test  IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN tert_positive_this_test IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN tp53_positive_this_test IS NULL THEN 1 ELSE 0 END)
        FROM main.genetics_per_test_master_v1
    """).fetchone()
    log(f"  NULL gene flags (expected >0 for untested): "
        f"braf={null_check[0]} ras={null_check[1]} "
        f"tert={null_check[2]} tp53={null_check[3]}")

    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 302 complete.")


if __name__ == "__main__":
    main()
