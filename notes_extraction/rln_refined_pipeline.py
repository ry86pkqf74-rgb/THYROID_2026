"""
Second-pass RLN injury refinement pipeline.

Creates `extracted_rln_injury_refined_v2` in MotherDuck with context-aware
classification of NLP-extracted RLN injury mentions, dramatically reducing
false positives from boilerplate risk discussions and preservation language.

Generalizable via `refine_extraction()` for any complication entity.

Usage:
    python -m notes_extraction.rln_refined_pipeline --md
    python -m notes_extraction.rln_refined_pipeline --local --dry-run
"""

from __future__ import annotations

import argparse
import sys
import textwrap
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from notes_extraction.intrinsic_evaluator import IntrinsicEvaluator, refine_extraction  # noqa: E402

# ---------------------------------------------------------------------------
# SQL: Refined RLN view with context-based classification
# ---------------------------------------------------------------------------

EXTRACTED_RLN_REFINED_V2_SQL = textwrap.dedent(r"""
    CREATE OR REPLACE TABLE extracted_rln_injury_refined_v2 AS
    WITH surgical_patients AS (
        SELECT CAST(research_id AS INT) AS research_id,
               MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
        FROM path_synoptics
        WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
        GROUP BY 1
    ),

    -- Tier 1: Laryngoscopy-confirmed
    tier1 AS (
        SELECT DISTINCT
            CAST(c.research_id AS INT) AS research_id,
            TRY_CAST(c.laryngoscopy_date AS DATE) AS detection_date,
            LOWER(TRIM(CAST(c.vocal_cord_status AS VARCHAR))) AS injury_type,
            c.affected_side AS laterality,
            1 AS rln_injury_tier,
            'strong' AS rln_injury_evidence_strength,
            TRUE AS rln_injury_is_confirmed,
            'laryngoscopy_confirmed' AS classification,
            CASE
                WHEN DATEDIFF('day', sp.first_surgery_date,
                     TRY_CAST(c.laryngoscopy_date AS DATE)) <= 180
                THEN 'transient_or_recovering'
                ELSE 'permanent'
            END AS temporality
        FROM complications c
        JOIN surgical_patients sp
            ON CAST(c.research_id AS INT) = sp.research_id
        WHERE LOWER(TRIM(CAST(c.vocal_cord_status AS VARCHAR)))
              IN ('paresis', 'paralysis')
          AND TRY_CAST(c.laryngoscopy_date AS DATE) > sp.first_surgery_date
    ),

    -- Tier 2: Chart-documented rln_injury='yes'
    tier2 AS (
        SELECT DISTINCT
            CAST(c.research_id AS INT) AS research_id,
            COALESCE(TRY_CAST(c.laryngoscopy_date AS DATE),
                     sp.first_surgery_date) AS detection_date,
            COALESCE(
                NULLIF(LOWER(TRIM(CAST(c.vocal_cord_status AS VARCHAR))), ''),
                'chart_documented'
            ) AS injury_type,
            c.affected_side AS laterality,
            2 AS rln_injury_tier,
            'medium' AS rln_injury_evidence_strength,
            TRUE AS rln_injury_is_confirmed,
            'chart_documented' AS classification,
            'unknown' AS temporality
        FROM complications c
        JOIN surgical_patients sp
            ON CAST(c.research_id AS INT) = sp.research_id
        WHERE LOWER(TRIM(CAST(
            c.rln_injury_or_vocal_cord_paralysis_vocal_cord_palsy AS VARCHAR
        ))) = 'yes'
          AND CAST(c.research_id AS INT)
              NOT IN (SELECT research_id FROM tier1)
    ),

    -- Tier 3: NLP-extracted with CONTEXT-BASED FILTERING
    nlp_raw AS (
        SELECT
            CAST(ne.research_id AS INT) AS research_id,
            COALESCE(ne.inferred_event_date,
                     TRY_CAST(ne.entity_date AS DATE),
                     TRY_CAST(ne.note_date AS DATE)) AS detection_date,
            ne.entity_value_norm,
            ne.note_type,
            COALESCE(TRY_CAST(ne.confidence AS DOUBLE), 0.9) AS confidence,
            ne.evidence_start,
            ne.note_row_id,
            sp.first_surgery_date,
            DATEDIFF('day', sp.first_surgery_date,
                     COALESCE(ne.inferred_event_date,
                              TRY_CAST(ne.entity_date AS DATE),
                              TRY_CAST(ne.note_date AS DATE))) AS days_post_surgery
        FROM note_entities_complications ne
        JOIN surgical_patients sp
            ON CAST(ne.research_id AS INT) = sp.research_id
        WHERE LOWER(ne.entity_value_norm) IN (
            'rln_injury', 'vocal_cord_paralysis', 'vocal_cord_paresis'
        )
        AND LOWER(COALESCE(ne.present_or_negated, '')) = 'present'
        AND (ne.confidence IS NULL
             OR TRY_CAST(ne.confidence AS DOUBLE) >= 0.65)
        AND COALESCE(ne.inferred_event_date,
                     TRY_CAST(ne.entity_date AS DATE),
                     TRY_CAST(ne.note_date AS DATE)) >= sp.first_surgery_date
        AND CAST(ne.research_id AS INT) NOT IN (
            SELECT research_id FROM tier1
            UNION SELECT research_id FROM tier2
        )
    ),

    -- Join with clinical notes for context-based classification
    nlp_with_context AS (
        SELECT
            nr.*,
            SUBSTRING(cn.note_text,
                      GREATEST(1, CAST(nr.evidence_start AS INT) - 300),
                      800) AS context_window
        FROM nlp_raw nr
        LEFT JOIN clinical_notes_long cn
            ON nr.note_row_id = cn.note_row_id
    ),

    -- Apply context-based filtering rules in SQL
    nlp_classified AS (
        SELECT
            research_id,
            detection_date,
            entity_value_norm,
            note_type,
            confidence,
            days_post_surgery,
            context_window,
            CASE
                -- Rule 1: Risk discussion in context (NOT in diagnosis section)
                WHEN (
                    REGEXP_MATCHES(LOWER(context_window),
                        'risk(?:s)?\\s+(?:of|including|for|and benefits)')
                    OR REGEXP_MATCHES(LOWER(context_window),
                        'discussed\\s+.*(?:risk|complication|consent)')
                    OR REGEXP_MATCHES(LOWER(context_window),
                        'counseled\\s+.*(?:risk|complication)')
                    OR REGEXP_MATCHES(LOWER(context_window),
                        'potential\\s+complication')
                    OR REGEXP_MATCHES(LOWER(context_window),
                        'informed\\s+consent')
                )
                AND NOT REGEXP_MATCHES(LOWER(context_window),
                    '(?:diagnos[ie]s?|assessment|complications?|impression)\s*:.*(?:rln|nerve\s+injur|cord\s+p(?:aralys|ares))')
                THEN 'risk_mentioned'

                -- Rule 2: Preservation/intact language
                WHEN REGEXP_MATCHES(LOWER(context_window),
                    '(?:identified|visualized)\s+and\s+(?:preserved|protected|intact)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    '(?:nerve|rln)\s+(?:was\s+)?(?:preserved|intact|protected)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    'no\s+(?:evidence\s+of\s+)?(?:injury|damage)')
                THEN 'identified_preserved'

                -- Rule 3: Historical reference
                WHEN REGEXP_MATCHES(LOWER(context_window),
                    '(?:history|hx)\s+of\b.*(?:rln|nerve\s+injur|cord\s+p)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    '(?:prior|previous|pre[\s-]?existing)\s+.*(?:rln|nerve\s+injur|cord\s+p)')
                THEN 'historical'

                -- Rule 4: Same-day H&P with generic rln_injury
                WHEN days_post_surgery = 0
                     AND note_type IN ('h_p', 'history_summary', 'other_history')
                     AND entity_value_norm = 'rln_injury'
                     AND NOT REGEXP_MATCHES(LOWER(context_window),
                         '(?:diagnos[ie]s?|assessment|complications?|impression)\s*:')
                THEN 'same_day_hp_excluded'

                -- Rule 5: True injury language detected
                WHEN REGEXP_MATCHES(LOWER(context_window),
                    '(?:noted|found|observed)\s+(?:to\s+have\s+)?(?:rln|nerve|vocal\s+cord)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    '(?:postoperative|post[\s-]?op)\s+.*(?:hoarseness|dysphonia|vocal\s+cord)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    '(?:scope|laryngoscopy)\s+show(?:s|ed)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    's/p\s+.*(?:thyroidectomy|surgery)\b.*(?:cord|rln|nerve)')
                  OR REGEXP_MATCHES(LOWER(context_window),
                    '(?:bilateral|unilateral|left|right)\s+(?:vocal\s+)?(?:cord|fold)\s+(?:paralys|pares)')
                THEN 'confirmed'

                -- Rule 6: In diagnosis/assessment section
                WHEN REGEXP_MATCHES(LOWER(context_window),
                    '(?:diagnos[ie]s?|assessment|complications?|impression|problem\s+list)\s*:.*(?:rln|nerve\s+injur|cord\s+p(?:aralys|ares)|vcp)')
                THEN 'confirmed'

                -- Rule 7: Specific entities (VCP/VCParesis) post-day-0
                WHEN entity_value_norm IN ('vocal_cord_paralysis', 'vocal_cord_paresis')
                     AND days_post_surgery > 0
                THEN 'confirmed'

                -- Rule 8: Same-day specific entities in non-H&P notes
                WHEN entity_value_norm IN ('vocal_cord_paralysis', 'vocal_cord_paresis')
                     AND days_post_surgery = 0
                     AND note_type NOT IN ('h_p', 'history_summary', 'other_history')
                THEN 'suspected'

                -- Fallthrough
                ELSE 'suspected'
            END AS nlp_classification
        FROM nlp_with_context
    ),

    -- Aggregate per patient: keep best classification
    tier3_refined AS (
        SELECT
            research_id,
            MIN(detection_date) AS detection_date,
            -- Best injury type across mentions
            CASE
                WHEN SUM(CASE WHEN entity_value_norm = 'vocal_cord_paralysis'
                              AND nlp_classification IN ('confirmed', 'suspected')
                         THEN 1 ELSE 0 END) > 0 THEN 'paralysis'
                WHEN SUM(CASE WHEN entity_value_norm = 'vocal_cord_paresis'
                              AND nlp_classification IN ('confirmed', 'suspected')
                         THEN 1 ELSE 0 END) > 0 THEN 'paresis'
                WHEN SUM(CASE WHEN nlp_classification IN ('confirmed', 'suspected')
                         THEN 1 ELSE 0 END) > 0 THEN 'rln_injury'
                ELSE 'excluded'
            END AS injury_type,
            CAST(NULL AS VARCHAR) AS laterality,
            CASE
                WHEN MAX(CASE WHEN nlp_classification = 'confirmed' THEN 1 ELSE 0 END) = 1
                THEN 3
                WHEN MAX(CASE WHEN nlp_classification = 'suspected' THEN 1 ELSE 0 END) = 1
                THEN 3
                ELSE 0
            END AS rln_injury_tier,
            CASE
                WHEN MAX(CASE WHEN nlp_classification = 'confirmed' THEN 1 ELSE 0 END) = 1
                THEN 'medium'
                WHEN MAX(CASE WHEN nlp_classification = 'suspected' THEN 1 ELSE 0 END) = 1
                THEN 'weak'
                ELSE 'none'
            END AS rln_injury_evidence_strength,
            MAX(CASE WHEN nlp_classification = 'confirmed' THEN 1 ELSE 0 END) = 1
                AS rln_injury_is_confirmed,
            CASE
                WHEN MAX(CASE WHEN nlp_classification = 'confirmed' THEN 1 ELSE 0 END) = 1
                THEN 'nlp_confirmed'
                WHEN MAX(CASE WHEN nlp_classification = 'suspected' THEN 1 ELSE 0 END) = 1
                THEN 'nlp_suspected'
                ELSE 'nlp_excluded'
            END AS classification,
            'unknown' AS temporality,
            -- Audit columns
            COUNT(*) AS total_mentions,
            SUM(CASE WHEN nlp_classification = 'confirmed' THEN 1 ELSE 0 END) AS confirmed_mentions,
            SUM(CASE WHEN nlp_classification = 'risk_mentioned' THEN 1 ELSE 0 END) AS risk_mentions,
            SUM(CASE WHEN nlp_classification = 'identified_preserved' THEN 1 ELSE 0 END) AS preserved_mentions,
            SUM(CASE WHEN nlp_classification = 'same_day_hp_excluded' THEN 1 ELSE 0 END) AS same_day_hp_excluded,
            SUM(CASE WHEN nlp_classification = 'historical' THEN 1 ELSE 0 END) AS historical_mentions
        FROM nlp_classified
        GROUP BY research_id
        HAVING MAX(CASE WHEN nlp_classification IN ('confirmed', 'suspected') THEN 1 ELSE 0 END) = 1
    ),

    -- Combine all tiers
    all_tiers AS (
        SELECT research_id, detection_date, injury_type, laterality,
               rln_injury_tier, rln_injury_evidence_strength,
               rln_injury_is_confirmed, classification, temporality
        FROM tier1
        UNION ALL
        SELECT research_id, detection_date, injury_type, laterality,
               rln_injury_tier, rln_injury_evidence_strength,
               rln_injury_is_confirmed, classification, temporality
        FROM tier2
        UNION ALL
        SELECT research_id, detection_date, injury_type, laterality,
               rln_injury_tier, rln_injury_evidence_strength,
               rln_injury_is_confirmed, classification, temporality
        FROM tier3_refined
    )

    SELECT
        a.research_id,
        a.detection_date,
        a.injury_type,
        a.laterality,
        a.rln_injury_tier,
        a.rln_injury_evidence_strength,
        a.rln_injury_is_confirmed,
        a.classification,
        a.temporality,
        sp.first_surgery_date,
        DATEDIFF('day', sp.first_surgery_date, a.detection_date) AS days_post_surgery,
        CASE
            WHEN a.detection_date IS NOT NULL
                 AND a.detection_date <= sp.first_surgery_date + INTERVAL '30 days'
            THEN '0-30d'
            WHEN a.detection_date IS NOT NULL
                 AND a.detection_date <= sp.first_surgery_date + INTERVAL '180 days'
            THEN '31-180d'
            WHEN a.detection_date IS NOT NULL
                 AND a.detection_date <= sp.first_surgery_date + INTERVAL '365 days'
            THEN '181-365d'
            WHEN a.detection_date IS NOT NULL
            THEN '>365d'
            ELSE 'unknown'
        END AS temporal_window
    FROM all_tiers a
    JOIN (
        SELECT CAST(research_id AS INT) AS research_id,
               MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
        FROM path_synoptics
        WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
        GROUP BY 1
    ) sp ON a.research_id = sp.research_id
""")


# Summary KPI view
RLN_REFINED_SUMMARY_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE extracted_rln_injury_refined_summary_v2 AS
    SELECT
        COUNT(DISTINCT r.research_id) AS total_rln_injury_patients,
        ROUND(100.0 * COUNT(DISTINCT r.research_id) / NULLIF(
            (SELECT COUNT(DISTINCT CAST(research_id AS INT))
             FROM path_synoptics
             WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL), 0
        ), 2) AS rln_injury_rate_pct,
        COUNT(DISTINCT CASE WHEN r.rln_injury_is_confirmed THEN r.research_id END)
            AS confirmed_patients,
        COUNT(DISTINCT CASE WHEN r.rln_injury_tier = 1 THEN r.research_id END)
            AS tier1_laryngoscopy,
        COUNT(DISTINCT CASE WHEN r.rln_injury_tier = 2 THEN r.research_id END)
            AS tier2_chart,
        COUNT(DISTINCT CASE WHEN r.rln_injury_tier = 3
                            AND r.rln_injury_is_confirmed THEN r.research_id END)
            AS tier3_nlp_confirmed,
        COUNT(DISTINCT CASE WHEN r.rln_injury_tier = 3
                            AND NOT r.rln_injury_is_confirmed THEN r.research_id END)
            AS tier3_nlp_suspected,
        ROUND(100.0 * COUNT(DISTINCT CASE WHEN r.rln_injury_is_confirmed THEN r.research_id END)
            / NULLIF(
                (SELECT COUNT(DISTINCT CAST(research_id AS INT))
                 FROM path_synoptics
                 WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL), 0
            ), 2) AS confirmed_rate_pct
    FROM extracted_rln_injury_refined_v2 r
""")


# Audit view: shows what was excluded and why
RLN_EXCLUSION_AUDIT_SQL = textwrap.dedent("""\
    CREATE OR REPLACE TABLE extracted_rln_exclusion_audit_v2 AS
    WITH surgical_patients AS (
        SELECT CAST(research_id AS INT) AS research_id,
               MIN(TRY_CAST(surg_date AS DATE)) AS first_surgery_date
        FROM path_synoptics
        WHERE TRY_CAST(surg_date AS DATE) IS NOT NULL
        GROUP BY 1
    ),
    all_nlp AS (
        SELECT DISTINCT CAST(ne.research_id AS INT) AS research_id
        FROM note_entities_complications ne
        JOIN surgical_patients sp
            ON CAST(ne.research_id AS INT) = sp.research_id
        WHERE LOWER(ne.entity_value_norm) IN (
            'rln_injury', 'vocal_cord_paralysis', 'vocal_cord_paresis'
        )
        AND LOWER(COALESCE(ne.present_or_negated, '')) = 'present'
        AND (ne.confidence IS NULL OR TRY_CAST(ne.confidence AS DOUBLE) >= 0.65)
        AND COALESCE(ne.inferred_event_date,
                     TRY_CAST(ne.entity_date AS DATE),
                     TRY_CAST(ne.note_date AS DATE)) >= sp.first_surgery_date
    ),
    refined AS (
        SELECT DISTINCT research_id FROM extracted_rln_injury_refined_v2
    )
    SELECT
        (SELECT COUNT(*) FROM all_nlp) AS original_tier3_patients,
        (SELECT COUNT(*) FROM refined WHERE research_id IN (
            SELECT research_id FROM extracted_rln_injury_refined_v2
            WHERE rln_injury_tier = 3
        )) AS refined_tier3_patients,
        (SELECT COUNT(*) FROM all_nlp
         WHERE research_id NOT IN (SELECT research_id FROM refined)
        ) AS excluded_patients,
        (SELECT COUNT(*) FROM refined) AS total_refined_patients
""")


def _get_connection(use_md: bool) -> duckdb.DuckDBPyConnection:
    if use_md:
        import toml
        secrets = toml.load(str(PROJECT_ROOT / ".streamlit" / "secrets.toml"))
        token = secrets["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(str(PROJECT_ROOT / "thyroid_master.duckdb"))


def deploy(con: duckdb.DuckDBPyConnection, dry_run: bool = False) -> dict:
    """Deploy the refined RLN views to the database."""
    results = {}
    for name, sql in [
        ("extracted_rln_injury_refined_v2", EXTRACTED_RLN_REFINED_V2_SQL),
        ("extracted_rln_injury_refined_summary_v2", RLN_REFINED_SUMMARY_SQL),
        ("extracted_rln_exclusion_audit_v2", RLN_EXCLUSION_AUDIT_SQL),
    ]:
        if dry_run:
            print(f"[DRY RUN] Would create: {name}")
            results[name] = -1
            continue
        print(f"Creating {name} ...", end=" ", flush=True)
        con.execute(sql)
        cnt = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
        print(f"{cnt} rows")
        results[name] = cnt
    return results


def run_intrinsic_evaluation(
    con: duckdb.DuckDBPyConnection,
    sample_size: int = 200,
) -> tuple[pd.DataFrame, str]:
    """Run the intrinsic evaluation on a sample and generate a report."""
    return refine_extraction(con, entity_name="rln_injury", sample_size=sample_size)


def main():
    parser = argparse.ArgumentParser(description="RLN injury refinement pipeline")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL without executing")
    parser.add_argument("--eval-only", action="store_true", help="Run intrinsic evaluation only")
    parser.add_argument("--sample-size", type=int, default=200)
    args = parser.parse_args()

    use_md = args.md or not args.local
    con = _get_connection(use_md)
    target = "MotherDuck" if use_md else "local DuckDB"
    print(f"Connected to {target}")

    if args.eval_only:
        print(f"\n=== Running intrinsic evaluation (n={args.sample_size}) ===")
        eval_df, report = run_intrinsic_evaluation(con, args.sample_size)
        out_dir = PROJECT_ROOT / "notes_extraction"
        eval_df.to_parquet(out_dir / "rln_intrinsic_eval_results.parquet", index=False)
        report_path = out_dir / "rln_intrinsic_eval_report.md"
        report_path.write_text(report)
        print(f"Results: {out_dir / 'rln_intrinsic_eval_results.parquet'}")
        print(f"Report: {report_path}")
        print(report)
        con.close()
        return

    print("\n=== Deploying refined RLN views ===")
    results = deploy(con, dry_run=args.dry_run)

    if not args.dry_run:
        print("\n=== Summary ===")
        summary = con.execute("SELECT * FROM extracted_rln_injury_refined_summary_v2").fetchdf()
        print(summary.to_string(index=False))

        print("\n=== Exclusion Audit ===")
        audit = con.execute("SELECT * FROM extracted_rln_exclusion_audit_v2").fetchdf()
        print(audit.to_string(index=False))

        print(f"\n=== Running intrinsic evaluation (n={args.sample_size}) ===")
        eval_df, report = run_intrinsic_evaluation(con, args.sample_size)
        out_dir = PROJECT_ROOT / "notes_extraction"
        eval_df.to_parquet(out_dir / "rln_intrinsic_eval_results.parquet", index=False)
        report_path = out_dir / "rln_intrinsic_eval_report.md"
        report_path.write_text(report)
        print(f"Evaluation report saved to {report_path}")

    con.close()
    print("\nDone!")


if __name__ == "__main__":
    main()
