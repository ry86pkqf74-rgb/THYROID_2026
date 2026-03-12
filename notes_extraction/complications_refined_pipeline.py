"""
Complication Extraction Refinement Pipeline — Phase 2 QA.

Deploys context-aware, SQL-based refined extraction tables for all 6 complication
entities with confirmed >85% false-positive rate in the Phase 2 intrinsic audit:

  - chyle_leak      (0% precision raw → target 70%+)
  - hypocalcemia    (6% precision raw → target 75%+)
  - seroma          (0% precision raw → target 70%+)
  - hypoparathyroidism (15% precision raw → target 70%+)
  - hematoma        (0% precision raw → target 70%+)
  - wound_infection (SSI collision → target 70%+)

Creates:
  1. Individual refined tables per entity:
       extracted_{entity}_refined_v2
  2. Consolidated summary:
       extracted_complications_refined_v5   (UNION ALL of all entities + structured data)
  3. Audit/exclusion trail:
       extracted_complications_exclusion_audit_v2

Usage:
    python -m notes_extraction.complications_refined_pipeline --md
    python -m notes_extraction.complications_refined_pipeline --local --dry-run
"""

from __future__ import annotations

import argparse
import json
import textwrap
from datetime import datetime
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# SQL helpers — shared CTE blocks
# ---------------------------------------------------------------------------

# The canonical consent boilerplate signature that contaminates ALL entities
CONSENT_BOILERPLATE_SQL = textwrap.dedent("""
    -- Consent/risk boilerplate detection: the standard surgical risk template
    -- containing the list "scarring, hypocalcemia, hoarseness, chyle leak, seroma..."
    CASE WHEN
        REGEXP_MATCHES(ctx, '(?i)scarring[,\\s]+hypocalcemia[,\\s]+hoarseness')
        OR REGEXP_MATCHES(ctx, '(?i)poor\\s+(?:wound\\s+)?healing[,\\s]+scarring')
        OR REGEXP_MATCHES(ctx, '(?i)numbness[,\\s]+orodental\\s+trauma')
        OR REGEXP_MATCHES(ctx, '(?i)(?:poor\\s+reaction\\s+to\\s+anesthesia|pulmonary\\s+embolus|heart\\s+attack)')
        OR REGEXP_MATCHES(ctx, '(?i)(?:hemothorax|pneumothorax)[,\\s]+hematoma')
        OR REGEXP_MATCHES(ctx, '(?i)(?:material\\s+)?risks?\\s+(?:for|of|include)[^.]{0,80}(?:hypocalcemia|chyle)')
        OR REGEXP_MATCHES(ctx, '(?i)informed\\s+consent[^.]{0,60}(?:hypocalcemia|chyle|seroma|hematoma)')
        OR REGEXP_MATCHES(ctx, '(?i)(?:discussed|counseled|consented)\\s+(?:the\\s+)?(?:patient|pt)[^.]{0,80}(?:hypocalcemia|chyle)')
        OR REGEXP_MATCHES(ctx, '(?i)complications?[,:\\s]+[^.]{0,40}(?:death|heart\\s+attack|bleeding)(?!\\s+was\\s+controlled)')
    THEN TRUE ELSE FALSE END AS is_consent_boilerplate
""").strip()

# H&P note type shortcut — all H&P mentions of complication entities are boilerplate
HP_NOTE_FP_SQL = "note_type = 'h_p'"


# ---------------------------------------------------------------------------
# Per-entity refined SQL
# ---------------------------------------------------------------------------

def _build_chyle_leak_sql(db_prefix: str = "") -> str:
    """
    chyle_leak refinement:
    - Exclude: all H&P (consent template), all op_note "lack of" phrases
    - Include: true chyle seen/observed/milky, dc_sum mentions
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_chyle_leak_refined_v2 AS
    WITH base AS (
        SELECT
            n.research_id,
            n.note_row_id,
            n.note_type,
            TRY_CAST(n.note_date AS DATE) AS note_date,
            n.evidence_span,
            n.evidence_start,
            n.evidence_end,
            n.present_or_negated,
            n.confidence,
            SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400) AS ctx
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'chyle_leak'
            AND n.present_or_negated = 'present'
    ),
    classified AS (
        SELECT *,
            CASE
                -- Explicit negation / absence confirmation
                WHEN REGEXP_MATCHES(ctx, '(?i)lack\\s+of\\s+a?\\s*chyle\\s+leak')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:confirm|absence\\s+of|no|rule\\s+out)\\s+chyle\\s+leak')
                    OR REGEXP_MATCHES(ctx, '(?i)chyle\\s+leak[^.]{0,40}(?:absent|none|not\\s+(?:seen|observed|present))')
                    THEN 'false_positive_negation'
                -- Prevent/ligate language (pre-event prevention, not actual event)
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:prevent|preventing|ligate[^.]{0,30}|care\\s+was\\s+taken[^.]{0,30})\\s+chyle')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:hemostasis|valsalva)[^.]{0,60}chyle\\s+leak')
                    THEN 'false_positive_prevention'
                -- Consent/risk boilerplate
                WHEN REGEXP_MATCHES(ctx, '(?i)scarring[,\\s]+hypocalcemia[,\\s]+hoarseness')
                    OR REGEXP_MATCHES(ctx, '(?i)numbness[,\\s]+orodental\\s+trauma')
                    OR note_type = 'h_p'
                    THEN 'false_positive_consent'
                -- True positive: chyle actually observed
                WHEN REGEXP_MATCHES(ctx, '(?i)chyle\\s+(?:was\\s+)?(?:seen|observed|noted|present|leak(?:ing)?|output)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:milky|cloudy|chylous)\\s+(?:fluid|output|drainage|effusion)')
                    OR REGEXP_MATCHES(ctx, '(?i)chyle\\s+leak\\s+(?:developed|occurred|present|confirmed|requiring|managed|treated)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:triglyceride|TG)\\s+(?:level|positive)\\s+in\\s+drain')
                    THEN 'true_positive_confirmed'
                -- Discharge summary without explicit FP markers → probable
                WHEN note_type IN ('dc_sum', 'endocrine_note', 'history_summary')
                    AND NOT REGEXP_MATCHES(ctx, '(?i)risk\\s+of\\s+chyle')
                    THEN 'probable'
                ELSE 'uncertain'
            END AS classification,
            CASE
                WHEN REGEXP_MATCHES(ctx, '(?i)chyle\\s+(?:was\\s+)?(?:seen|observed|noted|present|leak(?:ing)?)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:milky|chylous)\\s+fluid')
                    THEN 'strong'
                WHEN note_type IN ('dc_sum') THEN 'medium'
                ELSE 'weak'
            END AS evidence_strength
        FROM base
    ),
    filtered AS (
        SELECT *
        FROM classified
        WHERE classification IN ('true_positive_confirmed', 'probable', 'uncertain')
            AND classification NOT IN ('false_positive_negation', 'false_positive_prevention', 'false_positive_consent')
    ),
    per_patient AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            MIN(note_date) AS first_detection_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN note_date END) AS confirmed_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) AS is_confirmed,
            MAX(CASE WHEN classification IN ('true_positive_confirmed','probable') THEN 1 ELSE 0 END) AS is_probable,
            MAX(evidence_strength) AS best_evidence_strength,
            CASE
                WHEN MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) = 1 THEN 1
                WHEN MAX(CASE WHEN classification = 'probable' THEN 1 ELSE 0 END) = 1 THEN 2
                ELSE 3
            END AS tier,
            'nlp_refined' AS source_tier_label,
            COUNT(*) AS mention_count
        FROM filtered
        GROUP BY CAST(research_id AS INT)
    )
    SELECT
        p.research_id,
        COALESCE(p.confirmed_date, p.first_detection_date) AS detection_date,
        'chyle_leak' AS entity_name,
        CASE WHEN p.is_confirmed = 1 THEN TRUE ELSE FALSE END AS entity_is_confirmed,
        p.tier AS entity_tier,
        p.best_evidence_strength AS entity_evidence_strength,
        p.source_tier_label,
        p.mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM per_patient p
    """).strip()


def _build_hypocalcemia_sql(db_prefix: str = "") -> str:
    """
    hypocalcemia refinement:
    - Exclude: H&P (consent boilerplate), education/monitoring language
    - Include: clinical events (symptomatic, IV calcium, tetany, documented post-op)
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_hypocalcemia_refined_v2 AS
    WITH base AS (
        SELECT
            n.research_id,
            n.note_row_id,
            n.note_type,
            TRY_CAST(n.note_date AS DATE) AS note_date,
            n.evidence_span,
            n.evidence_start,
            n.confidence,
            n.present_or_negated,
            SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400) AS ctx
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'hypocalcemia'
            AND n.present_or_negated = 'present'
    ),
    classified AS (
        SELECT *,
            CASE
                -- H&P → always consent boilerplate
                WHEN note_type = 'h_p' THEN 'false_positive_consent'
                -- Education/monitoring language
                WHEN REGEXP_MATCHES(ctx, '(?i)signs?\\s+and\\s+symptoms?\\s+of\\s+hypocalcemia\\s+were\\s+reviewed')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:given|provide)\\s+instructions?[^.]{0,60}hypocalcemia')
                    OR REGEXP_MATCHES(ctx, '(?i)indications?\\s+to\\s+(?:present|go)\\s+to\\s+(?:the\\s+)?(?:ED|ER|emergency)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:monitor|check|watch)\\s+for[^.]{0,40}hypocalcemia')
                    THEN 'false_positive_monitoring'
                -- Risk/consent language in op_notes
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:Bleeding|Pain|Infection)[,\\s]+Hypocalcemia[,\\s]+Hypothyroidism')
                    OR REGEXP_MATCHES(ctx, '(?i)hypocalcemia[^.]{0,40}(?:temporary|transient)\\s+or\\s+permanent')
                    OR REGEXP_MATCHES(ctx, '(?i)risk\\s+of\\s+(?:temporary\\s+or\\s+permanent\\s+)?hypocalcemia')
                    THEN 'false_positive_consent'
                -- Strong true positive signals
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:post[\\s-]?op(?:erative)?|developed|experienced)\\s+hypocalcemia')
                    OR REGEXP_MATCHES(ctx, '(?i)hypocalcemia\\s+(?:noted|confirmed|documented|diagnosed|present|requiring)')
                    OR REGEXP_MATCHES(ctx, '(?i)symptomatic\\s+hypocalcemia')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:IV\\s+calcium|calcium\\s+gluconate|calcium\\s+infusion)\\s+(?:given|administered|required|started)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:tetany|carpopedal\\s+spasm|Chvostek|Trousseau)\\s+(?:sign|positive|noted|present)')
                    OR REGEXP_MATCHES(ctx, '(?i)perioral\\s+(?:numbness|tingling)\\s+(?:consistent|suggesting|due\\s+to)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:mild|moderate|severe)\\s+(?:post[\\s-]?op)?\\s*hypocalcemia')
                    THEN 'true_positive_confirmed'
                -- Endocrine notes with post-op context → probable
                WHEN note_type IN ('endocrine_note', 'history_summary')
                    AND NOT REGEXP_MATCHES(ctx, '(?i)risk\\s+of\\s+hypocalcemia')
                    THEN 'probable'
                -- Discharge summaries — moderate prior
                WHEN note_type = 'dc_sum'
                    AND NOT REGEXP_MATCHES(ctx, '(?i)signs?\\s+and\\s+symptoms?\\s+of\\s+hypocalcemia\\s+were\\s+reviewed')
                    AND NOT REGEXP_MATCHES(ctx, '(?i)indications?\\s+to\\s+(?:present|go)\\s+to')
                    THEN 'probable'
                -- Op_note: highly contaminated (consent template common); exclude unless TP already matched
                WHEN note_type = 'op_note' THEN 'false_positive_opnote_fallthrough'
                -- Unknown/other note types with no context → uncertain only if not h_p or op_note
                WHEN ctx IS NULL OR ctx = '' THEN 'uncertain_no_context'
                ELSE 'uncertain'
            END AS classification,
            CASE
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:IV\\s+calcium|tetany|Chvostek|symptomatic\\s+hypocalcemia|developed\\s+hypocalcemia)') THEN 'strong'
                WHEN note_type IN ('endocrine_note', 'dc_sum') THEN 'medium'
                ELSE 'weak'
            END AS evidence_strength
        FROM base
    ),
    filtered AS (
        -- Conservative filter: only keep confirmed, probable, or uncertain from
        -- clinical follow-up note types (endocrine, other_history, ed_note).
        -- Op_note and h_p are excluded entirely.
        SELECT * FROM classified
        WHERE classification NOT IN (
            'false_positive_consent',
            'false_positive_monitoring',
            'false_positive_opnote_fallthrough',
            'uncertain_no_context'
        )
        -- Restrict 'uncertain' to high-quality note types only
        AND NOT (classification = 'uncertain' AND note_type NOT IN ('endocrine_note', 'other_history', 'history_summary', 'ed_note', 'other_notes'))
    ),
    per_patient AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            MIN(note_date) AS first_detection_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN note_date END) AS confirmed_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) AS is_confirmed,
            MAX(CASE WHEN classification IN ('true_positive_confirmed','probable') THEN 1 ELSE 0 END) AS is_probable,
            MAX(evidence_strength) AS best_evidence_strength,
            CASE
                WHEN MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) = 1 THEN 1
                WHEN MAX(CASE WHEN classification = 'probable' THEN 1 ELSE 0 END) = 1 THEN 2
                ELSE 3
            END AS tier,
            'nlp_refined' AS source_tier_label,
            COUNT(*) AS mention_count
        FROM filtered
        GROUP BY CAST(research_id AS INT)
    )
    SELECT
        p.research_id,
        COALESCE(p.confirmed_date, p.first_detection_date) AS detection_date,
        'hypocalcemia' AS entity_name,
        CASE WHEN p.is_confirmed = 1 THEN TRUE ELSE FALSE END AS entity_is_confirmed,
        p.tier AS entity_tier,
        p.best_evidence_strength AS entity_evidence_strength,
        'nlp_refined' AS source_tier_label,
        p.mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM per_patient p
    """).strip()


def _build_seroma_sql(db_prefix: str = "") -> str:
    """
    seroma refinement:
    - All NLP seroma = 0% precision (consent boilerplate)
    - True cases come from structured complications.seroma = 'x'
    - NLP retains only dc_sum with true-positive signals
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_seroma_refined_v2 AS
    WITH structured_cases AS (
        -- Tier 1: structured-documented seroma ('x' marker in complications table)
        SELECT
            CAST(c.research_id AS INT) AS research_id,
            CAST(c.laryngoscopy_date AS DATE) AS detection_date,
            TRUE AS entity_is_confirmed,
            1 AS entity_tier,
            'strong' AS entity_evidence_strength,
            'structured_chart' AS source_tier_label,
            1 AS mention_count
        FROM complications c
        WHERE LOWER(COALESCE(CAST(c.seroma AS VARCHAR), '')) = 'x'
    ),
    nlp_probable AS (
        -- Tier 2: NLP mentions in dc_sum/ed_note with true-positive language
        SELECT
            CAST(n.research_id AS INT) AS research_id,
            MIN(TRY_CAST(n.note_date AS DATE)) AS detection_date,
            FALSE AS entity_is_confirmed,
            2 AS entity_tier,
            'medium' AS entity_evidence_strength,
            'nlp_refined' AS source_tier_label,
            COUNT(*) AS mention_count
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'seroma'
            AND n.present_or_negated = 'present'
            AND n.note_type IN ('dc_sum', 'ed_note', 'endocrine_note')
            AND (
                REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)(?:post[\\s-]?op(?:erative)?|seroma\\s+(?:noted|found|drained|aspirated|present|requiring)|(?:persistent|enlarging|symptomatic)\\s+seroma)'
                )
            )
            AND CAST(n.research_id AS INT) NOT IN (SELECT research_id FROM structured_cases)
        GROUP BY CAST(n.research_id AS INT)
    )
    SELECT research_id, detection_date, 'seroma' AS entity_name, entity_is_confirmed,
        entity_tier, entity_evidence_strength, source_tier_label, mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM structured_cases
    UNION ALL
    SELECT research_id, detection_date, 'seroma' AS entity_name, entity_is_confirmed,
        entity_tier, entity_evidence_strength, source_tier_label, mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM nlp_probable
    """).strip()


def _build_hematoma_sql(db_prefix: str = "") -> str:
    """
    hematoma refinement:
    - Structured cases from complications.hematoma = 'x' (Tier 1)
    - NLP true positives: procedure listing (hematoma evacuation), intraop findings (Tier 2)
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_hematoma_refined_v2 AS
    WITH structured_cases AS (
        SELECT
            CAST(c.research_id AS INT) AS research_id,
            NULL::DATE AS detection_date,
            TRUE AS entity_is_confirmed,
            1 AS entity_tier,
            'strong' AS entity_evidence_strength,
            'structured_chart' AS source_tier_label,
            1 AS mention_count
        FROM complications c
        WHERE LOWER(COALESCE(CAST(c.hematoma AS VARCHAR), '')) = 'x'
    ),
    nlp_confirmed AS (
        SELECT
            CAST(n.research_id AS INT) AS research_id,
            MIN(TRY_CAST(n.note_date AS DATE)) AS detection_date,
            TRUE AS entity_is_confirmed,
            1 AS entity_tier,
            'strong' AS entity_evidence_strength,
            'nlp_refined' AS source_tier_label,
            COUNT(*) AS mention_count
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'hematoma'
            AND n.present_or_negated = 'present'
            AND (
                REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)hematoma\\s+evacuation'
                )
                OR REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)(?:returned|taken)\\s+(?:to\\s+)?OR[^.]{0,60}hematoma'
                )
                OR REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)(?:expanding|enlarging)\\s+(?:neck\\s+)?hematoma'
                )
                OR REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)(?:clotted\\s+blood|old\\s+hematoma|hematoma\\s+in\\s+(?:the\\s+)?(?:bed|wound|resection))'
                )
                OR REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)postoperative\\s+(?:Diagnosis|dx)[^.]{0,60}hematoma'
                )
                OR REGEXP_MATCHES(
                    SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                    '(?i)(?:evacuated?|drained?)[^.]{0,30}hematoma'
                )
            )
            AND CAST(n.research_id AS INT) NOT IN (SELECT research_id FROM structured_cases)
        GROUP BY CAST(n.research_id AS INT)
    ),
    nlp_probable AS (
        SELECT
            CAST(n.research_id AS INT) AS research_id,
            MIN(TRY_CAST(n.note_date AS DATE)) AS detection_date,
            FALSE AS entity_is_confirmed,
            2 AS entity_tier,
            'medium' AS entity_evidence_strength,
            'nlp_refined' AS source_tier_label,
            COUNT(*) AS mention_count
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'hematoma'
            AND n.present_or_negated = 'present'
            AND n.note_type IN ('dc_sum', 'history_summary', 'ed_note')
            AND NOT REGEXP_MATCHES(
                SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400),
                '(?i)(?:risk|hematoma[,\\s]+seroma[,\\s]+poor\\s+scarring|hemothorax|including\\s+but\\s+not\\s+limited)'
            )
            AND CAST(n.research_id AS INT) NOT IN (SELECT research_id FROM structured_cases)
            AND CAST(n.research_id AS INT) NOT IN (SELECT research_id FROM nlp_confirmed)
        GROUP BY CAST(n.research_id AS INT)
    )
    SELECT research_id, detection_date, 'hematoma' AS entity_name, entity_is_confirmed,
        entity_tier, entity_evidence_strength, source_tier_label, mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM structured_cases
    UNION ALL
    SELECT research_id, detection_date, 'hematoma' AS entity_name, entity_is_confirmed,
        entity_tier, entity_evidence_strength, source_tier_label, mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM nlp_confirmed
    UNION ALL
    SELECT research_id, detection_date, 'hematoma' AS entity_name, entity_is_confirmed,
        entity_tier, entity_evidence_strength, source_tier_label, mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM nlp_probable
    """).strip()


def _build_hypoparathyroidism_sql(db_prefix: str = "") -> str:
    """
    hypoparathyroidism refinement:
    - Exclude: H&P consent boilerplate
    - Include: endocrine_note (high prior), other_history (PMH references), dc_sum (clinical context)
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_hypoparathyroidism_refined_v2 AS
    WITH base AS (
        SELECT
            n.research_id,
            n.note_row_id,
            n.note_type,
            TRY_CAST(n.note_date AS DATE) AS note_date,
            n.evidence_span,
            n.evidence_start,
            n.present_or_negated,
            SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400) AS ctx
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'hypoparathyroidism'
            AND n.present_or_negated = 'present'
    ),
    classified AS (
        SELECT *,
            CASE
                -- H&P → always consent ("hypocalcemia and hypoparathyroidism - temporary or permanent")
                WHEN note_type = 'h_p' THEN 'false_positive_consent'
                -- Risk mention without clinical context
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:risk|damage)\\s+to\\s+(?:the\\s+)?parathyroid')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:temporary|transient)\\s+hypoparathyroidism\\s+(?:is\\s+)?(?:a\\s+)?risk')
                    THEN 'false_positive_consent'
                -- Strong confirmed: post-op diagnosis, diagnosis section, PTH lab
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:post[\\s-]?op(?:erative)?)\\s+hypoparathyroidism')
                    OR REGEXP_MATCHES(ctx, '(?i)hypoparathyroidism\\s+(?:confirmed|noted|diagnosed|after|following|s/p)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:permanent|persistent|chronic)\\s+hypoparathyroidism')
                    OR REGEXP_MATCHES(ctx, '(?i)PTH\\s+(?:undetectable|<\\s*5|very\\s+low|of\\s+[0-9]+)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:idiopathic|surgical)\\s+hypoparathyroidism')
                    THEN 'true_positive_confirmed'
                -- Endocrine follow-up → probable
                WHEN note_type IN ('endocrine_note')
                    THEN 'probable'
                -- DC sum, other_history without risk markers → probable
                WHEN note_type IN ('dc_sum', 'other_history', 'history_summary')
                    AND NOT REGEXP_MATCHES(ctx, '(?i)risk\\s+of\\s+hypoparathyroidism')
                    THEN 'probable'
                -- Op note → uncertain (may be risk mention)
                WHEN note_type = 'op_note' THEN 'uncertain'
                ELSE 'uncertain'
            END AS classification
        FROM base
    ),
    filtered AS (
        SELECT * FROM classified
        WHERE classification NOT IN ('false_positive_consent')
    ),
    per_patient AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            MIN(note_date) AS first_detection_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN note_date END) AS confirmed_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) AS is_confirmed,
            MAX(CASE WHEN classification IN ('true_positive_confirmed','probable') THEN 1 ELSE 0 END) AS is_probable,
            CASE
                WHEN MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) = 1
                    THEN 'strong'
                WHEN MAX(CASE WHEN note_type = 'endocrine_note' THEN 1 ELSE 0 END) = 1
                    THEN 'medium'
                ELSE 'weak'
            END AS best_evidence_strength,
            CASE
                WHEN MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) = 1 THEN 1
                WHEN MAX(CASE WHEN classification = 'probable' THEN 1 ELSE 0 END) = 1 THEN 2
                ELSE 3
            END AS tier,
            COUNT(*) AS mention_count
        FROM filtered
        GROUP BY CAST(research_id AS INT)
    )
    SELECT
        p.research_id,
        COALESCE(p.confirmed_date, p.first_detection_date) AS detection_date,
        'hypoparathyroidism' AS entity_name,
        CASE WHEN p.is_confirmed = 1 THEN TRUE ELSE FALSE END AS entity_is_confirmed,
        p.tier AS entity_tier,
        p.best_evidence_strength AS entity_evidence_strength,
        'nlp_refined' AS source_tier_label,
        p.mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM per_patient p
    """).strip()


def _build_wound_infection_sql(db_prefix: str = "") -> str:
    """
    wound_infection refinement:
    - Remove SSI = sliding scale insulin collision
    - Retain only true surgical wound infection mentions
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_wound_infection_refined_v2 AS
    WITH base AS (
        SELECT
            n.research_id,
            n.note_row_id,
            n.note_type,
            TRY_CAST(n.note_date AS DATE) AS note_date,
            n.evidence_span,
            n.evidence_start,
            n.present_or_negated,
            SUBSTRING(cl.note_text, GREATEST(1, n.evidence_start - 150), 400) AS ctx
        FROM note_entities_complications n
        LEFT JOIN clinical_notes_long cl ON n.note_row_id = cl.note_row_id
        WHERE n.entity_value_norm = 'wound_infection'
            AND n.present_or_negated = 'present'
    ),
    classified AS (
        SELECT *,
            CASE
                -- SSI = Sliding Scale Insulin false positive
                WHEN n.evidence_span = 'SSI'
                    AND (
                        REGEXP_MATCHES(ctx, '(?i)SSI\\s+(?:per\\s+protocol|before\\s+meals|for\\s+glucose|for\\s+DM)')
                        OR REGEXP_MATCHES(ctx, '(?i)(?:insulin|novolog|humalog|regular\\s+insulin)[^.]{0,40}SSI')
                        OR REGEXP_MATCHES(ctx, '(?i)SSI[^.]{0,40}(?:insulin|units|glucose|BG|blood\\s+sugar)')
                        OR REGEXP_MATCHES(ctx, '(?i)basal[^.]{0,40}SSI')
                        OR REGEXP_MATCHES(ctx, '(?i)SSI\\s+as\\s+BG[-–]')
                    )
                    THEN 'false_positive_ssi_insulin'
                -- Consent boilerplate
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:risk|complication)[^.]{0,60}wound\\s+infection[^.]{0,30}(?:hoarseness|calcium|scarring)')
                    OR REGEXP_MATCHES(ctx, '(?i)risk[^.]{0,20}wound\\s+infection')
                    THEN 'false_positive_consent'
                -- PMH (prior wound infection at different time/site)
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:history|hx|prior|previous|s/p)\\s+(?:of\\s+)?(?:wound|surgical\\s+site)\\s+infection')
                    AND NOT REGEXP_MATCHES(ctx, '(?i)thyroid(?:ectomy)?\\s*:?\\s*(?:wound|infection)')
                    THEN 'historical_reference'
                -- True confirmed: documented thyroid surgical site infection
                WHEN REGEXP_MATCHES(ctx, '(?i)(?:wound|incision|surgical\\s+site)\\s+infection\\s+(?:requiring|with|confirmed|treated|from)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:wound\\s+vac|negative\\s+pressure\\s+wound|incision\\s+and\\s+drainage)')
                    OR REGEXP_MATCHES(ctx, '(?i)(?:tracheostomy\\s+tube|thyroid)[^.]{0,40}(?:wound|infection)')
                    THEN 'true_positive_confirmed'
                -- ED/history with infection as primary complaint
                WHEN note_type IN ('ed_note') AND evidence_span NOT IN ('SSI')
                    THEN 'probable'
                -- Op_note reference to prior infection affecting current surgery
                WHEN note_type = 'op_note' AND REGEXP_MATCHES(ctx, '(?i)complicated\\s+by\\s+wound\\s+infection')
                    THEN 'true_positive_confirmed'
                ELSE 'uncertain'
            END AS classification
        FROM base n
    ),
    filtered AS (
        SELECT * FROM classified
        WHERE classification NOT IN ('false_positive_ssi_insulin', 'false_positive_consent')
    ),
    per_patient AS (
        SELECT
            CAST(research_id AS INT) AS research_id,
            MIN(note_date) AS first_detection_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN note_date END) AS confirmed_date,
            MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) AS is_confirmed,
            CASE WHEN MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) = 1
                THEN 1
                WHEN MAX(CASE WHEN classification = 'probable' THEN 1 ELSE 0 END) = 1 THEN 2
                ELSE 3 END AS tier,
            CASE WHEN MAX(CASE WHEN classification = 'true_positive_confirmed' THEN 1 ELSE 0 END) = 1
                THEN 'strong' ELSE 'medium' END AS best_evidence_strength,
            COUNT(*) AS mention_count
        FROM filtered
        GROUP BY CAST(research_id AS INT)
    )
    SELECT
        p.research_id,
        COALESCE(p.confirmed_date, p.first_detection_date) AS detection_date,
        'wound_infection' AS entity_name,
        CASE WHEN p.is_confirmed = 1 THEN TRUE ELSE FALSE END AS entity_is_confirmed,
        p.tier AS entity_tier,
        p.best_evidence_strength AS entity_evidence_strength,
        'nlp_refined' AS source_tier_label,
        p.mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM per_patient p
    """).strip()


def _build_consolidated_v5_sql(db_prefix: str = "") -> str:
    """
    Consolidated extracted_complications_refined_v5:
    UNION ALL of all per-entity refined tables.
    This is the single source of truth for downstream models.
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_complications_refined_v5 AS
    SELECT * FROM {db_prefix}extracted_chyle_leak_refined_v2
    UNION ALL
    SELECT * FROM {db_prefix}extracted_hypocalcemia_refined_v2
    UNION ALL
    SELECT * FROM {db_prefix}extracted_seroma_refined_v2
    UNION ALL
    SELECT * FROM {db_prefix}extracted_hypoparathyroidism_refined_v2
    UNION ALL
    SELECT * FROM {db_prefix}extracted_hematoma_refined_v2
    UNION ALL
    SELECT * FROM {db_prefix}extracted_wound_infection_refined_v2
    -- RLN injury already refined separately in extracted_rln_injury_refined_v2
    UNION ALL
    SELECT
        research_id,
        detection_date,
        'rln_injury' AS entity_name,
        rln_injury_is_confirmed AS entity_is_confirmed,
        rln_injury_tier AS entity_tier,
        rln_injury_evidence_strength AS entity_evidence_strength,
        CASE rln_injury_tier
            WHEN 1 THEN 'laryngoscopy_confirmed'
            WHEN 2 THEN 'chart_documented'
            ELSE 'nlp_refined'
        END AS source_tier_label,
        1 AS mention_count,
        CURRENT_TIMESTAMP AS refined_at
    FROM {db_prefix}extracted_rln_injury_refined_v2
    """).strip()


def _build_exclusion_audit_sql(db_prefix: str = "") -> str:
    """
    Audit trail: original NLP counts vs refined counts vs excluded counts.
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}extracted_complications_exclusion_audit_v2 AS
    WITH original_counts AS (
        SELECT entity_value_norm AS entity_name,
            COUNT(DISTINCT CAST(research_id AS INT)) AS original_patients,
            SUM(CASE WHEN present_or_negated = 'present' THEN 1 ELSE 0 END) AS original_present_mentions
        FROM note_entities_complications
        GROUP BY entity_value_norm
    ),
    refined_counts AS (
        SELECT entity_name,
            COUNT(DISTINCT research_id) AS refined_patients,
            SUM(CASE WHEN entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_patients,
            COUNT(*) AS total_refined_rows
        FROM {db_prefix}extracted_complications_refined_v5
        GROUP BY entity_name
    )
    SELECT
        o.entity_name,
        o.original_patients,
        o.original_present_mentions,
        COALESCE(r.refined_patients, 0) AS refined_patients,
        COALESCE(r.confirmed_patients, 0) AS confirmed_patients,
        o.original_patients - COALESCE(r.refined_patients, 0) AS excluded_patients,
        ROUND(100.0 * COALESCE(r.confirmed_patients, 0) / NULLIF(o.original_patients, 0), 1) AS pct_confirmed,
        ROUND(100.0 * (o.original_patients - COALESCE(r.refined_patients, 0)) / NULLIF(o.original_patients, 0), 1) AS pct_excluded,
        CURRENT_TIMESTAMP AS computed_at
    FROM original_counts o
    LEFT JOIN refined_counts r ON o.entity_name = r.entity_name
    ORDER BY o.original_patients DESC
    """).strip()


# ---------------------------------------------------------------------------
# Patient-level flags view for H1/H2 models
# ---------------------------------------------------------------------------

def _build_patient_flags_sql(db_prefix: str = "") -> str:
    """
    per-patient wide-format refined complication flags — replaces nlp_* raw flags in H1/H2 models.
    """
    return textwrap.dedent(f"""
    CREATE OR REPLACE TABLE {db_prefix}patient_refined_complication_flags_v2 AS
    SELECT
        research_id,
        MAX(CASE WHEN entity_name = 'chyle_leak' THEN 1 ELSE 0 END) AS refined_chyle_leak,
        MAX(CASE WHEN entity_name = 'chyle_leak' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_chyle_leak,
        MAX(CASE WHEN entity_name = 'hypocalcemia' THEN 1 ELSE 0 END) AS refined_hypocalcemia,
        MAX(CASE WHEN entity_name = 'hypocalcemia' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_hypocalcemia,
        MAX(CASE WHEN entity_name = 'seroma' THEN 1 ELSE 0 END) AS refined_seroma,
        MAX(CASE WHEN entity_name = 'seroma' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_seroma,
        MAX(CASE WHEN entity_name = 'hematoma' THEN 1 ELSE 0 END) AS refined_hematoma,
        MAX(CASE WHEN entity_name = 'hematoma' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_hematoma,
        MAX(CASE WHEN entity_name = 'hypoparathyroidism' THEN 1 ELSE 0 END) AS refined_hypoparathyroidism,
        MAX(CASE WHEN entity_name = 'hypoparathyroidism' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_hypoparathyroidism,
        MAX(CASE WHEN entity_name = 'wound_infection' THEN 1 ELSE 0 END) AS refined_wound_infection,
        MAX(CASE WHEN entity_name = 'wound_infection' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_wound_infection,
        MAX(CASE WHEN entity_name = 'rln_injury' THEN 1 ELSE 0 END) AS refined_rln_injury,
        MAX(CASE WHEN entity_name = 'rln_injury' AND entity_is_confirmed THEN 1 ELSE 0 END) AS confirmed_rln_injury,
        MAX(CASE WHEN entity_tier = 1 THEN 1 ELSE 0 END) AS has_confirmed_complication,
        MAX(CASE WHEN entity_tier <= 2 THEN 1 ELSE 0 END) AS has_probable_complication
    FROM {db_prefix}extracted_complications_refined_v5
    GROUP BY research_id
    """).strip()


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

PIPELINE_STEPS = [
    ("chyle_leak refined", _build_chyle_leak_sql),
    ("hypocalcemia refined", _build_hypocalcemia_sql),
    ("seroma refined", _build_seroma_sql),
    ("hematoma refined", _build_hematoma_sql),
    ("hypoparathyroidism refined", _build_hypoparathyroidism_sql),
    ("wound_infection refined", _build_wound_infection_sql),
    ("consolidated v5", _build_consolidated_v5_sql),
    ("exclusion audit", _build_exclusion_audit_sql),
    ("patient flags", _build_patient_flags_sql),
]


def run_pipeline(
    con: Any,
    dry_run: bool = False,
    db_prefix: str = "",
    verbose: bool = True,
) -> dict:
    """
    Deploy all refinement tables to MotherDuck (or local DuckDB).

    Returns dict with row counts and timing.
    """
    results = {}
    ts_start = datetime.now()

    for step_name, sql_fn in PIPELINE_STEPS:
        sql = sql_fn(db_prefix=db_prefix)
        if verbose:
            print(f"\n[{step_name}] ... ", end="", flush=True)
        if dry_run:
            if verbose:
                print(f"DRY RUN (SQL={len(sql)} chars)")
            results[step_name] = {"status": "dry_run", "sql_len": len(sql)}
            continue
        try:
            t0 = datetime.now()
            con.execute(sql)
            elapsed = (datetime.now() - t0).total_seconds()
            # Infer table name from SQL
            import re as _re
            m = _re.search(r"CREATE OR REPLACE TABLE\s+(\S+)\s+AS", sql, _re.I)
            tbl = m.group(1) if m else "?"
            try:
                n_rows = con.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
            except Exception:
                n_rows = -1
            if verbose:
                print(f"OK ({elapsed:.1f}s, {n_rows} rows)")
            results[step_name] = {"status": "ok", "rows": n_rows, "elapsed": elapsed, "table": tbl}
        except Exception as e:
            if verbose:
                print(f"ERROR: {e}")
            results[step_name] = {"status": "error", "error": str(e)}

    total_elapsed = (datetime.now() - ts_start).total_seconds()
    if verbose:
        print(f"\n{'='*60}")
        print(f"Pipeline complete in {total_elapsed:.1f}s")
        print("Results:")
        for k, v in results.items():
            print(f"  {k}: {v}")

    return results


def _get_connection(use_md: bool, local_path: str = "thyroid_master.duckdb"):
    import duckdb
    import toml
    if use_md:
        token = toml.load(".streamlit/secrets.toml")["MOTHERDUCK_TOKEN"]
        return duckdb.connect(f"md:thyroid_research_2026?motherduck_token={token}")
    return duckdb.connect(local_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="Complication refinement pipeline")
    parser.add_argument("--md", action="store_true", help="Use MotherDuck")
    parser.add_argument("--local", action="store_true", help="Use local DuckDB")
    parser.add_argument("--dry-run", action="store_true", help="Print SQL lengths, don't execute")
    parser.add_argument("--db-prefix", type=str, default="", help="Optional table name prefix")
    args = parser.parse_args()

    use_md = args.md or (not args.local)
    con = _get_connection(use_md)

    results = run_pipeline(con, dry_run=args.dry_run, db_prefix=args.db_prefix, verbose=True)

    # Save manifest
    manifest = {
        "run_time": datetime.now().isoformat(),
        "mode": "motherduck" if use_md else "local",
        "steps": results,
    }
    manifest_path = Path(__file__).parent / "complications_refinement_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, default=str))
    print(f"\nManifest saved to {manifest_path}")

    con.close()


if __name__ == "__main__":
    main()
