"""
Script 431 — Phase E: Sonnet 4.6 audit + Opus 4.6 adjudication
================================================================
Implements CURSOR_PROMPT_PHASE_E_AUDIT_ADJUDICATION_20260507.md.

Phase E.1: Sonnet 4.6 stratified 5% audit (~500 nodules across 11 systems).
Phase E.2: Opus 4.6 adjudication on ≥ 2-category disagreements (from Step 5 queue).

Prerequisites:
  - All 11 TIRADS systems populated in canonical_us_nodule_tirads_multisystem_v1
  - qc_tirads_multisystem_disagreement_v1 built (script 430)
  - ANTHROPIC_API_KEY environment variable set
  - anthropic Python package installed (pip install anthropic)

Output tables:
  pub_workspace.qc_phase_e_audit_sample_v1
  pub_workspace.qc_phase_e_sonnet_audit_results_v1
  pub_workspace.qc_phase_e_opus_adjudication_v1

Usage:
    export ANTHROPIC_API_KEY=<your_key>
    python scripts/431_tirads_phase_e_audit_adjudication.py [--phase E1] [--phase E2] [--dry-run]

Cost estimate: Sonnet audit ~500 calls × $3/M tokens ≈ $5–10; Opus ≤500 calls × $15/M ≈ $5–15.
Total ≤ $20 per budget.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_GLAND_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_thyroid_gland_v2"
TABLE_DISAGQ = f"{PROJECT}.{DATASET_WS}.qc_tirads_multisystem_disagreement_v1"
TABLE_AUDIT_SAMPLE = f"{PROJECT}.{DATASET_WS}.qc_phase_e_audit_sample_v1"
TABLE_AUDIT_RESULTS = f"{PROJECT}.{DATASET_WS}.qc_phase_e_sonnet_audit_results_v1"
TABLE_ADJUD_RESULTS = f"{PROJECT}.{DATASET_WS}.qc_phase_e_opus_adjudication_v1"

SONNET_MODEL = "claude-sonnet-4-6"
OPUS_MODEL = "claude-opus-4-6"

# Cost ceilings
PHASE_E_COST_CEILING = 20.0  # total USD for Phase E

# Per-system concordance targets (from Phase E prompt)
CONCORDANCE_TARGETS = {
    "kwak": 0.90, "ktirads": 0.90, "ctirads": 0.90, "sru": 0.90, "park2009": 0.90,
    "acr": 0.90,   # deterministic
    "eu": 0.80, "ata": 0.80, "bta": 0.80, "aace": 0.80, "horvath": 0.80,
}


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> None:
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ {label}")


def _scalar(bq: bigquery.Client, sql: str):
    return list(bq.query(sql, location=LOCATION).result())[0][0]


# ---------------------------------------------------------------------------
# Phase E.1.a — Build audit sample (~500 nodules, stratified)
# ---------------------------------------------------------------------------

BUILD_SAMPLE_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_AUDIT_SAMPLE}`
CLUSTER BY research_id AS
WITH stratified AS (
  SELECT m.*, n.composition, n.echogenicity, n.size_cm_max,
    CASE
      WHEN n.size_cm_max < 1.0 THEN 'lt_1cm'
      WHEN n.size_cm_max < 2.0 THEN '1_to_2cm'
      WHEN n.size_cm_max < 4.0 THEN '2_to_4cm'
      ELSE 'gte_4cm'
    END AS size_band,
    CASE
      WHEN m.tirads_reported_in_text IS NULL THEN 'no_report'
      WHEN m.tirads_reported_system_validated = 'unspecified' THEN 'system_not_named'
      WHEN m.acr2017_category_imputed = CONCAT('TR', m.tirads_reported_in_text) THEN 'agree'
      ELSE 'disagree'
    END AS agreement_state
  FROM `{TABLE_MULTISYS}` m
  JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
),
sampled AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY agreement_state, composition, size_band
    ORDER BY FARM_FINGERPRINT(nodule_id)
  ) AS strata_rank
  FROM stratified
)
SELECT * FROM sampled
WHERE strata_rank <= GREATEST(10, CAST(0.05 * (
  SELECT COUNT(*) FROM stratified
) / (4 * 5 * 4) AS INT64))
LIMIT 500;
"""

# ---------------------------------------------------------------------------
# Phase E.1.b — Sonnet system prompt
# ---------------------------------------------------------------------------

SONNET_SYSTEM = """You are a thyroid radiology subspecialist auditor. For one nodule
described by structured features, gland-level context, and a short paraphrased
source-text excerpt, independently assign the best category for each of the eleven
TIRADS-style scoring systems below. Use ONLY the provided information. Be
conservative — if data is insufficient for a specific system, return null for
that system.

Output strict JSON matching this schema:
{
  "acr": "TR1|TR2|TR3|TR4|TR5|null",
  "kwak": "2|3|4A|4B|4C|5|null",
  "ktirads": "2|3|4|5|null",
  "ctirads": "2|3|4A|4B|4C|5|6|null",
  "eu": "EU2|EU3|EU4|EU5|null",
  "ata": "benign|very_low|low|intermediate|high|null",
  "bta": "U2|U3|U4|U5|null",
  "aace": "1|2|3|null",
  "horvath_pattern": "colloid_type_1|colloid_type_2|colloid_type_3|hashimoto_pseudonodule|white_knight_hashimoto|isolated_intraparenchymal_calc|benign_concordant_aspirated|de_quervain_unifocal|simple_neoplastic|suspicious_neoplastic|malignant_type_a|malignant_type_b|malignant_type_c|unassignable",
  "park2009": "P1|P2|P3|P4|P5|null",
  "sru": "no_fna|fna_consider|fna_strong|lymph_node_priority|null",
  "evidence_note": "Brief free-text note (≤140 chars) explaining key features driving assignments. NO PHI."
}

Paraphrase any evidence in ≤140 chars. Never include re-identifying detail."""

SONNET_USER_TEMPLATE = """<structured_features>
composition: {composition}
echogenicity: {echogenicity}
shape: {shape}
margins: {margins}
echogenic_foci: {echogenic_foci}
halo_presence: {halo_presence}
vascularity: {vascularity}
ete_on_us: {ete_on_us}
</structured_features>
<gland_context>
background_echogenicity: {background_echogenicity}
hashimoto_pattern: {hashimoto_pattern}
goiter: {goiter_flag}
</gland_context>
<size>{size_cm} cm</size>
<source_text>{source_text}</source_text>
<ln_context>suspicious_ln_within_60d: {has_suspicious_ln}</ln_context>"""

# ---------------------------------------------------------------------------
# Phase E.2 — Opus system prompt
# ---------------------------------------------------------------------------

OPUS_SYSTEM = """You are a senior thyroid radiologist adjudicating systematic
disagreements between TIRADS scoring systems on a single nodule. The user provides:
(1) the nodule's structured features and source text; (2) the categories assigned
by all 11 TIRADS systems; (3) which systems disagree by ≥ 2 suspicion ordinals.

For each disagreement pair, decide ONE of:
A. Override — one system's category is wrong (cite which and why); write an Override
   Decision recommendation.
B. Legitimate divergence — the systems weight features differently and the disagreement
   reflects that, not error. Recommend logging as a Notable Finding.
C. Data quality — the input data is insufficient or inconsistent; recommend a
   Verification Check.

Output strict JSON:
{
  "nodule_id": "...",
  "disagreement_pairs": [
    {
      "system_a": "acr", "category_a": "TR3",
      "system_b": "kwak", "category_b": "4A",
      "verdict": "A|B|C",
      "rationale": "≤140 chars, no PHI",
      "override_system": "acr|kwak|null",
      "override_corrected_category": "TR4|null",
      "notable_finding_title": "string if verdict=B, else null",
      "verification_check_column": "column_name if verdict=C, else null"
    }
  ],
  "overall_adjudication_status": "override|legitimate_divergence|data_quality|mixed|unresolved"
}"""

OPUS_USER_TEMPLATE = """<nodule_features>
composition: {composition}
echogenicity: {echogenicity}
shape: {shape}
margins: {margins}
echogenic_foci: {echogenic_foci}
size_cm: {size_cm}
halo_presence: {halo_presence}
vascularity: {vascularity}
ete_on_us: {ete_on_us}
hashimoto_gland: {hashimoto_pattern}
</nodule_features>
<all_system_categories>
ACR2017_imputed: {acr}
Kwak2011: {kwak}
K-TIRADS2021: {ktirads}
C-TIRADS2020: {ctirads}
EU-TIRADS: {eu}
ATA2015: {ata}
BTA2014: {bta}
AACE2016: {aace}
Horvath2009: {horvath}
Park2009: {park2009}
SRU2005: {sru}
</all_system_categories>
<disagreement_pairs>
{disagreement_pairs}
</disagreement_pairs>
<source_text>{source_text}</source_text>"""


# ---------------------------------------------------------------------------
# Phase E.1 — Run Sonnet audit
# ---------------------------------------------------------------------------

def run_sonnet_audit(bq: bigquery.Client, client, dry_run: bool) -> list[dict]:
    """Fetch audit sample, run Sonnet for each row, return results."""
    import anthropic

    n_sample = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_AUDIT_SAMPLE}`"))
    _log(f"Sonnet audit: {n_sample} rows in audit sample")

    # Pull sample + structured features
    sample_sql = f"""
    SELECT
      s.nodule_id, s.research_id,
      s.acr2017_category_imputed, s.kwak_category, s.ktirads_category,
      s.ctirads_category, s.eutirads_category, s.ata_pattern, s.bta_category,
      s.aace_class, s.horvath_category, s.park2009_category, s.sru_recommendation,
      s.composition, s.echogenicity, s.size_cm_max AS size_cm,
      n.shape, n.margins, n.echogenic_foci, n.halo_presence_simple,
      n.vascularity_distribution_simple, n.ete_on_us_presence_simple,
      COALESCE(g.background_echogenicity, 'unknown') AS background_echogenicity,
      COALESCE(g.hashimoto_pattern, 'none') AS hashimoto_pattern,
      COALESCE(CAST(g.goiter_flag AS STRING), 'unknown') AS goiter_flag,
      COALESCE(CAST(ln.has_suspicious_ln_within_60d AS BOOL), FALSE) AS has_suspicious_ln,
      COALESCE(LEFT(prim.source_text, 500), '[no source text]') AS source_text
    FROM `{TABLE_AUDIT_SAMPLE}` s
    JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
    LEFT JOIN (
      SELECT research_id, ANY_VALUE(background_echogenicity) AS background_echogenicity,
             ANY_VALUE(hashimoto_pattern) AS hashimoto_pattern,
             ANY_VALUE(goiter_flag) AS goiter_flag
      FROM `{TABLE_GLAND_V2}` GROUP BY research_id
    ) g ON s.research_id = g.research_id
    LEFT JOIN `{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1` ln USING (nodule_id)
    LEFT JOIN `{PROJECT}.{DATASET_WS}.tirads_primitive_backfill_input_v1` prim USING (nodule_id)
    """
    rows = [dict(r) for r in bq.query(sample_sql, location=LOCATION).result()]
    _log(f"  Fetched {len(rows)} audit rows with features")

    if dry_run:
        _log("  [dry-run] Processing only first 5 rows")
        rows = rows[:5]

    results = []
    total_input_tokens = 0
    total_output_tokens = 0

    for i, row in enumerate(rows):
        user_prompt = SONNET_USER_TEMPLATE.format(
            composition=row.get("composition") or "unknown",
            echogenicity=row.get("echogenicity") or "unknown",
            shape=row.get("shape") or "unknown",
            margins=row.get("margins") or "unknown",
            echogenic_foci=row.get("echogenic_foci") or "[]",
            halo_presence=row.get("halo_presence_simple") or "unstated",
            vascularity=row.get("vascularity_distribution_simple") or "unstated",
            ete_on_us=row.get("ete_on_us_presence_simple") or "unstated",
            background_echogenicity=row.get("background_echogenicity") or "unknown",
            hashimoto_pattern=row.get("hashimoto_pattern") or "none",
            goiter_flag=row.get("goiter_flag") or "unknown",
            size_cm=row.get("size_cm") or "unknown",
            source_text=str(row.get("source_text") or "")[:500],
            has_suspicious_ln=str(row.get("has_suspicious_ln") or False),
        )

        try:
            response = client.messages.create(
                model=SONNET_MODEL,
                max_tokens=512,
                system=SONNET_SYSTEM,
                messages=[{"role": "user", "content": user_prompt}],
            )
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            text = response.content[0].text
            # Parse JSON from response
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                # Try to extract JSON block
                import re
                m = re.search(r'\{.*\}', text, re.DOTALL)
                data = json.loads(m.group()) if m else {}

            result = {
                "nodule_id": row["nodule_id"],
                "research_id": row["research_id"],
                # Audit categories from Sonnet
                "audit_acr": data.get("acr"),
                "audit_kwak": data.get("kwak"),
                "audit_ktirads": data.get("ktirads"),
                "audit_ctirads": data.get("ctirads"),
                "audit_eu": data.get("eu"),
                "audit_ata": data.get("ata"),
                "audit_bta": data.get("bta"),
                "audit_aace": str(data.get("aace")) if data.get("aace") is not None else None,
                "audit_horvath": data.get("horvath_pattern"),
                "audit_park2009": data.get("park2009"),
                "audit_sru": data.get("sru"),
                "audit_evidence_note": str(data.get("evidence_note", ""))[:140],
                # Computed categories for comparison
                "computed_acr": row.get("acr2017_category_imputed"),
                "computed_kwak": row.get("kwak_category"),
                "computed_ktirads": row.get("ktirads_category"),
                "computed_ctirads": row.get("ctirads_category"),
                "computed_eu": row.get("eutirads_category"),
                "computed_ata": row.get("ata_pattern"),
                "computed_bta": row.get("bta_category"),
                "computed_aace": str(row.get("aace_class")) if row.get("aace_class") is not None else None,
                "computed_horvath": row.get("horvath_category"),
                "computed_park2009": row.get("park2009_category"),
                "computed_sru": row.get("sru_recommendation"),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            }
            results.append(result)

            if (i + 1) % 50 == 0:
                est_cost = (total_input_tokens * 3.0 + total_output_tokens * 15.0) / 1_000_000
                _log(f"  Progress: {i+1}/{len(rows)} | tokens: {total_input_tokens}+{total_output_tokens}"
                     f" | est_cost: ${est_cost:.2f}")

        except Exception as e:
            _log(f"  ERROR on row {i} ({row.get('nodule_id')}): {e}")
            results.append({"nodule_id": row["nodule_id"], "research_id": row["research_id"],
                             "error": str(e)[:200]})

        # Rate limit: max 5 req/sec for Sonnet
        time.sleep(0.2)

    final_cost = (total_input_tokens * 3.0 + total_output_tokens * 15.0) / 1_000_000
    _log(f"  Sonnet audit complete: {len(results)} results, ${final_cost:.2f} estimated")
    return results, final_cost


# ---------------------------------------------------------------------------
# Phase E.2 — Run Opus adjudication
# ---------------------------------------------------------------------------

def run_opus_adjudication(bq: bigquery.Client, client, dry_run: bool) -> list[dict]:
    """Pull disagreement queue, run Opus for each row, return adjudication results."""
    import anthropic

    n_queue = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_DISAGQ}` WHERE adjudication_status IS NULL"))
    _log(f"Opus adjudication: {n_queue} un-adjudicated disagreement rows")

    queue_sql = f"""
    SELECT d.*,
      n.composition, n.echogenicity, n.shape, n.margins,
      n.echogenic_foci, n.halo_presence_simple,
      n.vascularity_distribution_simple, n.ete_on_us_presence_simple,
      COALESCE(LEFT(prim.source_text, 500), '[no source text]') AS source_text,
      COALESCE(g.hashimoto_pattern, 'none') AS hashimoto_pattern
    FROM `{TABLE_DISAGQ}` d
    JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
    LEFT JOIN `{PROJECT}.{DATASET_WS}.tirads_primitive_backfill_input_v1` prim USING (nodule_id)
    LEFT JOIN (
      SELECT research_id, ANY_VALUE(hashimoto_pattern) AS hashimoto_pattern
      FROM `{TABLE_GLAND_V2}` GROUP BY research_id
    ) g ON d.research_id = g.research_id
    WHERE d.adjudication_status IS NULL
    ORDER BY d.suspicion_spread DESC
    LIMIT 500
    """
    queue_rows = [dict(r) for r in bq.query(queue_sql, location=LOCATION).result()]
    _log(f"  Fetched {len(queue_rows)} rows from queue (capped at 500)")

    if dry_run:
        _log("  [dry-run] Processing only first 3 rows")
        queue_rows = queue_rows[:3]

    results = []
    total_input_tokens = 0
    total_output_tokens = 0

    # Helper: identify which systems are in a ≥2-ordinal disagreement
    sus_map = {
        "acr": lambda r: {"TR1":1,"TR2":2,"TR3":3,"TR4":4,"TR5":5}.get(r.get("acr2017_category_imputed")),
        "kwak": lambda r: {"2":1,"3":2,"4A":3,"4B":4,"4C":5,"5":5}.get(r.get("kwak_category")),
        "ktirads": lambda r: {"1":1,"2":1,"3":2,"4":4,"5":5}.get(r.get("ktirads_category")),
        "ctirads": lambda r: {"2":1,"3":2,"4A":3,"4B":4,"4C":5,"5":5,"6":5}.get(r.get("ctirads_category")),
        "eu": lambda r: {"EU2":1,"EU3":2,"EU4":3,"EU5":5}.get(r.get("eutirads_category")),
        "ata": lambda r: {"benign":1,"very_low":1,"low":2,"intermediate":3,"high":5}.get(r.get("ata_pattern")),
        "bta": lambda r: {"U2":1,"U3":2,"U4":4,"U5":5}.get(r.get("bta_category")),
        "aace": lambda r: {1:1,2:3,3:5}.get(r.get("aace_class")),
        "park2009": lambda r: {"P1":1,"P2":2,"P3":3,"P4":4,"P5":5}.get(r.get("park2009_category")),
        "park_cohort": lambda r: {"P1":1,"P2":2,"P3":3,"P4":4,"P5":5}.get(r.get("park_cohort_category")),
        "horvath": lambda r: {"TIRADS_2":1,"2":1,"TIRADS_3":2,"3":2,"TIRADS_4A":3,"4A":3,
                              "TIRADS_4B":4,"4B":4,"TIRADS_4C":5,"4C":5,"TIRADS_5":5,"5":5}.get(r.get("horvath_category")),
    }

    cat_map = {
        "acr": lambda r: r.get("acr2017_category_imputed"),
        "kwak": lambda r: r.get("kwak_category"),
        "ktirads": lambda r: r.get("ktirads_category"),
        "ctirads": lambda r: r.get("ctirads_category"),
        "eu": lambda r: r.get("eutirads_category"),
        "ata": lambda r: r.get("ata_pattern"),
        "bta": lambda r: r.get("bta_category"),
        "aace": lambda r: str(r.get("aace_class")) if r.get("aace_class") is not None else None,
        "park2009": lambda r: r.get("park2009_category"),
        "park_cohort": lambda r: r.get("park_cohort_category"),
        "horvath": lambda r: r.get("horvath_category"),
        "sru": lambda r: r.get("sru_recommendation"),
    }

    for i, row in enumerate(queue_rows):
        # Build disagreement pairs (systems with ≥2 ordinal gap from the max/min system)
        sus_vals = {s: f(row) for s, f in sus_map.items()}
        sus_valid = {s: v for s, v in sus_vals.items() if v is not None}
        if not sus_valid:
            continue
        max_sus = max(sus_valid.values())
        min_sus = min(sus_valid.values())
        max_systems = [s for s, v in sus_valid.items() if v == max_sus]
        min_systems = [s for s, v in sus_valid.items() if v == min_sus]

        disagree_strs = []
        for s_high in max_systems:
            for s_low in min_systems:
                if max_sus - min_sus >= 2:
                    cat_h = cat_map.get(s_high, lambda r: None)(row)
                    cat_l = cat_map.get(s_low, lambda r: None)(row)
                    disagree_strs.append(
                        f"[{s_high.upper()}: {cat_h}] vs [{s_low.upper()}: {cat_l}]: "
                        f"{max_sus - min_sus}-ordinal gap"
                    )

        if not disagree_strs:
            continue

        # All system categories for context
        all_cats = {s: cat_map[s](row) for s in cat_map if cat_map[s](row)}

        user_prompt = OPUS_USER_TEMPLATE.format(
            composition=row.get("composition") or "unknown",
            echogenicity=row.get("echogenicity") or "unknown",
            shape=row.get("shape") or "unknown",
            margins=row.get("margins") or "unknown",
            echogenic_foci=row.get("echogenic_foci") or "[]",
            halo_presence=row.get("halo_presence_simple") or "unstated",
            vascularity=row.get("vascularity_distribution_simple") or "unstated",
            ete_on_us=row.get("ete_on_us_presence_simple") or "unstated",
            hashimoto_pattern=row.get("hashimoto_pattern") or "none",
            size_cm=row.get("size_cm_max") or "unknown",
            acr=all_cats.get("acr", "null"),
            kwak=all_cats.get("kwak", "null"),
            ktirads=all_cats.get("ktirads", "null"),
            ctirads=all_cats.get("ctirads", "null"),
            eu=all_cats.get("eu", "null"),
            ata=all_cats.get("ata", "null"),
            bta=all_cats.get("bta", "null"),
            aace=all_cats.get("aace", "null"),
            horvath=all_cats.get("horvath", "null"),
            park2009=all_cats.get("park2009", "null"),
            sru=all_cats.get("sru", "null"),
            disagreement_pairs="\n".join(disagree_strs),
            source_text=str(row.get("source_text") or "")[:500],
        )

        try:
            response = client.messages.create(
                model=OPUS_MODEL,
                max_tokens=1024,
                system=OPUS_SYSTEM,
                messages=[{"role": "user", "content": user_prompt}],
            )
            total_input_tokens += response.usage.input_tokens
            total_output_tokens += response.usage.output_tokens

            text = response.content[0].text
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                import re
                m = re.search(r'\{.*\}', text, re.DOTALL)
                data = json.loads(m.group()) if m else {}

            overall_status = data.get("overall_adjudication_status", "unresolved")
            results.append({
                "nodule_id": row["nodule_id"],
                "research_id": row["research_id"],
                "adjudication_status": overall_status,
                "disagreement_pairs_json": json.dumps(data.get("disagreement_pairs", [])),
                "override_system": next(
                    (p.get("override_system") for p in data.get("disagreement_pairs", [])
                     if p.get("verdict") == "A"), None
                ),
                "notable_finding_title": next(
                    (p.get("notable_finding_title") for p in data.get("disagreement_pairs", [])
                     if p.get("verdict") == "B"), None
                ),
                "processed_at": datetime.now(timezone.utc).isoformat(),
            })

            if (i + 1) % 20 == 0:
                est_cost = (total_input_tokens * 15.0 + total_output_tokens * 75.0) / 1_000_000
                _log(f"  Progress: {i+1}/{len(queue_rows)} | tokens: {total_input_tokens}+{total_output_tokens}"
                     f" | est_cost: ${est_cost:.2f}")

        except Exception as e:
            _log(f"  ERROR on row {i} ({row.get('nodule_id')}): {e}")
            results.append({"nodule_id": row["nodule_id"], "research_id": row["research_id"],
                             "adjudication_status": "unresolved", "error": str(e)[:200]})

        time.sleep(0.5)

    final_cost = (total_input_tokens * 15.0 + total_output_tokens * 75.0) / 1_000_000
    _log(f"  Opus adjudication complete: {len(results)} results, ${final_cost:.2f} estimated")
    return results, final_cost


# ---------------------------------------------------------------------------
# Per-system concordance computation
# ---------------------------------------------------------------------------

def compute_concordance(results: list[dict]) -> dict:
    """Per-system strict and binary concordance."""
    systems = ["acr","kwak","ktirads","ctirads","eu","ata","bta","aace","horvath","park2009","sru"]
    concordance = {}

    # Simplified binary mapping for concordance
    sus_map_cat = {
        "acr": {"TR1":1,"TR2":2,"TR3":3,"TR4":4,"TR5":5},
        "kwak": {"2":1,"3":2,"4A":3,"4B":4,"4C":5,"5":5},
        "ktirads": {"1":1,"2":1,"3":2,"4":4,"5":5},
        "ctirads": {"2":1,"3":2,"4A":3,"4B":4,"4C":5,"5":5,"6":5},
        "eu": {"EU2":1,"EU3":2,"EU4":3,"EU5":5},
        "ata": {"benign":1,"very_low":1,"low":2,"intermediate":3,"high":5},
        "bta": {"U2":1,"U3":2,"U4":4,"U5":5},
        "aace": {"1":1,"2":3,"3":5},
        "horvath": {"TIRADS_2":1,"2":1,"TIRADS_3":2,"3":2,"TIRADS_4A":3,"4A":3,
                    "TIRADS_4B":4,"4B":4,"TIRADS_4C":5,"4C":5,"TIRADS_5":5,"5":5},
        "park2009": {"P1":1,"P2":2,"P3":3,"P4":4,"P5":5},
        "sru": {"no_fna":1,"fna_consider":2,"fna_strong":4,"lymph_node_priority":5},
    }

    for s in systems:
        n_strict = n_binary = n_total = 0
        for r in results:
            audit = r.get(f"audit_{s}")
            computed = r.get(f"computed_{s}")
            if audit is None or computed is None:
                continue
            n_total += 1
            if audit == computed:
                n_strict += 1
            # Binary
            a_sus = sus_map_cat.get(s, {}).get(str(audit), 0) >= 3
            c_sus = sus_map_cat.get(s, {}).get(str(computed), 0) >= 3
            if a_sus == c_sus:
                n_binary += 1

        concordance[s] = {
            "strict": n_strict / max(1, n_total),
            "binary": n_binary / max(1, n_total),
            "n": n_total,
            "target_strict": CONCORDANCE_TARGETS.get(s, 0.90),
            "pass": (n_strict / max(1, n_total)) >= CONCORDANCE_TARGETS.get(s, 0.90),
        }

    return concordance


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase E: Sonnet audit + Opus adjudication")
    parser.add_argument("--phase", choices=["E1", "E2", "both"], default="both")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    # Check prerequisites
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        _log("ERROR: ANTHROPIC_API_KEY not set. Run: export ANTHROPIC_API_KEY=<key>")
        sys.exit(1)

    import anthropic
    client = anthropic.Anthropic(api_key=api_key)
    bq = bigquery.Client(project=args.project)

    import pandas as pd

    total_cost = 0.0

    # -----------------------------------------------------------------------
    # Phase E.1.a — Build audit sample
    # -----------------------------------------------------------------------
    if args.phase in ("E1", "both"):
        _log("Phase E.1.a: Build stratified audit sample")
        if not args.dry_run:
            _run_sql(bq, BUILD_SAMPLE_SQL, "Build audit sample")
        n_sample = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_AUDIT_SAMPLE}`"))
        _log(f"  Audit sample: {n_sample} rows")

        _log("Phase E.1.b: Run Sonnet 4.6 audit")
        e1_results, e1_cost = run_sonnet_audit(bq, client, args.dry_run)
        total_cost += e1_cost

        if not args.dry_run and e1_results:
            df_e1 = pd.DataFrame(e1_results)
            job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
            bq.load_table_from_dataframe(df_e1, TABLE_AUDIT_RESULTS,
                                          job_config=job_cfg, location=LOCATION).result()
            _log(f"  Saved {len(e1_results)} audit results to {TABLE_AUDIT_RESULTS}")

        _log("Phase E.1.c: Compute per-system concordance")
        concordance = compute_concordance(e1_results)
        _log("\n  Per-system concordance:")
        failing_systems = []
        for s, c in sorted(concordance.items()):
            status = "✓ PASS" if c["pass"] else "✗ FAIL"
            _log(f"    {s}: strict={c['strict']:.1%} binary={c['binary']:.1%} "
                 f"n={c['n']} {status}")
            if not c["pass"]:
                failing_systems.append(s)

        if failing_systems:
            _log(f"  WARNING: {len(failing_systems)} systems below concordance target: {failing_systems}")
            _log("  These will be routed to Phase E.2 adjudication.")

    # -----------------------------------------------------------------------
    # Phase E.2 — Opus adjudication
    # -----------------------------------------------------------------------
    if args.phase in ("E2", "both"):
        if total_cost > PHASE_E_COST_CEILING:
            _log(f"HALT: Estimated cost ${total_cost:.2f} exceeds Phase E ceiling ${PHASE_E_COST_CEILING}")
            sys.exit(2)

        _log("\nPhase E.2: Opus 4.6 adjudication on ≥2-category disagreements")
        e2_results, e2_cost = run_opus_adjudication(bq, client, args.dry_run)
        total_cost += e2_cost

        if not args.dry_run and e2_results:
            df_e2 = pd.DataFrame(e2_results)
            job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
            bq.load_table_from_dataframe(df_e2, TABLE_ADJUD_RESULTS,
                                          job_config=job_cfg, location=LOCATION).result()
            _log(f"  Saved {len(e2_results)} adjudication results to {TABLE_ADJUD_RESULTS}")

            # Update adjudication_status in the disagreement queue
            for r in e2_results:
                nid = r.get("nodule_id")
                status = r.get("adjudication_status", "unresolved")
                if nid:
                    update_sql = f"""
                    UPDATE `{TABLE_DISAGQ}`
                    SET adjudication_status = '{status}'
                    WHERE nodule_id = '{nid}'
                    """
                    try:
                        bq.query(update_sql, location=LOCATION).result()
                    except Exception as e:
                        _log(f"  WARNING: could not update status for {nid}: {e}")

            # Summarize verdicts
            verdicts = {}
            for r in e2_results:
                v = r.get("adjudication_status", "unresolved")
                verdicts[v] = verdicts.get(v, 0) + 1
            _log(f"  Verdict summary: {verdicts}")

    _log(f"\nPhase E complete. Total estimated cost: ${total_cost:.2f}")
    if total_cost > PHASE_E_COST_CEILING:
        _log(f"  WARNING: ${total_cost:.2f} exceeded Phase E ceiling ${PHASE_E_COST_CEILING}")
    else:
        _log(f"  Cost: PASS (${total_cost:.2f} ≤ ${PHASE_E_COST_CEILING})")


if __name__ == "__main__":
    main()
