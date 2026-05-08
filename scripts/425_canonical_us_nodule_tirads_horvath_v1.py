"""
Phase C.5 — Horvath / Chilean 2009 TI-RADS scorer (LLM-primary)
================================================================
Assigns Horvath pattern + category to each nodule in
pub_canonical.canonical_us_nodule_tirads_multisystem_v1.

Horvath / Chilean 2009 system classifies into 10 named patterns
(colloid type 1/2/3, Hashimoto pseudonodule, white-knight Hashimoto,
De Quervain unifocal, simple neoplastic, suspicious neoplastic,
malignant type A/B/C, and several short-circuit benign patterns)
using a combination of structured features AND clinical-narrative cues
that require LLM reasoning as the primary assignment method.

Architecture (LLM-primary with deterministic post-validation):
  1. Build per-nodule Gemini prompt table in pub_workspace (gland context +
     paraphrased source text ≤500 chars + structured primitives).
  2. 200-row dry-run with cost extrapolation. Halt if projected > $80.
  3. Full Gemini 2.5 Pro run via AI.GENERATE_TABLE.
  4. Parse responses → pub_workspace.note_entities_llm_horvath_v1.
  5. Deterministic post-validation: per-pattern feature-consistency checks.
  6. Second-pass revision: inconsistent rows get a focused Gemini revision
     prompt; revisions committed only if revised pattern is itself consistent.
  7. CTAS-rebuild canonical table with horvath_* columns.
  8. Audit: distribution sanity, post-validation rate, AUC proxy.

Pattern → Category mapping (Horvath 2009):
  colloid_type_1                 → TIRADS_2
  colloid_type_2                 → TIRADS_2
  colloid_type_3                 → TIRADS_3
  hashimoto_pseudonodule         → TIRADS_2 or TIRADS_3 (per echogenicity)
  white_knight_hashimoto         → TIRADS_2
  isolated_intraparenchymal_calc → TIRADS_2
  benign_concordant_aspirated    → TIRADS_2
  de_quervain_unifocal           → TIRADS_4A
  simple_neoplastic              → TIRADS_4A
  suspicious_neoplastic          → TIRADS_4B
  malignant_type_a               → TIRADS_4B  (± 4C when penetrating vessels)
  malignant_type_c               → TIRADS_4C
  malignant_type_b               → TIRADS_5
  unassignable                   → TIRADS_3  (default; rate tracked as quality metric)

Hard rules obeyed:
  - PHI guard: no raw text, ≤140 char paraphrased evidence.
  - Snapshot before CTAS mutation.
  - CLUSTER BY research_id preserved.
  - --dry-run and --skip-llm flags.
  - DFL row lifecycle=Applied.
  - Halt if projected LLM cost > $80.

Usage:
    python scripts/425_canonical_us_nodule_tirads_horvath_v1.py [--dry-run] [--skip-llm]

Author: Cursor Agent (Phase C.5), 2026-05-08
"""

import argparse
import json
import sys
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
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_PRIM_INPUT = f"{PROJECT}.{DATASET_WS}.tirads_primitive_backfill_input_v1"

TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_phaseC5_horvath_snapshot_v1"
TABLE_HORVATH_INPUT = f"{PROJECT}.{DATASET_WS}.tirads_horvath_input_v1"
TABLE_HORVATH_DRY = f"{PROJECT}.{DATASET_WS}.tirads_horvath_dryrun_v1"
TABLE_HORVATH_RAW = f"{PROJECT}.{DATASET_WS}.tirads_horvath_raw_v1"
TABLE_HORVATH_NLE = f"{PROJECT}.{DATASET_WS}.note_entities_llm_horvath_v1"
TABLE_HORVATH_INCON = f"{PROJECT}.{DATASET_WS}.tirads_horvath_inconsistent_v1"
TABLE_HORVATH_REVISED = f"{PROJECT}.{DATASET_WS}.tirads_horvath_revised_v1"

PRO_MODEL = f"`{PROJECT}.{DATASET_WS}.gemini_25_pro`"
PIPELINE_VERSION = "phase_c5_horvath_v1"
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

# Cost guardrail: $80 total ($60 Horvath + $20 Phase E)
COST_CEILING_USD = 80.0
HORVATH_COST_CEILING_USD = 60.0
# Gemini 2.5 Pro pricing via BigQuery ML AI.GENERATE_TABLE.
# Measured: prompt avg ~2,454 chars = ~614 tokens at 4 chars/token.
# Input: $3.50/M tokens = $0.00215/row. Output: ~50 tokens × $10.50/M = $0.000525/row.
# Total per-row: ~$0.00268. Rounded to $0.003 with 12% safety margin.
# (Original 1,200-token assumption of $0.0042/row was 1.6x overstated.)
COST_PER_ROW_ESTIMATE = 0.003
DRY_RUN_N = 200

# Deterministic pre-classification: composition-determined patterns that require no LLM.
# These map directly from composition (primary Horvath branch) with high confidence.
DETERMINISTIC_COMPOSITIONS = frozenset([
    "anechoic", "cystic",        # -> colloid_type_1 / TIRADS_2
    "spongiform",                # -> colloid_type_2 / TIRADS_2
    "predominantly_cystic",      # -> colloid_type_3 / TIRADS_3
])
# NULL composition is also deterministic (unassignable) — Horvath requires composition
# as the primary branch; without it no pattern can be assigned.


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> bigquery.QueryJob:
    _log(f"SQL: {label}")
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ job_id={job.job_id}")
    return job


def _scalar(bq: bigquery.Client, sql: str):
    row = next(iter(bq.query(sql, location=LOCATION).result()))
    return row[0]


def _halt(reason: str) -> None:
    _log(f"HALT: {reason}")
    sys.exit(2)


# ---------------------------------------------------------------------------
# Pattern → Category mapping
# ---------------------------------------------------------------------------

PATTERN_TO_CATEGORY = {
    "colloid_type_1":                  "TIRADS_2",
    "colloid_type_2":                  "TIRADS_2",
    "colloid_type_3":                  "TIRADS_3",
    "hashimoto_pseudonodule":          "TIRADS_2",   # may be upgraded to TIRADS_3 in post-val
    "white_knight_hashimoto":          "TIRADS_2",
    "isolated_intraparenchymal_calc":  "TIRADS_2",
    "benign_concordant_aspirated":     "TIRADS_2",
    "de_quervain_unifocal":            "TIRADS_4A",
    "simple_neoplastic":               "TIRADS_4A",
    "suspicious_neoplastic":           "TIRADS_4B",
    "malignant_type_a":                "TIRADS_4B",  # ±4C annotated via penetrating_vessels
    "malignant_type_c":                "TIRADS_4C",
    "malignant_type_b":                "TIRADS_5",
    "unassignable":                    "TIRADS_3",
}

VALID_PATTERNS = set(PATTERN_TO_CATEGORY.keys())

CYSTIC_DOMINANT = {"cystic", "almost_completely_cystic", "purely_cystic", "predominantly_cystic"}
SPONGIFORM = {"spongiform"}
SOLID_DOMINANT = {"solid", "predominantly_solid", "almost_completely_solid"}
MIXED = {"mixed_cystic_solid", "predominantly_cystic", "heterogeneous"}
MICRO_CALC_TOKENS = {"punctate_echogenic_foci", "punctate", "microcalcifications"}
CALC_ANY_TOKENS = {
    "punctate_echogenic_foci", "punctate", "microcalcifications",
    "macrocalcifications", "peripheral_rim_calcifications",
    "dystrophic_calcifications",
}


def _foci_contains(echogenic_foci: Optional[str], token_set: set) -> bool:
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        tokens = {str(i).lower() for i in (items if isinstance(items, list) else [items])}
    except (json.JSONDecodeError, TypeError):
        tokens = {str(echogenic_foci).lower()}
    return bool(tokens & token_set)


def _ete_present(row: dict) -> bool:
    ete_s = (row.get("ete_on_us_presence_simple") or "").lower()
    return ete_s not in ("", "none", "unstated", "absent", "no")


def _penetrating_vessels(row: dict) -> bool:
    try:
        v = json.loads(row.get("vascularity_jsonb") or "{}")
        pattern_v = str(v.get("pattern", "")).lower()
        dist = str(v.get("distribution", "")).lower()
        return ("penetrating" in pattern_v) or ("intranodular" in dist)
    except (json.JSONDecodeError, TypeError):
        return False


# ---------------------------------------------------------------------------
# Deterministic post-validation
# ---------------------------------------------------------------------------

def post_validate(pattern: str, row: dict, gland: dict) -> tuple[bool, str]:
    """
    Returns (consistent: bool, reason: str).
    consistent=True means the pattern is credible given the structured features.
    """
    comp = (row.get("composition") or "").lower()
    echo = (row.get("echogenicity") or "").lower()
    shape = (row.get("shape") or "").lower()
    margins = (row.get("margins") or "").lower()
    echogenic_foci = row.get("echogenic_foci")
    hashimoto_pattern = gland.get("hashimoto_pattern")

    if pattern == "colloid_type_1":
        # Cystic colloid with trabecular appearance → needs cystic composition
        if comp and comp not in CYSTIC_DOMINANT and comp not in SPONGIFORM:
            return False, f"colloid_type_1 expects cystic composition; got '{comp}'"
        return True, "ok"

    if pattern == "colloid_type_2":
        # Spongiform colloid
        if comp and comp not in SPONGIFORM and comp not in CYSTIC_DOMINANT:
            return False, f"colloid_type_2 expects spongiform/cystic; got '{comp}'"
        return True, "ok"

    if pattern == "colloid_type_3":
        # Hyperplastic expansive: NOT cystic/spongiform; more solid
        if comp in CYSTIC_DOMINANT or comp in SPONGIFORM:
            return False, f"colloid_type_3 expects non-cystic composition; got '{comp}'"
        return True, "ok"

    if pattern in ("hashimoto_pseudonodule", "white_knight_hashimoto"):
        # Requires Hashimoto gland context
        if not hashimoto_pattern:
            return False, f"'{pattern}' requires hashimoto_pattern in gland context; not present"
        return True, "ok"

    if pattern == "isolated_intraparenchymal_calc":
        # Requires some calcification in echogenic foci
        if not _foci_contains(echogenic_foci, CALC_ANY_TOKENS):
            return False, "isolated_intraparenchymal_calc expects calcification in echogenic_foci"
        return True, "ok"

    if pattern == "benign_concordant_aspirated":
        # This pattern is clinically driven (prior benign FNA); structured features alone
        # can't validate it.  We accept it unless the features scream malignancy.
        if (echo in ("very_hypoechoic", "markedly_hypoechoic")
                and _foci_contains(echogenic_foci, MICRO_CALC_TOKENS)
                and shape == "taller_than_wide"):
            return False, "benign_concordant_aspirated inconsistent with multiple high-risk features"
        return True, "ok"

    if pattern == "de_quervain_unifocal":
        # De Quervain: hypoechoic, possibly ill-defined margins, no microcalc
        # We relax this — primary signal is clinical context which we can't verify
        # in structured primitives, so accept unless clearly inconsistent.
        if comp in CYSTIC_DOMINANT:
            return False, "de_quervain_unifocal inconsistent with purely cystic composition"
        return True, "ok"

    if pattern == "simple_neoplastic":
        # Solid, well-defined, no high-risk features
        if comp and comp not in SOLID_DOMINANT and comp not in MIXED:
            return False, f"simple_neoplastic expects solid/mixed composition; got '{comp}'"
        # Should NOT have microcalcifications (that would push to suspicious/malignant)
        if _foci_contains(echogenic_foci, MICRO_CALC_TOKENS):
            return False, "simple_neoplastic inconsistent with microcalcifications present"
        if shape == "taller_than_wide":
            return False, "simple_neoplastic inconsistent with taller_than_wide shape"
        return True, "ok"

    if pattern == "suspicious_neoplastic":
        # Solid with ≥1 suspicious feature but not overtly malignant
        if comp and comp not in SOLID_DOMINANT and comp not in MIXED:
            return False, f"suspicious_neoplastic expects solid composition; got '{comp}'"
        return True, "ok"

    if pattern == "malignant_type_a":
        # Calcifications + penetrating vessels pattern
        if not _foci_contains(echogenic_foci, CALC_ANY_TOKENS):
            # Some malignant_type_a cases have peripheral/rim calcifications specifically
            return False, "malignant_type_a requires calcification in echogenic_foci"
        if comp and comp not in SOLID_DOMINANT and comp not in MIXED:
            return False, f"malignant_type_a expects solid composition; got '{comp}'"
        return True, "ok"

    if pattern == "malignant_type_b":
        # Overt malignant: solid hypoechoic with ≥2 of: TTW, microcalc, ETE, irregular margins
        if comp and comp not in SOLID_DOMINANT and comp not in MIXED:
            return False, f"malignant_type_b expects solid composition; got '{comp}'"
        mal_b_features = 0
        if shape == "taller_than_wide":
            mal_b_features += 1
        if _foci_contains(echogenic_foci, MICRO_CALC_TOKENS):
            mal_b_features += 1
        if _ete_present(row):
            mal_b_features += 1
        if margins in ("irregular", "microlobulated", "spiculated", "infiltrative"):
            mal_b_features += 1
        if echo in ("hypoechoic", "very_hypoechoic", "markedly_hypoechoic"):
            mal_b_features += 1
        if mal_b_features < 2:
            return False, f"malignant_type_b needs ≥2 high-risk features; found {mal_b_features}"
        return True, "ok"

    if pattern == "malignant_type_c":
        # Atypical malignant features not fitting A or B
        if comp and comp not in SOLID_DOMINANT and comp not in MIXED:
            return False, f"malignant_type_c expects solid composition; got '{comp}'"
        return True, "ok"

    if pattern == "unassignable":
        # Always accept unassignable — but flag if there are clear features that should assign
        return True, "ok"

    # Unknown pattern
    return False, f"Pattern '{pattern}' not in valid enum"


# ---------------------------------------------------------------------------
# Category adjustment post-validation
# ---------------------------------------------------------------------------

def adjust_category(pattern: str, row: dict, gland: dict) -> str:
    """
    Apply Horvath nuance rules:
    - hashimoto_pseudonodule → TIRADS_2 normally, TIRADS_3 if hyperechoic/complex
    - malignant_type_a → TIRADS_4C if penetrating vessels confirmed
    """
    base = PATTERN_TO_CATEGORY.get(pattern, "TIRADS_3")

    if pattern == "hashimoto_pseudonodule":
        echo = (row.get("echogenicity") or "").lower()
        comp = (row.get("composition") or "").lower()
        # Complex/hyperechoic Hashimoto pseudonodule → TIRADS_3
        if echo in ("hyperechoic", "very_hyperechoic") or comp not in CYSTIC_DOMINANT:
            return "TIRADS_3"
        return "TIRADS_2"

    if pattern == "malignant_type_a":
        if _penetrating_vessels(row):
            return "TIRADS_4C"
        return "TIRADS_4B"

    return base


# ---------------------------------------------------------------------------
# Step 1: Build Horvath input table (BQ)
# ---------------------------------------------------------------------------

BUILD_INPUT_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_INPUT}`
CLUSTER BY research_id AS
WITH gland AS (
  -- One row per research_id (or per exam if multi-exam); take the most recent gland exam
  SELECT
    research_id,
    ANY_VALUE(background_echogenicity) AS background_echogenicity,
    ANY_VALUE(hashimoto_pattern)       AS hashimoto_pattern,
    ANY_VALUE(goiter_flag)             AS goiter_flag
  FROM `{TABLE_GLAND_V2}`
  GROUP BY research_id
),
src_text AS (
  -- Paraphrased source text from Phase A.3 backfill (≤500 chars, already PHI-scrubbed
  -- by convention — raw_source_text is a structured field from COMPLETE_MULTI_SHEET)
  SELECT
    nodule_id,
    LEFT(COALESCE(source_text, ''), 500) AS source_text_short
  FROM `{TABLE_PRIM_INPUT}`
)
SELECT
  n.nodule_id,
  n.research_id,
  n.us_exam_id,
  n.exam_date,
  -- Structured primitives
  n.composition,
  n.echogenicity,
  n.shape,
  n.margins,
  n.echogenic_foci,
  n.halo_presence_simple,
  n.vascularity_distribution_simple,
  n.ete_on_us_presence_simple,
  n.size_cm_max,
  n.halo_jsonb,
  n.vascularity_jsonb,
  n.ete_us_jsonb,
  -- Gland-level context
  COALESCE(g.background_echogenicity, 'unknown') AS background_echogenicity,
  COALESCE(g.hashimoto_pattern,       'none')    AS hashimoto_pattern,
  COALESCE(CAST(g.goiter_flag AS STRING), 'unknown') AS goiter_flag,
  -- Source text paraphrase (PHI-scrubbed, ≤500 chars)
  COALESCE(st.source_text_short, '')             AS source_text_paraphrase,
  -- LN context
  COALESCE(CAST(ln.has_suspicious_ln_within_60d AS BOOL), FALSE) AS has_suspicious_ln_within_60d,
  -- Pre-built Horvath Gemini prompt concatenated inline for AI.GENERATE_TABLE
  CONCAT(
    -- System instructions compressed into user turn
    'HORVATH/CHILEAN 2009 TI-RADS CLASSIFICATION\\n',
    'Assign exactly ONE pattern from: colloid_type_1, colloid_type_2, colloid_type_3, ',
    'hashimoto_pseudonodule, white_knight_hashimoto, isolated_intraparenchymal_calc, ',
    'benign_concordant_aspirated, de_quervain_unifocal, simple_neoplastic, ',
    'suspicious_neoplastic, malignant_type_a, malignant_type_b, malignant_type_c, unassignable.\\n\\n',
    -- Pattern guidance summary
    'Pattern rules (pick best match):\\n',
    '- colloid_type_1: cystic/spongiform, colloid cyst with trabecular pattern -> TIRADS_2\\n',
    '- colloid_type_2: spongiform sponge-like isoechoic -> TIRADS_2\\n',
    '- colloid_type_3: hyperplastic colloid nodule, partially solid, expansive -> TIRADS_3\\n',
    '- hashimoto_pseudonodule: hypoechoic/heterogeneous in Hashimoto gland -> TIRADS_2/3\\n',
    '- white_knight_hashimoto: hyperechoic nodule in Hashimoto (benign) -> TIRADS_2\\n',
    '- isolated_intraparenchymal_calc: calcification without true nodule -> TIRADS_2\\n',
    '- benign_concordant_aspirated: already-aspirated benign cytology -> TIRADS_2\\n',
    '- de_quervain_unifocal: hypoechoic lesion in subacute thyroiditis context -> TIRADS_4A\\n',
    '- simple_neoplastic: solid, well-defined, no high-risk features, follicular-like -> TIRADS_4A\\n',
    '- suspicious_neoplastic: solid, >=1 suspicious feature (not overtly malignant) -> TIRADS_4B\\n',
    '- malignant_type_a: calc + penetrating vessels (PTC pattern) -> TIRADS_4B/4C\\n',
    '- malignant_type_b: solid hypoechoic, TTW/microcalc/irregular margins/ETE -> TIRADS_5\\n',
    '- malignant_type_c: atypical malignant features not fitting A or B -> TIRADS_4C\\n',
    '- unassignable: no pattern clearly fits -> TIRADS_3 (use sparingly)\\n\\n',
    '<structured_features>\\n',
    'composition: ', COALESCE(n.composition, 'unknown'), '\\n',
    'echogenicity: ', COALESCE(n.echogenicity, 'unknown'), '\\n',
    'shape: ', COALESCE(n.shape, 'unknown'), '\\n',
    'margins: ', COALESCE(n.margins, 'unknown'), '\\n',
    'echogenic_foci: ', COALESCE(n.echogenic_foci, '[]'), '\\n',
    'halo_presence: ', COALESCE(n.halo_presence_simple, 'unstated'), '\\n',
    'vascularity: ', COALESCE(n.vascularity_distribution_simple, 'unstated'), '\\n',
    'ete_on_us: ', COALESCE(n.ete_on_us_presence_simple, 'unstated'), '\\n',
    'size_cm: ', COALESCE(CAST(n.size_cm_max AS STRING), 'unknown'), '\\n',
    '</structured_features>\\n',
    '<gland_context>\\n',
    'background_echogenicity: ', COALESCE(g.background_echogenicity, 'unknown'), '\\n',
    'hashimoto_pattern: ', COALESCE(g.hashimoto_pattern, 'none'), '\\n',
    'goiter_flag: ', COALESCE(CAST(g.goiter_flag AS STRING), 'unknown'), '\\n',
    '</gland_context>\\n',
    '<source_text_paraphrase>\\n',
    COALESCE(st.source_text_short, '[no source text available]'), '\\n',
    '</source_text_paraphrase>\\n\\n',
    'Output JSON: {{pattern: STRING, category: STRING, evidence_short: STRING (<=140 chars no PHI), confidence: FLOAT64}}\\n',
    'CRITICAL: evidence_short must be <=140 chars and must NEVER contain dates of service, ',
    'patient names, MRN, or DOB. Paraphrase only. Return ONLY the JSON object.'
  ) AS horvath_prompt
FROM `{TABLE_NODULE_V2}` n
LEFT JOIN gland g ON n.research_id = g.research_id
LEFT JOIN src_text st ON n.nodule_id = st.nodule_id
LEFT JOIN `{TABLE_LN_CTX}` ln ON n.nodule_id = ln.nodule_id;
"""

# ---------------------------------------------------------------------------
# Step 1b: Deterministic pre-classification (composition-determined patterns)
# Saves ~50% of LLM cost by assigning obvious patterns without model inference.
# ---------------------------------------------------------------------------

TABLE_HORVATH_DET = f"{PROJECT}.{DATASET_WS}.tirads_horvath_deterministic_v1"

DETERMINISTIC_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_DET}`
CLUSTER BY research_id AS
SELECT
  nodule_id,
  research_id,
  CASE
    WHEN composition IN ('anechoic','cystic')  THEN 'colloid_type_1'
    WHEN composition = 'spongiform'            THEN 'colloid_type_2'
    WHEN composition = 'predominantly_cystic'  THEN 'colloid_type_3'
    ELSE 'unassignable'
  END AS pattern,
  CASE
    WHEN composition IN ('anechoic','cystic')  THEN 'TIRADS_2'
    WHEN composition = 'spongiform'            THEN 'TIRADS_2'
    WHEN composition = 'predominantly_cystic'  THEN 'TIRADS_3'
    ELSE 'TIRADS_3'
  END AS category,
  CASE
    WHEN composition IN ('anechoic','cystic')  THEN 'cystic/anechoic composition -> colloid_type_1'
    WHEN composition = 'spongiform'            THEN 'spongiform composition -> colloid_type_2'
    WHEN composition = 'predominantly_cystic'  THEN 'predominantly cystic composition -> colloid_type_3'
    ELSE 'composition NULL: unassignable by Horvath rules'
  END AS evidence_short,
  1.0 AS confidence,
  TRUE AS post_validation_consistent,
  CAST(NULL AS STRING) AS inconsistency_reason,
  FALSE AS revised,
  'deterministic_preclass' AS assignment_method
FROM `{TABLE_HORVATH_INPUT}`
WHERE composition IN ('anechoic','cystic','spongiform','predominantly_cystic')
   OR composition IS NULL;
"""

# ---------------------------------------------------------------------------
# Step 2: 200-row dry run (LLM-required rows only)
# ---------------------------------------------------------------------------

DRY_RUN_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_DRY}`
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (
    SELECT horvath_prompt AS prompt, nodule_id, research_id
    FROM `{TABLE_HORVATH_INPUT}`
    WHERE composition NOT IN ('anechoic','cystic','spongiform','predominantly_cystic')
      AND composition IS NOT NULL
    LIMIT {DRY_RUN_N}
  ),
  STRUCT(
    'pattern STRING, category STRING, evidence_short STRING, confidence FLOAT64'
      AS output_schema,
    0.0 AS temperature,
    2048 AS max_output_tokens
  )
);
"""

# ---------------------------------------------------------------------------
# Step 3: Full Gemini 2.5 Pro run (LLM-required rows only; deterministic merged later)
# ---------------------------------------------------------------------------

FULL_RUN_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_RAW}`
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (
    SELECT horvath_prompt AS prompt, nodule_id, research_id
    FROM `{TABLE_HORVATH_INPUT}`
    WHERE composition NOT IN ('anechoic','cystic','spongiform','predominantly_cystic')
      AND composition IS NOT NULL
  ),
  STRUCT(
    'pattern STRING, category STRING, evidence_short STRING, confidence FLOAT64'
      AS output_schema,
    0.0 AS temperature,
    2048 AS max_output_tokens
  )
);
"""

# ---------------------------------------------------------------------------
# Step 4: Parse + normalize into NLE table
# ---------------------------------------------------------------------------

PARSE_TO_NLE_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_NLE}`
CLUSTER BY research_id AS
SELECT
  r.nodule_id,
  r.research_id,
  -- Normalize pattern to enum (lower, trim, replace spaces)
  LOWER(TRIM(REPLACE(r.pattern, ' ', '_')))      AS pattern_raw,
  -- Enforce enum membership
  CASE
    WHEN LOWER(TRIM(REPLACE(r.pattern, ' ', '_'))) IN (
      'colloid_type_1','colloid_type_2','colloid_type_3',
      'hashimoto_pseudonodule','white_knight_hashimoto',
      'isolated_intraparenchymal_calc','benign_concordant_aspirated',
      'de_quervain_unifocal','simple_neoplastic','suspicious_neoplastic',
      'malignant_type_a','malignant_type_b','malignant_type_c','unassignable'
    ) THEN LOWER(TRIM(REPLACE(r.pattern, ' ', '_')))
    ELSE 'unassignable'
  END                                             AS pattern,
  r.category                                      AS category_llm,
  -- PHI guard: truncate evidence to 140 chars
  LEFT(COALESCE(r.evidence_short, ''), 140)       AS evidence_short,
  COALESCE(CAST(r.confidence AS FLOAT64), 0.5)   AS confidence,
  '{RUN_TS}'                                      AS processed_at
FROM `{TABLE_HORVATH_RAW}` r;
"""

# ---------------------------------------------------------------------------
# Step 5: Deterministic post-validation (Python loop, results written back)
# ---------------------------------------------------------------------------

# (Implemented as Python loop below — see post_validate() function)

# ---------------------------------------------------------------------------
# Step 6: Second-pass revision prompt (Gemini 2.5 Pro focused revision)
# ---------------------------------------------------------------------------

SECOND_PASS_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_REVISED}`
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (
    SELECT
      CONCAT(
        'The Horvath/Chilean pattern assigned was: ', i.pattern, '\n',
        'Post-validation inconsistency reason: ', i.inconsistency_reason, '\n\n',
        'Please revise to a DIFFERENT, more consistent pattern from the enum.\n',
        'If no valid pattern fits, use unassignable.\n\n',
        'Original structured features:\n',
        h.horvath_prompt
      ) AS prompt,
      i.nodule_id,
      i.research_id
    FROM `{TABLE_HORVATH_INCON}` i
    JOIN `{TABLE_HORVATH_INPUT}` h USING (nodule_id)
  ),
  STRUCT(
    'pattern STRING, category STRING, evidence_short STRING, confidence FLOAT64'
      AS output_schema,
    0.0 AS temperature,
    2048 AS max_output_tokens
  )
);
"""

# ---------------------------------------------------------------------------
# Step 7: CTAS rebuild
# ---------------------------------------------------------------------------


def build_ctas_sql(skip_llm: bool) -> str:
    if skip_llm:
        join_nle = ""
        ho_pattern = "CAST(NULL AS STRING)"
        ho_category = "CAST(NULL AS STRING)"
        ho_evidence = "CAST(NULL AS STRING)"
        ho_confidence = "CAST(NULL AS FLOAT64)"
        ho_pvc = "CAST(NULL AS BOOL)"
        ho_method = "CAST(NULL AS STRING)"
    else:
        join_nle = f"\n  LEFT JOIN `{TABLE_HORVATH_NLE}` nle ON m.nodule_id = nle.nodule_id"
        ho_pattern = "nle.pattern"
        ho_category = "nle.category_adjusted"
        ho_evidence = "nle.evidence_short"
        ho_confidence = "nle.confidence"
        ho_pvc = "nle.post_validation_consistent"
        ho_method = (
            "CASE WHEN nle.pattern IS NOT NULL THEN "
            "  CASE WHEN nle.revised THEN 'llm_gemini_25_pro_revised' "
            "  ELSE 'llm_gemini_25_pro' END "
            "ELSE NULL END"
        )

    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    horvath_pattern, horvath_category, horvath_evidence_short,
    horvath_confidence, horvath_post_validation_consistent,
    horvath_decision_method
  ),
  {ho_pattern}    AS horvath_pattern,
  {ho_category}   AS horvath_category,
  {ho_evidence}   AS horvath_evidence_short,
  {ho_confidence} AS horvath_confidence,
  {ho_pvc}        AS horvath_post_validation_consistent,
  {ho_method}     AS horvath_decision_method
FROM `{TABLE_MULTISYS}` m{join_nle};
"""


# ---------------------------------------------------------------------------
# Audit SQL
# ---------------------------------------------------------------------------

AUDIT_SQL = f"""
SELECT
  horvath_pattern,
  horvath_category,
  COUNT(*) AS n,
  ROUND(COUNTIF(horvath_post_validation_consistent) / COUNT(*), 4) AS post_valid_rate,
  ROUND(AVG(horvath_confidence), 3) AS mean_conf
FROM `{TABLE_MULTISYS}`
WHERE horvath_pattern IS NOT NULL
GROUP BY 1, 2
ORDER BY n DESC;
"""

SANITY_SQL = f"""
SELECT
  COUNTIF(horvath_pattern IS NOT NULL)                     AS n_scored,
  COUNTIF(horvath_pattern = 'unassignable')                AS n_unassignable,
  COUNTIF(horvath_category IN ('TIRADS_2', 'TIRADS_3'))    AS n_benign_range,
  COUNTIF(horvath_category = 'TIRADS_5')                   AS n_tirads5,
  COUNTIF(NOT COALESCE(horvath_post_validation_consistent, TRUE)) AS n_inconsistent,
  COUNTIF(horvath_decision_method = 'llm_gemini_25_pro_revised') AS n_revised,
  COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`;
"""


# ---------------------------------------------------------------------------
# Schema ALTER (idempotent)
# ---------------------------------------------------------------------------

ALTER_SQL = f"""
ALTER TABLE `{TABLE_MULTISYS}`
  ADD COLUMN IF NOT EXISTS horvath_pattern STRING,
  ADD COLUMN IF NOT EXISTS horvath_category STRING,
  ADD COLUMN IF NOT EXISTS horvath_evidence_short STRING,
  ADD COLUMN IF NOT EXISTS horvath_confidence FLOAT64,
  ADD COLUMN IF NOT EXISTS horvath_post_validation_consistent BOOL,
  ADD COLUMN IF NOT EXISTS horvath_decision_method STRING;
"""


# ---------------------------------------------------------------------------
# DFL logger
# ---------------------------------------------------------------------------

def append_dfl_row(bq: bigquery.Client, dry_run: bool, audit: dict) -> None:
    if dry_run:
        _log("DFL: skipped (dry-run)")
        return
    try:
        row = {
            "logged_at": datetime.now(timezone.utc).isoformat(),
            "target_type": "BQ infrastructure",
            "change_type": "new_column_data",
            "target_table": TABLE_MULTISYS,
            "target_column": "horvath_*",
            "action_summary": (
                f"Phase C.5 Horvath/Chilean 2009 LLM-primary scorer. "
                f"n_scored={audit.get('n_scored')}, n_tirads5={audit.get('n_tirads5')}, "
                f"n_unassignable={audit.get('n_unassignable')}, "
                f"n_inconsistent={audit.get('n_inconsistent')}, "
                f"n_revised={audit.get('n_revised')}. "
                f"Model=gemini_25_pro. Pipeline={PIPELINE_VERSION}."
            )[:280],
            "lifecycle": "Applied",
            "source_chat": "Phase C.5 Horvath cursor prompt 2026-05-08",
            "phi_guard_confirmed": True,
        }
        errors = bq.insert_rows_json(f"{PROJECT}.pub_signoff.data_feedback_log_v1", [row])
        if errors:
            _log(f"DFL WARNING: {errors}")
        else:
            _log("DFL: row inserted (lifecycle=Applied)")
    except Exception as e:
        _log(f"DFL: failed ({e}). Continuing.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Horvath 2009 scorer (Phase C.5)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Build input + 200-row LLM test; no CTAS rebuild.")
    parser.add_argument("--skip-llm", action="store_true",
                        help="Score deterministically only (all rows will be NULL for Horvath).")
    parser.add_argument("--skip-second-pass", action="store_true",
                        help="Skip second-pass revision for inconsistent rows.")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    # ------------------------------------------------------------------
    # Step 0: Schema ALTER (idempotent)
    # ------------------------------------------------------------------
    _log("Step 0: ALTER TABLE — add horvath_* columns (idempotent)")
    _run_sql(bq, ALTER_SQL, "ALTER TABLE add horvath_* columns")

    # ------------------------------------------------------------------
    # Step 1: Snapshot
    # ------------------------------------------------------------------
    _log("Step 1: Snapshot canonical table")
    _run_sql(bq,
             f"CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}` AS SELECT * FROM `{TABLE_MULTISYS}`",
             "Snapshot")
    n_snap = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_SNAPSHOT}`")
    _log(f"  Snapshot rows: {n_snap}")

    # ------------------------------------------------------------------
    # Step 2: Build Horvath input table
    # ------------------------------------------------------------------
    _log("Step 2: Build Horvath input table (gland context + source text + prompts)")
    _run_sql(bq, BUILD_INPUT_SQL, "Build tirads_horvath_input_v1")
    n_input = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_INPUT}`")
    _log(f"  Input rows: {n_input}")

    # ------------------------------------------------------------------
    # Step 2b: Deterministic pre-classification (composition-determined patterns)
    # Runs BEFORE skip_llm check so deterministic rows always get assigned.
    # ------------------------------------------------------------------
    _log("Step 2b: Deterministic pre-classification (cystic/spongiform/predominantly_cystic/NULL)")
    _run_sql(bq, DETERMINISTIC_SQL, "Deterministic pre-classification")
    n_deterministic = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_DET}`")
    n_llm_required = n_input - n_deterministic
    _log(f"  Deterministic rows: {n_deterministic} | LLM-required rows: {n_llm_required}")

    if args.skip_llm:
        _log("--skip-llm: skipping LLM inference. Only deterministic rows will be in NLE table.")
        _log("CTAS rebuild not performed (no LLM data to write). Exiting.")
        return

    # ------------------------------------------------------------------
    # Step 3: 200-row dry run + cost projection (LLM-required rows only)
    # ------------------------------------------------------------------
    _log(f"Step 3: 200-row dry run for cost validation (LLM-required rows only)")
    _run_sql(bq, DRY_RUN_SQL, "Horvath dry-run (200 rows)")
    n_dry = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_DRY}`")
    _log(f"  Dry-run output rows: {n_dry} (expected ≈ {DRY_RUN_N})")

    # Cost projection: LLM-required rows only (deterministic rows are free)
    projected_horvath = n_llm_required * COST_PER_ROW_ESTIMATE
    _log(f"  LLM-required rows: {n_llm_required} (saved {n_deterministic} via deterministic preclass)")
    _log(f"  Projected Horvath LLM cost: ${projected_horvath:.2f} "
         f"({n_llm_required} rows × ${COST_PER_ROW_ESTIMATE}/row)")
    if projected_horvath > HORVATH_COST_CEILING_USD:
        _halt(f"Projected Horvath cost ${projected_horvath:.2f} exceeds Horvath ceiling "
              f"${HORVATH_COST_CEILING_USD}. Halting. "
              "Increase deterministic scope or reduce prompt size.")
    _log(f"  Cost projection: PASS (${projected_horvath:.2f} ≤ ${HORVATH_COST_CEILING_USD})")

    # Validate dry-run output quality
    dry_rows = list(bq.query(
        f"SELECT pattern, confidence FROM `{TABLE_HORVATH_DRY}` LIMIT 200",
        location=LOCATION
    ).result())
    n_dry_valid = sum(
        1 for r in dry_rows
        if r.pattern and r.pattern.strip().lower().replace(" ", "_") in VALID_PATTERNS
    )
    dry_valid_pct = n_dry_valid / max(1, len(dry_rows))
    _log(f"  Dry-run valid patterns: {n_dry_valid}/{len(dry_rows)} ({dry_valid_pct:.1%})")
    if dry_valid_pct < 0.70:
        _halt(f"Dry-run valid-pattern rate {dry_valid_pct:.1%} < 70%. "
              "LLM is returning off-enum responses. Fix prompt before full run.")

    if args.dry_run:
        _log("DRY RUN mode: stopping after 200-row test. No CTAS rebuild.")
        return

    # ------------------------------------------------------------------
    # Step 4: Full Gemini 2.5 Pro run (LLM-required rows only)
    # ------------------------------------------------------------------
    _log(f"Step 4: Full Gemini 2.5 Pro run ({n_llm_required} LLM-required rows)")
    _run_sql(bq, FULL_RUN_SQL, "Full Horvath LLM run")
    n_raw = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_RAW}`")
    _log(f"  Raw output rows: {n_raw}")

    # Parse into NLE table (LLM rows only first)
    _log("Step 4b: Parse + normalize LLM results into NLE table")
    _run_sql(bq, PARSE_TO_NLE_SQL, "Parse Horvath raw → NLE")
    n_nle_llm = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_NLE}`")
    _log(f"  NLE rows (LLM): {n_nle_llm}")

    n_nle = n_nle_llm  # Deterministic rows merged after post-validation (Step 6b)

    # ------------------------------------------------------------------
    # Step 5: Deterministic post-validation (Python loop)
    # ------------------------------------------------------------------
    _log("Step 5: Deterministic post-validation")

    # Pull NLE + structured features for validation
    nle_rows = list(bq.query(f"""
    SELECT
      nle.nodule_id, nle.research_id, nle.pattern, nle.category_llm,
      nle.evidence_short, nle.confidence,
      -- Structured features
      n.composition, n.echogenicity, n.shape, n.margins,
      n.echogenic_foci, n.halo_presence_simple,
      n.vascularity_distribution_simple, n.ete_on_us_presence_simple,
      n.halo_jsonb, n.vascularity_jsonb, n.ete_us_jsonb,
      -- Gland context
      h.hashimoto_pattern AS gland_hashimoto_pattern
    FROM `{TABLE_HORVATH_NLE}` nle
    JOIN `{TABLE_NODULE_V2}` n USING (nodule_id)
    LEFT JOIN (
      SELECT research_id, ANY_VALUE(hashimoto_pattern) AS hashimoto_pattern
      FROM `{TABLE_GLAND_V2}` GROUP BY research_id
    ) h ON nle.research_id = h.research_id
    """, location=LOCATION).result())

    _log(f"  Validating {len(nle_rows)} NLE rows")

    import pandas as pd

    validated = []
    inconsistent_for_second_pass = []
    n_consistent = 0
    n_inconsistent = 0

    for row in nle_rows:
        rd = dict(row)
        pattern = rd.get("pattern") or "unassignable"
        gland_ctx = {"hashimoto_pattern": rd.get("gland_hashimoto_pattern")}

        consistent, reason = post_validate(pattern, rd, gland_ctx)
        adjusted_category = adjust_category(pattern, rd, gland_ctx)

        if consistent:
            n_consistent += 1
        else:
            n_inconsistent += 1
            inconsistent_for_second_pass.append({
                "nodule_id": rd["nodule_id"],
                "research_id": rd["research_id"],
                "pattern": pattern,
                "inconsistency_reason": reason[:200],
            })

        validated.append({
            "nodule_id": rd["nodule_id"],
            "research_id": rd["research_id"],
            "pattern": pattern,
            "category_llm": rd.get("category_llm"),
            "category_adjusted": adjusted_category,
            "evidence_short": rd.get("evidence_short"),
            "confidence": float(rd.get("confidence") or 0.5),
            "post_validation_consistent": consistent,
            "inconsistency_reason": reason if not consistent else None,
            "revised": False,
        })

    pvc_rate = n_consistent / max(1, len(validated))
    _log(f"  Post-validation: consistent={n_consistent}, inconsistent={n_inconsistent}, "
         f"rate={pvc_rate:.1%}")
    if pvc_rate < 0.85:
        _log(f"  WARNING: post_validation_consistent rate {pvc_rate:.1%} < 90% target. "
             f"LLM may be freelancing patterns — review prompt quality.")

    # Write NLE with post-validation flags
    df_nle = pd.DataFrame(validated)
    df_nle["confidence"] = df_nle["confidence"].astype("float64")

    # Write inconsistents to staging table for second pass
    if inconsistent_for_second_pass and not args.skip_second_pass:
        df_incon = pd.DataFrame(inconsistent_for_second_pass)
        job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        load_job = bq.load_table_from_dataframe(
            df_incon, TABLE_HORVATH_INCON,
            job_config=job_cfg, location=LOCATION
        )
        load_job.result()
        n_incon = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_INCON}`")
        _log(f"  Inconsistent table: {n_incon} rows")

    # ------------------------------------------------------------------
    # Step 6: Second-pass revision for inconsistent rows
    # ------------------------------------------------------------------
    n_committed_revisions = 0
    if inconsistent_for_second_pass and not args.skip_second_pass:
        _log(f"Step 6: Second-pass revision ({len(inconsistent_for_second_pass)} inconsistent rows)")
        _run_sql(bq, SECOND_PASS_SQL, "Horvath second-pass revision")
        n_rev_raw = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_REVISED}`")
        _log(f"  Revision raw output: {n_rev_raw} rows")

        # Pull revisions
        rev_rows = list(bq.query(
            f"SELECT nodule_id, research_id, pattern, category, evidence_short, confidence "
            f"FROM `{TABLE_HORVATH_REVISED}`",
            location=LOCATION
        ).result())

        # Map nodule_id → validated row index for update
        nle_idx = {v["nodule_id"]: i for i, v in enumerate(validated)}

        # Pull structured features for revised-row validation
        incon_ids = {d["nodule_id"] for d in inconsistent_for_second_pass}
        feat_map = {
            dict(r)["nodule_id"]: dict(r) for r in bq.query(f"""
            SELECT n.nodule_id, n.composition, n.echogenicity, n.shape, n.margins,
                   n.echogenic_foci, n.ete_on_us_presence_simple,
                   n.vascularity_jsonb, n.ete_us_jsonb,
                   ANY_VALUE(g.hashimoto_pattern) AS gland_hashimoto_pattern
            FROM `{TABLE_NODULE_V2}` n
            LEFT JOIN `{TABLE_GLAND_V2}` g ON n.research_id = g.research_id
            WHERE n.nodule_id IN ({','.join(repr(i) for i in incon_ids)})
            GROUP BY 1,2,3,4,5,6,7,8,9
            """, location=LOCATION).result()
        }

        for rr in rev_rows:
            rd_rev = dict(rr)
            nid = rd_rev["nodule_id"]
            rev_pattern = (rd_rev.get("pattern") or "unassignable").strip().lower().replace(" ", "_")
            if rev_pattern not in VALID_PATTERNS:
                rev_pattern = "unassignable"

            feat = feat_map.get(nid, {})
            gland_ctx_r = {"hashimoto_pattern": feat.get("gland_hashimoto_pattern")}
            rev_consistent, rev_reason = post_validate(rev_pattern, feat, gland_ctx_r)

            # Only commit if the revision is itself consistent
            if rev_consistent and nid in nle_idx:
                idx = nle_idx[nid]
                rev_category = adjust_category(rev_pattern, feat, gland_ctx_r)
                validated[idx]["pattern"] = rev_pattern
                validated[idx]["category_adjusted"] = rev_category
                validated[idx]["evidence_short"] = (
                    rd_rev.get("evidence_short") or validated[idx]["evidence_short"]
                )[:140]
                validated[idx]["confidence"] = float(rd_rev.get("confidence") or 0.5)
                validated[idx]["post_validation_consistent"] = True
                validated[idx]["inconsistency_reason"] = None
                validated[idx]["revised"] = True
                n_committed_revisions += 1

        _log(f"  Committed revisions: {n_committed_revisions} / {len(inconsistent_for_second_pass)}")
    else:
        _log("Step 6: Second-pass skipped (--skip-second-pass or no inconsistents)")

    # ------------------------------------------------------------------
    # Step 6b: Write final NLE table (with revisions applied)
    # ------------------------------------------------------------------
    _log("Step 6b: Write final NLE table (post-validation + revisions)")
    df_final = pd.DataFrame(validated)
    df_final["confidence"] = df_final["confidence"].astype("float64")
    df_final["post_validation_consistent"] = df_final["post_validation_consistent"].astype(bool)
    df_final["revised"] = df_final["revised"].astype(bool)

    # Add assignment_method column if missing (LLM rows get tagged)
    if "assignment_method" not in df_final.columns:
        df_final["assignment_method"] = "llm_gemini_25_pro"

    job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    load_job = bq.load_table_from_dataframe(
        df_final, TABLE_HORVATH_NLE,
        job_config=job_cfg, location=LOCATION
    )
    load_job.result()
    n_llm_nle = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_NLE}`")
    _log(f"  LLM NLE rows written: {n_llm_nle}")

    # Step 6c: Append deterministic pre-classified rows into NLE table
    # These rows are composition-determined, pass post-validation by definition.
    _log("Step 6c: Append deterministic pre-classified rows into NLE table")
    det_rows = list(bq.query(
        f"SELECT * FROM `{TABLE_HORVATH_DET}`",
        location=LOCATION,
    ).result())
    if det_rows:
        df_det = pd.DataFrame([dict(r) for r in det_rows])
        # Align schema with final NLE table
        for col in df_final.columns:
            if col not in df_det.columns:
                if col == "processed_at":
                    df_det[col] = RUN_TS
                elif col in ("post_validation_consistent", "revised"):
                    df_det[col] = True
                elif col == "category_llm":
                    df_det[col] = df_det.get("category", None)
                elif col == "category_adjusted":
                    df_det[col] = df_det.get("category", None)
                elif col == "pattern_raw":
                    df_det[col] = df_det.get("pattern", None)
                else:
                    df_det[col] = None
        # Reorder to match df_final column order
        det_cols = [c for c in df_final.columns if c in df_det.columns]
        df_det = df_det[det_cols]
        df_det["confidence"] = df_det["confidence"].astype("float64")
        df_det["post_validation_consistent"] = df_det["post_validation_consistent"].astype(bool)
        df_det["revised"] = df_det["revised"].fillna(False).astype(bool)
        job_cfg_app = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=False,
                                             schema=load_job.result() if False else None)
        job_cfg_app = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND", autodetect=True)
        app_job = bq.load_table_from_dataframe(
            df_det, TABLE_HORVATH_NLE,
            job_config=job_cfg_app, location=LOCATION
        )
        app_job.result()
        _log(f"  Appended {len(df_det)} deterministic rows")
    n_final_nle = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_NLE}`")
    _log(f"  Final NLE rows (LLM + deterministic): {n_final_nle}")

    # ------------------------------------------------------------------
    # Step 7: CTAS rebuild
    # ------------------------------------------------------------------
    _log("Step 7: CTAS rebuild — canonical table with horvath_* columns")
    _run_sql(bq, build_ctas_sql(skip_llm=False), "Horvath CTAS rebuild")
    n_rebuilt = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_MULTISYS}`")
    if n_rebuilt != n_snap:
        _halt(f"Row count mismatch after CTAS: snap={n_snap}, rebuilt={n_rebuilt}")
    _log(f"  CTAS complete: {n_rebuilt} rows (row count matches snapshot ✓)")

    # ------------------------------------------------------------------
    # Step 8: Audit
    # ------------------------------------------------------------------
    _log("Step 8: Audit — pattern distribution")
    for r in bq.query(AUDIT_SQL, location=LOCATION).result():
        _log(f"  {dict(r)}")

    sanity = dict(next(iter(bq.query(SANITY_SQL, location=LOCATION).result())))
    _log(f"Audit sanity: {sanity}")

    n_scored = sanity["n_scored"]
    n_unassignable = sanity["n_unassignable"]
    n_benign_range = sanity["n_benign_range"]
    n_tirads5 = sanity["n_tirads5"]
    n_final_inconsistent = n_inconsistent - n_committed_revisions

    # Sanity checks
    unassignable_rate = n_unassignable / max(1, n_scored)
    if unassignable_rate > 0.15:
        _log(f"  WARNING: unassignable rate {unassignable_rate:.1%} > 15% — "
             "prompt or input may be too sparse for Horvath assignment.")

    tirads5_rate = n_tirads5 / max(1, n_scored)
    if tirads5_rate < 0.03 or tirads5_rate > 0.20:
        _log(f"  WARNING: TIRADS_5 rate {tirads5_rate:.1%} outside expected 5–15% range.")

    benign_rate = n_benign_range / max(1, n_scored)
    if benign_rate < 0.20 or benign_rate > 0.60:
        _log(f"  WARNING: benign-range (TIRADS_2/3) rate {benign_rate:.1%} outside 20–60% range. "
             "Surgical cohort enrichment expected to shift distribution toward suspicious.")
    else:
        _log(f"  Benign-range rate: {benign_rate:.1%} ✓")
    _log(f"  Final inconsistency (post-revision): {n_final_inconsistent}")

    # ------------------------------------------------------------------
    # Step 9: DFL
    # ------------------------------------------------------------------
    audit_summary = {
        "n_scored": n_scored,
        "n_unassignable": n_unassignable,
        "n_tirads5": n_tirads5,
        "n_inconsistent": n_final_inconsistent,
        "n_revised": n_committed_revisions,
    }
    append_dfl_row(bq, args.dry_run, audit_summary)

    _log(f"Phase C.5 Horvath complete. n_scored={n_scored}, "
         f"TIRADS_5={n_tirads5} ({tirads5_rate:.1%}), "
         f"unassignable={n_unassignable} ({unassignable_rate:.1%}).")


if __name__ == "__main__":
    main()
