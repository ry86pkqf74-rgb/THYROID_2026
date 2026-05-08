"""
Phase C.3 — BTA U1–U5 2014 pattern scorer
==========================================
Assigns BTA category (U2–U5) to each nodule in
pub_canonical.canonical_us_nodule_tirads_multisystem_v1.

BTA 2014 (Perros et al., Clinical Endocrinology 2014):
  U1 = Normal thyroid (no nodule) — skip, table is per-nodule
  U2 = Benign: halo present, OR cystic change, OR spongiform,
                OR peripheral eggshell calcification, OR peripheral vascularity
                (and not U4/U5 features)
  U3 = Indeterminate: hyperechoic + halo (follicular lesion), OR hypoechoic +
                equivocal foci + cystic change, OR central/mixed vascularity
  U4 = Suspicious: solid hypoechoic, OR very hypoechoic, OR disrupted peripheral
                calcification + hypoechoic, OR lobulated outline
  U5 = Malignant: hypoechoic lobulated/irregular + microcalcification (PTC),
                OR + globular calcification (MTC),
                OR intranodular vascularity, OR taller-than-wide,
                OR characteristic LN adenopathy

Priority: U5 first → U4 → U3 → U2 → LLM fallback if primitives sufficient.

NOTE: BTA relies heavily on halo and vascularity JSON fields.
  halo_presence_simple = 'present'/'absent'/'unstated'
  vascularity_distribution_simple = 'peripheral'/'intranodular'/'mixed'/'central'/'absent'/'unstated'
  If both are 'unstated' AND the nodule has composition + echogenicity, we can still
  fire U4/U5 rules. If composition is also missing, the nodule stays NULL.

Hard rules obeyed:
  - No PHI in any output column.
  - CTAS-rebuild preserves CLUSTER BY research_id.
  - Anti-pattern: rows with insufficient primitives stay NULL (not routed to LLM).
  - BTA's high LLM fallback rate is the canary for halo/vasc coverage — log it.
  - --dry-run and --skip-llm flags.

Usage:
    python scripts/422_canonical_us_nodule_tirads_bta_v1.py [--dry-run] [--skip-llm]

Author: Cursor Agent (Phase C.3), 2026-05-08
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Optional

from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_LN_CTX = f"{PROJECT}.{DATASET_WS}.us_nodule_ln_context_v1"
TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_phaseC3_bta_snapshot_v1"
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_bta_scored_v1"
TABLE_FALLBACK_IN = f"{PROJECT}.{DATASET_WS}.tirads_bta_fallback_input_v1"
TABLE_FALLBACK_OUT = f"{PROJECT}.{DATASET_WS}.tirads_bta_fallback_output_v1"

PRO_MODEL = f"`{PROJECT}.{DATASET_WS}.gemini_25_pro`"
PIPELINE_VERSION = "phase_c3_bta_v1"
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


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
# Primitive helpers
# ---------------------------------------------------------------------------

MICRO_CALC_TOKENS = {"punctate_echogenic_foci", "punctate", "microcalcifications"}
MACRO_CALC_TOKENS = {"macrocalcifications", "macro", "coarse_calcifications", "dystrophic"}
SOLID = {"solid", "predominantly_solid", "almost_completely_solid"}
CYSTIC_ANY = {"cystic", "almost_completely_cystic", "purely_cystic",
              "predominantly_cystic", "mixed_cystic_solid", "mixed", "partially_cystic"}


def _foci_contains(echogenic_foci: Optional[str], token_set: set) -> bool:
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        tokens = {str(i).lower() for i in (items if isinstance(items, list) else [items])}
    except (json.JSONDecodeError, TypeError):
        tokens = {str(echogenic_foci).lower()}
    return bool(tokens & token_set)


def _has_rim_calc(echogenic_foci: Optional[str]) -> bool:
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        tokens = {str(i).lower() for i in (items if isinstance(items, list) else [items])}
    except (json.JSONDecodeError, TypeError):
        tokens = {str(echogenic_foci).lower()}
    return any("rim" in t or "peripheral" in t or "eggshell" in t for t in tokens)


def _get_halo(row: dict) -> Optional[str]:
    """Get halo presence: 'present', 'absent', or None (unstated)."""
    simple = (row.get("halo_presence_simple") or "").lower()
    if simple == "present":
        return "present"
    if simple == "absent":
        return "absent"
    # Try JSON
    try:
        halo = json.loads(row.get("halo_jsonb") or "{}")
        p = str(halo.get("presence", "")).lower()
        if p == "present":
            return "present"
        if p == "absent":
            return "absent"
    except (json.JSONDecodeError, TypeError):
        pass
    return None  # genuinely unstated


def _get_vascularity(row: dict) -> Optional[str]:
    """Get vascularity distribution: peripheral/intranodular/mixed/central/absent or None."""
    simple = (row.get("vascularity_distribution_simple") or "").lower()
    if simple in ("peripheral", "intranodular", "mixed", "central", "absent", "none"):
        return simple
    try:
        v = json.loads(row.get("vascularity_jsonb") or "{}")
        dist = str(v.get("distribution", "")).lower()
        if dist in ("peripheral", "intranodular", "mixed", "central", "absent", "none"):
            return dist
    except (json.JSONDecodeError, TypeError):
        pass
    return None  # genuinely unstated


def score_bta(row: dict) -> dict:
    """Apply BTA U1-U5 decision tree."""
    composition = (row.get("composition") or "").lower()
    echogenicity = (row.get("echogenicity") or "").lower()
    shape = (row.get("shape") or "").lower()
    margins = (row.get("margins") or "").lower()
    echogenic_foci = row.get("echogenic_foci")
    size_cm = row.get("size_cm_max")
    has_suspicious_ln = row.get("has_suspicious_ln_within_60d") or False

    halo = _get_halo(row)
    vascularity = _get_vascularity(row)

    has_composition = bool(composition)
    has_echo = bool(echogenicity)
    # BTA primitives sufficient = composition OR echogenicity (since halo/vasc help but aren't required for U4/U5)
    primitives_sufficient = has_composition or has_echo

    category = None
    features_used = []
    decision_method = "deterministic"

    # -----------------------------------------------------------------------
    # U5 — Malignant pattern (apply FIRST)
    # -----------------------------------------------------------------------
    u5_triggered = False
    if has_echo and echogenicity in ("hypoechoic", "very_hypoechoic", "markedly_hypoechoic",
                                      "slightly_hypoechoic"):
        is_hypo = True
    else:
        is_hypo = False

    is_lobulated = margins in ("lobulated", "irregular", "microlobulated", "spiculated")
    has_microcalc = _foci_contains(echogenic_foci, MICRO_CALC_TOKENS)
    has_macrocalc = _foci_contains(echogenic_foci, MACRO_CALC_TOKENS)
    is_taller = shape == "taller_than_wide"

    # U5 condition 1: hypoechoic + lobulated + microcalcifications (PTC pattern)
    if is_hypo and is_lobulated and has_microcalc:
        category = "U5"
        features_used = ["hypoechoic", "lobulated_margins", "microcalcifications"]
        u5_triggered = True

    # U5 condition 2: hypoechoic + lobulated + globular calcifications (MTC pattern)
    if not u5_triggered and is_hypo and is_lobulated and has_macrocalc:
        category = "U5"
        features_used = ["hypoechoic", "lobulated_margins", "macrocalcifications_globular"]
        u5_triggered = True

    # U5 condition 3: intranodular vascularity
    if not u5_triggered and vascularity == "intranodular":
        category = "U5"
        features_used = ["intranodular_vascularity"]
        u5_triggered = True

    # U5 condition 4: taller-than-wide
    if not u5_triggered and is_taller:
        category = "U5"
        features_used = ["taller_than_wide"]
        u5_triggered = True

    # U5 condition 5: suspicious LN adenopathy
    if not u5_triggered and has_suspicious_ln:
        category = "U5"
        features_used = ["characteristic_lymphadenopathy"]
        u5_triggered = True

    # -----------------------------------------------------------------------
    # U4 — Suspicious (apply second)
    # -----------------------------------------------------------------------
    if category is None:
        u4_triggered = False

        # U4 condition 1: solid hypoechoic
        if has_composition and composition in SOLID and is_hypo:
            category = "U4"
            features_used = ["solid", "hypoechoic"]
            u4_triggered = True

        # U4 condition 2: solid very hypoechoic (even without explicit composition)
        if not u4_triggered and echogenicity in ("very_hypoechoic", "markedly_hypoechoic"):
            category = "U4"
            features_used = ["very_hypoechoic"]
            u4_triggered = True

        # U4 condition 3: disrupted peripheral calcification + hypoechoic component
        if not u4_triggered and _has_rim_calc(echogenic_foci) and is_hypo:
            category = "U4"
            features_used = ["disrupted_rim_calcification", "hypoechoic"]
            u4_triggered = True

        # U4 condition 4: lobulated outline alone (without hypoechoic)
        if not u4_triggered and is_lobulated and has_composition:
            category = "U4"
            features_used = ["lobulated_outline"]

    # -----------------------------------------------------------------------
    # U3 — Indeterminate (apply BEFORE U2 to catch hyperechoic+solid+halo)
    # BTA: hyperechoic solid with halo = follicular lesion pattern (U3),
    # which takes priority over the generic halo→U2 assignment.
    # -----------------------------------------------------------------------
    if category is None:
        u3_triggered = False

        # U3 condition 1: hyperechoic solid + halo → follicular lesion
        # BTA spec: "homogeneous markedly hyperechoic solid with halo"
        # isoechoic + halo remains U2 (benign halo pattern, not follicular lesion)
        if (has_echo and echogenicity in ("hyperechoic", "hyper")
                and halo == "present"
                and has_composition and composition in SOLID):
            category = "U3"
            features_used = ["hyperechoic_solid_with_halo"]
            u3_triggered = True

        # U3 condition 2: central hilum or mixed vascularity
        if not u3_triggered and vascularity in ("central", "central_hilum", "mixed"):
            category = "U3"
            features_used = [f"vascularity_{vascularity}"]
            u3_triggered = True

        # U3 condition 3: hypoechoic + equivocal foci + cystic change
        if not u3_triggered and is_hypo and composition in CYSTIC_ANY:
            category = "U3"
            features_used = ["hypoechoic_with_cystic_change"]

    # -----------------------------------------------------------------------
    # U2 — Benign (apply after U3; halo→U2 only when not already U3-classified)
    # -----------------------------------------------------------------------
    if category is None:
        u2_triggered = False

        if halo == "present":
            category = "U2"
            features_used = ["halo_present"]
            u2_triggered = True

        if not u2_triggered and composition in CYSTIC_ANY:
            category = "U2"
            features_used = ["cystic_component"]
            u2_triggered = True

        if not u2_triggered and composition == "spongiform":
            category = "U2"
            features_used = ["spongiform"]
            u2_triggered = True

        if not u2_triggered and _has_rim_calc(echogenic_foci) and not is_hypo:
            category = "U2"
            features_used = ["peripheral_eggshell_calcification"]
            u2_triggered = True

        if not u2_triggered and vascularity == "peripheral":
            category = "U2"
            features_used = ["peripheral_vascularity"]

    # Track halo and vascularity known status for DFL
    bta_halo_present = halo == "present" if halo is not None else None
    bta_vascularity_class = vascularity  # may be None if unstated

    # LLM fallback: route only if primitives sufficient but no rule fired
    needs_llm = (category is None) and primitives_sufficient

    return {
        "category": category,
        "features_used_json": json.dumps(features_used) if features_used else None,
        "halo_present": bta_halo_present,
        "vascularity_class": bta_vascularity_class,
        "decision_method": decision_method if category is not None else None,
        "needs_llm_fallback": needs_llm,
        "primitives_sufficient": primitives_sufficient,
    }


# ---------------------------------------------------------------------------
# LLM fallback
# ---------------------------------------------------------------------------

BTA_FALLBACK_OUTPUT_SCHEMA = """\
category STRING,
features_used_json STRING,
evidence_short STRING,
confidence FLOAT64"""


def build_fallback_input_sql() -> str:
    return f"""
CREATE OR REPLACE TABLE `{TABLE_FALLBACK_IN}`
CLUSTER BY research_id AS
SELECT
  s.nodule_id,
  s.research_id,
  s.us_exam_id,
  s.exam_date,
  CONCAT(
    '[BTA 2014 assignment] Features: ',
    'composition=', COALESCE(v.composition, 'unknown'), '; ',
    'echogenicity=', COALESCE(v.echogenicity, 'unknown'), '; ',
    'shape=', COALESCE(v.shape, 'unknown'), '; ',
    'margins=', COALESCE(v.margins, 'unknown'), '; ',
    'echogenic_foci=', COALESCE(v.echogenic_foci, 'unknown'), '; ',
    'halo=', COALESCE(v.halo_presence_simple, 'unstated'), '; ',
    'vascularity=', COALESCE(v.vascularity_distribution_simple, 'unstated'), '; ',
    'size_cm=', COALESCE(CAST(v.size_cm_max AS STRING), 'unknown'), '. ',
    'Assign BTA 2014 category: U2 | U3 | U4 | U5. ',
    'U5=malignant: hypo+lobulated+microcalc, intranodular vasc, taller-than-wide, or LN; ',
    'U4=suspicious: solid hypo, very hypo, disrupted rim+hypo, or lobulated alone; ',
    'U2=benign: halo, cystic change, spongiform, eggshell calc, peripheral vasc; ',
    'U3=indeterminate: hyperechoic+halo, central/mixed vasc, hypo+cystic. ',
    'evidence_short<=140 chars no PHI.'
  ) AS prompt
FROM `{TABLE_STAGING}` s
JOIN `{TABLE_NODULE_V2}` v USING (nodule_id)
WHERE s.needs_llm_fallback = TRUE;
"""


LLM_FALLBACK_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_FALLBACK_OUT}`
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (SELECT prompt, nodule_id, research_id FROM `{TABLE_FALLBACK_IN}`),
  STRUCT(
    '{BTA_FALLBACK_OUTPUT_SCHEMA}' AS output_schema,
    0.0 AS temperature,
    1024 AS max_output_tokens
  )
);
"""


def build_ctas_sql(skip_llm: bool) -> str:
    if skip_llm:
        llm_join = ""
        llm_cat = "CAST(NULL AS STRING)"
        llm_feat = "CAST(NULL AS STRING)"
        llm_method = "CAST(NULL AS STRING)"
    else:
        llm_join = f"\n  LEFT JOIN `{TABLE_FALLBACK_OUT}` llm ON m.nodule_id = llm.nodule_id"
        llm_cat = "llm.category"
        llm_feat = "llm.features_used_json"
        llm_method = "CASE WHEN llm.category IS NOT NULL THEN 'llm_gemini_25_pro' ELSE NULL END"

    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    bta_category, bta_features_used_json,
    bta_halo_present, bta_vascularity_class, bta_decision_method
  ),
  COALESCE(s.category, {llm_cat}) AS bta_category,
  COALESCE(s.features_used_json, {llm_feat}) AS bta_features_used_json,
  s.halo_present AS bta_halo_present,
  s.vascularity_class AS bta_vascularity_class,
  COALESCE(s.decision_method, {llm_method}) AS bta_decision_method
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s ON m.nodule_id = s.nodule_id{llm_join};
"""


AUDIT_SQL = f"""
SELECT
  bta_category,
  bta_decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}`
GROUP BY 1, 2
ORDER BY 1, 2;
"""

SANITY_SQL = f"""
SELECT
  COUNTIF(bta_category IS NOT NULL) AS n_scored,
  COUNTIF(bta_category = 'U2') AS n_u2,
  COUNTIF(bta_category = 'U3') AS n_u3,
  COUNTIF(bta_category = 'U4') AS n_u4,
  COUNTIF(bta_category = 'U5') AS n_u5,
  COUNTIF(bta_decision_method = 'llm_gemini_25_pro') AS n_llm,
  COUNTIF(bta_halo_present IS NOT NULL) AS n_halo_stated,
  COUNTIF(bta_vascularity_class IS NOT NULL) AS n_vasc_stated,
  COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`;
"""


def run_audit(bq: bigquery.Client) -> dict:
    _log("Audit: BTA distribution")
    for r in bq.query(AUDIT_SQL, location=LOCATION).result():
        _log(f"  {dict(r)}")
    sanity = dict(next(iter(bq.query(SANITY_SQL, location=LOCATION).result())))
    _log(f"Audit sanity: {sanity}")

    n_scored = sanity["n_scored"]
    if n_scored == 0:
        _halt("Audit: 0 scored rows")

    for cat, key in [("U2", "n_u2"), ("U5", "n_u5")]:
        n_cat = sanity[key]
        if n_scored and n_cat / n_scored > 0.70:
            _halt(f"Audit: {cat} dominates at {n_cat/n_scored:.1%} > 70%")

    n_llm = sanity["n_llm"]
    llm_rate = n_llm / max(1, n_scored)
    # BTA can have higher fallback due to halo/vasc being 'unstated'
    _log(f"  LLM fallback rate: {llm_rate:.1%} "
         f"(target <30%; halo_stated={sanity['n_halo_stated']}, vasc_stated={sanity['n_vasc_stated']})")
    if llm_rate > 0.30:
        _log(f"  NOTABLE FINDING: BTA fallback rate {llm_rate:.1%} > 30%. "
             f"This is the canary for halo/vasc primitive coverage gap. "
             f"Halo stated: {sanity['n_halo_stated']}, Vasc stated: {sanity['n_vasc_stated']}. "
             f"BTA and EU-TIRADS may disagree on intermediate-risk nodules where halo info "
             f"drives BTA assignment but is unstated for EU-TIRADS.")
    return sanity


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
            "target_column": "bta_*",
            "action_summary": (
                f"Phase C.3 BTA 2014 scorer. n_scored={audit.get('n_scored')}, "
                f"n_u5={audit.get('n_u5')}, n_llm={audit.get('n_llm')}, "
                f"halo_stated={audit.get('n_halo_stated')}, vasc_stated={audit.get('n_vasc_stated')}. "
                f"Pipeline={PIPELINE_VERSION}."
            )[:280],
            "lifecycle": "Applied",
            "source_chat": "Phase C.3 BTA cursor prompt 2026-05-08",
            "phi_guard_confirmed": True,
        }
        errors = bq.insert_rows_json(f"{PROJECT}.pub_signoff.data_feedback_log_v1", [row])
        if errors:
            _log(f"DFL WARNING: {errors}")
        else:
            _log("DFL: row inserted (lifecycle=Applied)")
    except Exception as e:
        _log(f"DFL: failed ({e}). Continuing.")


def main() -> None:
    parser = argparse.ArgumentParser(description="BTA U1-U5 2014 scorer (Phase C.3)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-llm", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    # Step 1: Snapshot
    _log("Step 1: Snapshot")
    _run_sql(bq, f"CREATE OR REPLACE TABLE `{TABLE_SNAPSHOT}` AS SELECT * FROM `{TABLE_MULTISYS}`",
             "Snapshot")
    n_snap = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_SNAPSHOT}`")

    # Step 2: Pull data
    _log("Step 2: Pull nodule data")
    rows = list(bq.query(f"""
    SELECT
      n.nodule_id, n.research_id, n.us_exam_id, n.exam_date,
      n.composition, n.echogenicity, n.shape, n.margins,
      n.echogenic_foci, n.halo_presence_simple, n.vascularity_distribution_simple,
      n.ete_on_us_presence_simple, n.size_cm_max,
      n.halo_jsonb, n.vascularity_jsonb,
      COALESCE(CAST(ln.has_suspicious_ln_within_60d AS BOOL), FALSE) AS has_suspicious_ln_within_60d
    FROM `{TABLE_NODULE_V2}` n
    LEFT JOIN `{TABLE_LN_CTX}` ln USING (nodule_id)
    """, location=LOCATION).result())
    _log(f"  Pulled {len(rows)} rows")

    # Step 3: Score
    _log("Step 3: Apply BTA decision tree")
    scored = []
    n_fallback = 0
    n_null_prim = 0
    for row in rows:
        rd = dict(row)
        result = score_bta(rd)
        scored.append({
            "nodule_id": rd["nodule_id"],
            "research_id": rd["research_id"],
            "category": result["category"],
            "features_used_json": result["features_used_json"],
            "halo_present": result["halo_present"],
            "vascularity_class": result["vascularity_class"],
            "decision_method": result["decision_method"],
            "needs_llm_fallback": result["needs_llm_fallback"],
        })
        if result["needs_llm_fallback"]:
            n_fallback += 1
        if not result["primitives_sufficient"]:
            n_null_prim += 1

    n_det = sum(1 for s in scored if s["category"] is not None)
    _log(f"  Deterministic: {n_det}, LLM fallback: {n_fallback}, insufficient primitives: {n_null_prim}")

    # Step 4: Write staging
    _log("Step 4: Write BTA staging table")
    import pandas as pd
    df_staged = pd.DataFrame(scored)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    load_job = bq.load_table_from_dataframe(df_staged, TABLE_STAGING,
                                             job_config=job_config, location=LOCATION)
    load_job.result()
    n_staged = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_STAGING}`")
    _log(f"  Staging: {n_staged} rows")

    if args.dry_run:
        _log("DRY RUN: stopping before LLM + CTAS.")
        return

    # Step 5: LLM fallback
    skip_llm = args.skip_llm or n_fallback == 0
    if not skip_llm:
        _log(f"Step 5: LLM fallback ({n_fallback} rows)")
        _run_sql(bq, build_fallback_input_sql(), "Build BTA fallback input")
        _run_sql(bq, LLM_FALLBACK_SQL, "BTA LLM fallback")
        n_out = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_FALLBACK_OUT}`")
        _log(f"  LLM output: {n_out} rows")
    else:
        _log("Step 5: Skipping LLM fallback")

    # Step 6: CTAS rebuild
    _log("Step 6: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(skip_llm), "BTA CTAS rebuild")
    n_rebuilt = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_MULTISYS}`")
    if n_rebuilt != n_snap:
        _halt(f"Row count mismatch: snap={n_snap}, rebuilt={n_rebuilt}")

    # Step 7: Audit + DFL
    audit = run_audit(bq)
    append_dfl_row(bq, args.dry_run, audit)
    _log(f"Phase C.3 BTA complete. n_scored={audit.get('n_scored')}")


if __name__ == "__main__":
    main()
