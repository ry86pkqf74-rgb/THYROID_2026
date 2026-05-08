"""
Phase C.4 — AACE/ACE/AME 2016 3-class scorer (no elastography)
================================================================
Assigns AACE class (1, 2, 3) to each nodule in
pub_canonical.canonical_us_nodule_tirads_multisystem_v1.

AACE 2016 (Gharib et al., Endocr Pract 2016) — Logan v0.2 (no elasto):
  Class 1 (Low-risk, ~1%): mostly cystic >50% OR spongiform+iso/hyper OR
            confluent/regular halo
  Class 2 (Intermediate, 5–15%): slightly hypo or isoechoic, ovoid-round,
            smooth or ill-defined margins. Intranodular vascularization,
            macrocalcifications, hyperechoic spots MAY be present.
            (Elastography dropped — not available.)
  Class 3 (High-risk, 50–90%): ≥1 of: marked hypoechogenicity, spiculated/
            microlobulated margins, microcalcifications, taller-than-wide,
            ETE on US, pathologic adenopathy

Priority: Class 3 features first → Class 1 features → Class 2 otherwise.

FNA thresholds:
  Class 1: FNA only if >20mm + growing OR risk history OR pre-surgical
  Class 2: FNA if >20mm
  Class 3: FNA if ≥10mm

Elastography note: The AACE 2016 system originally includes elastography
stiffness as a modifier between Class 2 and Class 3. This project drops
that modifier (per Logan v0.2 decision) because elastography data is not
available in the canonical US dataset.

Hard rules obeyed:
  - No PHI in any output column.
  - CTAS-rebuild preserves CLUSTER BY research_id.
  - --dry-run and --skip-llm flags.
  - DFL row appended lifecycle=Applied.

Usage:
    python scripts/423_canonical_us_nodule_tirads_aace_v1.py [--dry-run] [--skip-llm]

Author: Cursor Agent (Phase C.4), 2026-05-08
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
TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_phaseC4_aace_snapshot_v1"
TABLE_STAGING = f"{PROJECT}.{DATASET_WS}.tirads_aace_scored_v1"
TABLE_FALLBACK_IN = f"{PROJECT}.{DATASET_WS}.tirads_aace_fallback_input_v1"
TABLE_FALLBACK_OUT = f"{PROJECT}.{DATASET_WS}.tirads_aace_fallback_output_v1"

PRO_MODEL = f"`{PROJECT}.{DATASET_WS}.gemini_25_pro`"
PIPELINE_VERSION = "phase_c4_aace_v1"
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
# Helpers
# ---------------------------------------------------------------------------

MICRO_CALC_TOKENS = {"punctate_echogenic_foci", "punctate", "microcalcifications"}
CYSTIC_DOMINANT = {"cystic", "almost_completely_cystic", "purely_cystic",
                   "predominantly_cystic"}
SPONGIFORM = {"spongiform"}


def _foci_contains(echogenic_foci: Optional[str], token_set: set) -> bool:
    if not echogenic_foci:
        return False
    try:
        items = json.loads(echogenic_foci)
        tokens = {str(i).lower() for i in (items if isinstance(items, list) else [items])}
    except (json.JSONDecodeError, TypeError):
        tokens = {str(echogenic_foci).lower()}
    return bool(tokens & token_set)


def _get_halo_regularity(row: dict) -> Optional[str]:
    """Return halo regularity: 'regular', 'irregular', or None."""
    try:
        h = json.loads(row.get("halo_jsonb") or "{}")
        p = str(h.get("presence", "")).lower()
        if p != "present":
            return None
        reg = str(h.get("regularity", "")).lower()
        if reg in ("regular", "smooth", "complete"):
            return "regular"
        if reg in ("irregular", "incomplete", "disrupted"):
            return "irregular"
    except (json.JSONDecodeError, TypeError):
        pass
    # If halo_presence_simple = 'present' but no regularity detail, assume regular
    simple = (row.get("halo_presence_simple") or "").lower()
    if simple == "present":
        return "regular"
    return None


def _aace_class3_features(row: dict) -> list[str]:
    """Return list of Class 3 high-risk features."""
    f3 = []
    echogenicity = (row.get("echogenicity") or "").lower()
    margins = (row.get("margins") or "").lower()
    shape = (row.get("shape") or "").lower()
    echogenic_foci = row.get("echogenic_foci")
    ete_presence = (row.get("ete_on_us_presence_simple") or "").lower()
    has_suspicious_ln = row.get("has_suspicious_ln_within_60d") or False

    if echogenicity in ("very_hypoechoic", "markedly_hypoechoic"):
        f3.append(f"marked_hypoechogenicity:{echogenicity}")
    if margins in ("microlobulated", "irregular", "spiculated", "infiltrative"):
        f3.append(f"spiculated_margins:{margins}")
    if _foci_contains(echogenic_foci, MICRO_CALC_TOKENS):
        f3.append("microcalcifications")
    if shape == "taller_than_wide":
        f3.append("taller_than_wide")
    if ete_presence not in ("", "none", "unstated", "absent"):
        f3.append(f"ete_on_us:{ete_presence}")
    if has_suspicious_ln:
        f3.append("pathologic_adenopathy")
    return f3


def score_aace(row: dict) -> dict:
    """Apply AACE 2016 (no elasto) 3-class decision tree."""
    composition = (row.get("composition") or "").lower()
    echogenicity = (row.get("echogenicity") or "").lower()
    margins = (row.get("margins") or "").lower()
    size_cm = row.get("size_cm_max")

    has_composition = bool(composition)
    has_echo = bool(echogenicity)
    primitives_sufficient = has_composition and has_echo

    aace_class = None
    features_used = []
    decision_method = "deterministic"

    # -----------------------------------------------------------------------
    # Class 3 — High-risk (check FIRST; any one feature fires Class 3)
    # -----------------------------------------------------------------------
    f3 = _aace_class3_features(row)
    if f3 and (has_echo or has_composition):
        aace_class = 3
        features_used = f3

    # -----------------------------------------------------------------------
    # Class 1 — Low-risk
    # -----------------------------------------------------------------------
    if aace_class is None:
        # Mostly cystic (>50% cystic component) with no Class 3 features
        if composition in CYSTIC_DOMINANT:
            if not f3:
                aace_class = 1
                features_used = [f"cystic_dominant:{composition}"]

        # Spongiform + isoechoic or hyperechoic
        if aace_class is None and composition in SPONGIFORM:
            if echogenicity in ("isoechoic", "hyperechoic", "iso", "hyper", ""):
                aace_class = 1
                features_used = ["spongiform"]

        # Confluent with regular halo
        if aace_class is None:
            halo_reg = _get_halo_regularity(row)
            if halo_reg == "regular" and not f3:
                aace_class = 1
                features_used = ["regular_halo"]

    # -----------------------------------------------------------------------
    # Class 2 — Intermediate (default for hypo/iso + smooth/ill-defined, no Class 3)
    # -----------------------------------------------------------------------
    if aace_class is None and primitives_sufficient:
        if (echogenicity in ("hypoechoic", "isoechoic", "slightly_hypoechoic",
                              "mildly_hypoechoic", "iso", "hyper")
                and margins in ("smooth", "ill_defined", "ill-defined",
                                "indistinct", "well_defined", "")
                and not f3):
            aace_class = 2
            features_used = [f"echogenicity:{echogenicity}", f"margins:{margins}"]

    # FNA recommendations
    fna_recommended = None
    if aace_class and size_cm is not None:
        if aace_class == 1:
            fna_recommended = size_cm > 2.0  # >20mm growing/risk history context
        elif aace_class == 2:
            fna_recommended = size_cm > 2.0
        elif aace_class == 3:
            fna_recommended = size_cm >= 1.0

    needs_llm = (aace_class is None) and primitives_sufficient

    return {
        "aace_class": aace_class,
        "features_used_json": json.dumps(features_used) if features_used else None,
        "decision_method": decision_method if aace_class is not None else None,
        "fna_recommended": fna_recommended,
        "needs_llm_fallback": needs_llm,
        "primitives_sufficient": primitives_sufficient,
    }


# ---------------------------------------------------------------------------
# LLM fallback
# ---------------------------------------------------------------------------

AACE_FALLBACK_OUTPUT_SCHEMA = """\
aace_class INT64,
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
    '[AACE 2016 assignment] Features: ',
    'composition=', COALESCE(v.composition, 'unknown'), '; ',
    'echogenicity=', COALESCE(v.echogenicity, 'unknown'), '; ',
    'shape=', COALESCE(v.shape, 'unknown'), '; ',
    'margins=', COALESCE(v.margins, 'unknown'), '; ',
    'echogenic_foci=', COALESCE(v.echogenic_foci, 'unknown'), '; ',
    'halo=', COALESCE(v.halo_presence_simple, 'unstated'), '; ',
    'ete_us=', COALESCE(v.ete_on_us_presence_simple, 'unstated'), '; ',
    'size_cm=', COALESCE(CAST(v.size_cm_max AS STRING), 'unknown'), '. ',
    'Assign AACE 2016 class (integer 1, 2, or 3). No elastography. ',
    'Class 3 if any: marked hypoechoic, microlobulated/spiculated margins, microcalcifications, TTW, ETE, pathologic LN. ',
    'Class 1 if: mostly cystic no class3, or spongiform+iso/hyper, or regular halo. ',
    'Class 2 otherwise: hypo/iso + smooth/ill-defined + no class3. ',
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
    '{AACE_FALLBACK_OUTPUT_SCHEMA}' AS output_schema,
    0.0 AS temperature,
    512 AS max_output_tokens
  )
);
"""


def build_ctas_sql(skip_llm: bool) -> str:
    if skip_llm:
        llm_join = ""
        llm_class = "CAST(NULL AS INT64)"
        llm_feat = "CAST(NULL AS STRING)"
        llm_method = "CAST(NULL AS STRING)"
        llm_fna = "CAST(NULL AS BOOL)"
    else:
        llm_join = f"\n  LEFT JOIN `{TABLE_FALLBACK_OUT}` llm ON m.nodule_id = llm.nodule_id"
        llm_class = "llm.aace_class"
        llm_feat = "llm.features_used_json"
        llm_method = "CASE WHEN llm.aace_class IS NOT NULL THEN 'llm_gemini_25_pro' ELSE NULL END"
        llm_fna = (
            "CASE "
            "  WHEN llm.aace_class = 3 AND m.size_cm_max >= 1.0 THEN TRUE "
            "  WHEN llm.aace_class = 2 AND m.size_cm_max > 2.0 THEN TRUE "
            "  WHEN llm.aace_class = 1 AND m.size_cm_max > 2.0 THEN TRUE "
            "  WHEN llm.aace_class IS NOT NULL THEN FALSE "
            "  ELSE NULL END"
        )

    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (
    aace_class, aace_features_used_json,
    aace_decision_method, aace_fna_recommended
  ),
  COALESCE(s.aace_class, {llm_class}) AS aace_class,
  COALESCE(s.features_used_json, {llm_feat}) AS aace_features_used_json,
  COALESCE(s.decision_method, {llm_method}) AS aace_decision_method,
  COALESCE(s.fna_recommended, {llm_fna}) AS aace_fna_recommended
FROM `{TABLE_MULTISYS}` m
LEFT JOIN `{TABLE_STAGING}` s ON m.nodule_id = s.nodule_id{llm_join};
"""


AUDIT_SQL = f"""
SELECT
  aace_class,
  aace_decision_method,
  COUNT(*) AS n,
  ROUND(COUNT(*) / SUM(COUNT(*)) OVER (), 4) AS pct
FROM `{TABLE_MULTISYS}`
GROUP BY 1, 2
ORDER BY 1, 2;
"""

SANITY_SQL = f"""
SELECT
  COUNTIF(aace_class IS NOT NULL) AS n_scored,
  COUNTIF(aace_class = 1) AS n_class1,
  COUNTIF(aace_class = 2) AS n_class2,
  COUNTIF(aace_class = 3) AS n_class3,
  COUNTIF(aace_decision_method = 'llm_gemini_25_pro') AS n_llm,
  COUNT(*) AS n_total
FROM `{TABLE_MULTISYS}`;
"""


def run_audit(bq: bigquery.Client) -> dict:
    _log("Audit: AACE distribution")
    for r in bq.query(AUDIT_SQL, location=LOCATION).result():
        _log(f"  {dict(r)}")
    sanity = dict(next(iter(bq.query(SANITY_SQL, location=LOCATION).result())))
    _log(f"Audit sanity: {sanity}")

    n_scored = sanity["n_scored"]
    if n_scored == 0:
        _halt("Audit: 0 scored rows")

    for cls in (1, 2, 3):
        n_c = sanity[f"n_class{cls}"]
        if n_scored and n_c / n_scored > 0.70:
            _halt(f"Audit: Class {cls} dominates at {n_c/n_scored:.1%} > 70%")

    n_llm = sanity["n_llm"]
    rate = n_llm / max(1, n_scored)
    _log(f"  LLM fallback rate: {rate:.1%} {'✓' if rate <= 0.20 else 'WARNING > 20%'}")
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
            "target_column": "aace_*",
            "action_summary": (
                f"Phase C.4 AACE 2016 (no elasto) scorer. "
                f"n_scored={audit.get('n_scored')}, n_class3={audit.get('n_class3')}, "
                f"n_llm={audit.get('n_llm')}. Elastography: dropped per Logan v0.2. "
                f"Pipeline={PIPELINE_VERSION}."
            )[:280],
            "lifecycle": "Applied",
            "source_chat": "Phase C.4 AACE cursor prompt 2026-05-08",
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
    parser = argparse.ArgumentParser(description="AACE 2016 scorer (Phase C.4)")
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
    _log(f"  Snapshot rows: {n_snap}")

    # Step 2: Pull data
    _log("Step 2: Pull nodule data")
    rows = list(bq.query(f"""
    SELECT
      n.nodule_id, n.research_id, n.us_exam_id, n.exam_date,
      n.composition, n.echogenicity, n.shape, n.margins,
      n.echogenic_foci, n.halo_presence_simple, n.vascularity_distribution_simple,
      n.ete_on_us_presence_simple, n.size_cm_max,
      n.halo_jsonb, n.vascularity_jsonb, n.ete_us_jsonb,
      COALESCE(CAST(ln.has_suspicious_ln_within_60d AS BOOL), FALSE) AS has_suspicious_ln_within_60d
    FROM `{TABLE_NODULE_V2}` n
    LEFT JOIN `{TABLE_LN_CTX}` ln USING (nodule_id)
    """, location=LOCATION).result())
    _log(f"  Pulled {len(rows)} rows")

    # Step 3: Score
    _log("Step 3: Apply AACE decision tree")
    scored = []
    n_fallback = 0
    n_null_prim = 0
    for row in rows:
        rd = dict(row)
        result = score_aace(rd)
        scored.append({
            "nodule_id": rd["nodule_id"],
            "research_id": rd["research_id"],
            "aace_class": result["aace_class"],
            "features_used_json": result["features_used_json"],
            "decision_method": result["decision_method"],
            "fna_recommended": result["fna_recommended"],
            "needs_llm_fallback": result["needs_llm_fallback"],
        })
        if result["needs_llm_fallback"]:
            n_fallback += 1
        if not result["primitives_sufficient"]:
            n_null_prim += 1

    n_det = sum(1 for s in scored if s["aace_class"] is not None)
    _log(f"  Deterministic: {n_det}, LLM fallback: {n_fallback}, null primitives: {n_null_prim}")

    # Step 4: Write staging
    _log("Step 4: Write AACE staging table")
    import pandas as pd
    df_staged = pd.DataFrame(scored)
    # aace_class must be Int64 (nullable integer) for BQ compatibility
    if "aace_class" in df_staged.columns:
        df_staged["aace_class"] = df_staged["aace_class"].astype("Int64")
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
        _run_sql(bq, build_fallback_input_sql(), "Build AACE fallback input")
        _run_sql(bq, LLM_FALLBACK_SQL, "AACE LLM fallback")
        n_out = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_FALLBACK_OUT}`")
        _log(f"  LLM output: {n_out} rows")
    else:
        _log("Step 5: Skipping LLM fallback")

    # Step 6: CTAS rebuild
    _log("Step 6: CTAS rebuild")
    _run_sql(bq, build_ctas_sql(skip_llm), "AACE CTAS rebuild")
    n_rebuilt = _scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_MULTISYS}`")
    if n_rebuilt != n_snap:
        _halt(f"Row count mismatch: snap={n_snap}, rebuilt={n_rebuilt}")

    # Step 7: Audit + DFL
    audit = run_audit(bq)
    append_dfl_row(bq, args.dry_run, audit)
    _log(f"Phase C.4 AACE complete. n_scored={audit.get('n_scored')}")


if __name__ == "__main__":
    main()
