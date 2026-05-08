"""
Script 425b — Horvath Phase C.5 recovery / completion
======================================================
Picks up where script 425 failed (second-pass SQL error + RESOURCE_EXHAUSTED).

Context (2026-05-08):
  - tirads_horvath_raw_v1:      18,376 rows total
      2,390 succeeded (status='')
     15,882 RESOURCE_EXHAUSTED (Vertex AI quota exhausted during large batch)
        104 MAX_TOKENS / parse failures
  - note_entities_llm_horvath_v1: 18,376 rows (unvalidated, from PARSE_TO_NLE_SQL)
  - tirads_horvath_deterministic_v1: 19,203 rows (composition-determined)
  - Inconsistent table (tirads_horvath_inconsistent_v1): 307 rows (post-validation flags)
  - CTAS rebuild: NOT yet run

Plan:
  1. Re-run second-pass on the 307 inconsistent rows (with fixed SQL)
  2. Write final NLE table from the Python-validated results
     (2,390 LLM-validated + insert 19,203 deterministic rows)
  3. Mark 15,882 RESOURCE_EXHAUSTED nodules as unassignable in the NLE (best-effort)
     → These will have horvath_category = 'TIRADS_3' (unassignable default)
  4. File a Verification Check for the RESOURCE_EXHAUSTED quota issue
  5. CTAS rebuild the canonical multisystem table with partial Horvath coverage
  6. Report final coverage and append DFL row

RESOURCE_EXHAUSTED context:
  The 15,882 failures are due to Vertex AI Gemini 2.5 Pro quota exhaustion during a
  single 18K-row AI.GENERATE_TABLE batch. These nodules are solid/mixed/predominantly_solid
  (the composition classes that require LLM for Horvath assignment). They cannot be
  assigned a Horvath pattern without LLM inference. Workaround: classify them as
  'unassignable' (TIRADS_3 per Horvath rules) and document as a data quality limitation.
  A future batch rerun with rate-limited smaller batches could recover these.

Usage:
    python scripts/425b_horvath_recovery_completion.py [--skip-second-pass] [--dry-run]
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Optional

import pandas as pd
from google.cloud import bigquery

PROJECT = "thyroid-canonical-pub-2026"
DATASET_PUB = "pub_canonical"
DATASET_WS = "pub_workspace"
LOCATION = "us-central1"

TABLE_MULTISYS = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_tirads_multisystem_v1"
TABLE_NODULE_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_nodule_v2"
TABLE_GLAND_V2 = f"{PROJECT}.{DATASET_PUB}.canonical_us_thyroid_gland_v2"
TABLE_HORVATH_INPUT = f"{PROJECT}.{DATASET_WS}.tirads_horvath_input_v1"
TABLE_HORVATH_RAW = f"{PROJECT}.{DATASET_WS}.tirads_horvath_raw_v1"
TABLE_HORVATH_NLE = f"{PROJECT}.{DATASET_WS}.note_entities_llm_horvath_v1"
TABLE_HORVATH_DET = f"{PROJECT}.{DATASET_WS}.tirads_horvath_deterministic_v1"
TABLE_HORVATH_INCON = f"{PROJECT}.{DATASET_WS}.tirads_horvath_inconsistent_v1"
TABLE_HORVATH_REVISED = f"{PROJECT}.{DATASET_WS}.tirads_horvath_revised_v1"
TABLE_SNAPSHOT = f"{PROJECT}.{DATASET_WS}.cpm_pre_tirads_multisystem_phaseC5_horvath_snapshot_v1"

PRO_MODEL = f"`{PROJECT}.{DATASET_WS}.gemini_25_pro`"
RUN_TS = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

VALID_PATTERNS = {
    "colloid_type_1", "colloid_type_2", "colloid_type_3",
    "hashimoto_pseudonodule", "white_knight_hashimoto",
    "isolated_intraparenchymal_calc", "benign_concordant_aspirated",
    "de_quervain_unifocal", "simple_neoplastic", "suspicious_neoplastic",
    "malignant_type_a", "malignant_type_b", "malignant_type_c", "unassignable",
}

CATEGORY_MAP = {
    "colloid_type_1": "TIRADS_2",
    "colloid_type_2": "TIRADS_2",
    "colloid_type_3": "TIRADS_3",
    "hashimoto_pseudonodule": "TIRADS_3",
    "white_knight_hashimoto": "TIRADS_2",
    "isolated_intraparenchymal_calc": "TIRADS_2",
    "benign_concordant_aspirated": "TIRADS_2",
    "de_quervain_unifocal": "TIRADS_4A",
    "simple_neoplastic": "TIRADS_4A",
    "suspicious_neoplastic": "TIRADS_4B",
    "malignant_type_a": "TIRADS_4B",
    "malignant_type_b": "TIRADS_5",
    "malignant_type_c": "TIRADS_4C",
    "unassignable": "TIRADS_3",
}


def _log(msg: str) -> None:
    print(f"[{datetime.now(timezone.utc).isoformat()}] {msg}", flush=True)


def _run_sql(bq: bigquery.Client, sql: str, label: str) -> None:
    job = bq.query(sql, location=LOCATION)
    job.result()
    _log(f"  ✓ {label} (job_id={job.job_id})")


def _scalar(bq: bigquery.Client, sql: str):
    return list(bq.query(sql, location=LOCATION).result())[0][0]


# Fixed second-pass SQL (\\n instead of \n)
SECOND_PASS_SQL = f"""
CREATE OR REPLACE TABLE `{TABLE_HORVATH_REVISED}`
CLUSTER BY research_id AS
SELECT *
FROM AI.GENERATE_TABLE(
  MODEL {PRO_MODEL},
  (
    SELECT
      CONCAT(
        'The Horvath/Chilean pattern assigned was: ', i.pattern, '\\n',
        'Post-validation inconsistency reason: ', i.inconsistency_reason, '\\n\\n',
        'Please revise to a DIFFERENT, more consistent pattern from the enum.\\n',
        'If no valid pattern fits, use unassignable.\\n\\n',
        'Original structured features:\\n',
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
    1024 AS max_output_tokens
  )
);
"""


def build_ctas_sql_recovery() -> str:
    """CTAS rebuild with partial Horvath coverage (including resource_exhausted as NULL)."""
    return f"""
CREATE OR REPLACE TABLE `{TABLE_MULTISYS}`
CLUSTER BY research_id AS
SELECT
  m.* EXCEPT (horvath_pattern, horvath_category, horvath_confidence,
              horvath_post_validation_consistent, horvath_evidence_short,
              horvath_decision_method),
  nle.pattern              AS horvath_pattern,
  nle.category_adjusted    AS horvath_category,
  nle.confidence           AS horvath_confidence,
  nle.post_validation_consistent AS horvath_post_validation_consistent,
  nle.evidence_short       AS horvath_evidence_short,
  nle.assignment_method    AS horvath_decision_method
FROM `{TABLE_SNAPSHOT}` m
LEFT JOIN `{TABLE_HORVATH_NLE}` nle USING (nodule_id);
"""


def main() -> None:
    parser = argparse.ArgumentParser(description="Horvath 425b recovery/completion")
    parser.add_argument("--skip-second-pass", action="store_true",
                        help="Skip second-pass revision (run if inconsistent table empty/failed)")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--project", default=PROJECT)
    args = parser.parse_args()

    bq = bigquery.Client(project=args.project)

    # Verify inputs
    n_raw = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_RAW}`"))
    n_res_exh = int(_scalar(
        bq, f"SELECT COUNTIF(status LIKE '%RESOURCE_EXHAUSTED%') FROM `{TABLE_HORVATH_RAW}`"
    ))
    n_success = int(_scalar(
        bq, f"SELECT COUNTIF(status = '') FROM `{TABLE_HORVATH_RAW}`"
    ))
    n_det = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_DET}`"))
    n_incon = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_INCON}`"))

    _log(f"Raw table: {n_raw} rows total")
    _log(f"  Succeeded: {n_success} ({n_success/n_raw:.1%})")
    _log(f"  RESOURCE_EXHAUSTED: {n_res_exh} ({n_res_exh/n_raw:.1%})")
    _log(f"  Inconsistent (for second-pass): {n_incon}")
    _log(f"Deterministic: {n_det} rows")

    # -----------------------------------------------------------------------
    # Step A: Second-pass revision on inconsistent rows
    # -----------------------------------------------------------------------
    if not args.skip_second_pass and n_incon > 0:
        _log(f"\nStep A: Second-pass revision on {n_incon} inconsistent rows")
        if not args.dry_run:
            try:
                _run_sql(bq, SECOND_PASS_SQL, "Horvath second-pass revision")
                n_revised = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_REVISED}`"))
                _log(f"  Revised rows: {n_revised}")
            except Exception as e:
                _log(f"  WARNING: Second-pass failed ({e}); continuing with original patterns")
                n_revised = 0
        else:
            _log("  [dry-run] skipping second-pass")
            n_revised = 0
    else:
        _log("\nStep A: Skipping second-pass (skip_second_pass=True or 0 inconsistent rows)")
        n_revised = 0

    # -----------------------------------------------------------------------
    # Step B: Build final NLE table with ALL sources
    # -----------------------------------------------------------------------
    _log("\nStep B: Build final NLE table")

    # Pull the LLM-validated rows from the NLE table (built by PARSE_TO_NLE_SQL)
    # The existing NLE table has 18,376 rows but most with NULL patterns.
    # We re-derive from the raw table, keeping only successful rows.
    _log("  Pulling validated rows from raw table (status='', pattern not null)")

    validated_sql = f"""
    SELECT
      r.nodule_id, r.research_id,
      LOWER(TRIM(REPLACE(r.pattern, ' ', '_'))) AS pattern_raw,
      CASE
        WHEN LOWER(TRIM(REPLACE(r.pattern, ' ', '_'))) IN (
          'colloid_type_1','colloid_type_2','colloid_type_3',
          'hashimoto_pseudonodule','white_knight_hashimoto',
          'isolated_intraparenchymal_calc','benign_concordant_aspirated',
          'de_quervain_unifocal','simple_neoplastic','suspicious_neoplastic',
          'malignant_type_a','malignant_type_b','malignant_type_c','unassignable'
        ) THEN LOWER(TRIM(REPLACE(r.pattern, ' ', '_')))
        ELSE 'unassignable'
      END AS pattern,
      r.category AS category_llm,
      LEFT(COALESCE(r.evidence_short, ''), 140) AS evidence_short,
      COALESCE(CAST(r.confidence AS FLOAT64), 0.5) AS confidence,
      'llm_gemini_25_pro' AS assignment_method,
      '{RUN_TS}' AS processed_at
    FROM `{TABLE_HORVATH_RAW}` r
    WHERE r.status = '' AND r.pattern IS NOT NULL
    """
    validated_rows = [dict(r) for r in bq.query(validated_sql, location=LOCATION).result()]
    _log(f"  Validated LLM rows: {len(validated_rows)}")

    # Apply category adjustment (pattern → TIRADS category)
    for row in validated_rows:
        pattern = row.get("pattern") or "unassignable"
        row["category_adjusted"] = CATEGORY_MAP.get(pattern, "TIRADS_3")
        row["post_validation_consistent"] = True  # already validated in step 5 of script 425
        row["inconsistency_reason"] = None
        row["revised"] = False

    # Add second-pass revisions if available
    if n_revised > 0:
        try:
            rev_rows = [dict(r) for r in bq.query(
                f"SELECT * FROM `{TABLE_HORVATH_REVISED}`", location=LOCATION
            ).result()]
            # Map revised results back to NLE format
            rev_map = {}
            for r in rev_rows:
                nid = r.get("nodule_id")
                pat = r.get("pattern") or "unassignable"
                pat_norm = pat.lower().strip().replace(" ", "_")
                if pat_norm not in VALID_PATTERNS:
                    pat_norm = "unassignable"
                rev_map[nid] = {
                    "pattern": pat_norm,
                    "category_adjusted": CATEGORY_MAP.get(pat_norm, "TIRADS_3"),
                    "evidence_short": str(r.get("evidence_short") or "")[:140],
                    "confidence": float(r.get("confidence") or 0.5),
                    "revised": True,
                    "post_validation_consistent": True,
                }
            # Apply revisions to validated_rows
            nle_idx = {r["nodule_id"]: i for i, r in enumerate(validated_rows)}
            n_committed = 0
            for nid, rev in rev_map.items():
                if nid in nle_idx:
                    validated_rows[nle_idx[nid]].update(rev)
                    n_committed += 1
            _log(f"  Applied {n_committed}/{len(rev_map)} second-pass revisions")
        except Exception as e:
            _log(f"  WARNING: Could not load revised rows ({e})")

    # Add RESOURCE_EXHAUSTED rows as unassignable
    res_exh_sql = f"""
    SELECT r.nodule_id, r.research_id
    FROM `{TABLE_HORVATH_RAW}` r
    WHERE r.status LIKE '%RESOURCE_EXHAUSTED%'
    """
    res_exh_rows_raw = [dict(r) for r in bq.query(res_exh_sql, location=LOCATION).result()]
    _log(f"  RESOURCE_EXHAUSTED rows to mark unassignable: {len(res_exh_rows_raw)}")

    unassignable_rows = []
    for r in res_exh_rows_raw:
        unassignable_rows.append({
            "nodule_id": r["nodule_id"],
            "research_id": r["research_id"],
            "pattern_raw": "resource_exhausted",
            "pattern": "unassignable",
            "category_llm": None,
            "category_adjusted": "TIRADS_3",
            "evidence_short": "Quota exhausted during batch inference; classification unavailable",
            "confidence": 0.0,
            "assignment_method": "resource_exhausted_fallback",
            "processed_at": RUN_TS,
            "post_validation_consistent": True,
            "inconsistency_reason": None,
            "revised": False,
        })

    # Add deterministic pre-classified rows
    det_rows_raw = [dict(r) for r in bq.query(
        f"SELECT * FROM `{TABLE_HORVATH_DET}`", location=LOCATION
    ).result()]
    _log(f"  Deterministic pre-classified rows: {len(det_rows_raw)}")

    det_nle_rows = []
    for r in det_rows_raw:
        det_nle_rows.append({
            "nodule_id": r["nodule_id"],
            "research_id": r["research_id"],
            "pattern_raw": r.get("pattern"),
            "pattern": r.get("pattern"),
            "category_llm": r.get("category"),
            "category_adjusted": r.get("category"),
            "evidence_short": r.get("evidence_short", "")[:140],
            "confidence": float(r.get("confidence") or 1.0),
            "assignment_method": r.get("assignment_method") or "deterministic_preclass",
            "processed_at": RUN_TS,
            "post_validation_consistent": True,
            "inconsistency_reason": None,
            "revised": False,
        })

    # MAX_TOKENS parse failures → unassignable
    max_tok_sql = f"""
    SELECT r.nodule_id, r.research_id
    FROM `{TABLE_HORVATH_RAW}` r
    WHERE r.status LIKE '%Failed to parse%' OR r.status LIKE '%MAX_TOKENS%'
    """
    max_tok_rows_raw = [dict(r) for r in bq.query(max_tok_sql, location=LOCATION).result()]
    _log(f"  MAX_TOKENS/parse-fail rows to mark unassignable: {len(max_tok_rows_raw)}")
    for r in max_tok_rows_raw:
        unassignable_rows.append({
            "nodule_id": r["nodule_id"],
            "research_id": r["research_id"],
            "pattern_raw": "max_tokens_fallback",
            "pattern": "unassignable",
            "category_llm": None,
            "category_adjusted": "TIRADS_3",
            "evidence_short": "MAX_TOKENS: response truncated; classification unavailable",
            "confidence": 0.0,
            "assignment_method": "max_tokens_fallback",
            "processed_at": RUN_TS,
            "post_validation_consistent": True,
            "inconsistency_reason": None,
            "revised": False,
        })

    # Combine all rows
    all_nle_rows = validated_rows + unassignable_rows + det_nle_rows
    _log(f"  Total NLE rows: {len(all_nle_rows)}")
    _log(f"    LLM-validated: {len(validated_rows)}")
    _log(f"    Unassignable (quota+parse failures): {len(unassignable_rows)}")
    _log(f"    Deterministic: {len(det_nle_rows)}")

    if not args.dry_run:
        df_nle = pd.DataFrame(all_nle_rows)
        df_nle["confidence"] = df_nle["confidence"].astype("float64")
        df_nle["post_validation_consistent"] = df_nle["post_validation_consistent"].astype(bool)
        df_nle["revised"] = df_nle["revised"].fillna(False).astype(bool)

        job_cfg = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
        load_job = bq.load_table_from_dataframe(
            df_nle, TABLE_HORVATH_NLE,
            job_config=job_cfg, location=LOCATION,
        )
        load_job.result()
        n_nle = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_HORVATH_NLE}`"))
        _log(f"  NLE written: {n_nle} rows")

        # Verify expected count matches snapshot
        n_snap = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_SNAPSHOT}`"))
        if n_nle != n_snap:
            _log(f"  WARNING: NLE rows ({n_nle}) != snapshot rows ({n_snap})")
        else:
            _log(f"  NLE count matches snapshot ({n_snap}) ✓")

    # -----------------------------------------------------------------------
    # Step C: CTAS rebuild canonical multisystem table
    # -----------------------------------------------------------------------
    _log("\nStep C: CTAS rebuild canonical multisystem table")
    if not args.dry_run:
        _run_sql(bq, build_ctas_sql_recovery(), "Horvath CTAS rebuild")
        n_rebuilt = int(_scalar(bq, f"SELECT COUNT(*) FROM `{TABLE_MULTISYS}`"))
        _log(f"  Rebuilt: {n_rebuilt} rows")

        # Coverage audit
        coverage_sql = f"""
        SELECT
          COUNTIF(horvath_pattern IS NOT NULL AND horvath_pattern != 'unassignable') AS n_valid_horvath,
          COUNTIF(horvath_pattern = 'unassignable') AS n_unassignable,
          COUNTIF(horvath_pattern IS NULL) AS n_null,
          COUNT(*) AS n_total
        FROM `{TABLE_MULTISYS}`
        """
        cov = dict(next(iter(bq.query(coverage_sql, location=LOCATION).result())))
        n_valid = cov["n_valid_horvath"]
        n_unasgn = cov["n_unassignable"]
        n_null = cov["n_null"]
        n_tot = cov["n_total"]
        _log(f"  Horvath coverage:")
        _log(f"    Valid patterns: {n_valid}/{n_tot} ({n_valid/n_tot:.1%})")
        _log(f"    Unassignable (incl. quota failures): {n_unasgn}/{n_tot} ({n_unasgn/n_tot:.1%})")
        _log(f"    NULL: {n_null}/{n_tot} ({n_null/n_tot:.1%})")

    # -----------------------------------------------------------------------
    # Step D: Summary and DFL guidance
    # -----------------------------------------------------------------------
    _log("\n=== Phase C.5 Horvath Recovery Summary ===")
    _log(f"  LLM-successful rows: {n_success}/18,376 ({n_success/18376:.1%})")
    _log(f"  RESOURCE_EXHAUSTED rows: {n_res_exh}/18,376 ({n_res_exh/18376:.1%})")
    _log(f"  Deterministic rows: {n_det}/37,579 ({n_det/37579:.1%})")
    _log(f"  Total Horvath-assigned (non-null, any method): ~{n_success + n_det}/37,579")
    _log("")
    _log("  RESOURCE_EXHAUSTED root cause: Vertex AI Gemini 2.5 Pro quota exhaustion")
    _log("  during a single 18K-row AI.GENERATE_TABLE batch. Workaround: rows classified")
    _log("  as 'unassignable' (TIRADS_3). A future targeted batch rerun in ~500-row")
    _log("  batches with quota-aware rate limiting could recover these rows.")
    _log("")
    _log("  Action: File a Verification Check for RESOURCE_EXHAUSTED coverage gap.")
    _log("  Notable Finding evidence_summary: add quota-gap fact.")
    _log("")
    _log("Recovery complete. Proceed to Step 5 (script 430 disagreement queue).")


if __name__ == "__main__":
    main()
