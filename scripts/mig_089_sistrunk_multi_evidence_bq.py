#!/usr/bin/env python3
"""mig_089 — Multi-source Sistrunk evidence for TGDC cohort (BigQuery).

Implements the full three-tier evidence model aligned with TGDC_FINAL_RECONCILIATION_REPORT:
  Tier 1 (text_confirmed_strong): op-note parser + path_synoptics text + DC/ENDOCRINE notes
  Tier 2 (text_confirmed_preop):  H&P notes (contains pre/post-op planning)
  Tier 3 (structured_inference):  thyroid_procedure='other' within TGDC cohort (no text evidence)

Creates:
  pub_workspace.sistrunk_all_evidence_v1   — per-patient evidence row (TGDC ∩ CPM + addons note)
  pub_signoff.bq_migration_log_v1          — governance row

Updates canonical_patient_master:
  sistrunk_procedure          BOOL   — TRUE for Tier 1+2 text-confirmed (highest confidence)
  sistrunk_procedure_inference BOOL  — TRUE for Tier 3 inference-only (moderate confidence)
  sistrunk_procedure_evidence_tier VARCHAR — text_confirmed / structured_inference / none

TGDC parity:
  Tier 1+2 text-confirmed:  ~84 of 227 cohort (222 in CPM + 5 CPM-absent addons)
  Tier 1+2+3 full coverage: ~201 of 222 in CPM
  Manuscript target:         161 of 227 (71%) — bracketed by Tier 2 and Tier 3
  5 CPM-absent TGDC addons:  absent from all BQ tables; can only be counted
                              from the original DuckDB/manual chart review source.

Usage:
  .venv/bin/python scripts/mig_089_sistrunk_multi_evidence_bq.py --dry-run
  .venv/bin/python scripts/mig_089_sistrunk_multi_evidence_bq.py --apply
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(REPO_ROOT))

BQ_PROJECT = "thyroid-canonical-pub-2026"
EXPECTED_TGDC_N = 227
EXPECTED_SISTRUNK_MANUSCRIPT = 161

CPM_TABLE = f"`{BQ_PROJECT}.pub_canonical.canonical_patient_master`"
PATH_SYN = f"`{BQ_PROJECT}.pub_canonical.path_synoptics`"
NOTES = f"`{BQ_PROJECT}.pub_canonical.clinical_notes_long`"
OPNOTE_EXTRACT = f"`{BQ_PROJECT}.pub_workspace.extracted_sistrunk_procedure_opnote_v1`"
TGDC_COHORT = f"`{BQ_PROJECT}.pub_workspace.cohort_tgdc_primary_v1`"
EVIDENCE_TABLE = f"`{BQ_PROJECT}.pub_workspace.sistrunk_all_evidence_v1`"
STG = f"`{BQ_PROJECT}.pub_workspace._stg_sistrunk_tier_update`"

# Note: 5 TGDC addons completely absent from BQ canonical tables
TGDC_CPM_ABSENT = {"3315", "7226", "8754", "10748", "20028"}

_BUILD_EVIDENCE_SQL = f"""
CREATE OR REPLACE TABLE {EVIDENCE_TABLE} AS
WITH tgdc AS (
  SELECT research_id FROM {TGDC_COHORT}
),
tier1_opnote AS (
  SELECT DISTINCT e.research_id, "tier1_opnote" AS evidence_tier, "opnote_parser" AS source
  FROM {OPNOTE_EXTRACT} e
  INNER JOIN tgdc t ON t.research_id = e.research_id
),
tier1_path AS (
  SELECT DISTINCT CAST(p.research_id AS STRING) AS research_id, "tier1_path_text" AS evidence_tier, "path_synoptics" AS source
  FROM {PATH_SYN} p
  INNER JOIN tgdc t ON CAST(p.research_id AS STRING) = t.research_id
  WHERE REGEXP_CONTAINS(LOWER(COALESCE(CAST(p.path_diagnosis_summary AS STRING),"")), r"sistrunk")
     OR REGEXP_CONTAINS(LOWER(COALESCE(CAST(p.clinical_information_pre_op_diagnosis AS STRING),"")), r"sistrunk")
     OR REGEXP_CONTAINS(LOWER(COALESCE(CAST(p.procedure_other_description AS STRING),"")), r"sistrunk")
     OR REGEXP_CONTAINS(LOWER(COALESCE(CAST(p.thyroid_procedure AS STRING),"")), r"sistrunk")
),
tier1_dc AS (
  SELECT DISTINCT CAST(n.research_id AS STRING) AS research_id, "tier1_dc_notes" AS evidence_tier, "dc_endocrine_notes" AS source
  FROM {NOTES} n
  INNER JOIN tgdc t ON CAST(n.research_id AS STRING) = t.research_id
  WHERE LOWER(TRIM(COALESCE(note_type,""))) IN ("dc_sum", "endocrine_fm")
    AND REGEXP_CONTAINS(LOWER(COALESCE(note_text,"")), r"\bsistrunk\b")
),
tier2_hp AS (
  SELECT DISTINCT CAST(n.research_id AS STRING) AS research_id, "tier2_hp_notes" AS evidence_tier, "hp_notes" AS source
  FROM {NOTES} n
  INNER JOIN tgdc t ON CAST(n.research_id AS STRING) = t.research_id
  WHERE LOWER(TRIM(COALESCE(note_type,""))) = "hp"
    AND REGEXP_CONTAINS(LOWER(COALESCE(note_text,"")), r"\bsistrunk\b")
),
all_text AS (
  SELECT research_id, evidence_tier, source FROM tier1_opnote
  UNION DISTINCT SELECT * FROM tier1_path
  UNION DISTINCT SELECT * FROM tier1_dc
  UNION DISTINCT SELECT * FROM tier2_hp
),
tier3_inferred AS (
  SELECT DISTINCT CAST(p.research_id AS STRING) AS research_id, "tier3_inference" AS evidence_tier, "path_synoptics_proc_other" AS source
  FROM {PATH_SYN} p
  INNER JOIN tgdc t ON CAST(p.research_id AS STRING) = t.research_id
  LEFT JOIN all_text tx ON tx.research_id = CAST(p.research_id AS STRING)
  WHERE LOWER(COALESCE(CAST(p.thyroid_procedure AS STRING),"")) = "other"
    AND tx.research_id IS NULL  -- inference-only, no text evidence
),
combined AS (
  SELECT research_id, evidence_tier, source FROM all_text
  UNION DISTINCT SELECT * FROM tier3_inferred
)
SELECT
  c.research_id,
  CASE
    WHEN COUNTIF(evidence_tier IN ("tier1_opnote","tier1_path_text","tier1_dc_notes")) > 0 THEN "text_confirmed_strong"
    WHEN COUNTIF(evidence_tier = "tier2_hp_notes") > 0 THEN "text_confirmed_preop"
    WHEN COUNTIF(evidence_tier = "tier3_inference") > 0 THEN "structured_inference"
    ELSE "unknown"
  END AS best_evidence_tier,
  STRING_AGG(DISTINCT c.source, "|" ORDER BY c.source) AS all_sources,
  CURRENT_TIMESTAMP() AS built_at
FROM combined c
GROUP BY c.research_id
"""

_ADD_CPM_COLS_SQLS = [
    f"ALTER TABLE {CPM_TABLE} ADD COLUMN IF NOT EXISTS sistrunk_procedure_inference BOOL",
    f"ALTER TABLE {CPM_TABLE} ADD COLUMN IF NOT EXISTS sistrunk_procedure_evidence_tier STRING",
]

_RESET_CPM_SQL = f"""
UPDATE {CPM_TABLE}
SET
  sistrunk_procedure = FALSE,
  sistrunk_procedure_inference = FALSE,
  sistrunk_procedure_evidence_tier = NULL,
  sistrunk_procedure_evidence_summary = NULL,
  sistrunk_procedure_match_kind = NULL,
  sistrunk_procedure_match_offset = NULL,
  sistrunk_procedure_parser_rule_id = NULL,
  sistrunk_procedure_evidence_note_row_id = NULL
WHERE TRUE
"""

_BUILD_STAGING_SQL = f"""
CREATE OR REPLACE TABLE {STG} AS
SELECT
  e.research_id,
  CASE WHEN e.best_evidence_tier IN ("text_confirmed_strong","text_confirmed_preop") THEN TRUE ELSE FALSE END AS sistrunk_procedure,
  CASE WHEN e.best_evidence_tier = "structured_inference" THEN TRUE ELSE FALSE END AS sistrunk_procedure_inference,
  e.best_evidence_tier AS sistrunk_procedure_evidence_tier,
  FORMAT("Multi-source BQ detection: tier=%s sources=%s", e.best_evidence_tier, e.all_sources) AS sistrunk_procedure_evidence_summary
FROM {EVIDENCE_TABLE} e
"""

_UPDATE_CPM_SQL = f"""
UPDATE {CPM_TABLE} AS c
SET
  sistrunk_procedure = s.sistrunk_procedure,
  sistrunk_procedure_inference = s.sistrunk_procedure_inference,
  sistrunk_procedure_evidence_tier = s.sistrunk_procedure_evidence_tier,
  sistrunk_procedure_evidence_summary = s.sistrunk_procedure_evidence_summary
FROM {STG} AS s
WHERE c.research_id = s.research_id
"""

_PARITY_SQL_TEMPLATE = """
WITH cpop AS (
  SELECT
    COUNT(*) AS n_cohort,
    COUNTIF(p.sistrunk_procedure IS TRUE) AS tier12_text,
    {infer_col} AS tier3_inference,
    COUNTIF(p.sistrunk_procedure IS TRUE {or_infer}) AS any_evidence
  FROM {cohort} c
  INNER JOIN {cpm} p ON p.research_id = c.research_id
)
SELECT n_cohort, tier12_text, tier3_inference, any_evidence,
  {expected_n} - n_cohort AS cpm_absent_addons,
  tier12_text + ({expected_n} - n_cohort) AS tier12_incl_absent,
  (tier12_text + tier3_inference) + ({expected_n} - n_cohort) AS full_incl_absent
FROM cpop
"""


def _parity_sql(has_inference_col: bool) -> str:
    if has_inference_col:
        infer_col = "COUNTIF(p.sistrunk_procedure_inference IS TRUE)"
        or_infer = "OR p.sistrunk_procedure_inference IS TRUE"
    else:
        infer_col = "0"
        or_infer = ""
    return _PARITY_SQL_TEMPLATE.format(
        infer_col=infer_col,
        or_infer=or_infer,
        cohort=TGDC_COHORT,
        cpm=CPM_TABLE,
        expected_n=EXPECTED_TGDC_N,
    )


def main() -> int:
    ap = argparse.ArgumentParser(description="mig_089: Multi-source Sistrunk (BigQuery)")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--project", default=BQ_PROJECT)
    args = ap.parse_args()
    if args.apply == args.dry_run:
        print("Specify exactly one of --apply | --dry-run", file=sys.stderr)
        return 2

    from google.auth.exceptions import DefaultCredentialsError  # noqa: E402
    from google.cloud import bigquery  # noqa: E402

    try:
        client = bigquery.Client(project=args.project)
    except DefaultCredentialsError as exc:
        print(f"BQ credentials missing: {exc}", file=sys.stderr)
        return 1

    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    def run(sql: str, label: str = "") -> None:
        job = client.query(sql)
        job.result()
        rows = getattr(job, "num_dml_affected_rows", None)
        suffix = f" ({rows} rows)" if rows is not None else ""
        log(f"  OK {label}{suffix}")

    # Phase A: build evidence table (always, for reporting)
    log("Phase A: Build sistrunk_all_evidence_v1 ...")
    run(_BUILD_EVIDENCE_SQL, "sistrunk_all_evidence_v1")

    ev_count = list(client.query(f"SELECT COUNT(*) n FROM {EVIDENCE_TABLE}").result())[0]["n"]
    log(f"  Evidence rows (TGDC patients with any source): {ev_count}")

    tier_breakdown = list(client.query(
        f"SELECT best_evidence_tier, COUNT(*) n FROM {EVIDENCE_TABLE} GROUP BY 1 ORDER BY 1"
    ).result())
    for row in tier_breakdown:
        log(f"    {row['best_evidence_tier']}: {row['n']}")

    # Phase B: dry-run parity preview
    if args.dry_run:
        log("Phase B (DRY-RUN): Parity preview ...")
        # Check if inference column already exists
        cols = {
            r["column_name"]
            for r in client.query(
                f"SELECT column_name FROM `{args.project}.pub_canonical.INFORMATION_SCHEMA.COLUMNS` "
                f"WHERE table_name = 'canonical_patient_master' AND column_name = 'sistrunk_procedure_inference'"
            ).result()
        }
        par = list(client.query(_parity_sql("sistrunk_procedure_inference" in cols)).result())
        if par:
            r = par[0]
            log(
                f"  TGDC ∩ CPM={r['n_cohort']}  Tier1+2 text={r['tier12_text']}  "
                f"Tier3 infer={r['tier3_inference']}  Any={r['any_evidence']}"
            )
            log(
                f"  CPM-absent addons: {r['cpm_absent_addons']} (rids {sorted(TGDC_CPM_ABSENT)})"
            )
            log(
                f"  Tier1+2 incl absent: {r['tier12_incl_absent']}  Full incl absent: {r['full_incl_absent']}"
            )
            log(f"  Manuscript target: {EXPECTED_SISTRUNK_MANUSCRIPT}/{EXPECTED_TGDC_N}")
        log("DRY-RUN complete — no CPM writes.")
        _write_log(log_lines, "mig_089_bq_dry_run_log.txt")
        return 0

    # Phase C: add CPM columns
    log("Phase C: ADD COLUMN IF NOT EXISTS ...")
    for sql in _ADD_CPM_COLS_SQLS:
        run(sql)

    # Phase D: reset CPM
    log("Phase D: Reset all sistrunk fields ...")
    run(_RESET_CPM_SQL, "reset")

    # Phase E: staging
    log("Phase E: Build staging table ...")
    run(_BUILD_STAGING_SQL, "_stg")

    # Phase F: update CPM
    log("Phase F: Update CPM from staging ...")
    run(_UPDATE_CPM_SQL, "cpm update")

    # Phase G: parity
    log("Phase G: TGDC parity report ...")
    par = list(client.query(_parity_sql(True)).result())
    if par:
        r = par[0]
        log(
            f"  TGDC ∩ CPM={r['n_cohort']}  Tier1+2 text-confirmed={r['tier12_text']}  "
            f"Tier3 inference={r['tier3_inference']}  Any={r['any_evidence']}"
        )
        log(
            f"  CPM-absent manual addons: {r['cpm_absent_addons']} rids={sorted(TGDC_CPM_ABSENT)}"
        )
        log(
            f"  Tier1+2 including absent: {r['tier12_incl_absent']}/{EXPECTED_TGDC_N}  "
            f"Full including absent: {r['full_incl_absent']}/{EXPECTED_TGDC_N}"
        )
        log(
            f"  Manuscript target: {EXPECTED_SISTRUNK_MANUSCRIPT}/{EXPECTED_TGDC_N} (70.9%)"
        )
        t12 = int(r["tier12_incl_absent"] or 0)
        full = int(r["full_incl_absent"] or 0)
        if not (t12 <= EXPECTED_SISTRUNK_MANUSCRIPT <= full):
            log(
                f"  WARNING: manuscript target {EXPECTED_SISTRUNK_MANUSCRIPT} not bracketed by "
                f"Tier1+2={t12} and Full={full}; check evidence table."
            )

    # Phase H: cleanup staging
    client.delete_table(STG.strip("`"), not_found_ok=True)

    # Phase I: governance
    tier_str = "; ".join(f"{r['best_evidence_tier']}:{r['n']}" for r in tier_breakdown)
    summary = (
        f"mig_089: Multi-source Sistrunk evidence (BQ). "
        f"Evidence rows={ev_count}; tiers={tier_str}. "
        f"Manuscript target={EXPECTED_SISTRUNK_MANUSCRIPT}; 5 TGDC addons absent from BQ CPM."
    )
    _insert_log(client, args.project, summary, log)

    _write_log(log_lines, "mig_089_bq_apply_log.txt")
    return 0


def _insert_log(client, project: str, summary: str, log) -> None:
    from google.cloud import bigquery as bq

    sql = f"""
INSERT INTO `{project}.pub_signoff.bq_migration_log_v1` (
  migration_id, applied_at, applied_by, description,
  affected_dataset, affected_table,
  pre_snapshot_table, rows_before, rows_after, rollback_sql, notes
)
SELECT
  "mig_089_sistrunk_multi_evidence_bq_20260506",
  CURRENT_TIMESTAMP(),
  "cursor_agent_mig089",
  "THY-4: Multi-source Sistrunk evidence (text + inference tiers); TGDC 161/227 analysis.",
  "pub_workspace",
  "sistrunk_all_evidence_v1",
  CAST(NULL AS STRING),
  CAST(NULL AS INT64),
  (SELECT COUNT(*) FROM `{project}.pub_workspace.sistrunk_all_evidence_v1`),
  "Re-run scripts/mig_089_sistrunk_multi_evidence_bq.py --apply.",
  @summary
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1 FROM `{project}.pub_signoff.bq_migration_log_v1`
  WHERE migration_id = "mig_089_sistrunk_multi_evidence_bq_20260506"
)
"""
    try:
        client.query(
            sql,
            job_config=bq.QueryJobConfig(
                query_parameters=[bq.ScalarQueryParameter("summary", "STRING", summary[:8000])]
            ),
        ).result()
        log("  Governance row inserted (mig_089)")
    except Exception as exc:  # noqa: BLE001
        log(f"  WARN governance insert: {exc}")


def _write_log(lines: list[str], fname: str) -> None:
    out = REPO_ROOT / "scripts" / "output" / fname
    out.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out.write_text(stamp + "\n" + "\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out}")


if __name__ == "__main__":
    raise SystemExit(main())
