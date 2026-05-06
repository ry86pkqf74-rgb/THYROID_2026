#!/usr/bin/env python3
"""mig_088 / mig_322 BQ lane — operative-note Sistrunk parser → canonical_patient_master (BigQuery).

MotherDuck is **not** the publication path for this project anymore; apply on BQ:

  # One-time DDL (idempotent):

  bq query --project_id=thyroid-canonical-pub-2026 \
    < bq_migrations/mig_088_sistrunk_procedure_cpm_bq_20260506.sql

  # Populate extract + roll CPM flags:

  .venv/bin/python scripts/mig_322_sistrunk_procedure_bq.py --dry-run
  .venv/bin/python scripts/mig_322_sistrunk_procedure_bq.py --apply

DFL before first apply on production: ``DFL-20260506-SISTRUNKPARSE-BQ`` (Data Feedback Log,
THYROID_MANUSCRIPT base). Scripted Airtable log is optional; see ``--log-airtable`` (skipped if
credentials unset).

Operational note: BigQuery ``clinical_notes_long.note_type`` is ``OPNOTE`` (uppercase) in exports;
MotherDuck used ``op_note``. This script accepts both via case-insensitive matching.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(SCRIPT_DIR))
sys.path.insert(0, str(REPO_ROOT))

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
EXPECTED_TGDC_SISTRUNK = 161  # TGDC_FINAL_RECONCILIATION_REPORT manuscript lock


def _note_surrogate_key(research_id: str, note_text: str) -> str:
    blob = f"{research_id}\0{(note_text or '')[:8000]}"
    return hashlib.sha256(blob.encode("utf-8", errors="replace")).hexdigest()[:32]


def _clinical_notes_sql(project: str) -> str:
    return f"""
SELECT research_id, note_text
FROM `{project}.pub_canonical.clinical_notes_long`
WHERE LOWER(TRIM(COALESCE(note_type, ''))) IN ('op_note', 'opnote')
  AND note_text IS NOT NULL
  AND LENGTH(TRIM(note_text)) > 0
"""


def _tgdc_sistrunk_audit_bq(client, project: str) -> tuple[int, int] | None:
    from google.cloud import bigquery as bq_module

    sql = f"""
SELECT
  COUNT(*) AS n_cohort,
  SUM(CASE WHEN p.sistrunk_procedure IS TRUE THEN 1 ELSE 0 END) AS n_sistrunk
FROM `{project}.pub_workspace.cohort_tgdc_primary_v1` AS c
INNER JOIN `{project}.pub_canonical.canonical_patient_master` AS p
  ON p.research_id = c.research_id
"""
    try:
        rows = list(client.query(sql).result())
    except bq_module.NotFound:
        return None
    if not rows:
        return None
    r = rows[0]
    return int(r["n_cohort"] or 0), int(r["n_sistrunk"] or 0)


def _maybe_log_airtable() -> None:
    """Append Data Feedback Log row via Airtable REST if env is configured."""

    api_key = (__import__("os").environ.get("AIRTABLE_API_KEY") or "").strip()
    if not api_key:
        print("Skipping Airtable DFL (--log-airtable: AIRTABLE_API_KEY unset).")
        return
    import json
    import urllib.error
    import urllib.parse
    import urllib.request

    base_id = "appJYOnUb7KrHKwpV"
    table_name = "Data Feedback Log"
    url = (
        f"https://api.airtable.com/v0/{base_id}/"
        f"{urllib.parse.quote(table_name)}"
    )
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    payload = {
        "fields": {
            "feedback_id": "DFL-20260506-SISTRUNKPARSE-BQ",
            "timestamp": now_iso,
            "target_type": "BQ infrastructure",
            "target_record": (
                "pub_canonical.canonical_patient_master + "
                "pub_workspace.extracted_sistrunk_procedure_opnote_v1"
            ),
            "change_type": "migration",
            "your_request_summary": (
                "User moved publication off MotherDuck; complete BigQuery DDL + populate "
                "Sistrunk parser columns and extract table; keep TGDC numerator parity."
            ),
            "my_action_summary": (
                "Applied mig_088 DDL (ADD COLUMN IF NOT EXISTS + extract table) and ran "
                "scripts/mig_322_sistrunk_procedure_bq.py --apply to scan "
                "pub_canonical.clinical_notes_long (OPNOTE/op_note) and populate fields."
            ),
            "before_value": "canonical_patient_master lacked Sistrunk procedure columns.",
            "after_value": (
                "six sistrunk_* columns on CPM plus pub_workspace.extracted_sistrunk_"
                "procedure_opnote_v1 refreshed (paraphrase evidence only)."
            ),
            "source_chat": "THY-4 follow-up: BQ+Airtable",
            "lifecycle": "Logged",
        }
    }
    req = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            resp.read()
        print("Airtable Data Feedback Log: created DFL-20260506-SISTRUNKPARSE-BQ")
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Airtable HTTP {e.code}: {body}") from e


def _insert_migration_log(client, project: str, summary: str) -> None:
    from google.cloud import bigquery as bq

    clip = summary[:8182]
    sql = f"""
INSERT INTO `{project}.pub_signoff.bq_migration_log_v1` (
  migration_id,
  applied_at,
  applied_by,
  description,
  affected_dataset,
  affected_table,
  pre_snapshot_table,
  rows_before,
  rows_after,
  rollback_sql,
  notes
)
SELECT
  'mig_088_sistrunk_procedure_cpm_data_apply_20260506',
  CURRENT_TIMESTAMP(),
  'cursor_agent_sistrunk_bq_apply',
  'THY-4: Populate Sistrunk extract + canonical_patient_master flags from operative notes.',
  'pub_workspace',
  'extracted_sistrunk_procedure_opnote_v1',
  CAST(NULL AS STRING),
  CAST(NULL AS INT64),
  (SELECT COUNT(*) FROM `{project}.pub_workspace.extracted_sistrunk_procedure_opnote_v1`),
  'Re-run scripts/mig_322_sistrunk_procedure_bq.py --apply.',
  FORMAT('%s | expect TGDC numerator %d.', @summary, @want)
FROM UNNEST([1])
WHERE NOT EXISTS (
  SELECT 1
  FROM `{project}.pub_signoff.bq_migration_log_v1`
  WHERE migration_id = 'mig_088_sistrunk_procedure_cpm_data_apply_20260506'
)
"""
    job_cfg = bq.QueryJobConfig(
        query_parameters=[
            bq.ScalarQueryParameter("summary", "STRING", clip),
            bq.ScalarQueryParameter("want", "INT64", EXPECTED_TGDC_SISTRUNK),
        ]
    )
    client.query(sql, job_config=job_cfg).result()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="BigQuery Sistrunk procedure extractor (mig_088 data apply)"
    )
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    ap.add_argument(
        "--log-airtable",
        action="store_true",
        help="POST Data Feedback Log row (needs AIRTABLE_API_KEY)",
    )
    args = ap.parse_args()
    if args.apply == args.dry_run:
        print("Specify exactly one of --apply | --dry-run", file=sys.stderr)
        return 2

    from google.auth.exceptions import DefaultCredentialsError  # noqa: E402
    from google.cloud import bigquery  # noqa: E402
    import pandas as pd  # noqa: E402

    from pipelines.extraction.sistrunk_parser import (  # noqa: E402
        parse_sistrunk_in_note,
        pick_best_per_patient,
    )

    try:
        client = bigquery.Client(project=args.project)
    except DefaultCredentialsError as e:
        print(
            "BigQuery credentials missing. Run "
            "`gcloud auth application-default login` "
            f"with access to `{args.project}`.",
            file=sys.stderr,
        )
        raise SystemExit(1) from e

    log_lines: list[str] = []

    def log(msg: str) -> None:
        print(msg)
        log_lines.append(msg)

    sql_notes = _clinical_notes_sql(args.project)
    hits: list = []
    n_scan = 0
    for row in client.query(sql_notes).result():
        n_scan += 1
        rid = str(row["research_id"] or "").strip()
        ntext = row["note_text"] or ""
        nid = _note_surrogate_key(rid, str(ntext))
        hit = parse_sistrunk_in_note(str(ntext), research_id=rid, note_row_id=nid)
        if hit is not None:
            hits.append(hit)

    log(f"Scanned operative-note rows: {n_scan}")
    log(f"Raw parser hits: {len(hits)}")
    best = pick_best_per_patient(hits)
    log(f"Patients with ≥1 Sistrunk hit: {len(best)}")

    audit = _tgdc_sistrunk_audit_bq(client, args.project)
    if audit:
        n_c, n_s = audit
        log(f"TGDC cohort (BQ join CPM): n={n_c}; sistrunk_procedure=TRUE pre-apply={n_s}")

    if args.dry_run:
        log("DRY-RUN complete (no BQ writes).")
        out = REPO_ROOT / "scripts" / "output" / "mig_322_bq_dry_run_log.txt"
        out.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        out.write_text(stamp + "\n" + "\n".join(log_lines) + "\n", encoding="utf-8")
        log(f"Wrote {out}")
        return 0

    if args.log_airtable:
        try:
            _maybe_log_airtable()
        except Exception as exc:  # noqa: BLE001
            print(f"WARN Airtable DFL: {exc}", file=sys.stderr)

    built_stamp = datetime.now(timezone.utc)
    rows_extract = []
    for h in hits:
        rows_extract.append(
            {
                "research_id": h.research_id,
                "note_row_id": h.note_row_id,
                "parser_rule_id": h.rule_id,
                "match_kind": h.sistrunk_match_kind,
                "match_offset": int(h.match_offset),
                "evidence_summary": h.sistrunk_text_evidence,
                "built_at": built_stamp.replace(tzinfo=None),
            }
        )

    df_e = pd.DataFrame(rows_extract)
    extract_tbl = f"{args.project}.pub_workspace.extracted_sistrunk_procedure_opnote_v1"
    if df_e.empty:
        client.query(f"DELETE FROM `{extract_tbl}` WHERE TRUE").result()
        log(f"Truncate {extract_tbl} (0 extract rows)")
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("research_id", "STRING"),
                bigquery.SchemaField("note_row_id", "STRING"),
                bigquery.SchemaField("parser_rule_id", "STRING"),
                bigquery.SchemaField("match_kind", "STRING"),
                bigquery.SchemaField("match_offset", "INT64"),
                bigquery.SchemaField("evidence_summary", "STRING"),
                bigquery.SchemaField("built_at", "TIMESTAMP"),
            ],
        )
        job = client.load_table_from_dataframe(df_e, extract_tbl, job_config=job_config)
        job.result()
        log(f"Loaded {len(df_e)} rows into {extract_tbl}")

    reset_sql = f"""
UPDATE `{args.project}.pub_canonical.canonical_patient_master`
SET
  sistrunk_procedure = FALSE,
  sistrunk_procedure_evidence_summary = NULL,
  sistrunk_procedure_match_kind = NULL,
  sistrunk_procedure_match_offset = NULL,
  sistrunk_procedure_parser_rule_id = NULL,
  sistrunk_procedure_evidence_note_row_id = NULL
WHERE TRUE
"""
    client.query(reset_sql).result()
    log("Reset all CPM.sistrunk_* fields")

    if best:
        rows_best = []
        for h in best.values():
            rows_best.append(
                {
                    "research_id": h.research_id.strip(),
                    "sistrunk_procedure_evidence_summary": h.sistrunk_text_evidence,
                    "sistrunk_procedure_match_kind": h.sistrunk_match_kind,
                    "sistrunk_procedure_match_offset": int(h.match_offset),
                    "sistrunk_procedure_parser_rule_id": h.rule_id,
                    "sistrunk_procedure_evidence_note_row_id": h.note_row_id or "",
                }
            )
        df_b = pd.DataFrame(rows_best)
        stg = f"{args.project}.pub_workspace._stg_sistrunk_procedure_cpm_apply"
        stg_job = client.load_table_from_dataframe(
            df_b,
            stg,
            job_config=bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE),
        )
        stg_job.result()

        upsert_sql = f"""
UPDATE `{args.project}.pub_canonical.canonical_patient_master` AS c
SET
  sistrunk_procedure = TRUE,
  sistrunk_procedure_evidence_summary = s.sistrunk_procedure_evidence_summary,
  sistrunk_procedure_match_kind = s.sistrunk_procedure_match_kind,
  sistrunk_procedure_match_offset = s.sistrunk_procedure_match_offset,
  sistrunk_procedure_parser_rule_id = s.sistrunk_procedure_parser_rule_id,
  sistrunk_procedure_evidence_note_row_id = s.sistrunk_procedure_evidence_note_row_id
FROM `{stg}` AS s
WHERE c.research_id = s.research_id
"""
        client.query(upsert_sql).result()
        log(f"CPM positives updated: {len(df_b)} patients")

        client.delete_table(stg, not_found_ok=True)
    else:
        log("No CPM positives to set.")

    nrow = next(
        client.query(
            f"SELECT COUNTIF(sistrunk_procedure) AS n FROM `{args.project}.pub_canonical.canonical_patient_master`"
        ).result()
    )["n"]
    log(f"CPM sistrunk_procedure=TRUE count: {nrow}")

    audit2 = _tgdc_sistrunk_audit_bq(client, args.project)
    if audit2:
        n_c, n_s = audit2
        log(f"POST TGDC cohort n={n_c}; sistrunk TRUE within cohort: {n_s}")
        if int(n_s) != EXPECTED_TGDC_SISTRUNK:
            log(
                f"WARNING: TGDC numerator {n_s} != manuscript-locked {EXPECTED_TGDC_SISTRUNK}; "
                "investigate cohort or/parser."
            )

    summary = (
        f"BQ mig_088 apply: scanned={n_scan}, hits_note={len(hits)}, patients_pos={len(best)}, "
        f"cpm_true={int(nrow)}"
    )
    try:
        _insert_migration_log(client, args.project, summary)
        log("Inserted bq_migration_log_v1 mig_088_sistrunk_procedure_cpm_data_apply_20260506 (if new)")
    except Exception as e:  # noqa: BLE001
        log(f"WARN migration log insert: {e}")

    out_apply = REPO_ROOT / "scripts" / "output" / "mig_322_bq_apply_log.txt"
    stamp2 = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    out_apply.parent.mkdir(parents=True, exist_ok=True)
    out_apply.write_text(stamp2 + "\n" + "\n".join(log_lines) + "\n", encoding="utf-8")
    log(f"Wrote {out_apply}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
