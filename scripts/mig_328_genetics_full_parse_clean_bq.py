#!/usr/bin/env python3
"""
mig_328: canonical_molecular_genetics_v2 — Afirma ThyroSeq-pipeline contamination cleanup
+ ThyroSeq parser-tail recovery + Afirma missing-call rescue (BigQuery).

Spec: studies/.../CURSOR_PROMPT_mig_328_genetics_full_parse_clean.md

Phases:
  A — Snapshot, staging, MERGE Afirma ROM/descriptor cleanup (platform_raw thyroseq slice)
  B — ThyroSeq rows with text but missing band and/or ROM%: thyroseq_detailed_parser (mig_321 pattern)
  C — Afirma rows with NULL overall_result_class: parse_afirma_result(molecular_testing.result)
  D — Document-only bucket counts → scripts/output CSV
  V — Post verification + regression checks

Usage:
  .venv/bin/python scripts/mig_328_genetics_full_parse_clean_bq.py --dry-run
  .venv/bin/python scripts/mig_328_genetics_full_parse_clean_bq.py --apply

Environment:
  GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default
"""
from __future__ import annotations

import argparse
import csv
import importlib.util
import json
import re
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MCONS = REPO_ROOT / "molecular_consolidation_20260421"
SCRIPTS_OUTPUT = REPO_ROOT / "scripts" / "output"
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(MCONS))

from afirma_result_field_parser import parse_afirma_result  # noqa: E402

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
CANONICAL_DATASET = "pub_canonical"
WORKSPACE_DATASET = "pub_workspace"
ARCHIVE_DATASET = "pub_archive"
CMG_TABLE = "canonical_molecular_genetics_v2"

RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID_HEX = uuid.uuid4().hex[:10]
RUN_ID = f"mig_328_{RUN_DATE}_{RUN_ID_HEX}"
SCRIPT_TAG = "mig_328_genetics_full_parse_clean_bq"

ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_mig328_{RUN_DATE}"
STAGE_A = f"mig328_afirma_contamination_{RUN_DATE}"
STAGE_B = f"mig328_thyroseq_parser_tail_{RUN_DATE}"
STAGE_C = f"mig328_afirma_label_recovery_{RUN_DATE}"
PARSER_VERSION = "thyroseq_detailed_parser_v4_mig328_tail"


def _active_cmg_predicate(alias: str | None = None) -> str:
    """When alias is None, emit unqualified columns (for queries with no table alias)."""
    p = f"{alias}." if alias else ""
    return f"""(
  {p}platform_reclass_status IS NULL
  OR {p}platform_reclass_status NOT IN (
    'superseded_by_afirma_row','non_diagnostic_cancelled','non_diagnostic'
  )
)"""


def _bq(project: str):
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def _run(client, sql: str, desc: str = "") -> None:
    if desc:
        print(f"  [BQ] {desc}")
    client.query(sql).result()


def _load_thyroseq_parser():
    spec = importlib.util.spec_from_file_location(
        "thyroseq_detailed_parser",
        MCONS / "thyroseq_detailed_parser.py",
    )
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Phase A — Afirma contamination cleanup (SQL-only)
# ---------------------------------------------------------------------------

def phase_a_snapshot(client, project: str, apply: bool, skip: bool) -> None:
    arch = f"`{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}`"
    src = f"`{project}.{CANONICAL_DATASET}.{CMG_TABLE}`"
    ddl = f"CREATE OR REPLACE TABLE {arch} AS SELECT * FROM {src}"
    print(f"[{SCRIPT_TAG}] Phase A0 snapshot → {ARCHIVE_TABLE}")
    if skip:
        print("  SKIP snapshot (--skip-snapshot)")
        return
    if not apply:
        print("  DRY-RUN: skip snapshot")
        return
    _run(client, ddl, f"snapshot {ARCHIVE_TABLE}")


def phase_a_staging_and_merge(client, project: str, apply: bool) -> int:
    stage = f"`{project}.{WORKSPACE_DATASET}.{STAGE_A}`"
    cmg = f"`{project}.{CANONICAL_DATASET}.{CMG_TABLE}`"

    staging_sql = f"""
CREATE OR REPLACE TABLE {stage} AS
SELECT
  research_id,
  molecular_episode_id,
  report_source_table,
  platform,
  platform_raw,
  overall_result_class,
  rom_descriptor,
  rom_percent_point,
  rom_percent_low,
  rom_percent_high,
  rom_percent_raw,
  rom_description,
  band_backfill_source,
  CASE
    WHEN band_backfill_source = 'numeric_rom_inferred' THEN 'null_all_rom_fields'
    WHEN rom_percent_point > 100 THEN 'null_all_rom_fields_ocr_garbage'
    WHEN rom_descriptor IS NOT NULL THEN 'null_descriptor_only'
    ELSE 'no_action'
  END AS contamination_action,
  CASE
    WHEN rom_description IS NOT NULL AND (
      LOWER(rom_description) LIKE '%thyroseq gc%'
      OR LOWER(rom_description) LIKE '%dna copy number alterations%'
      OR LOWER(rom_description) LIKE '%molecular profile is associated with%'
    ) THEN TRUE
    ELSE FALSE
  END AS rom_description_is_thyroseq_style
FROM {cmg}
WHERE platform = 'Afirma'
  AND REGEXP_CONTAINS(LOWER(COALESCE(platform_raw, '')), r'thyroseq')
  AND {_active_cmg_predicate()}
  AND (
    rom_descriptor IS NOT NULL
    OR rom_percent_point IS NOT NULL
    OR rom_percent_low IS NOT NULL
    OR rom_percent_high IS NOT NULL
    OR rom_percent_raw IS NOT NULL
    OR rom_description IS NOT NULL
  )
"""
    merge_sql = f"""
MERGE {cmg} AS T
USING (
  SELECT * FROM {stage}
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY research_id,
                 IFNULL(CAST(molecular_episode_id AS STRING), '__NULL_EP__'),
                 IFNULL(report_source_table, '')
    ORDER BY contamination_action
  ) = 1
) AS S
ON T.research_id = S.research_id
 AND T.molecular_episode_id IS NOT DISTINCT FROM S.molecular_episode_id
 AND T.report_source_table IS NOT DISTINCT FROM S.report_source_table
WHEN MATCHED THEN UPDATE SET
  rom_descriptor = NULL,
  rom_percent_point = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage')
      THEN NULL
    ELSE T.rom_percent_point
  END,
  rom_percent_low = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage')
      THEN NULL
    ELSE T.rom_percent_low
  END,
  rom_percent_high = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage')
      THEN NULL
    ELSE T.rom_percent_high
  END,
  rom_percent_raw = CASE
    WHEN S.contamination_action IN ('null_all_rom_fields','null_all_rom_fields_ocr_garbage')
      THEN NULL
    ELSE T.rom_percent_raw
  END,
  rom_description = CASE
    WHEN S.rom_description_is_thyroseq_style THEN NULL
    ELSE T.rom_description
  END,
  band_backfill_source = IF(
    CONTAINS_SUBSTR(COALESCE(T.band_backfill_source, ''), 'mig_328_afirma_contamination'),
    T.band_backfill_source,
    CONCAT(COALESCE(T.band_backfill_source, ''), '|mig_328_afirma_contamination_clean')
  ),
  band_backfill_applied_at = CURRENT_TIMESTAMP(),
  band_backfill_run_id = '{RUN_ID}'
"""

    print(f"[{SCRIPT_TAG}] Phase A1 staging {STAGE_A}")
    if not apply:
        n = list(
            client.query(
                f"""
SELECT COUNT(*) AS n FROM {cmg}
WHERE platform = 'Afirma'
  AND REGEXP_CONTAINS(LOWER(COALESCE(platform_raw, '')), r'thyroseq')
  AND {_active_cmg_predicate()}
  AND (
    rom_descriptor IS NOT NULL OR rom_percent_point IS NOT NULL
    OR rom_percent_low IS NOT NULL OR rom_percent_high IS NOT NULL
    OR rom_percent_raw IS NOT NULL OR rom_description IS NOT NULL
  )
"""
            ).result()
        )[0]["n"]
        print(f"  DRY-RUN: would stage ~{n} Afirma contamination rows")
        return int(n)

    _run(client, staging_sql, f"CREATE {STAGE_A}")
    job = client.query(merge_sql)
    job.result()
    n = job.num_dml_affected_rows or 0
    print(f"[{SCRIPT_TAG}] Phase A2 MERGE affected rows: {n}")

    # Rows with Afirma reports may carry LOW/INT/HIGH in rom_descriptor even when
    # platform_raw lacks the substring "thyroseq" (Arm A slice misses them). Per
    # VC-MOL-PLATFORM-002, Afirma does not expose ThyroSeq-style bands in this column.
    merge_a3 = f"""
MERGE {cmg} AS T
USING (
  SELECT research_id, molecular_episode_id, report_source_table
  FROM {cmg}
  WHERE platform = 'Afirma'
    AND rom_descriptor IS NOT NULL
    AND {_active_cmg_predicate()}
) S
ON T.research_id = S.research_id
 AND T.molecular_episode_id IS NOT DISTINCT FROM S.molecular_episode_id
 AND T.report_source_table IS NOT DISTINCT FROM S.report_source_table
WHEN MATCHED THEN UPDATE SET
  rom_descriptor = NULL,
  band_backfill_source = IF(
    CONTAINS_SUBSTR(COALESCE(T.band_backfill_source, ''), 'mig_328_afirma_all_descriptor'),
    T.band_backfill_source,
    CONCAT(COALESCE(T.band_backfill_source, ''), '|mig_328_afirma_all_descriptor_null')
  ),
  band_backfill_applied_at = CURRENT_TIMESTAMP(),
  band_backfill_run_id = '{RUN_ID}'
"""
    if apply:
        job3 = client.query(merge_a3)
        job3.result()
        print(f"[{SCRIPT_TAG}] Phase A3 (all Afirma rom_descriptor NULL) affected: {job3.num_dml_affected_rows or 0}")
    return int(n)


# ---------------------------------------------------------------------------
# Phase B — ThyroSeq parser tail (Python parse + staging MERGE)
# ---------------------------------------------------------------------------

def _arm_b_source_sql(project: str) -> str:
    cmg = f"`{project}.{CANONICAL_DATASET}.{CMG_TABLE}`"
    return f"""
SELECT
    cmg.research_id,
    cmg.molecular_episode_id,
    cmg.report_source_table,
    cmg.rom_descriptor,
    cmg.rom_percent_point,
    cmg.parse_status,
    cmg.report_text_ref,
    cmg.report_text_length,
    COALESCE(
        NULLIF(TRIM(tme.pathology_raw), ''),
        NULLIF(TRIM(mt.detailed_findings), '')
    ) AS report_text_joined
FROM {cmg} AS cmg
LEFT JOIN `{project}.{CANONICAL_DATASET}.thyroseq_molecular_enrichment` tme
       ON CAST(tme.research_id AS STRING) = CAST(cmg.research_id AS STRING)
LEFT JOIN (
    SELECT research_id,
           (ARRAY_AGG(
               detailed_findings
               ORDER BY CHAR_LENGTH(COALESCE(detailed_findings, '')) DESC
               LIMIT 1
           ))[OFFSET(0)] AS detailed_findings
    FROM `{project}.{CANONICAL_DATASET}.molecular_testing`
    WHERE detailed_findings IS NOT NULL
      AND CHAR_LENGTH(TRIM(detailed_findings)) > 50
    GROUP BY research_id
) mt ON CAST(mt.research_id AS STRING) = CAST(cmg.research_id AS STRING)
WHERE cmg.platform = 'ThyroSeq'
  AND {_active_cmg_predicate('cmg')}
  AND COALESCE(cmg.report_text_length, 0) > 0
  AND (cmg.rom_descriptor IS NULL OR cmg.rom_percent_point IS NULL)
ORDER BY cmg.research_id, COALESCE(CAST(cmg.molecular_episode_id AS STRING), '')
"""


def phase_b_thyroseq_tail(
    client, project: str, apply: bool, parser_mod
) -> tuple[int, list[dict]]:
    rows = list(client.query(_arm_b_source_sql(project)).result())
    print(f"[{SCRIPT_TAG}] Phase B: fetched {len(rows)} ThyroSeq tail candidates")

    staging: list[dict] = []
    audit_rows: list[dict] = []

    for row in rows:
        rd = dict(row)
        text = (rd.get("report_text_joined") or "").strip()
        existing_rom = rd.get("rom_percent_point")
        before_band = rd.get("rom_descriptor")
        before_pt = rd.get("rom_percent_point")

        excerpt = (text[:200] + "…") if len(text) > 200 else text
        method = "hard_pattern"

        if text:
            parsed = parser_mod.parse(text, platform="ThyroSeq")
            if parsed.get("rom_percent_point") is None and existing_rom is not None:
                parsed["rom_percent_point"] = float(existing_rom)
                parser_mod._apply_band_fallbacks(parsed, text)
        else:
            parsed = {"parse_status": "no_text", "parser": "thyroseq"}
            if existing_rom is not None:
                parsed["rom_percent_point"] = float(existing_rom)
            parser_mod._apply_band_fallbacks(parsed, "")

        rom_desc_new = parsed.get("rom_descriptor")
        rom_pt_new = parsed.get("rom_percent_point")
        band_src = parsed.get("band_source", "manual_review")
        if band_src == "manual_review" and rom_desc_new is None and rom_pt_new is None:
            continue

        if not text and rom_desc_new is None and rom_pt_new == existing_rom:
            continue

        if rom_desc_new and text and band_src == "manual_review":
            pat = re.sub(r"_", r"[\s_-]?", re.escape(str(rom_desc_new)))
            if not re.search(pat, text, re.I):
                method = "parser_unverified"
                rom_desc_new = None

        useful = False
        if before_band is None and rom_desc_new is not None:
            useful = True
        if before_pt is None and rom_pt_new is not None:
            useful = True
        if not useful:
            continue

        staging.append(
            {
                "research_id": str(rd["research_id"]),
                "molecular_episode_id": rd.get("molecular_episode_id"),
                "report_source_table": rd.get("report_source_table"),
                "rom_descriptor_new": rom_desc_new,
                "rom_percent_point_new": rom_pt_new,
                "overall_result_class_inferred": parsed.get("overall_result_class_inferred"),
                "band_source": band_src,
                "parse_status_new": parsed.get("parse_status"),
                "parser_version": PARSER_VERSION,
                "band_backfill_run_id": RUN_ID,
            }
        )
        audit_rows.append(
            {
                "molecular_episode_id": rd.get("molecular_episode_id"),
                "research_id": rd.get("research_id"),
                "before_band": before_band,
                "after_band": rom_desc_new,
                "before_rom_pt": before_pt,
                "after_rom_pt": rom_pt_new,
                "extraction_method": method,
                "source_text_excerpt_first_200_chars": excerpt,
            }
        )

    print(f"[{SCRIPT_TAG}] Phase B: {len(staging)} rows with recoverable values")

    if not apply:
        return len(staging), audit_rows

    if not staging:
        return 0, audit_rows

    from google.cloud import bigquery as bq

    stg_ref = f"{project}.{WORKSPACE_DATASET}.{STAGE_B}"
    schema = [
        bq.SchemaField("research_id", "STRING"),
        bq.SchemaField("molecular_episode_id", "INTEGER"),
        bq.SchemaField("report_source_table", "STRING"),
        bq.SchemaField("rom_descriptor_new", "STRING"),
        bq.SchemaField("rom_percent_point_new", "FLOAT64"),
        bq.SchemaField("overall_result_class_inferred", "STRING"),
        bq.SchemaField("band_source", "STRING"),
        bq.SchemaField("parse_status_new", "STRING"),
        bq.SchemaField("parser_version", "STRING"),
        bq.SchemaField("band_backfill_run_id", "STRING"),
    ]
    client.delete_table(stg_ref, not_found_ok=True)
    client.create_table(bq.Table(stg_ref, schema=schema))
    errors = client.insert_rows_json(client.get_table(stg_ref), staging)
    if errors:
        raise RuntimeError(f"staging B insert errors: {errors[:3]}")

    merge_b = f"""
MERGE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` AS tgt
USING (
    SELECT * FROM `{project}.{WORKSPACE_DATASET}.{STAGE_B}`
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY research_id,
                     CAST(molecular_episode_id AS STRING),
                     COALESCE(report_source_table, '')
        ORDER BY band_backfill_run_id
    ) = 1
) AS src
ON tgt.research_id = src.research_id
   AND tgt.molecular_episode_id IS NOT DISTINCT FROM src.molecular_episode_id
   AND tgt.report_source_table IS NOT DISTINCT FROM src.report_source_table
WHEN MATCHED
  AND tgt.platform = 'ThyroSeq'
  AND (
    (tgt.rom_descriptor IS NULL AND src.rom_descriptor_new IS NOT NULL)
    OR (tgt.rom_percent_point IS NULL AND src.rom_percent_point_new IS NOT NULL)
  )
THEN UPDATE SET
  tgt.rom_descriptor = COALESCE(tgt.rom_descriptor, src.rom_descriptor_new),
  tgt.rom_percent_point = COALESCE(tgt.rom_percent_point, src.rom_percent_point_new),
  tgt.overall_result_class = CASE
    WHEN tgt.overall_result_class NOT IN ('positive', 'negative', 'intermediate')
         OR tgt.overall_result_class IS NULL
    THEN COALESCE(tgt.overall_result_class, src.overall_result_class_inferred)
    ELSE tgt.overall_result_class
  END,
  tgt.band_backfill_applied_at = CURRENT_TIMESTAMP(),
  tgt.band_backfill_source = IF(
    CONTAINS_SUBSTR(COALESCE(tgt.band_backfill_source, ''), 'mig_328_tail'),
    tgt.band_backfill_source,
    CONCAT(COALESCE(tgt.band_backfill_source, ''), '|mig_328_thyroseq_tail')
  ),
  tgt.band_backfill_run_id = '{RUN_ID}'
"""
    job = client.query(merge_b)
    job.result()
    print(f"[{SCRIPT_TAG}] Phase B MERGE affected: {job.num_dml_affected_rows or 0}")
    return int(job.num_dml_affected_rows or 0), audit_rows


# ---------------------------------------------------------------------------
# Phase C — Afirma label recovery
# ---------------------------------------------------------------------------

def _arm_c_fetch(client, project: str) -> list[dict]:
    sql = f"""
SELECT
  g.molecular_episode_id,
  g.research_id,
  g.report_source_table,
  g.overall_result_class,
  mt.result AS mt_result,
  SUBSTR(COALESCE(mt.detailed_findings, ''), 1, 2500) AS mt_detail_head
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` g
LEFT JOIN `{project}.{CANONICAL_DATASET}.molecular_testing` mt
  ON CAST(mt.research_id AS STRING) = CAST(g.research_id AS STRING)
WHERE g.platform = 'Afirma'
  AND {_active_cmg_predicate('g')}
  AND g.overall_result_class IS NULL
  AND COALESCE(g.report_text_length, 0) > 0
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY g.molecular_episode_id
  ORDER BY CHAR_LENGTH(COALESCE(mt.result, '')) DESC
) = 1
"""
    return [dict(r) for r in client.query(sql).result()]


def _afirma_fallback_text(txt: str) -> dict | None:
    if not txt or len(txt.strip()) < 3:
        return None
    s = txt.lower()
    if re.search(r"\bno\s+result\b|\bcancelled\b", s):
        return {"overall_result_class": "non_diagnostic", "band_source": "mig_328_text_fallback"}
    if re.search(r"\b(?:insufficient|inadequate|non[- ]?diagnostic)\b", s):
        return {"overall_result_class": "non_diagnostic", "band_source": "mig_328_text_fallback"}
    if re.search(r"\bsuspicious\b", s):
        return {"overall_result_class": "positive", "band_source": "mig_328_text_fallback"}
    if re.search(r"\bbenign\b|\bnegative\b", s):
        return {"overall_result_class": "negative", "band_source": "mig_328_text_fallback"}
    if re.search(r"\bpositive\b", s):
        return {"overall_result_class": "positive", "band_source": "mig_328_text_fallback"}
    return None


def phase_c_afirma_labels(client, project: str, apply: bool) -> int:
    rows = _arm_c_fetch(client, project)
    print(f"[{SCRIPT_TAG}] Phase C: {len(rows)} Afirma rows with NULL overall_result_class")

    staging: list[dict] = []
    for rd in rows:
        mt_result = rd.get("mt_result") or ""
        parsed = parse_afirma_result(mt_result)
        orc = parsed.get("overall_result_class")
        src = parsed.get("band_source")
        if orc is None:
            fb = _afirma_fallback_text(rd.get("mt_detail_head") or "")
            if fb:
                orc = fb["overall_result_class"]
                src = fb["band_source"]
        if orc is None:
            continue
        staging.append(
            {
                "molecular_episode_id": rd["molecular_episode_id"],
                "research_id": str(rd["research_id"]),
                "proposed_overall_result_class": orc,
                "band_source": src or "afirma_mig328",
                "band_backfill_run_id": RUN_ID,
            }
        )

    print(f"[{SCRIPT_TAG}] Phase C: {len(staging)} rows to update")
    if not apply or not staging:
        return len(staging)

    from google.cloud import bigquery as bq

    stg_ref = f"{project}.{WORKSPACE_DATASET}.{STAGE_C}"
    schema = [
        bq.SchemaField("molecular_episode_id", "INTEGER"),
        bq.SchemaField("research_id", "STRING"),
        bq.SchemaField("proposed_overall_result_class", "STRING"),
        bq.SchemaField("band_source", "STRING"),
        bq.SchemaField("band_backfill_run_id", "STRING"),
    ]
    client.delete_table(stg_ref, not_found_ok=True)
    client.create_table(bq.Table(stg_ref, schema=schema))
    err = client.insert_rows_json(client.get_table(stg_ref), staging)
    if err:
        raise RuntimeError(err[:3])

    merge_c = f"""
MERGE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` AS T
USING `{project}.{WORKSPACE_DATASET}.{STAGE_C}` AS S
ON T.molecular_episode_id = S.molecular_episode_id
WHEN MATCHED AND T.platform = 'Afirma' AND T.overall_result_class IS NULL
  AND S.proposed_overall_result_class IS NOT NULL
THEN UPDATE SET
  overall_result_class = S.proposed_overall_result_class,
  band_backfill_applied_at = CURRENT_TIMESTAMP(),
  band_backfill_source = IF(
    CONTAINS_SUBSTR(COALESCE(T.band_backfill_source, ''), 'mig_328_afirma_label'),
    T.band_backfill_source,
    CONCAT(COALESCE(T.band_backfill_source, ''), '|mig_328_afirma_label_recovery')
  ),
  band_backfill_run_id = '{RUN_ID}'
"""
    job = client.query(merge_c)
    job.result()
    print(f"[{SCRIPT_TAG}] Phase C MERGE affected: {job.num_dml_affected_rows or 0}")
    return int(job.num_dml_affected_rows or 0)


# ---------------------------------------------------------------------------
# Phase D — document-only counts
# ---------------------------------------------------------------------------

def phase_d_document(client, project: str) -> None:
    sql = f"""
SELECT
  platform,
  report_source_table,
  COUNT(*) AS n
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE platform IN ('NGS_unspecified', 'Other')
  AND {_active_cmg_predicate()}
GROUP BY 1, 2
UNION ALL
SELECT
  'extracted_braf_recovery_v1' AS platform,
  report_source_table,
  COUNT(*) AS n
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE report_source_table = 'extracted_braf_recovery_v1'
  AND {_active_cmg_predicate()}
GROUP BY 1, 2
"""
    rows = [dict(r) for r in client.query(sql).result()]
    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
    outp = SCRIPTS_OUTPUT / f"mig_328_document_only_buckets_{RUN_DATE}.csv"
    if rows:
        with outp.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    print(f"[{SCRIPT_TAG}] Phase D wrote {outp}")


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

def phase_verify(client, project: str, apply: bool) -> None:
    ver = f"""
SELECT
  platform,
  COUNT(*) AS n_active,
  COUNTIF(overall_result_class IS NOT NULL) AS n_with_call,
  COUNTIF(rom_descriptor IS NOT NULL) AS n_with_descriptor,
  COUNTIF(rom_descriptor IS NOT NULL AND platform = 'Afirma') AS n_afirma_descriptor,
  COUNTIF(rom_percent_point > 100) AS n_impossible_rom,
  COUNTIF(
    rom_percent_point IS NOT NULL OR rom_percent_low IS NOT NULL OR rom_percent_high IS NOT NULL
  ) AS n_with_numeric_rom
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE platform_reclass_status IS NULL
   OR platform_reclass_status NOT IN (
     'superseded_by_afirma_row','non_diagnostic_cancelled','non_diagnostic'
   )
GROUP BY platform
ORDER BY platform
"""
    print(f"[{SCRIPT_TAG}] Verification coverage:")
    for r in client.query(ver).result():
        print(f"  {dict(r)}")

    if not apply:
        return

    reg = f"""
SELECT COUNT(*) AS n_ts_orc_regressions
FROM `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}` pre
JOIN `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` post
  ON pre.research_id = post.research_id
 AND pre.molecular_episode_id IS NOT DISTINCT FROM post.molecular_episode_id
 AND pre.report_source_table IS NOT DISTINCT FROM post.report_source_table
WHERE pre.platform = 'ThyroSeq'
  AND pre.overall_result_class IS NOT NULL
  AND post.overall_result_class IS DISTINCT FROM pre.overall_result_class
"""
    n = list(client.query(reg).result())[0]["n_ts_orc_regressions"]
    print(f"[{SCRIPT_TAG}] ThyroSeq overall_result_class regressions (expect 0): {n}")
    if n != 0:
        raise SystemExit(f"REGRESSION: {n} ThyroSeq overall_result_class rows changed")

    pre_n = list(
        client.query(
            f"SELECT COUNTIF(overall_result_class IS NOT NULL) AS n "
            f"FROM `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}` WHERE platform = 'Afirma'"
        ).result()
    )[0]["n"]
    post_n = list(
        client.query(
            f"SELECT COUNTIF(overall_result_class IS NOT NULL) AS n "
            f"FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` WHERE platform = 'Afirma'"
        ).result()
    )[0]["n"]
    if post_n < pre_n:
        raise SystemExit(f"REGRESSION: Afirma non-null overall_result_class dropped {pre_n}→{post_n}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-snapshot", action="store_true",
                    help="Do not overwrite pub_archive snapshot (repair runs after partial apply).")
    ap.add_argument("--phase", default="all", choices=("all", "a", "b", "c", "d", "v"))
    args = ap.parse_args()

    if args.apply == args.dry_run:
        print("Specify exactly one of --apply / --dry-run")
        return 2

    apply = bool(args.apply)
    project = args.project
    SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)

    print(f"[{SCRIPT_TAG}] run_id={RUN_ID} apply={apply} project={project}")

    parser_mod = _load_thyroseq_parser()
    smoke = parser_mod.parse(
        "TEST\nThyroSeq GC: NEGATIVE - LOW (3%)\nDETAILED RESULTS\n",
        platform="ThyroSeq",
    )
    assert smoke.get("rom_descriptor") == "LOW"
    print(f"[{SCRIPT_TAG}] Parser smoke OK")

    try:
        from google.cloud import bigquery
    except ImportError:
        print("Install google-cloud-bigquery")
        return 1

    client = bigquery.Client(project=project)

    phase_d_document(client, project)

    if args.phase in ("all", "a"):
        phase_a_snapshot(client, project, apply, args.skip_snapshot)
        phase_a_staging_and_merge(client, project, apply)

    if args.phase in ("all", "b"):
        _, audit = phase_b_thyroseq_tail(client, project, apply, parser_mod)
        outp = SCRIPTS_OUTPUT / f"mig_328_thyroseq_parser_tail_recovery_{RUN_DATE}.csv"
        if audit:
            with outp.open("w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(audit[0].keys()))
                w.writeheader()
                w.writerows(audit)
            print(f"[{SCRIPT_TAG}] Wrote audit {outp}")

    if args.phase in ("all", "c"):
        phase_c_afirma_labels(client, project, apply)

    if args.phase in ("all", "v"):
        phase_verify(client, project, apply)

    meta = {
        "run_id": RUN_ID,
        "archive_table": ARCHIVE_TABLE,
        "staging_a": STAGE_A,
        "staging_b": STAGE_B,
        "staging_c": STAGE_C,
        "apply": apply,
    }
    (SCRIPTS_OUTPUT / f"mig_328_run_{RUN_DATE}.json").write_text(
        json.dumps(meta, indent=2), encoding="utf-8"
    )
    print(f"[{SCRIPT_TAG}] Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
