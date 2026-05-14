#!/usr/bin/env python3
"""
mig_329: Backfill CT/MRI canonical lymph-node rows for legacy structured-positive exams
that never produced per-node regex rows (empty lymph_node_details).

Root cause: exports/ln_multimodal_20260507/extract_ln_multimodal_v3.py only ingests rows
where lymph_node_details is non-empty (fetch_ct / fetch_mri), so structured flags /
location arrays / short-axis mm alone did not emit canonical rows.

This script INSERTs one minimal row per qualifying legacy exam for patients who have
ZERO rows in the modality canonical table. Rows carry legacy-available size/suspicion
hints only; nlp_backfill_pending=TRUE.

Negative-only exams are NOT added (table remains "positive LN signal per row").

Also recreates canonical_lymph_node_unified_VIEW_v1 from its current definition so the
view stays bound to live tables (definition unchanged).

Validation (user spec):
  CT/MRI: positive_pts_missing → 0 after apply.
  Unified VIEW row count increases by exactly (CT inserts + MRI inserts).

Usage:
  .venv/bin/python scripts/mig_329_ct_mri_ln_legacy_positive_backfill_bq.py --dry-run
  .venv/bin/python scripts/mig_329_ct_mri_ln_legacy_positive_backfill_bq.py --apply

Env: GOOGLE_APPLICATION_CREDENTIALS or gcloud auth application-default
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

BQ_PROJECT = "thyroid-canonical-pub-2026"
LEGACY_DS = "pub_legacy_source_20260416"
CANON_DS = "pub_canonical"
ARCHIVE_DS = "pub_archive"

LEGACY_CT = f"`{BQ_PROJECT}.{LEGACY_DS}.ct_imaging`"
LEGACY_MRI = f"`{BQ_PROJECT}.{LEGACY_DS}.mri_imaging`"
T_CT = f"`{BQ_PROJECT}.{CANON_DS}.canonical_ct_lymph_node_v1`"
T_MRI = f"`{BQ_PROJECT}.{CANON_DS}.canonical_mri_lymph_node_v1`"
V_UNIFIED = f"`{BQ_PROJECT}.{CANON_DS}.canonical_lymph_node_unified_VIEW_v1`"

SCRIPT_TAG = "mig_329_ct_mri_ln_legacy_positive_backfill_bq"
RUN_stamp = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
ARCH_CT = f"canonical_ct_lymph_node_v1_pre_mig329_{RUN_stamp}"
ARCH_MRI = f"canonical_mri_lymph_node_v1_pre_mig329_{RUN_stamp}"

OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Row-level “positive finding” predicates (match user validation; legacy per exam).
CT_POSITIVE = """(
  LOWER(CAST(pathologic_lymph_nodes AS STRING)) = 'true'
  OR LOWER(CAST(lymph_nodes_suspicious AS STRING)) = 'true'
  OR LOWER(CAST(lymph_nodes_enlarged AS STRING)) = 'true'
  OR largest_lymph_node_short_axis_mm IS NOT NULL
  OR (
    lymph_node_locations IS NOT NULL
    AND TRIM(CAST(lymph_node_locations AS STRING)) NOT IN ('', '[]', '{}', 'null', 'None')
  )
)"""

MRI_POSITIVE = """(
  lymph_node_locations_with_size IS NOT NULL
  AND TRIM(CAST(lymph_node_locations_with_size AS STRING)) NOT IN ('', '[]', '{}', 'null', 'None')
)"""


def _client():
    from google.cloud import bigquery

    return bigquery.Client(project=BQ_PROJECT)


def _run(client, sql: str, desc: str = "") -> None:
    if desc:
        print(f"  [BQ] {desc}")
    client.query(sql).result()


def _scalar(client, sql: str):
    return list(client.query(sql).result())[0][0]


def validation_sql_ct() -> str:
    return f"""
WITH ct AS (
  SELECT research_id FROM {LEGACY_CT}
  WHERE {CT_POSITIVE}
)
SELECT COUNT(DISTINCT research_id) AS positive_pts_missing
FROM ct
WHERE research_id NOT IN (
  SELECT research_id FROM {T_CT}
);
"""


def validation_sql_mri() -> str:
    return f"""
WITH mr AS (
  SELECT research_id FROM {LEGACY_MRI}
  WHERE {MRI_POSITIVE}
)
SELECT COUNT(DISTINCT research_id) AS positive_pts_missing
FROM mr
WHERE research_id NOT IN (
  SELECT research_id FROM {T_MRI}
);
"""


def build_insert_ct() -> str:
    """One synthetic LN row per qualifying legacy CT exam for patients missing from canonical."""
    return f"""
INSERT INTO {T_CT} (
  research_id, exam_id, exam_date, ln_index_within_exam, ln_id, source_modality,
  laterality, neck_level, neck_level_subdivision, region,
  size_short_mm, size_long_mm, size_max_mm, size_short_long_ratio,
  shape, echogenicity, hilum_preserved, cortex_thickness,
  necrosis_present, matting, conglomerate, calcifications, cystic_component,
  extranodal_extension, margins,
  suspicious_flag, suspicion_level, evidence_text, source_note_type, source_report_id,
  llm_model, confidence, extracted_at, nlp_backfill_pending,
  enhancement_pattern, density_hu, petct_fdg_avid, series_image_ref
)
WITH missing_pt AS (
  SELECT DISTINCT research_id
  FROM {LEGACY_CT}
  WHERE {CT_POSITIVE}
  AND research_id NOT IN (SELECT research_id FROM {T_CT})
),
src AS (
  SELECT c.*
  FROM {LEGACY_CT} c
  INNER JOIN missing_pt m USING (research_id)
  WHERE {CT_POSITIVE}
),
numbered AS (
  SELECT
    src.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        research_id,
        COALESCE(SAFE_CAST(date_of_exam AS DATE), DATE '1900-01-01'),
        COALESCE(ct_column, '')
      ORDER BY
        (CASE WHEN COALESCE(lymph_node_details, '') = '' THEN 0 ELSE 1 END) DESC,
        COALESCE(lymph_node_details, '')
    ) AS src_rn
  FROM src
)
SELECT
  research_id,
  CONCAT(
    'mig329|CT|', research_id, '|',
    COALESCE(FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_exam AS DATE)), 'undated'), '|',
    COALESCE(ct_column, ''), '|', CAST(src_rn AS STRING)
  ) AS exam_id,
  SAFE_CAST(date_of_exam AS DATE) AS exam_date,
  1 AS ln_index_within_exam,
  CONCAT(
    'mig329|CT|', research_id, '|',
    COALESCE(FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_exam AS DATE)), 'undated'), '|',
    COALESCE(ct_column, ''), '|', CAST(src_rn AS STRING), '|ln1'
  ) AS ln_id,
  'CT' AS source_modality,
  CAST(NULL AS STRING) AS laterality,
  CAST(NULL AS STRING) AS neck_level,
  CAST(NULL AS STRING) AS neck_level_subdivision,
  CAST(NULL AS STRING) AS region,
  largest_lymph_node_short_axis_mm AS size_short_mm,
  CAST(NULL AS FLOAT64) AS size_long_mm,
  largest_lymph_node_short_axis_mm AS size_max_mm,
  CAST(NULL AS FLOAT64) AS size_short_long_ratio,
  CAST(NULL AS STRING) AS shape,
  CAST(NULL AS STRING) AS echogenicity,
  CAST(NULL AS BOOL) AS hilum_preserved,
  CAST(NULL AS STRING) AS cortex_thickness,
  CAST(NULL AS BOOL) AS necrosis_present,
  CAST(NULL AS BOOL) AS matting,
  CAST(NULL AS BOOL) AS conglomerate,
  CAST(NULL AS STRING) AS calcifications,
  CAST(NULL AS BOOL) AS cystic_component,
  CAST(NULL AS BOOL) AS extranodal_extension,
  CAST(NULL AS STRING) AS margins,
  TRUE AS suspicious_flag,
  CASE
    WHEN LOWER(CAST(pathologic_lymph_nodes AS STRING)) = 'true' THEN 'suspicious'
    WHEN LOWER(CAST(lymph_nodes_suspicious AS STRING)) = 'true' THEN 'suspicious'
    ELSE 'indeterminate'
  END AS suspicion_level,
  'Structured LN fields positive (legacy snapshot); NLP enrichment pending' AS evidence_text,
  'legacy_structured_positive_backfill' AS source_note_type,
  CONCAT(
    'legacy|ct|', research_id, '|',
    COALESCE(FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_exam AS DATE)), 'undated'), '|',
    COALESCE(ct_column, '')
  ) AS source_report_id,
  'legacy_structured_mig329' AS llm_model,
  CASE
    WHEN LOWER(CAST(pathologic_lymph_nodes AS STRING)) = 'true'
      OR LOWER(CAST(lymph_nodes_suspicious AS STRING)) = 'true'
    THEN 0.55
    ELSE 0.45
  END AS confidence,
  CURRENT_TIMESTAMP() AS extracted_at,
  TRUE AS nlp_backfill_pending,
  CAST(NULL AS STRING) AS enhancement_pattern,
  CAST(NULL AS FLOAT64) AS density_hu,
  CAST(NULL AS BOOL) AS petct_fdg_avid,
  CAST(NULL AS STRING) AS series_image_ref
FROM numbered
;
"""


def build_insert_mri() -> str:
    return f"""
INSERT INTO {T_MRI} (
  research_id, exam_id, exam_date, ln_index_within_exam, ln_id, source_modality,
  laterality, neck_level, neck_level_subdivision, region,
  size_short_mm, size_long_mm, size_max_mm, size_short_long_ratio,
  shape, echogenicity, hilum_preserved, cortex_thickness,
  necrosis_present, matting, conglomerate, calcifications, cystic_component,
  extranodal_extension, margins,
  suspicious_flag, suspicion_level, evidence_text, source_note_type, source_report_id,
  llm_model, confidence, extracted_at, nlp_backfill_pending,
  t1_signal, t2_signal, stir_signal, dwi_restriction, gadolinium_enhancement
)
WITH missing_pt AS (
  SELECT DISTINCT research_id
  FROM {LEGACY_MRI}
  WHERE {MRI_POSITIVE}
  AND research_id NOT IN (SELECT research_id FROM {T_MRI})
),
src AS (
  SELECT mri.*
  FROM {LEGACY_MRI} mri
  INNER JOIN missing_pt m USING (research_id)
  WHERE {MRI_POSITIVE}
),
numbered AS (
  SELECT
    src.*,
    ROW_NUMBER() OVER (
      PARTITION BY
        research_id,
        COALESCE(SAFE_CAST(date_of_exam AS DATE), DATE '1900-01-01'),
        COALESCE(mri_label, '')
      ORDER BY
        (CASE WHEN COALESCE(lymph_node_details, '') = '' THEN 0 ELSE 1 END) DESC,
        COALESCE(lymph_node_details, '')
    ) AS src_rn
  FROM src
)
SELECT
  research_id,
  CONCAT(
    'mig329|MRI|', research_id, '|',
    COALESCE(FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_exam AS DATE)), 'undated'), '|',
    COALESCE(mri_label, ''), '|', CAST(src_rn AS STRING)
  ) AS exam_id,
  SAFE_CAST(date_of_exam AS DATE) AS exam_date,
  1 AS ln_index_within_exam,
  CONCAT(
    'mig329|MRI|', research_id, '|',
    COALESCE(FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_exam AS DATE)), 'undated'), '|',
    COALESCE(mri_label, ''), '|', CAST(src_rn AS STRING), '|ln1'
  ) AS ln_id,
  'MRI' AS source_modality,
  CAST(NULL AS STRING) AS laterality,
  CAST(NULL AS STRING) AS neck_level,
  CAST(NULL AS STRING) AS neck_level_subdivision,
  CAST(NULL AS STRING) AS region,
  CAST(NULL AS FLOAT64) AS size_short_mm,
  CAST(NULL AS FLOAT64) AS size_long_mm,
  CAST(NULL AS FLOAT64) AS size_max_mm,
  CAST(NULL AS FLOAT64) AS size_short_long_ratio,
  CAST(NULL AS STRING) AS shape,
  CAST(NULL AS STRING) AS echogenicity,
  CAST(NULL AS BOOL) AS hilum_preserved,
  CAST(NULL AS STRING) AS cortex_thickness,
  CAST(NULL AS BOOL) AS necrosis_present,
  CAST(NULL AS BOOL) AS matting,
  CAST(NULL AS BOOL) AS conglomerate,
  CAST(NULL AS STRING) AS calcifications,
  CAST(NULL AS BOOL) AS cystic_component,
  CAST(NULL AS BOOL) AS extranodal_extension,
  CAST(NULL AS STRING) AS margins,
  TRUE AS suspicious_flag,
  CASE
    WHEN LOWER(CAST(pathologic_lymph_nodes AS STRING)) = 'true' THEN 'suspicious'
    ELSE 'indeterminate'
  END AS suspicion_level,
  'Structured LN locations-with-size present (legacy snapshot); NLP enrichment pending' AS evidence_text,
  'legacy_structured_positive_backfill' AS source_note_type,
  CONCAT(
    'legacy|mri|', research_id, '|',
    COALESCE(FORMAT_DATE('%Y-%m-%d', SAFE_CAST(date_of_exam AS DATE)), 'undated'), '|',
    COALESCE(mri_label, '')
  ) AS source_report_id,
  'legacy_structured_mig329' AS llm_model,
  CASE
    WHEN LOWER(CAST(pathologic_lymph_nodes AS STRING)) = 'true' THEN 0.55
    ELSE 0.45
  END AS confidence,
  CURRENT_TIMESTAMP() AS extracted_at,
  TRUE AS nlp_backfill_pending,
  CAST(NULL AS STRING) AS t1_signal,
  CAST(NULL AS STRING) AS t2_signal,
  CAST(NULL AS STRING) AS stir_signal,
  CAST(NULL AS BOOL) AS dwi_restriction,
  CAST(NULL AS STRING) AS gadolinium_enhancement
FROM numbered
;
"""


def count_backfill_rows(client, modality: str) -> int:
    if modality == "CT":
        inner = f"""
WITH missing_pt AS (
  SELECT DISTINCT research_id
  FROM {LEGACY_CT}
  WHERE {CT_POSITIVE}
  AND research_id NOT IN (SELECT research_id FROM {T_CT})
)
SELECT COUNT(*) AS n
FROM {LEGACY_CT} c
INNER JOIN missing_pt m USING (research_id)
WHERE {CT_POSITIVE}
"""
    else:
        inner = f"""
WITH missing_pt AS (
  SELECT DISTINCT research_id
  FROM {LEGACY_MRI}
  WHERE {MRI_POSITIVE}
  AND research_id NOT IN (SELECT research_id FROM {T_MRI})
)
SELECT COUNT(*) AS n
FROM {LEGACY_MRI} c
INNER JOIN missing_pt m USING (research_id)
WHERE {MRI_POSITIVE}
"""
    return int(_scalar(client, inner))


def recreate_unified_view(client) -> None:
    t = client.get_table(f"{BQ_PROJECT}.{CANON_DS}.canonical_lymph_node_unified_VIEW_v1")
    if not getattr(t, "view_query", None):
        raise SystemExit("Could not read view_query for unified LN view")
    sql = (
        f"CREATE OR REPLACE VIEW `{BQ_PROJECT}.{CANON_DS}.canonical_lymph_node_unified_VIEW_v1` "
        f"AS {t.view_query}"
    )
    _run(client, sql, "CREATE OR REPLACE canonical_lymph_node_unified_VIEW_v1 (same SQL)")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Run snapshots, INSERTs, view recreate")
    ap.add_argument("--dry-run", action="store_true", help="Validation + backfill counts only")
    ap.add_argument(
        "--skip-snapshot",
        action="store_true",
        help="Skip archive snapshots (not recommended for --apply)",
    )
    ap.add_argument(
        "--skip-view-recreate",
        action="store_true",
        help="Skip CREATE OR REPLACE VIEW unified (view still reflects new rows if not skipped)",
    )
    args = ap.parse_args()

    if not args.apply and not args.dry_run:
        args.dry_run = True

    client = _client()
    report: dict = {
        "script": SCRIPT_TAG,
        "project": BQ_PROJECT,
        "dry_run": bool(args.dry_run),
        "apply": bool(args.apply),
    }

    ct_miss = _scalar(client, validation_sql_ct())
    mri_miss = _scalar(client, validation_sql_mri())
    n_ct = count_backfill_rows(client, "CT")
    n_mri = count_backfill_rows(client, "MRI")
    vu_before = _scalar(client, f"SELECT COUNT(*) FROM {V_UNIFIED}")

    print(f"[{SCRIPT_TAG}] Before: CT missing pts={ct_miss}, MRI missing pts={mri_miss}")
    print(f"  Backfill row counts: CT={n_ct}, MRI={n_mri}, unified_rows_before={vu_before}")

    report["before"] = {
        "ct_positive_pts_missing": int(ct_miss),
        "mri_positive_pts_missing": int(mri_miss),
        "n_rows_to_insert_ct": n_ct,
        "n_rows_to_insert_mri": n_mri,
        "unified_view_rows": int(vu_before),
    }

    if args.dry_run and not args.apply:
        out = OUT_DIR / f"mig_329_ln_backfill_preview_{RUN_stamp}.json"
        out.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(f"  Wrote {out}")
        return 0

    if not args.apply:
        return 0

    if not args.skip_snapshot:
        arch_ct = f"`{BQ_PROJECT}.{ARCHIVE_DS}.{ARCH_CT}`"
        arch_mri = f"`{BQ_PROJECT}.{ARCHIVE_DS}.{ARCH_MRI}`"
        _run(
            client,
            f"CREATE OR REPLACE TABLE {arch_ct} AS SELECT * FROM {T_CT}",
            f"snapshot {ARCH_CT}",
        )
        _run(
            client,
            f"CREATE OR REPLACE TABLE {arch_mri} AS SELECT * FROM {T_MRI}",
            f"snapshot {ARCH_MRI}",
        )
        report["snapshots"] = [ARCH_CT, ARCH_MRI]

    if n_ct:
        _run(client, build_insert_ct(), f"INSERT CT ({n_ct} rows)")
    if n_mri:
        _run(client, build_insert_mri(), f"INSERT MRI ({n_mri} rows)")

    if not args.skip_view_recreate:
        recreate_unified_view(client)

    ct_after = _scalar(client, validation_sql_ct())
    mri_after = _scalar(client, validation_sql_mri())
    vu_after = _scalar(client, f"SELECT COUNT(*) FROM {V_UNIFIED}")

    report["after"] = {
        "ct_positive_pts_missing": int(ct_after),
        "mri_positive_pts_missing": int(mri_after),
        "unified_view_rows": int(vu_after),
    }
    delta = int(vu_after) - int(vu_before)
    inserts_total = int(n_ct) + int(n_mri)
    report["unified_view_delta"] = delta
    report["expected_delta"] = inserts_total

    out = OUT_DIR / f"mig_329_ln_backfill_apply_{RUN_stamp}.json"
    out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"[{SCRIPT_TAG}] After: CT missing pts={ct_after}, MRI missing pts={mri_after}")
    print(f"  unified_rows after={vu_after}  delta={delta} (expected +{inserts_total})")
    print(f"  Wrote {out}")

    if ct_after != 0 or mri_after != 0:
        print("ERROR: validation queries did not reach 0 missing patients.", file=sys.stderr)
        return 1
    if delta != inserts_total:
        print(
            f"ERROR: unified view delta {delta} != inserts {inserts_total}",
            file=sys.stderr,
        )
        return 1

    print(f"[{SCRIPT_TAG}] OK — validations passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
