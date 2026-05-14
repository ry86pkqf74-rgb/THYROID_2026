#!/usr/bin/env python3
"""
mig_325: reported_text guard cleanup — parser hallucination ThyroSeq rows (decision matrix).

Source: studies/proposal_2to4cm_extent_molecular_20260326/elicit_expansion_20260509/
        guard_rows_16_decision_matrix.md + .csv

Pre-mig_323, 16 rows were blocked from platform reclassification because
band_backfill_source = 'reported_text'. This pass:
  - Supersedes 13 fabricated ThyroSeq rows (Afirma is the real test in chart).
  - Downgrades 5724 (cancelled ThyroSeq + non-result Afirma) on BOTH rows.
  - Reclasses 11156 ThyroSeq → platform Other (Quest panel; no ThyroSeq band).
  - Leaves 8729 unchanged (genuine dual-platform).
  - Repairs Afirma overall_result_class where the matrix specifies.

Hard rules:
  - Snapshot pub_archive before MERGE/UPDATE.
  - No PHI in logs (research_id integers only).
  - Append MFL/DFL per thyroid-integration BEFORE production --apply when required.

Usage:
  .venv/bin/python scripts/mig_325_reported_text_guard_cleanup_bq.py --dry-run
  .venv/bin/python scripts/mig_325_reported_text_guard_cleanup_bq.py --apply

Environment:
  GOOGLE_APPLICATION_CREDENTIALS or gcloud application-default credentials
"""
from __future__ import annotations

import argparse
import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_OUTPUT = REPO_ROOT / "scripts" / "output"
sys.path.insert(0, str(REPO_ROOT))

BQ_PROJECT_DEFAULT = "thyroid-canonical-pub-2026"
CANONICAL_DATASET = "pub_canonical"
ARCHIVE_DATASET = "pub_archive"
CMG_TABLE = "canonical_molecular_genetics_v2"

RUN_DATE = datetime.now(tz=timezone.utc).strftime("%Y%m%d")
RUN_ID = f"mig_325_{RUN_DATE}_{uuid.uuid4().hex[:8]}"
ARCHIVE_TABLE = f"canonical_molecular_genetics_v2_pre_guard_cleanup_{RUN_DATE}"

# ThyroSeq fabricated rows → supersede (13 rids); excludes 5724, 8729, 11156.
SUPERSEDE_TYROSEQ_RIDS = (
    5999, 7012, 8218, 8233, 9154, 9991, 10174, 10237, 10699, 10926, 10939, 11039, 11087
)

AFIRMA_OTHER_TO_NEGATIVE_RIDS = (8233, 10174, 10699, 10926, 10939)

# Expected missing Afirma companion (verified 2026-05-13 pre-apply); still supersede TS per matrix.
ORPHAN_SUPERSEDE_EXPECTED = frozenset({8218, 9154})


def _bq(project: str):
    from google.cloud import bigquery

    return bigquery.Client(project=project)


def _snapshot(client, project: str, dry_run: bool) -> None:
    sql = f"""
CREATE OR REPLACE TABLE `{project}.{ARCHIVE_DATASET}.{ARCHIVE_TABLE}`
AS SELECT * FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
"""
    if dry_run:
        print(f"  [DRY-RUN] Would snapshot → {ARCHIVE_DATASET}.{ARCHIVE_TABLE}")
        return
    client.query(sql).result()
    print(f"  ✓ Snapshot → {ARCHIVE_DATASET}.{ARCHIVE_TABLE}")


def _add_column(client, project: str, dry_run: bool) -> None:
    sql = (
        f"ALTER TABLE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` "
        "ADD COLUMN IF NOT EXISTS platform_reclass_status STRING"
    )
    if dry_run:
        print(f"  [DRY-RUN] {sql[:90]}…")
        return
    client.query(sql).result()
    print("  ✓ Column platform_reclass_status present")


def _apply_updates(client, project: str, dry_run: bool) -> None:
    rid_list = ",".join(str(x) for x in SUPERSEDE_TYROSEQ_RIDS)
    afirma_neg_list = ",".join(str(x) for x in AFIRMA_OTHER_TO_NEGATIVE_RIDS)
    ts = datetime.now(tz=timezone.utc).isoformat()

    batches = [
        (
            "ThyroSeq supersede (13 fabricated rows)",
            f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` t
SET
  t.rom_descriptor = NULL,
  t.overall_result_class = 'superseded',
  t.platform_reclass_status = 'superseded_by_afirma_row',
  t.platform_reclass_applied_at = TIMESTAMP('{ts}'),
  t.platform_reclass_run_id = @run_id,
  t.platform_reclass_source = 'mig_325_guard_cleanup'
WHERE t.platform = 'ThyroSeq'
  AND t.band_backfill_source = 'reported_text'
  AND CAST(t.research_id AS INT64) IN ({rid_list})
""",
        ),
        (
            "5724 both rows — non_diagnostic_cancelled",
            f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` t
SET
  t.rom_descriptor = NULL,
  t.overall_result_class = 'non_diagnostic',
  t.platform_reclass_status = 'non_diagnostic_cancelled',
  t.platform_reclass_applied_at = TIMESTAMP('{ts}'),
  t.platform_reclass_run_id = @run_id,
  t.platform_reclass_source = 'mig_325_guard_cleanup'
WHERE CAST(t.research_id AS INT64) = 5724
""",
        ),
        (
            "11156 — Quest in-house panel (platform Other, drop band)",
            f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` t
SET
  t.platform = 'Other',
  t.rom_descriptor = NULL,
  t.platform_reclass_status = 'quest_in_house_panel',
  t.platform_reclass_applied_at = TIMESTAMP('{ts}'),
  t.platform_reclass_run_id = @run_id,
  t.platform_reclass_source = 'quest_diagnostics_in_house_panel'
WHERE CAST(t.research_id AS INT64) = 11156
  AND t.platform = 'ThyroSeq'
""",
        ),
        (
            "Afirma other → negative (5 rows)",
            f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` t
SET
  t.overall_result_class = 'negative',
  t.band_backfill_source = 'afirma_result_field',
  t.platform_reclass_applied_at = TIMESTAMP('{ts}'),
  t.platform_reclass_run_id = @run_id,
  t.platform_reclass_source = 'mig_325_afirma_orc_repair'
WHERE t.platform = 'Afirma'
  AND CAST(t.research_id AS INT64) IN ({afirma_neg_list})
  AND t.overall_result_class = 'other'
""",
        ),
        (
            "9991 — all Afirma rows non_diagnostic",
            f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` t
SET
  t.overall_result_class = 'non_diagnostic',
  t.platform_reclass_applied_at = TIMESTAMP('{ts}'),
  t.platform_reclass_run_id = @run_id,
  t.platform_reclass_source = 'mig_325_afirma_orc_repair'
WHERE t.platform = 'Afirma'
  AND CAST(t.research_id AS INT64) = 9991
  AND t.overall_result_class = 'other'
""",
        ),
        (
            "8218 — Afirma suspicious (narrative correction; only if row exists)",
            f"""
UPDATE `{project}.{CANONICAL_DATASET}.{CMG_TABLE}` t
SET
  t.overall_result_class = 'suspicious',
  t.platform_reclass_applied_at = TIMESTAMP('{ts}'),
  t.platform_reclass_run_id = @run_id,
  t.platform_reclass_source = 'mig_325_afirma_orc_repair'
WHERE t.platform = 'Afirma'
  AND CAST(t.research_id AS INT64) = 8218
  AND t.overall_result_class = 'negative'
""",
        ),
    ]

    for label, sql in batches:
        if dry_run:
            print(f"  [DRY-RUN] {label}")
            continue
        from google.cloud.bigquery import QueryJobConfig, ScalarQueryParameter

        cfg = QueryJobConfig(
            query_parameters=[ScalarQueryParameter("run_id", "STRING", RUN_ID)]
        )
        job = client.query(sql, job_config=cfg)
        job.result()
        n = job.num_dml_affected_rows if job.num_dml_affected_rows is not None else -1
        print(f"  ✓ {label} (dml_rows={n})")


def _verify(client, project: str) -> dict:
    rid_all = tuple(sorted(set(SUPERSEDE_TYROSEQ_RIDS) | {5724, 8729, 11156}))

    def _pull(q: str) -> list[dict]:
        return [dict(r) for r in client.query(q).result()]

    q_state = f"""
SELECT CAST(research_id AS INT64) rid, platform, overall_result_class, rom_descriptor,
       platform_reclass_status, band_backfill_source
FROM `{project}.{CANONICAL_DATASET}.{CMG_TABLE}`
WHERE CAST(research_id AS INT64) IN {rid_all}
ORDER BY rid, platform
"""
    rows = _pull(q_state)

    checks = {
        "run_id": RUN_ID,
        "n_thyroseq_superseded": len(
            [
                r
                for r in rows
                if r["platform"] == "ThyroSeq"
                and r["platform_reclass_status"] == "superseded_by_afirma_row"
            ]
        ),
        "n_5724_non_diag": len(
            [
                r
                for r in rows
                if r["rid"] == 5724
                and r["overall_result_class"] == "non_diagnostic"
                and r["platform_reclass_status"] == "non_diagnostic_cancelled"
            ]
        ),
        "n_11156_other": len(
            [
                r
                for r in rows
                if r["rid"] == 11156 and r["platform"] == "Other"
            ]
        ),
        "8729_thyroseq_unchanged": next(
            (
                r
                for r in rows
                if r["rid"] == 8729 and r["platform"] == "ThyroSeq"
            ),
            None,
        ),
        "afirma_negative_repairs": len(
            [
                r
                for r in rows
                if r["platform"] == "Afirma"
                and r["rid"] in AFIRMA_OTHER_TO_NEGATIVE_RIDS
                and r["overall_result_class"] == "negative"
            ]
        ),
    }

    orphans = []
    for rid in SUPERSEDE_TYROSEQ_RIDS:
        pr = [r for r in rows if r["rid"] == rid]
        ts_sup = [r for r in pr if r["platform"] == "ThyroSeq" and r["overall_result_class"] == "superseded"]
        af = [r for r in pr if r["platform"] == "Afirma"]
        if ts_sup and not af:
            orphans.append(rid)

    checks["supersede_without_afirma_row"] = orphans
    checks["expected_orphan_warnings"] = sorted(ORPHAN_SUPERSEDE_EXPECTED)
    checks["detail_rows"] = rows
    return checks


def main() -> int:
    ap = argparse.ArgumentParser(description="mig_325 guard cleanup")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--verify-only", action="store_true")
    ap.add_argument("--project", default=BQ_PROJECT_DEFAULT)
    args = ap.parse_args()

    if not any([args.dry_run, args.apply, args.verify_only]):
        print("Specify --dry-run, --apply, or --verify-only")
        return 1

    client = _bq(args.project)
    dry = args.dry_run

    print(f"mig_325 run_id={RUN_ID} project={args.project}")

    if args.verify_only:
        v = _verify(client, args.project)
        slim = {k: v[k] for k in v if k != "detail_rows"}
        print(json.dumps(slim, indent=2))
        return 0

    _snapshot(client, args.project, dry)
    _add_column(client, args.project, dry)
    print("\nApplying updates…")
    _apply_updates(client, args.project, dry)

    if not dry:
        print("\nVerification…")
        v = _verify(client, args.project)
        SCRIPTS_OUTPUT.mkdir(parents=True, exist_ok=True)
        out_path = SCRIPTS_OUTPUT / f"mig_325_verification_{RUN_DATE}.json"
        out_path.write_text(json.dumps(v, indent=2, default=str))
        print(f"  Wrote {out_path}")
        print(
            "  ThyroSeq superseded count:",
            v["n_thyroseq_superseded"],
            "(expect 13)",
        )
        print("  5724 non_diagnostic rows:", v["n_5724_non_diag"], "(expect 2)")
        print("  11156 platform Other:", v["n_11156_other"], "(expect 1)")
        af = v["8729_thyroseq_unchanged"]
        ok_8729 = af and af.get("rom_descriptor") == "INTERMEDIATE-HIGH"
        print("  8729 ThyroSeq preserved:", ok_8729)
        print("  Afirma former-other now negative:", v["afirma_negative_repairs"], "(expect 5)")
        ow = v["supersede_without_afirma_row"]
        if ow:
            print("  NOTE: superseded ThyroSeq with no Afirma CMG row:", ow)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
