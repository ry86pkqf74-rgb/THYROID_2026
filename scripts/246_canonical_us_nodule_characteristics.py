#!/usr/bin:env python3
"""
Script 246 — Build canonical_us_nodule_characteristics_v1 (per-(exam, nodule) canonical)

Date:    2026-04-16
Author:  THYROID_2026 canonical-finalization run (v1_0 lock)

Architecture
============
Pre-flight surfaced 5 sources at 3 different grains. Build identity from
imaging_nodule_master_v1 (the only true per-(exam, nodule) source, 6,126
pts / 37,016 rows) with tirads_llm_extracted_v2 (1,429 pts) overlay for
ACR per-component scoring. Source-coverage decisions:

  - imaging_nodule_master_v1   →  identity + size + location + structural TI-RADS
  - tirads_llm_extracted_v2    →  per-component points (composition_pts, etc.)
                                  joined via (research_id, exam_date, nodule_number)
                                  parsed from tirads_llm.deterministic_key
  - us_nodules_tirads          →  NOT merged. Per-patient wide format with
                                  free-text nodule_N descriptions; cross-source
                                  discordance captured separately as
                                  us_nodules_tirads_vs_inm_v1_discordance_v1.
  - imaging_nodule_long_v2     →  archived + dropped (TI-RADS components 100%
                                  NULL; subset of inm_v1 by patients;
                                  size_cm_max duplicative with
                                  inm_v1.max_dimension_cm).
  - extracted_tirads_validated_v1 → patient-level rollup; preserved as-is.

The 4,736 patients in us_nodules_tirads but NOT in inm_v1 were verified
(0% n*_tr scores, 0% nodule_N text, 0% us_1_date) to be empty
placeholder rows — included in the source for cohort-shape only.
Documented as a v1_1 NLP-extraction TODO.

Tables READ
-----------
  thyroid_canonical_publication_v1_0.main.imaging_nodule_master_v1
  thyroid_canonical_publication_v1_0.main.tirads_llm_extracted_v2
  thyroid_canonical_publication_v1_0.main.us_nodules_tirads
  thyroid_canonical_publication_v1_0.main.imaging_nodule_long_v2 (archive then drop)
  thyroid_canonical_publication_v1_0.main.canonical_patient_master
  thyroid_canonical_publication_v1_0.manuscript_workspace.detail_table_registry_v1

Tables WRITTEN
--------------
  CREATE TABLE  main.canonical_us_nodule_characteristics_v1                   (NEW)
  CREATE TABLE  main.us_nodules_tirads_vs_inm_v1_discordance_v1               (NEW audit)
  ARCHIVE       "Thyroid 2026 UPdated".archive_pub_v1_0.imaging_nodule_long_v2_<ts>
  DROP TABLE    main.imaging_nodule_long_v2
  UPDATE        manuscript_workspace.detail_table_registry_v1 (add 2, remove 1)
  Decision log: scripts/output/246_decision_log.json

NOT done in this script (deferred to v1_1):
  - Migrating imaging_patient_summary_v1 source query to read from
    canonical_us_nodule_characteristics_v1 (would change CPM rollup
    cols; needs validation pass first; same pattern as Script 245).
  - NLP extraction from us_nodules_tirads.nodule_N text columns to
    cover the 4,745 placeholder-only patients.

Rollback plan
-------------
  1. DROP TABLE canonical_us_nodule_characteristics_v1
  2. DROP TABLE us_nodules_tirads_vs_inm_v1_discordance_v1
  3. CREATE OR REPLACE TABLE main.imaging_nodule_long_v2 AS
       SELECT * FROM "Thyroid 2026 UPdated".archive_pub_v1_0.imaging_nodule_long_v2_<ts>
  4. DELETE / re-INSERT registry entries from previous state.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from _md_connect import connect_locked, PUBLICATION_DB  # noqa: E402

REPO = Path(__file__).resolve().parent.parent
OUTPUT_DIR = REPO / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DECISION_LOG_PATH = OUTPUT_DIR / "246_decision_log.json"

ARCHIVE_DB = "Thyroid 2026 UPdated"
ARCHIVE_SCHEMA = "archive_pub_v1_0"
ARCHIVE_QUALIFIED = f'"{ARCHIVE_DB}"."{ARCHIVE_SCHEMA}"'
SCRIPT_TAG = "Script 246"
RUN_DATE = "2026-04-16"


def ts_utc() -> str:
    return datetime.utcnow().strftime("%H:%M:%S.") + f"{datetime.utcnow().microsecond // 1000:03d}Z"


def log(msg: str) -> None:
    print(f"[{ts_utc()}] {msg}", flush=True)


class DecisionLog:
    def __init__(self, script: str, run_ts: str) -> None:
        self.payload: dict = {"script": script, "run_ts": run_ts, "run_date": RUN_DATE, "decisions": []}

    def add(self, category: str, **kw) -> None:
        entry = {"category": category, **kw}
        self.payload["decisions"].append(entry)
        log(f"  decision[{category}]: {json.dumps({k: v for k, v in kw.items() if k != 'rows_sample'}, default=str)}")

    def dump(self, path: Path) -> None:
        with path.open("w") as f:
            json.dump(self.payload, f, indent=2, default=str)
        log(f"  wrote decision log -> {path.name} ({len(self.payload['decisions'])} entries)")


# ---------------------------------------------------------------------------
# Build SQL
# ---------------------------------------------------------------------------

BUILD_CANONICAL_SQL = """
CREATE OR REPLACE TABLE canonical_us_nodule_characteristics_v1 AS
WITH llm AS (
  -- Parse tirads_llm.deterministic_key (format: <rid>|<date>|<nodule_number>)
  -- and dedupe to a single row per (rid, date, nodule_number) to avoid
  -- broadcast-join inflation. Use the row with the most components scored.
  SELECT
    research_id,
    TRY_CAST(SPLIT_PART(deterministic_key, '|', 2) AS DATE) AS llm_exam_date,
    CAST(nodule_number AS INTEGER)                          AS llm_nodule_number,
    composition_pts, echogenicity_pts, shape_pts, margin_pts, foci_pts,
    total_pts_2017, tirads_level_2017,
    total_pts_modified, tirads_level_modified,
    extracted_size_cm, extracted_location, echogenic_foci,
    n_categories_scored
  FROM (
    SELECT *,
      ROW_NUMBER() OVER (
        PARTITION BY research_id, TRY_CAST(SPLIT_PART(deterministic_key, '|', 2) AS DATE), nodule_number
        ORDER BY n_categories_scored DESC NULLS LAST, total_pts_2017 DESC NULLS LAST
      ) AS rn
    FROM tirads_llm_extracted_v2
  )
  WHERE rn = 1
)
SELECT
  -- ── identity ────────────────────────────────────────────────────────
  CAST(i.research_id AS INTEGER)                                           AS research_id,
  i.exam_id                                                                AS us_exam_id,
  i.exam_date                                                              AS exam_date,
  i.nodule_number                                                          AS nodule_index_within_exam,
  i.nodule_id                                                              AS nodule_id,
  -- ── location ────────────────────────────────────────────────────────
  COALESCE(i.laterality, l.extracted_location)                             AS laterality,
  i.location_raw                                                           AS location_raw,
  l.extracted_location                                                     AS location_detail,
  -- ── size (inm_v1 wins; tirads_llm fallback) ────────────────────────
  i.max_dimension_cm                                                       AS size_cm_max,
  i.length_mm                                                              AS length_mm,
  i.width_mm                                                               AS width_mm,
  i.height_mm                                                              AS height_mm,
  i.volume_ml                                                              AS volume_ml,
  l.extracted_size_cm                                                      AS extracted_size_cm,
  -- ── TI-RADS components (inm_v1 wins; tirads_llm fallback for echogenic_foci) ─
  i.composition                                                            AS composition,
  i.echogenicity                                                           AS echogenicity,
  i.shape                                                                  AS shape,
  i.margins                                                                AS margins,
  i.calcifications                                                         AS calcifications,
  l.echogenic_foci                                                         AS echogenic_foci,
  -- ── TI-RADS component points (tirads_llm only) ─────────────────────
  l.composition_pts                                                        AS composition_pts,
  l.echogenicity_pts                                                       AS echogenicity_pts,
  l.shape_pts                                                              AS shape_pts,
  l.margin_pts                                                             AS margin_pts,
  l.foci_pts                                                               AS foci_pts,
  -- ── TI-RADS scores ──────────────────────────────────────────────────
  i.tirads_reported                                                        AS tirads_reported,
  i.tirads_acr_recalculated                                                AS tirads_acr_recalculated,
  l.total_pts_2017                                                         AS tirads_score_2017,
  l.tirads_level_2017                                                      AS tirads_level_2017,
  i.tirads_category                                                        AS tirads_category,
  l.tirads_level_modified                                                  AS tirads_category_modified,
  i.tirads_concordant_flag                                                 AS tirads_concordant_flag,
  -- ── dynamics ────────────────────────────────────────────────────────
  i.suspicious_flag                                                        AS suspicious_flag,
  -- ── provenance ──────────────────────────────────────────────────────
  CASE
    WHEN l.research_id IS NOT NULL THEN 'imaging_nodule_master_v1|tirads_llm_extracted_v2'
    ELSE 'imaging_nodule_master_v1'
  END                                                                      AS source_tables,
  CASE WHEN l.research_id IS NOT NULL THEN 'inm_v1+llm' ELSE 'inm_v1_only' END AS resolution_rule,
  ROUND(100.0 * (
      (CASE WHEN i.max_dimension_cm IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.composition IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.echogenicity IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.shape IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.margins IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.tirads_acr_recalculated IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.location_raw IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN i.laterality IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN l.total_pts_2017 IS NOT NULL THEN 1 ELSE 0 END) +
      (CASE WHEN l.composition_pts IS NOT NULL THEN 1 ELSE 0 END)
    ) / 10.0, 1)                                                           AS data_completeness_pct
FROM imaging_nodule_master_v1 i
LEFT JOIN llm l
  ON l.research_id = i.research_id
 AND l.llm_exam_date = i.exam_date
 AND l.llm_nodule_number = i.nodule_number
"""

BUILD_DISCORDANCE_SQL = """
CREATE OR REPLACE TABLE us_nodules_tirads_vs_inm_v1_discordance_v1 AS
WITH unt_max AS (
  SELECT
    CAST(u.research_id AS INTEGER) AS research_id,
    GREATEST(
      TRY_CAST(u.n1_tr  AS INTEGER), TRY_CAST(u.n2_tr  AS INTEGER), TRY_CAST(u.n3_tr  AS INTEGER),
      TRY_CAST(u.n4_tr  AS INTEGER), TRY_CAST(u.n5_tr  AS INTEGER), TRY_CAST(u.n6_tr  AS INTEGER),
      TRY_CAST(u.n7_tr  AS INTEGER), TRY_CAST(u.n8_tr  AS INTEGER), TRY_CAST(u.n9_tr  AS INTEGER),
      TRY_CAST(u.n10_tr AS INTEGER), TRY_CAST(u.n11_tr AS INTEGER), TRY_CAST(u.n12_tr AS INTEGER),
      TRY_CAST(u.n13_tr AS INTEGER), TRY_CAST(u.n14_tr AS INTEGER)
    ) AS unt_max_tr,
    u.us_1_date AS unt_first_us_date
  FROM us_nodules_tirads u
),
inm_max AS (
  SELECT
    CAST(research_id AS INTEGER) AS research_id,
    MAX(tirads_acr_recalculated) AS inm_max_tr,
    MIN(exam_date) AS inm_first_exam_date,
    MAX(exam_date) AS inm_last_exam_date,
    COUNT(DISTINCT exam_date) AS inm_n_exams
  FROM imaging_nodule_master_v1
  WHERE tirads_acr_recalculated IS NOT NULL GROUP BY 1
)
SELECT
  u.research_id,
  u.unt_max_tr,
  i.inm_max_tr,
  ABS(u.unt_max_tr - i.inm_max_tr) AS abs_diff,
  CASE WHEN u.unt_max_tr > i.inm_max_tr THEN 'unt_higher'
       WHEN u.unt_max_tr < i.inm_max_tr THEN 'inm_higher'
       ELSE 'concordant' END AS direction,
  u.unt_first_us_date,
  i.inm_first_exam_date,
  i.inm_last_exam_date,
  i.inm_n_exams,
  CASE WHEN ABS(u.unt_max_tr - i.inm_max_tr) >= 2 THEN 'HIGH' ELSE 'MEDIUM' END AS review_priority
FROM unt_max u JOIN inm_max i USING (research_id)
WHERE u.unt_max_tr IS NOT NULL AND i.inm_max_tr IS NOT NULL
  AND u.unt_max_tr <> i.inm_max_tr
"""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    t0 = time.time()
    log(f"=== START {Path(__file__).name}")
    con = connect_locked()
    log(f"connected to {PUBLICATION_DB}")

    run_ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    dlog = DecisionLog(script="246", run_ts=run_ts)

    # --- PHASE 0: baseline + decision log ───────────────────────────────
    log("PHASE 0 — baseline capture")
    pf = con.execute(
        """SELECT
             (SELECT COUNT(*) FROM imaging_nodule_master_v1) AS inm_rows,
             (SELECT COUNT(DISTINCT research_id) FROM imaging_nodule_master_v1) AS inm_pts,
             (SELECT COUNT(*) FROM tirads_llm_extracted_v2) AS llm_rows,
             (SELECT COUNT(DISTINCT research_id) FROM tirads_llm_extracted_v2) AS llm_pts,
             (SELECT COUNT(*) FROM imaging_nodule_long_v2) AS inl_rows,
             (SELECT COUNT(*) FROM us_nodules_tirads) AS unt_rows,
             (SELECT COUNT(*) FROM canonical_patient_master) AS cpm_rows
         """
    ).fetchone()
    dlog.add(
        "preflight_baseline",
        imaging_nodule_master_v1={"rows": pf[0], "patients": pf[1]},
        tirads_llm_extracted_v2={"rows": pf[2], "patients": pf[3]},
        imaging_nodule_long_v2={"rows": pf[4]},
        us_nodules_tirads={"rows": pf[5]},
        canonical_patient_master={"rows": pf[6]},
    )
    dlog.add(
        "source_grain_finding",
        finding="us_nodules_tirads is per-patient wide (1 row/pt × 14 nodule slots). Only imaging_nodule_master_v1 is true per-(exam, nodule) grain. The 4,736 patients in us_nodules_tirads but NOT in inm_v1 are confirmed empty placeholder rows (0% n*_tr scores, 0% nodule_N text, 0% us_1_date).",
        consequence="Build canonical_us_nodule_characteristics_v1 from inm_v1 only (6,126 pts / 37,016 rows). The 4,736 placeholder pts are documented as a v1_1 NLP-extraction TODO.",
    )
    dlog.add(
        "inl_v2_decision",
        action="archive + drop",
        rationale="100% NULL TI-RADS components (composition, echogenicity, shape, margins, calcifications) — its stated purpose. Patient set (3,439) is a strict subset of inm_v1 (6,126). size_cm_max is duplicative with inm_v1.max_dimension_cm. The imaging_exam_id column is a per-patient sequence, not a global ID — adds no unique value.",
    )

    if args.dry_run:
        log("--dry-run mode: skipping write phases")
        dlog.dump(DECISION_LOG_PATH)
        return

    # --- PHASE 1: archive imaging_nodule_long_v2 ──────────────────────
    log("PHASE 1 — archive + drop imaging_nodule_long_v2")
    archive_name = f"imaging_nodule_long_v2_pre246_backup_{run_ts}"
    full_dest = f'{ARCHIVE_QUALIFIED}."{archive_name}"'
    log(f"  archiving -> {full_dest}")
    src_rc = con.execute("SELECT COUNT(*) FROM imaging_nodule_long_v2").fetchone()[0]
    con.execute(f"CREATE OR REPLACE TABLE {full_dest} AS SELECT * FROM imaging_nodule_long_v2")
    dst_rc = con.execute(f"SELECT COUNT(*) FROM {full_dest}").fetchone()[0]
    if src_rc != dst_rc:
        raise RuntimeError(f"archive row mismatch src={src_rc} dst={dst_rc}")
    con.execute(
        f"""COMMENT ON TABLE {full_dest} IS
            '{SCRIPT_TAG} ({RUN_DATE}): pre-DROP archive of imaging_nodule_long_v2.
            Dropped from canonical because: TI-RADS components 100% NULL (table''s stated
            purpose); patient set is a strict subset of imaging_nodule_master_v1 (3,439 vs
            6,126); size_cm_max duplicative with inm_v1.max_dimension_cm; imaging_exam_id
            is a per-patient sequence number, not a global identifier. Use
            canonical_us_nodule_characteristics_v1 as the canonical replacement.'"""
    )
    log(f"  archived {src_rc} rows; dropping live table")
    con.execute("DROP TABLE imaging_nodule_long_v2")

    # --- PHASE 2: build canonical_us_nodule_characteristics_v1 ───────
    log("PHASE 2 — build canonical_us_nodule_characteristics_v1")
    t1 = time.time()
    con.execute(BUILD_CANONICAL_SQL)
    log(f"  built in {time.time()-t1:.1f}s")
    n_rows = con.execute("SELECT COUNT(*) FROM canonical_us_nodule_characteristics_v1").fetchone()[0]
    n_pts = con.execute("SELECT COUNT(DISTINCT research_id) FROM canonical_us_nodule_characteristics_v1").fetchone()[0]
    n_with_llm = con.execute(
        "SELECT COUNT(*) FROM canonical_us_nodule_characteristics_v1 WHERE source_tables LIKE '%tirads_llm%'"
    ).fetchone()[0]
    avg_complete = con.execute(
        "SELECT AVG(data_completeness_pct) FROM canonical_us_nodule_characteristics_v1"
    ).fetchone()[0]
    log(f"  rows={n_rows} pts={n_pts} with_llm_overlay={n_with_llm} ({100*n_with_llm/max(n_rows,1):.1f}%) avg_complete={avg_complete:.1f}%")
    dlog.add(
        "build_summary_canonical",
        rows=n_rows, patients=n_pts,
        rows_with_llm_overlay=n_with_llm,
        rows_with_llm_overlay_pct=round(100 * n_with_llm / max(n_rows, 1), 1),
        avg_data_completeness_pct=round(avg_complete or 0.0, 1),
    )

    # --- PHASE 3: COMMENTs ────────────────────────────────────────────
    log("PHASE 3 — annotate canonical")
    con.execute(
        """COMMENT ON TABLE canonical_us_nodule_characteristics_v1 IS
           'Script 246 (2026-04-16): one row per US nodule per exam. Identity from imaging_nodule_master_v1 (the only true per-(exam, nodule)-grain source). Per-component ACR points enriched from tirads_llm_extracted_v2 via (research_id, exam_date, nodule_number) join (parsed from tirads_llm.deterministic_key). 6,126 patients with structured per-exam US nodule data. The other 4,745 CPM patients with US imaging are in us_nodules_tirads as wide-format placeholders (verified empty: 0% n*_tr scores, 0% nodule_N text, 0% us_1_date) — v1_1 NLP-extraction TODO. imaging_nodule_long_v2 was dropped (100% NULL TI-RADS, subset of inm_v1, duplicative sizes). See us_nodules_tirads_vs_inm_v1_discordance_v1 for cross-source TIRADS discordance audit.'"""
    )

    # --- PHASE 4: discordance audit table ───────────────────────────
    log("PHASE 4 — build us_nodules_tirads_vs_inm_v1_discordance_v1 (audit)")
    con.execute(BUILD_DISCORDANCE_SQL)
    n_disc = con.execute("SELECT COUNT(*) FROM us_nodules_tirads_vs_inm_v1_discordance_v1").fetchone()[0]
    avg_diff = con.execute(
        "SELECT AVG(abs_diff) FROM us_nodules_tirads_vs_inm_v1_discordance_v1"
    ).fetchone()[0]
    log(f"  rows={n_disc} mean_abs_diff={avg_diff:.2f}")
    con.execute(
        f"""COMMENT ON TABLE us_nodules_tirads_vs_inm_v1_discordance_v1 IS
            '{SCRIPT_TAG} ({RUN_DATE}): per-patient discordance audit between us_nodules_tirads max(n*_tr) and imaging_nodule_master_v1 max(tirads_acr_recalculated). One row per patient where the two sources disagree on the patient-level max TIRADS level. Captures direction (unt_higher / inm_higher), abs_diff, exam-date context, and a HIGH/MEDIUM review priority. Intended for v1_1 reconciliation; NOT a canonical clinical signal.'"""
    )
    dlog.add(
        "discordance_audit",
        rows=n_disc, mean_abs_diff=round(avg_diff or 0.0, 2),
        threshold_expected_around=1722,
        within_threshold=abs(n_disc - 1722) <= 50,
    )

    # --- PHASE 5: registry updates ─────────────────────────────────
    log("PHASE 5 — registry updates")
    # Remove inl_v2 entry
    con.execute(
        "DELETE FROM manuscript_workspace.detail_table_registry_v1 WHERE detail_table_name='imaging_nodule_long_v2'"
    )
    # Add canonical entry
    con.execute(
        "DELETE FROM manuscript_workspace.detail_table_registry_v1 WHERE detail_table_name='canonical_us_nodule_characteristics_v1'"
    )
    con.execute(
        f"""INSERT INTO manuscript_workspace.detail_table_registry_v1
            (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
             domain, feeds_master_columns, description, canonical_version)
            VALUES (
              'canonical_us_nodule_characteristics_v1', 'main',
              'research_id; us_exam_id; nodule_index_within_exam',
              'one row per US nodule per exam',
              {n_rows}, {n_pts},
              'Imaging',
              'feeds imaging_patient_summary_v1 (v1_1 migration); rollup feeds CPM cols tirads_best_category_v12, tirads_best_score_v12, tirads_worst_category_v12, n_us_nodules_total, dominant_nodule_size_cm, max_tirads_ever',
              'Per-(exam,nodule) canonical built {RUN_DATE} ({SCRIPT_TAG}). inm_v1 identity + tirads_llm overlay. 6,126 patients with structured per-exam data; 4,745 placeholder-only us_nodules_tirads patients excluded (v1_1 NLP TODO).',
              'v1_0'
            )"""
    )
    # Add discordance audit entry
    con.execute(
        "DELETE FROM manuscript_workspace.detail_table_registry_v1 WHERE detail_table_name='us_nodules_tirads_vs_inm_v1_discordance_v1'"
    )
    con.execute(
        f"""INSERT INTO manuscript_workspace.detail_table_registry_v1
            (detail_table_name, schema_name, join_key, grain, total_rows, total_patients,
             domain, feeds_master_columns, description, canonical_version)
            VALUES (
              'us_nodules_tirads_vs_inm_v1_discordance_v1', 'main',
              'research_id', 'one row per patient with TIRADS discordance',
              {n_disc}, {n_disc},
              'Imaging/Audit',
              '(audit only, no canonical column)',
              'Audit of patient-level max TIRADS discordance between us_nodules_tirads and imaging_nodule_master_v1 (Script 246, {RUN_DATE}). For v1_1 reconciliation review; NOT a canonical clinical signal.',
              'v1_0'
            )"""
    )
    log("  registry updated: -1 (inl_v2), +2 (canonical + discordance)")

    # --- PHASE 6: assertions ─────────────────────────────────────
    log("PHASE 6 — assertions")
    checks: list[tuple[str, bool, str]] = []

    # (a) row count near inm_v1
    checks.append((f"COUNT(*) >= 35000 (inm_v1 baseline 37,016, expect equal modulo sampling)  got {n_rows}",
                   n_rows >= 35000, ""))

    # (b) distinct pts == inm_v1 distinct pts (lossless on identity)
    inm_pts = con.execute("SELECT COUNT(DISTINCT research_id) FROM imaging_nodule_master_v1").fetchone()[0]
    checks.append((f"distinct_pts ({n_pts}) == inm_v1 distinct_pts ({inm_pts}) — lossless",
                   n_pts == inm_pts, ""))

    # (c) 100% CPM-aligned (every row's research_id is in CPM)
    n_unaligned = con.execute(
        """SELECT COUNT(DISTINCT u.research_id) FROM canonical_us_nodule_characteristics_v1 u
           WHERE CAST(u.research_id AS VARCHAR) NOT IN (SELECT research_id FROM canonical_patient_master)"""
    ).fetchone()[0]
    checks.append((f"all canonical pts are in CPM (got {n_unaligned} unaligned)", n_unaligned == 0, ""))

    # (d) inm_v1 patients NOT in canonical: hard zero
    n_lost = con.execute(
        """SELECT COUNT(*) FROM (SELECT DISTINCT research_id FROM imaging_nodule_master_v1) i
           WHERE i.research_id NOT IN (SELECT DISTINCT research_id FROM canonical_us_nodule_characteristics_v1)"""
    ).fetchone()[0]
    checks.append((f"inm_v1 pts not in canonical: HARD ZERO (got {n_lost})", n_lost == 0, ""))

    # (e) TI-RADS component coverage > 0 for each component (NOT 100% NULL like in dropped inl_v2)
    component_coverage = con.execute(
        """SELECT
             COUNT(composition) AS comp,
             COUNT(echogenicity) AS echo,
             COUNT(shape) AS shape,
             COUNT(margins) AS margins,
             COUNT(echogenic_foci) AS foci,
             COUNT(*) AS total
           FROM canonical_us_nodule_characteristics_v1"""
    ).fetchone()
    all_components_present = all(component_coverage[i] > 0 for i in range(5))
    checks.append((
        f"TI-RADS components NOT all-NULL (comp={component_coverage[0]}, echo={component_coverage[1]}, "
        f"shape={component_coverage[2]}, margins={component_coverage[3]}, foci={component_coverage[4]} of {component_coverage[5]})",
        all_components_present, "",
    ))

    # (f) imaging_nodule_long_v2 dropped from main
    inl_exists = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{PUBLICATION_DB}' AND table_schema='main' AND table_name='imaging_nodule_long_v2'"""
    ).fetchone()[0]
    checks.append(("imaging_nodule_long_v2 absent from canonical.main", inl_exists == 0, ""))

    # (g) archive copy exists
    arch_exists = con.execute(
        f"""SELECT COUNT(*) FROM information_schema.tables
            WHERE table_catalog='{ARCHIVE_DB}' AND table_schema='{ARCHIVE_SCHEMA}'
              AND table_name='{archive_name}'"""
    ).fetchone()[0]
    checks.append(("archive copy of imaging_nodule_long_v2 present", arch_exists == 1, ""))

    # (h) discordance audit row count near 1,722
    checks.append((f"discordance audit row count ~1,722 (got {n_disc})", abs(n_disc - 1722) <= 50, ""))

    # (i) canonical_patient_master unchanged
    n_cpm = con.execute("SELECT COUNT(*) FROM canonical_patient_master").fetchone()[0]
    checks.append((f"canonical_patient_master unchanged at 10,871 (got {n_cpm})", n_cpm == 10871, ""))

    # (j) registry has the 2 new entries
    n_reg = con.execute(
        """SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1
           WHERE detail_table_name IN ('canonical_us_nodule_characteristics_v1','us_nodules_tirads_vs_inm_v1_discordance_v1')"""
    ).fetchone()[0]
    checks.append((f"registry has 2 new entries (got {n_reg})", n_reg == 2, ""))

    # (k) registry inl_v2 removed
    n_reg_inl = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.detail_table_registry_v1 WHERE detail_table_name='imaging_nodule_long_v2'"
    ).fetchone()[0]
    checks.append((f"registry: imaging_nodule_long_v2 entry removed (got {n_reg_inl})", n_reg_inl == 0, ""))

    failures = 0
    for label, ok, _ in checks:
        tag = "PASS" if ok else "FAIL"
        log(f"  ASSERT [{tag}] {label}")
        if not ok:
            failures += 1

    dlog.add(
        "final_assertions",
        n_checks=len(checks), n_pass=len(checks) - failures, n_fail=failures,
        details=[{"label": l, "pass": ok} for l, ok, _ in checks],
    )

    dlog.dump(DECISION_LOG_PATH)
    elapsed = time.time() - t0
    if failures:
        log(f"FAILURES: {failures}")
        sys.exit(1)
    log(f"=== END {Path(__file__).name}  elapsed={elapsed:.1f}s  failures=0")


if __name__ == "__main__":
    main()
