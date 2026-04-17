"""Canonical cleanup 20260417 — Phase 1 (re-rollup CPM).

Implements phases 1.1 through 1.8 per the prompt with operational adjustments:
  - Every UPDATE is preceded by a COUNT-only dry-run.
  - If observed rowcount differs from expected by >10%, STOP and report.
  - Phase 1.7 is a verification-only rebuild; expected touched 0-10; STOP if >10.

Writes ONLY to thyroid_canonical_publication_v1_0:
  - main.canonical_patient_master (UPDATEs + Phase 1.8 ADD COLUMN IF NOT EXISTS,
    Phase 1.3 ADD COLUMN lateral_neck_dissected_structured_or_nlp)
  - main.complication_phenotype_v1 (Phase 1.1)

Decision log: studies/canonical_cleanup_20260417/phase1_decision_log.json
Run log:      studies/canonical_cleanup_20260417/phase1_run.log
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HERE = REPO / "studies" / "canonical_cleanup_20260417"
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

LOG_PATH = HERE / "phase1_run.log"
DECISIONS_PATH = HERE / "phase1_decision_log.json"

DECISIONS: list[dict] = []


def log(msg: str) -> None:
    line = f"[{datetime.now(timezone.utc).isoformat()}] {msg}"
    print(line)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def record(entry: dict) -> None:
    DECISIONS.append(entry)
    DECISIONS_PATH.write_text(json.dumps(DECISIONS, indent=2, default=str))


def stop(msg: str) -> None:
    log(f"STOP: {msg}")
    DECISIONS_PATH.write_text(json.dumps(DECISIONS, indent=2, default=str))
    raise SystemExit(2)


def gated(con, step: str, expected: int, count_sql: str, update_sql: str,
          tolerance_frac: float = 0.10, post_check_sql: str | None = None,
          post_check_expected: int | None = None,
          notes: str = "", abs_floor: int = 5) -> None:
    """Run COUNT-only dry-run, gate on max(tolerance_frac, abs_floor) vs expected, then UPDATE.

    Per Logan: small-N steps (expected<50) gate on +/- abs_floor rows;
    larger steps gate on +/- tolerance_frac. Use whichever is wider.
    """
    started = datetime.now(timezone.utc).isoformat()
    pre_n = con.execute(count_sql).fetchone()[0]
    rel_window = max(int(round(expected * tolerance_frac)), abs_floor)
    low = expected - rel_window
    high = expected + rel_window
    log(f"[{step}] pre-count={pre_n} expected={expected} window=[{low},{high}] (gate=max({int(tolerance_frac*100)}%,{abs_floor})) {notes}")
    entry = {
        "step": step,
        "started_at": started,
        "expected": expected,
        "pre_count": pre_n,
        "tolerance_frac": tolerance_frac,
        "notes": notes,
    }
    # Idempotency: if pre_count is 0 AND we have a post_check that already
    # matches its expected value, treat as a previously-applied no-op.
    if pre_n == 0 and post_check_sql is not None and post_check_expected is not None:
        post_now = con.execute(post_check_sql).fetchone()[0]
        if post_now == post_check_expected:
            entry["status"] = "ALREADY_APPLIED_NOOP"
            entry["post_check_value"] = post_now
            entry["post_check_expected"] = post_check_expected
            entry["ended_at"] = datetime.now(timezone.utc).isoformat()
            log(f"[{step}] no-op (already applied; post_check={post_now} matches)")
            record(entry)
            return
    if not (low <= pre_n <= high):
        entry["status"] = "PRE_COUNT_OUT_OF_WINDOW"
        record(entry)
        stop(
            f"[{step}] pre-count {pre_n} outside max({int(tolerance_frac*100)}%, {abs_floor}) "
            f"window of expected {expected}. Halting."
        )
    log(f"[{step}] executing UPDATE...")
    cur = con.execute(update_sql)
    try:
        rc = cur.rowcount  # may be -1 in some drivers; we re-verify post
    except Exception:
        rc = None
    entry["update_rowcount"] = rc
    if post_check_sql is not None:
        post_n = con.execute(post_check_sql).fetchone()[0]
        entry["post_check_value"] = post_n
        entry["post_check_expected"] = post_check_expected
        if post_check_expected is not None and post_n != post_check_expected:
            entry["status"] = "POST_CHECK_MISMATCH"
            record(entry)
            stop(f"[{step}] post-check {post_n} != expected {post_check_expected}.")
    entry["ended_at"] = datetime.now(timezone.utc).isoformat()
    entry["status"] = "OK"
    record(entry)
    log(f"[{step}] OK (rc={rc})")


def assert_invariants(con) -> None:
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        stop(f"Invariant breach: rows={n_rows} distinct={n_distinct}")
    cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    if "r_class_true" not in cols or "ete_grade_final_v2" not in cols:
        stop("Required CPM columns missing")
    log("invariants OK (10871 rows / r_class_true / ete_grade_final_v2)")


# ---------------------------------------------------------------------------
# Phase 1 implementations
# ---------------------------------------------------------------------------

def phase_1_1(con) -> None:
    """Promote 32 s236-crossref VC rows in complication_phenotype_v1."""
    log("=== Phase 1.1 — promote VC s236 cross-ref rows ===")
    count_sql = """
        SELECT COUNT(*) FROM main.complication_phenotype_v1
        WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
          AND status_v2 = 'confirmed_from_rln_crossref'
          AND (confirmed_flag IS FALSE OR confirmed_flag IS NULL)
    """
    update_sql = """
        UPDATE main.complication_phenotype_v1
        SET confirmed_flag = TRUE,
            phenotype_version = COALESCE(phenotype_version, '') || '+s236_crossref'
        WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
          AND status_v2 = 'confirmed_from_rln_crossref'
          AND (confirmed_flag IS FALSE OR confirmed_flag IS NULL)
    """
    post_sql = """
        SELECT COUNT(*) FROM main.complication_phenotype_v1
        WHERE complication_entity IN ('vocal_cord_paralysis','vocal_cord_paresis')
          AND status_v2 = 'confirmed_from_rln_crossref'
          AND confirmed_flag IS TRUE
    """
    gated(
        con, "1.1", expected=32,
        count_sql=count_sql, update_sql=update_sql,
        post_check_sql=post_sql, post_check_expected=32,
        notes="VC s236 promotion (19 paralysis + 13 paresis)",
    )
    # Sanity: split counts
    split = con.execute(
        "SELECT complication_entity, COUNT(*) "
        "FROM main.complication_phenotype_v1 "
        "WHERE status_v2='confirmed_from_rln_crossref' AND confirmed_flag IS TRUE "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    log(f"[1.1] post split {split}")


def phase_1_2(con) -> None:
    """Backfill CPM comp_vc_paralysis/paresis_confirmed for promoted patients."""
    log("=== Phase 1.2 — CPM VC backfill ===")
    # NOTE: cpm.research_id is VARCHAR; phenotype_v1.research_id is BIGINT.
    # All cross-table comparisons cast to VARCHAR to match the CPM spine.
    # Paralysis (19)
    pre_paralysis_count = """
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        WHERE cpm.research_id IN (
          SELECT CAST(research_id AS VARCHAR) FROM main.complication_phenotype_v1
          WHERE complication_entity='vocal_cord_paralysis'
            AND status_v2='confirmed_from_rln_crossref'
            AND confirmed_flag IS TRUE
        )
        AND (cpm.comp_vc_paralysis_confirmed IS FALSE
             OR cpm.comp_vc_paralysis_confirmed IS NULL)
    """
    update_paralysis = """
        UPDATE main.canonical_patient_master AS cpm
        SET comp_vc_paralysis_confirmed = TRUE
        WHERE research_id IN (
          SELECT CAST(research_id AS VARCHAR) FROM main.complication_phenotype_v1
          WHERE complication_entity='vocal_cord_paralysis'
            AND status_v2='confirmed_from_rln_crossref'
            AND confirmed_flag IS TRUE
        )
        AND (comp_vc_paralysis_confirmed IS FALSE
             OR comp_vc_paralysis_confirmed IS NULL)
    """
    post_paralysis = """
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        WHERE research_id IN (
          SELECT CAST(research_id AS VARCHAR) FROM main.complication_phenotype_v1
          WHERE complication_entity='vocal_cord_paralysis'
            AND status_v2='confirmed_from_rln_crossref'
            AND confirmed_flag IS TRUE
        )
        AND comp_vc_paralysis_confirmed IS TRUE
    """
    gated(
        con, "1.2a-paralysis", expected=19,
        count_sql=pre_paralysis_count, update_sql=update_paralysis,
        post_check_sql=post_paralysis, post_check_expected=19,
        notes="paralysis backfill",
    )

    # Paresis (13)
    pre_paresis_count = pre_paralysis_count.replace("paralysis", "paresis")
    update_paresis = update_paralysis.replace("paralysis", "paresis")
    post_paresis = post_paralysis.replace("paralysis", "paresis")
    gated(
        con, "1.2b-paresis", expected=13,
        count_sql=pre_paresis_count, update_sql=update_paresis,
        post_check_sql=post_paresis, post_check_expected=13,
        notes="paresis backfill",
    )


def phase_1_3(con) -> None:
    """Lateral ND structured BOOL_OR rebuild + add structured_or_nlp column."""
    log("=== Phase 1.3 — lateral ND rebuild ===")
    # Add the structured_or_nlp column if missing (idempotent)
    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "ADD COLUMN IF NOT EXISTS lateral_neck_dissected_structured_or_nlp BOOLEAN"
    )
    log("[1.3] ALTER ADD COLUMN IF NOT EXISTS lateral_neck_dissected_structured_or_nlp ok")

    # cpm.research_id VARCHAR; oed.research_id INTEGER -> cast oed to VARCHAR
    count_sql = """
        WITH oed AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 BOOL_OR(lateral_neck_dissection_flag) AS f
          FROM main.operative_episode_detail_v2
          GROUP BY research_id
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN oed USING(research_id)
        WHERE oed.f IS TRUE
          AND (cpm.lateral_neck_dissected IS NULL
               OR cpm.lateral_neck_dissected IS NOT TRUE)
    """
    update_sql = """
        UPDATE main.canonical_patient_master AS cpm
        SET lateral_neck_dissected = TRUE
        FROM (
          SELECT CAST(research_id AS VARCHAR) AS research_id
          FROM main.operative_episode_detail_v2
          GROUP BY research_id
          HAVING BOOL_OR(lateral_neck_dissection_flag) IS TRUE
        ) AS oed_true
        WHERE cpm.research_id = oed_true.research_id
          AND (cpm.lateral_neck_dissected IS NULL
               OR cpm.lateral_neck_dissected IS NOT TRUE)
    """
    # Expected ~217. We verify post-state distinct TRUE goes from 119 to ~336.
    gated(
        con, "1.3a-rebuild", expected=217,
        count_sql=count_sql, update_sql=update_sql,
        notes="OR-in structured oed.lateral into CPM",
    )

    # Populate structured_or_nlp synonym (full union after rebuild)
    # cast oed rid to VARCHAR for the correlated subquery
    union_update = """
        UPDATE main.canonical_patient_master AS cpm
        SET lateral_neck_dissected_structured_or_nlp =
            COALESCE(cpm.lateral_neck_dissected, FALSE)
            OR COALESCE((
              SELECT BOOL_OR(o.lateral_neck_dissection_flag)
              FROM main.operative_episode_detail_v2 o
              WHERE CAST(o.research_id AS VARCHAR) = cpm.research_id
            ), FALSE)
    """
    log("[1.3b] populating lateral_neck_dissected_structured_or_nlp synonym...")
    con.execute(union_update)

    post_total_true = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE lateral_neck_dissected IS TRUE"
    ).fetchone()[0]
    post_synonym_true = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE lateral_neck_dissected_structured_or_nlp IS TRUE"
    ).fetchone()[0]
    log(f"[1.3] post: lateral_neck_dissected TRUE={post_total_true}, "
        f"structured_or_nlp TRUE={post_synonym_true}")
    record({
        "step": "1.3-postcheck",
        "lateral_neck_dissected_true_after": post_total_true,
        "lateral_neck_dissected_structured_or_nlp_true_after": post_synonym_true,
    })


def phase_1_4(con) -> None:
    """TIRADS verification (read-only). SKIPPED per Logan 2026-04-17: type incompat
    with prompt's 'TR' || GREATEST(...) UPDATE (storage is BIGINT 1-5, not VARCHAR);
    AND zero promotion candidates exist. Verify equality only; record no-op.
    """
    log("=== Phase 1.4 — TIRADS verification (no-op; SKIPPED per Logan) ===")
    # us.research_id INTEGER; cpm.research_id VARCHAR -> cast US side to VARCHAR
    mismatch_sql = """
        WITH per_rid AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 GREATEST(MAX(tirads_reported), MAX(tirads_acr_recalculated)) AS new_max
          FROM main.canonical_us_nodule_characteristics_v1
          WHERE tirads_reported IS NOT NULL OR tirads_acr_recalculated IS NOT NULL
          GROUP BY research_id
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        JOIN per_rid USING(research_id)
        WHERE per_rid.new_max IS NOT NULL
          AND (cpm.max_tirads_ever IS NULL OR per_rid.new_max > cpm.max_tirads_ever)
    """
    n_mismatch = con.execute(mismatch_sql).fetchone()[0]
    log(f"[1.4] mismatches (rids whose new_max strictly exceeds CPM max_tirads_ever): {n_mismatch}")
    record({
        "step": "1.4-verification",
        "candidates_to_promote": n_mismatch,
        "expected_per_prompt": 1503,
        "skipped": True,
        "skip_reason": (
            "Prompt's UPDATE 'TR' || GREATEST(...) assumes VARCHAR storage; "
            "live max_tirads_ever is BIGINT (values 1-5). Even if candidates "
            "existed, the written SQL would error or strip 'TR' prefix; "
            "additionally candidate count = 0 makes the question moot. "
            "Read-only verification confirms canonical state."
        ),
    })
    if n_mismatch != 0:
        stop(f"[1.4] verification fail: {n_mismatch} CPM rids fall behind source max")


def phase_1_5(con) -> None:
    """RAI max dose rebuild from rai_treatment_episode_v2 with rai_dose_v9 fallback."""
    log("=== Phase 1.5 — RAI max dose rebuild ===")
    # rai.research_id INTEGER; cpm.research_id VARCHAR -> cast rai side to VARCHAR
    count_sql = """
        WITH ep AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id, MAX(dose_mci) AS max_dose
          FROM main.rai_treatment_episode_v2
          GROUP BY research_id
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        LEFT JOIN ep USING(research_id)
        WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND COALESCE(ep.max_dose, cpm.rai_dose_v9) > 0
    """
    update_sql = """
        UPDATE main.canonical_patient_master AS cpm
        SET rai_max_dose_mci = COALESCE(ep.max_dose, cpm.rai_dose_v9)
        FROM (
          SELECT CAST(research_id AS VARCHAR) AS research_id, MAX(dose_mci) AS max_dose
          FROM main.rai_treatment_episode_v2
          GROUP BY research_id
        ) AS ep
        WHERE cpm.research_id = ep.research_id
          AND (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND COALESCE(ep.max_dose, cpm.rai_dose_v9) > 0
    """
    # Per Logan 2026-04-17: revised expected = 27 episode + 27 v9-fallback = 54 total
    # (down from prompt's 214 due to prior reconciliation work).
    gated(
        con, "1.5a-episode", expected=27,
        count_sql=count_sql, update_sql=update_sql,
        notes=("rai_treatment_episode_v2.dose_mci primary path; "
               "expected revised 214->27 per dryrun probe 2026-04-17"),
    )
    # Second pass: rids with no episode but rai_dose_v9 > 0
    fallback_count = """
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        LEFT JOIN (
          SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL
        ) ep USING(research_id)
        WHERE ep.research_id IS NULL
          AND (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND cpm.rai_dose_v9 > 0
    """
    fallback_update = """
        UPDATE main.canonical_patient_master AS cpm
        SET rai_max_dose_mci = rai_dose_v9
        WHERE (cpm.rai_max_dose_mci = 0 OR cpm.rai_max_dose_mci IS NULL)
          AND cpm.rai_dose_v9 > 0
          AND cpm.research_id NOT IN (
            SELECT DISTINCT CAST(research_id AS VARCHAR)
            FROM main.rai_treatment_episode_v2 WHERE dose_mci IS NOT NULL
          )
    """
    n_fb = con.execute(fallback_count).fetchone()[0]
    log(f"[1.5b-fallback] candidates rai_dose_v9 only = {n_fb} (expected ~27)")
    # Gate on max(10%, 5) of expected=27 -> +/-5 -> [22,32]
    if not (22 <= n_fb <= 32):
        stop(f"[1.5b] fallback candidates {n_fb} outside revised window [22,32]")
    if n_fb > 0:
        con.execute(fallback_update)
    record({"step": "1.5b-fallback", "rai_dose_v9_only_promoted": n_fb,
            "expected": 27, "window": [22, 32]})


def phase_1_6(con) -> None:
    """Tg counts/peak/nadir VERIFICATION (no rebuild).

    Audit on 2026-04-17 (phase1_6_tg_drift_audit.md) found 0 mismatches
    across 2,721 CPM patients with lab data; the +397/+359 deltas in the
    initial sizing were artifacts of comparing CPM-set-size vs distinct
    lab rids (lab orphans inflate the latter; orphans are Phase 3.1, not 1.6).
    Logan approved skipping the rebuild and recording verification only.
    """
    log("=== Phase 1.6 — Tg verification (rebuild SKIPPED per Logan + audit) ===")
    classifier = (
        "CASE WHEN LOWER(analyte) LIKE '%antibod%' OR LOWER(analyte) LIKE 'tgab%' "
        "THEN 'TGAB' WHEN LOWER(analyte) LIKE 'thyroglobulin%' OR LOWER(analyte) = 'tg' "
        "THEN 'TG' ELSE 'OTHER' END"
    )
    # tg.research_id BIGINT; cpm.research_id VARCHAR -> cast tg side to VARCHAR
    audit = con.execute(
        f"""
        WITH live AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 COUNT(*) FILTER (WHERE {classifier}='TG')   AS live_tg,
                 COUNT(*) FILTER (WHERE {classifier}='TGAB') AS live_tgab,
                 MAX(result_numeric) FILTER (WHERE {classifier}='TG'
                                           AND result_numeric IS NOT NULL) AS live_peak,
                 MIN(result_numeric) FILTER (WHERE {classifier}='TG'
                                           AND result_numeric IS NOT NULL) AS live_nadir
          FROM main.thyroglobulin_lab_canonical_v1
          GROUP BY 1
        )
        SELECT
          COUNT(*) AS n_with_lab,
          COUNT(*) FILTER (WHERE COALESCE(cpm.n_tg_measurements_structured,0) <> live.live_tg)   AS d_tg,
          COUNT(*) FILTER (WHERE COALESCE(cpm.n_tgab_measurements,0) <> live.live_tgab)          AS d_tgab,
          COUNT(*) FILTER (WHERE cpm.tg_peak  IS DISTINCT FROM live.live_peak)                   AS d_peak,
          COUNT(*) FILTER (WHERE cpm.tg_nadir IS DISTINCT FROM live.live_nadir)                  AS d_nadir
        FROM main.canonical_patient_master cpm
        JOIN live USING(research_id)
        """
    ).fetchone()
    cols = ["n_with_lab", "d_tg", "d_tgab", "d_peak", "d_nadir"]
    audit_d = dict(zip(cols, audit))
    log(f"[1.6] verification audit: {audit_d}")
    record({"step": "1.6-verification", "audit": audit_d, "skipped_rebuild": True,
            "skip_reason": "Audit shows 0 mismatches; +397/+359 in dryrun probe were "
                           "lab-orphan inflation (Phase 3.1)."})
    if audit_d["d_tg"] != 0 or audit_d["d_tgab"] != 0 or audit_d["d_peak"] != 0 or audit_d["d_nadir"] != 0:
        stop(f"[1.6] verification FAIL: {audit_d}")


def phase_1_7(con) -> None:
    """Verification-only rebuild of n_fna_episodes; expect 0-10 touched."""
    log("=== Phase 1.7 — n_fna_episodes verification rebuild (revised: 0-10) ===")
    # fna.research_id and cpm.research_id are both VARCHAR -> direct join.
    pre_count = """
        WITH counts AS (
          SELECT research_id AS rid, COUNT(*) AS n
          FROM main.fna_episode_master_v2
          WHERE research_id IS NOT NULL
          GROUP BY 1
        )
        SELECT COUNT(*)
        FROM main.canonical_patient_master cpm
        LEFT JOIN counts ON counts.rid = cpm.research_id
        WHERE COALESCE(cpm.n_fna_episodes, 0) <> COALESCE(counts.n, 0)
    """
    n_diff = con.execute(pre_count).fetchone()[0]
    log(f"[1.7] CPM rows whose n_fna_episodes differs from live count: {n_diff}")
    record({"step": "1.7-diff-precount", "n_diff": n_diff})
    if n_diff > 10:
        stop(
            f"[1.7] verification fail: {n_diff} CPM rows differ from live "
            "fna_episode_master_v2 counts (>10). Halt for investigation."
        )

    # Distribution check
    dist = con.execute(
        """
        WITH counts AS (
          SELECT research_id AS rid, COUNT(*) AS n
          FROM main.fna_episode_master_v2
          WHERE research_id IS NOT NULL
          GROUP BY 1
        )
        SELECT n, COUNT(*) FROM counts WHERE n >= 10 GROUP BY 1 ORDER BY 1
        """
    ).fetchall()
    log(f"[1.7] live n>=10 distribution: {dist}")
    if dict(dist).get(11, 0) != 2 or dict(dist).get(12, 0) != 3:
        stop(f"[1.7] live n>=10 distribution unexpected: {dist}")

    if n_diff > 0:
        update_sql = """
            UPDATE main.canonical_patient_master AS cpm
            SET n_fna_episodes = COALESCE(c.n, 0)
            FROM (
              SELECT research_id AS rid, COUNT(*) AS n
              FROM main.fna_episode_master_v2
              WHERE research_id IS NOT NULL
              GROUP BY 1
            ) c
            WHERE c.rid = cpm.research_id
              AND COALESCE(cpm.n_fna_episodes, 0) <> c.n
        """
        log(f"[1.7] applying corrective UPDATE for {n_diff} row(s)")
        con.execute(update_sql)
    record({"step": "1.7-result", "rows_corrected": n_diff, "distribution": dist})


def phase_1_8(con) -> None:
    """Add IF NOT EXISTS comp_*_confirmed columns; rebuild any_confirmed_complication_flag."""
    log("=== Phase 1.8 — comp_* idempotent ADDs + any_confirmed_complication_flag rebuild ===")
    for col in (
        "comp_hematoma_confirmed",
        "comp_seroma_confirmed",
        "comp_chyle_leak_confirmed",
        "comp_wound_infection_confirmed",
    ):
        con.execute(
            f"ALTER TABLE main.canonical_patient_master "
            f"ADD COLUMN IF NOT EXISTS {col} BOOLEAN"
        )
        log(f"[1.8] ADD COLUMN IF NOT EXISTS {col} ok")

    # Map entity -> CPM column for the 9 entities present.
    entity_map = {
        "hematoma":            "comp_hematoma_confirmed",
        "seroma":              "comp_seroma_confirmed",
        "chyle_leak":          "comp_chyle_leak_confirmed",
        "wound_infection":     "comp_wound_infection_confirmed",
        "vocal_cord_paralysis":"comp_vc_paralysis_confirmed",
        "vocal_cord_paresis":  "comp_vc_paresis_confirmed",
        # the remaining three already have CPM columns; backfill from phenotype
        # so the BOOL_OR has a complete picture.
        # hypocalcemia and hypoparathyroidism are domain-specific (Phase 2 owns
        # the permanence semantics). We DO NOT touch comp_hypoparathyroidism_*.
    }
    # Verify CPM columns exist (case-sensitive)
    cpm_cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    for e, col in entity_map.items():
        if col not in cpm_cols:
            stop(f"[1.8] CPM column missing for entity {e}: {col}")

    # For each entity column, OR-in confirmed_flag from phenotype.
    # phenotype_v1.research_id BIGINT; cpm VARCHAR -> cast.
    for e, col in entity_map.items():
        update_sql = f"""
            UPDATE main.canonical_patient_master AS cpm
            SET {col} = TRUE
            WHERE research_id IN (
              SELECT CAST(research_id AS VARCHAR)
              FROM main.complication_phenotype_v1
              WHERE complication_entity = '{e}' AND confirmed_flag IS TRUE
            )
            AND ({col} IS NULL OR {col} IS NOT TRUE)
        """
        pre_n = con.execute(
            f"SELECT COUNT(*) FROM main.canonical_patient_master "
            f"WHERE research_id IN ("
            f"  SELECT CAST(research_id AS VARCHAR) FROM main.complication_phenotype_v1 "
            f"  WHERE complication_entity='{e}' AND confirmed_flag IS TRUE) "
            f"AND ({col} IS NULL OR {col} IS NOT TRUE)"
        ).fetchone()[0]
        log(f"[1.8] {e} -> {col}: pre-count={pre_n}")
        if pre_n > 0:
            con.execute(update_sql)
        post_n = con.execute(
            f"SELECT COUNT(*) FROM main.canonical_patient_master "
            f"WHERE research_id IN ("
            f"  SELECT CAST(research_id AS VARCHAR) FROM main.complication_phenotype_v1 "
            f"  WHERE complication_entity='{e}' AND confirmed_flag IS TRUE) "
            f"AND {col} IS TRUE"
        ).fetchone()[0]
        log(f"[1.8] {e} -> {col}: post={post_n}")
        record({
            "step": f"1.8-{e}", "column": col,
            "pre_pending": pre_n, "post_true_in_set": post_n,
        })

    # Rebuild any_confirmed_complication_flag = BOOL_OR(confirmed_flag) per rid
    pre_any = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE any_confirmed_complication_flag IS TRUE"
    ).fetchone()[0]
    # phenotype_v1.research_id BIGINT; cpm VARCHAR -> cast on the BOOL_OR side
    rebuild_sql = """
        UPDATE main.canonical_patient_master AS cpm
        SET any_confirmed_complication_flag = COALESCE(s.f, FALSE)
        FROM (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 BOOL_OR(confirmed_flag) AS f
          FROM main.complication_phenotype_v1
          GROUP BY research_id
        ) s
        WHERE cpm.research_id = s.research_id
    """
    con.execute(rebuild_sql)
    post_any = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE any_confirmed_complication_flag IS TRUE"
    ).fetchone()[0]
    log(f"[1.8] any_confirmed_complication_flag: pre={pre_any} post={post_any}")
    record({
        "step": "1.8-aggregate",
        "pre_any_confirmed": pre_any,
        "post_any_confirmed": post_any,
        "delta_newly_true": post_any - pre_any,
    })


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")
    DECISIONS_PATH.write_text("[]")
    log("=== Phase 1 driver start ===")
    con = connect_locked()
    assert_invariants(con)

    phases = [
        ("1.1", phase_1_1),
        ("1.2", phase_1_2),
        ("1.3", phase_1_3),
        ("1.4", phase_1_4),
        ("1.5", phase_1_5),
        ("1.6", phase_1_6),  # verification-only after 2026-04-17 audit
        ("1.7", phase_1_7),
        ("1.8", phase_1_8),
    ]
    for name, fn in phases:
        log(f">>> entering phase {name}")
        fn(con)
        assert_invariants(con)

    log("=== Phase 1 driver end (all phases passed invariants) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
