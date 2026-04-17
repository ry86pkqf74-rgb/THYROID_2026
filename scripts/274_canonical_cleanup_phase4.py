"""Canonical cleanup 20260417 — Phase 4 (MED-severity cleanups).

4.1 multifocal downgrade (344 expected, exact match — Logan GO 2026-04-17)
4.2 imaging_nodule_master_v1 exam_date_quality + clean view
4.3 worst_bethesda_source verification (672 already populated -> no-op)
4.4 path_tumor_size_cm COMMENT + invariant view
4.5 documentation COMMENTs (FNA/RAI date drift, n_us_exams)
4.6 ajcc8_t_stage PRE-GATE; STOPS if any bare ref remains, else renames

Writes only to thyroid_canonical_publication_v1_0.
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HERE = REPO / "studies" / "canonical_cleanup_20260417"
sys.path.insert(0, str(REPO / "scripts"))

from _md_connect import connect_locked  # type: ignore

LOG_PATH = HERE / "phase4_run.log"
DECISIONS_PATH = HERE / "phase4_decision_log.json"
MIGRATION_CSV = HERE / "ajcc8_t_stage_migration_needed.csv"

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


def assert_invariants(con) -> None:
    n_rows, n_distinct = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT research_id) "
        "FROM main.canonical_patient_master"
    ).fetchone()
    if n_rows != 10871 or n_distinct != 10871:
        stop(f"Invariant breach: rows={n_rows} distinct={n_distinct}")
    log("invariants OK")


# ---------------------------------------------------------------------------

def phase_4_1(con) -> None:
    log("=== Phase 4.1 — multifocal downgrade UPDATE (344 expected exact) ===")
    pre_total_true = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE multifocal_flag_path IS TRUE"
    ).fetchone()[0]
    pre_nlp_supported = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE multifocal_flag_path IS TRUE AND nlp_path_multifocal_mentioned IS TRUE"
    ).fetchone()[0]

    update_sql = """
        UPDATE main.canonical_patient_master AS cpm
        SET multifocal_flag_path = FALSE
        WHERE cpm.multifocal_flag_path IS TRUE
          AND cpm.research_id IN (
            WITH stl_counts AS (
              SELECT CAST(research_id AS VARCHAR) AS research_id, surg_date,
                     COUNT(*) AS n_tumors
              FROM main.synoptic_tumor_long_v1 WHERE research_id IS NOT NULL
              GROUP BY 1, 2
            ),
            per_rid_max AS (
              SELECT research_id, MAX(n_tumors) AS max_t
              FROM stl_counts GROUP BY 1
            )
            SELECT research_id FROM per_rid_max WHERE max_t = 1
          )
          AND (cpm.nlp_path_multifocal_mentioned IS NULL
               OR cpm.nlp_path_multifocal_mentioned IS NOT TRUE)
    """
    con.execute(update_sql)

    post_total_true = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE multifocal_flag_path IS TRUE"
    ).fetchone()[0]
    post_nlp_supported = con.execute(
        "SELECT COUNT(*) FROM main.canonical_patient_master "
        "WHERE multifocal_flag_path IS TRUE AND nlp_path_multifocal_mentioned IS TRUE"
    ).fetchone()[0]
    delta = pre_total_true - post_total_true
    log(f"[4.1] pre_total_TRUE={pre_total_true} post_total_TRUE={post_total_true} delta={delta}")
    log(f"[4.1] preserved NLP-supported pre={pre_nlp_supported} post={post_nlp_supported}")

    expected_post = 1784 - 344  # 1440
    if post_total_true != expected_post:
        record({
            "step": "4.1", "pre_total_true": pre_total_true,
            "post_total_true": post_total_true,
            "expected_post": expected_post, "delta": delta,
            "pre_nlp_supported": pre_nlp_supported,
            "post_nlp_supported": post_nlp_supported,
            "status": "POST_MISMATCH",
        })
        stop(f"[4.1] post_total_true {post_total_true} != expected {expected_post}")
    if pre_nlp_supported != post_nlp_supported:
        stop(f"[4.1] NLP-supported count changed {pre_nlp_supported}->{post_nlp_supported}")
    record({
        "step": "4.1",
        "pre_total_true": pre_total_true, "post_total_true": post_total_true,
        "expected_post": expected_post, "delta": delta,
        "pre_nlp_supported": pre_nlp_supported,
        "post_nlp_supported": post_nlp_supported,
        "status": "OK",
    })


# ---------------------------------------------------------------------------

def phase_4_2(con) -> None:
    log("=== Phase 4.2 — imaging_nodule_master_v1 exam_date_quality ===")
    # Add column if missing
    cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='imaging_nodule_master_v1'"
        ).fetchall()
    }
    if "exam_date_quality" not in cols:
        con.execute(
            "ALTER TABLE main.imaging_nodule_master_v1 "
            "ADD COLUMN exam_date_quality VARCHAR"
        )
        log("[4.2] ADD COLUMN exam_date_quality ok")
    else:
        log("[4.2] exam_date_quality already exists; will repopulate")

    # Populate
    con.execute(
        """
        UPDATE main.imaging_nodule_master_v1
        SET exam_date_quality =
          CASE
            WHEN exam_date IS NULL THEN 'MISSING'
            WHEN exam_date < DATE '1990-01-01' THEN 'PRE_1990'
            WHEN exam_date > CURRENT_DATE THEN 'FUTURE'
            ELSE 'OK'
          END
        """
    )
    dist = con.execute(
        "SELECT exam_date_quality, COUNT(*) FROM main.imaging_nodule_master_v1 "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    log(f"[4.2] exam_date_quality distribution: {dist}")

    con.execute(
        """
        CREATE OR REPLACE VIEW manuscript_workspace.imaging_nodule_master_clean_v1 AS
        SELECT *
        FROM main.imaging_nodule_master_v1
        WHERE exam_date_quality = 'OK'
        """
    )
    n_clean = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.imaging_nodule_master_clean_v1"
    ).fetchone()[0]
    log(f"[4.2] clean view rows: {n_clean}")
    record({"step": "4.2", "exam_date_quality_dist": dist, "clean_view_rows": n_clean})


# ---------------------------------------------------------------------------

def phase_4_3(con) -> None:
    log("=== Phase 4.3 — worst_bethesda_source verification (no value changes) ===")
    n_over = con.execute(
        """
        WITH fna_max AS (
          SELECT research_id AS rid, MAX(bethesda_category) AS max_b
          FROM main.fna_episode_master_v2
          WHERE bethesda_category IS NOT NULL GROUP BY 1
        )
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        LEFT JOIN fna_max ON fna_max.rid = cpm.research_id
        WHERE cpm.worst_bethesda_num IS NOT NULL
          AND (fna_max.max_b IS NULL OR cpm.worst_bethesda_num > fna_max.max_b)
        """
    ).fetchone()[0]
    n_over_with_source = con.execute(
        """
        WITH fna_max AS (
          SELECT research_id AS rid, MAX(bethesda_category) AS max_b
          FROM main.fna_episode_master_v2
          WHERE bethesda_category IS NOT NULL GROUP BY 1
        )
        SELECT COUNT(*) FROM main.canonical_patient_master cpm
        LEFT JOIN fna_max ON fna_max.rid = cpm.research_id
        WHERE cpm.worst_bethesda_num IS NOT NULL
          AND (fna_max.max_b IS NULL OR cpm.worst_bethesda_num > fna_max.max_b)
          AND cpm.worst_bethesda_source IS NOT NULL
        """
    ).fetchone()[0]
    log(f"[4.3] CPM-over cases: {n_over}; with source already populated: {n_over_with_source}")
    record({
        "step": "4.3",
        "cpm_over_cases": n_over,
        "with_source_populated": n_over_with_source,
        "action": "no-op (all already populated)",
    })
    if n_over != n_over_with_source:
        stop(f"[4.3] {n_over - n_over_with_source} CPM-over cases lack source; needs fix")


# ---------------------------------------------------------------------------

def phase_4_4(con) -> None:
    log("=== Phase 4.4 — path_tumor_size_cm COMMENT + invariant view ===")
    # COMMENT
    con.execute(
        "COMMENT ON COLUMN main.canonical_patient_master.path_tumor_size_cm IS "
        "'Pathology dominant tumor size (cm). Convention: dominant lesion, not "
        "max-of-all-lesions. For multifocal patients, use tumor_size_cm_max for "
        "worst-of-all. Documented 2026-04-17 in canonical cleanup.'"
    )
    log("[4.4] COMMENT applied")

    # Invariant view: path_tumor_size_cm <= tumor_size_cm_max
    con.execute(
        """
        CREATE OR REPLACE VIEW manuscript_workspace.path_tumor_size_invariant_v1 AS
        SELECT research_id, path_tumor_size_cm, tumor_size_cm_max
        FROM main.canonical_patient_master
        WHERE path_tumor_size_cm IS NOT NULL
          AND tumor_size_cm_max IS NOT NULL
          AND path_tumor_size_cm > tumor_size_cm_max
        """
    )
    n_violators = con.execute(
        "SELECT COUNT(*) FROM manuscript_workspace.path_tumor_size_invariant_v1"
    ).fetchone()[0]
    log(f"[4.4] invariant violators (should be 0): {n_violators}")
    record({"step": "4.4", "invariant_violators": n_violators})


# ---------------------------------------------------------------------------

def phase_4_5(con) -> None:
    log("=== Phase 4.5 — documentation COMMENTs (FNA/RAI date / n_us_exams) ===")
    # n_us_exams
    con.execute(
        "COMMENT ON COLUMN main.canonical_patient_master.n_us_exams IS "
        "'Count of distinct exam_date values in canonical_us_nodule_characteristics_v1 "
        "per research_id. Depends on exam_date completeness; see "
        "imaging_nodule_master_clean_v1 view for OK-quality subset. Documented "
        "2026-04-17 in canonical cleanup (PART2 \u00a71.4 provenance).'"
    )
    # Detect FNA / RAI date columns and comment on them if present
    cpm_cols = {
        r[0]
        for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    documented = []
    for col, body in [
        ("fna_first_date",
         "First FNA episode date per fna_episode_master_v2.resolved_fna_date "
         "(falls back to fna_date_native; entity_date when episode resolution missing). "
         "Documented 2026-04-17 (PART2 \u00a72.8)."),
        ("fna_last_date",
         "Last FNA episode date per fna_episode_master_v2.resolved_fna_date "
         "(falls back to fna_date_native). Documented 2026-04-17 (PART2 \u00a72.8)."),
        ("rai_first_date",
         "First RAI episode date per rai_treatment_episode_v2.resolved_rai_date "
         "(falls back to note_date_parsed). Documented 2026-04-17 (PART2 \u00a73.5/3.6)."),
        ("rai_last_date",
         "Last RAI episode date per rai_treatment_episode_v2.resolved_rai_date "
         "(falls back to note_date_parsed). Documented 2026-04-17 (PART2 \u00a73.5/3.6)."),
    ]:
        if col in cpm_cols:
            con.execute(
                f"COMMENT ON COLUMN main.canonical_patient_master.{col} IS '{body}'"
            )
            documented.append(col)
    log(f"[4.5] commented columns: n_us_exams + {documented}")
    record({"step": "4.5", "commented": ["n_us_exams"] + documented,
            "missing": [c for c in ("fna_first_date", "fna_last_date", "rai_first_date", "rai_last_date")
                        if c not in cpm_cols]})


# ---------------------------------------------------------------------------

def phase_4_6_pre_gate(con) -> int:
    """Return count of distinct views that contain a bare ajcc8_t_stage reference.

    Bare = matches 'ajcc8_t_stage' but not 'ajcc8_t_stage_corrected' or 'ajcc8_t_stage_v2'.
    Implemented without lookahead: mask the two suffix forms then count remaining
    occurrences via length-difference / needle-length.
    """
    log("=== Phase 4.6 — ajcc8_t_stage PRE-GATE ===")
    rows = con.execute(
        """
        WITH all_views AS (
          SELECT table_schema, table_name, LOWER(view_definition) AS d
          FROM information_schema.views
          WHERE table_catalog = 'thyroid_canonical_publication_v1_0'
            AND view_definition IS NOT NULL
            AND LOWER(view_definition) LIKE '%ajcc8_t_stage%'
        ),
        masked AS (
          SELECT table_schema, table_name, d,
                 REPLACE(REPLACE(d, 'ajcc8_t_stage_corrected', ''),
                                  'ajcc8_t_stage_v2', '') AS d_masked
          FROM all_views
        )
        SELECT table_schema, table_name,
               (LENGTH(d_masked) - LENGTH(REPLACE(d_masked, 'ajcc8_t_stage', '')))
                 / 13 AS bare_count
        FROM masked
        WHERE (LENGTH(d_masked) - LENGTH(REPLACE(d_masked, 'ajcc8_t_stage', ''))) > 0
        ORDER BY 1, 2
        """
    ).fetchall()
    log(f"[4.6] PRE-GATE result: {len(rows)} view(s) reference bare 'ajcc8_t_stage'")
    if rows:
        with MIGRATION_CSV.open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["table_schema", "table_name", "bare_ajcc8_t_stage_count"])
            for r in rows:
                w.writerow([r[0], r[1], r[2]])
                log(f"[4.6]   - {r[0]}.{r[1]}  bare_refs={r[2]}")
    record({
        "step": "4.6-pregate",
        "n_views_with_bare_refs": len(rows),
        "rows": [list(r) for r in rows],
        "migration_csv": str(MIGRATION_CSV) if rows else None,
    })
    return len(rows)


def phase_4_6_rename(con) -> None:
    log("=== Phase 4.6 — executing ajcc8_t_stage rename ===")
    cpm_cols = {
        r[0] for r in con.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='thyroid_canonical_publication_v1_0' "
            "AND table_schema='main' AND table_name='canonical_patient_master'"
        ).fetchall()
    }
    needed = {"ajcc8_t_stage", "ajcc8_t_stage_corrected"}
    if not needed.issubset(cpm_cols):
        stop(f"[4.6] missing columns for rename; have {sorted(cpm_cols & needed)}")

    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "RENAME COLUMN ajcc8_t_stage "
        "TO ajcc8_t_stage_with_microete_t3b_DEPRECATED"
    )
    con.execute(
        "ALTER TABLE main.canonical_patient_master "
        "RENAME COLUMN ajcc8_t_stage_corrected TO ajcc8_t_stage"
    )
    con.execute(
        "COMMENT ON COLUMN main.canonical_patient_master.ajcc8_t_stage_with_microete_t3b_DEPRECATED "
        "IS 'Do not use. AJCC 8 rule preserved for audit only. Superseded "
        "2026-04-17 by canonical cleanup script 274 rename of "
        "ajcc8_t_stage_corrected -> ajcc8_t_stage.'"
    )
    log("[4.6] rename executed and COMMENT applied")
    record({"step": "4.6-rename", "status": "OK"})


# ---------------------------------------------------------------------------

def main() -> int:
    HERE.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("")
    DECISIONS_PATH.write_text("[]")
    log("=== Phase 4 driver start ===")
    con = connect_locked()
    assert_invariants(con)

    phase_4_1(con); assert_invariants(con)
    phase_4_2(con); assert_invariants(con)
    phase_4_3(con); assert_invariants(con)
    phase_4_4(con); assert_invariants(con)
    phase_4_5(con); assert_invariants(con)
    n_bare = phase_4_6_pre_gate(con)
    if n_bare > 0:
        log(f"[4.6] PRE-GATE result: {n_bare} view(s) reference bare ajcc8_t_stage. "
            "Migration list written. STOPPING (do not rename) per Logan instruction.")
        stop("Phase 4.6 PRE-GATE: bare references found; do not rename")
    log("[4.6] PRE-GATE passed (0 bare refs). RENAME deferred to "
        "scripts/274b_canonical_cleanup_phase4_6_rename.py per checkpoint policy.")
    log("=== Phase 4 driver end (4.1-4.5 + 4.6 PRE-GATE done; rename pending Logan go) ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
