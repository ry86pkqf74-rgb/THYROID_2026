"""Script 342 — Populate canonical op_esophageal_inv_any column.

Problem (verified 2026-04-21):
  - main.canonical_patient_master.op_esophageal_inv_any is 0 nonnull.
  - Sibling main.canonical_patient_master.op_nlp_esophageal_involvement is
    4,028 nonnull (TRUE=2, FALSE=4,026) — that is the existing harvested
    signal.
  - main.note_entities_operative_detail has only 2 esophageal_involvement
    entity rows (2 RIDs).
  - main.note_entities_llm_airway_invasion result_json contains 'esophag'
    substring across 381 distinct RIDs — Script 334 was supposed to surface
    these into op_esophageal_inv_any but its UPDATE didn't land.

This script writes the canonical read column from two sources:
  (A) Operative entity rows where entity_type='esophageal_involvement' and
      present_or_negated='present' → TRUE.
  (B) Airway-invasion LLM JSON entities mentioning 'esophag' that are NOT
      negated → TRUE.
For RIDs that have an op-note (i.e. appear in operative_episode_detail_v2
or note_entities_operative_detail) but no positive evidence: FALSE.
For RIDs with no op-note evidence at all: leave NULL.

Real esophageal-invasion coverage requires dedicated extraction on 4,727
op-notes (RunPod Job 3). This script only makes the existing signal
readable on the canonical column.

PHI safety: research_id only in stdout; evidence_text never printed.

Usage:
    .venv/bin/python scripts/342_backfill_op_esophageal_inv_any.py            # dry-run
    .venv/bin/python scripts/342_backfill_op_esophageal_inv_any.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "342_backfill_op_esophageal_inv_any"


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def cpm_invariants(con, label=""):
    r = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id),
               SUM(CASE WHEN fna_path_outcome IS NULL THEN 1 ELSE 0 END)
          FROM main.canonical_patient_master
    """).fetchone()
    log(f"  CPM invariants {label}: rows={r[0]} distinct_rid={r[1]} null_fna={r[2]}")
    if r[0] != 10871 or r[1] != 10871 or r[2] != 0:
        raise SystemExit("CPM invariant violation")


def ensure_log_tables(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS manuscript_workspace.prompt5_remediation_log_v1 (
            ts TIMESTAMP, script_n VARCHAR, phase VARCHAR,
            target_table VARCHAR, target_column VARCHAR,
            metric_name VARCHAR, metric_value DOUBLE,
            metric_text VARCHAR, notes VARCHAR
        )
    """)


def log_metric(con, phase, target_table, target_column, metric_name,
               metric_value=None, metric_text=None, notes=None):
    con.execute("""
        INSERT INTO manuscript_workspace.prompt5_remediation_log_v1 VALUES
        (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [dt.datetime.utcnow(), SCRIPT, phase, target_table, target_column,
          metric_name,
          float(metric_value) if metric_value is not None else None,
          metric_text, notes])


def snapshot_state(con, label):
    out = {}
    for col in ["op_esophageal_inv_any", "op_nlp_esophageal_involvement"]:
        r = con.execute(f"""
            SELECT COUNT(*),
                   SUM(CASE WHEN "{col}" = TRUE THEN 1 ELSE 0 END),
                   SUM(CASE WHEN "{col}" = FALSE THEN 1 ELSE 0 END)
              FROM main.canonical_patient_master
             WHERE "{col}" IS NOT NULL
        """).fetchone()
        out[col] = (r[0], r[1] or 0, r[2] or 0)
        log(f"  CPM.{col} [{label}]: nonnull={r[0]} TRUE={r[1] or 0} FALSE={r[2] or 0}")
        log_metric(con, label, "canonical_patient_master", col, "nonnull", r[0])
        log_metric(con, label, "canonical_patient_master", col, "true_count", r[1] or 0)
        log_metric(con, label, "canonical_patient_master", col, "false_count", r[2] or 0)
    r = con.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN esophageal_involvement_flag = TRUE THEN 1 ELSE 0 END)
          FROM main.operative_episode_detail_v2
         WHERE esophageal_involvement_flag IS NOT NULL
    """).fetchone()
    out["oed_v2.esophageal_involvement_flag"] = (r[0], r[1] or 0)
    log(f"  oed_v2.esophageal_involvement_flag [{label}]: nonnull={r[0]} TRUE={r[1] or 0}")
    log_metric(con, label, "operative_episode_detail_v2",
               "esophageal_involvement_flag", "nonnull", r[0])
    log_metric(con, label, "operative_episode_detail_v2",
               "esophageal_involvement_flag", "true_count", r[1] or 0)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_tables(con)
    log("=" * 72)
    log(f"Script 342 — backfill op_esophageal_inv_any "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    cpm_invariants(con, "pre")
    snapshot_state(con, "pre")

    log("  Step 1: collect operative-entity esophageal positives")
    con.execute("DROP TABLE IF EXISTS _esoph_op")
    con.execute("""
        CREATE TEMP TABLE _esoph_op AS
        SELECT DISTINCT
               CAST(research_id AS VARCHAR) AS research_id,
               CAST(note_row_id AS VARCHAR) AS source_note_ref,
               CAST(note_date AS DATE) AS evidence_date
          FROM main.note_entities_operative_detail
         WHERE entity_type = 'esophageal_involvement'
           AND COALESCE(present_or_negated, '') = 'present'
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _esoph_op").fetchone()
    log(f"    operative-entity esoph positives: rows={n[0]} rids={n[1]}")
    log_metric(con, "source", "note_entities_operative_detail",
               "entity_type=esophageal_involvement", "rids_present", n[1])

    log("  Step 2: parse note_entities_llm_airway_invasion JSON for 'esophag'")
    con.execute("DROP TABLE IF EXISTS _esoph_llm_raw")
    con.execute("""
        CREATE TEMP TABLE _esoph_llm_raw AS
        WITH src AS (
          SELECT CAST(research_id AS VARCHAR) AS research_id,
                 note_row_id, note_date,
                 CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
            FROM main.note_entities_llm_airway_invasion
           WHERE result_json IS NOT NULL
             AND json_type(json_extract(result_json, '$.entities')) = 'ARRAY'
        ),
        ent AS (
          SELECT s.research_id, s.note_row_id, s.note_date,
                 json_extract_string(e, '$.entity_type') AS entity_type,
                 json_extract_string(e, '$.entity_value') AS entity_value,
                 json_extract_string(e, '$.present_or_negated') AS present_or_negated,
                 json_extract_string(e, '$.evidence_text') AS evidence_text
            FROM src s, UNNEST(s.arr) AS t(e)
        )
        SELECT * FROM ent
         WHERE entity_type = 'esophageal_invasion'
            OR LOWER(COALESCE(entity_value, '')) LIKE '%esophag%'
            OR LOWER(COALESCE(evidence_text, '')) LIKE '%esophag%'
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _esoph_llm_raw").fetchone()
    log(f"    LLM airway esoph mentions: rows={n[0]} rids={n[1]}")
    log_metric(con, "source", "note_entities_llm_airway_invasion",
               "esophag_substring", "rids_total", n[1])

    log("  Step 3: classify per-RID positive vs negated")
    con.execute("DROP TABLE IF EXISTS _esoph_llm")
    con.execute("""
        CREATE TEMP TABLE _esoph_llm AS
        SELECT
            research_id,
            BOOL_OR(COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')) AS has_positive,
            BOOL_OR(COALESCE(present_or_negated, '') IN ('negated', 'absent')) AS has_negative,
            MIN(CASE WHEN COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')
                     THEN TRY_CAST(note_date AS DATE) END) AS first_positive_date,
            MIN(CASE WHEN COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')
                     THEN CAST(note_row_id AS VARCHAR) END) AS first_positive_note_ref,
            COUNT(*) FILTER (WHERE COALESCE(present_or_negated, '') NOT IN ('negated', 'absent'))
                AS n_notes_documenting
          FROM _esoph_llm_raw
         GROUP BY research_id
    """)
    n = con.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN has_positive THEN 1 ELSE 0 END),
               SUM(CASE WHEN has_negative AND NOT has_positive THEN 1 ELSE 0 END)
          FROM _esoph_llm
    """).fetchone()
    log(f"    LLM classified: rids={n[0]} positive={n[1]} negated_only={n[2]}")
    log_metric(con, "source", "note_entities_llm_airway_invasion",
               "esophag_positive_only", "rids_positive", n[1] or 0)

    log("  Step 4: build per-RID union (positive evidence wins)")
    con.execute("DROP TABLE IF EXISTS _esoph_union")
    con.execute("""
        CREATE TEMP TABLE _esoph_union AS
        SELECT
            COALESCE(o.research_id, l.research_id) AS research_id,
            (o.research_id IS NOT NULL OR l.has_positive = TRUE) AS final_positive,
            (l.has_negative = TRUE AND COALESCE(l.has_positive, FALSE) = FALSE
                AND o.research_id IS NULL) AS llm_negated_only,
            COALESCE(o.evidence_date, l.first_positive_date) AS first_positive_date,
            COALESCE(o.source_note_ref, l.first_positive_note_ref) AS first_positive_note_ref,
            CASE WHEN o.research_id IS NOT NULL THEN 'note_entities_operative_detail'
                 WHEN l.has_positive THEN 'note_entities_llm_airway_invasion'
                 ELSE 'note_entities_llm_airway_invasion(negated)' END AS source_table,
            COALESCE(l.n_notes_documenting, 0)
                + CASE WHEN o.research_id IS NOT NULL THEN 1 ELSE 0 END AS n_notes_documenting
          FROM _esoph_op o
          FULL OUTER JOIN _esoph_llm l USING (research_id)
    """)
    pos = con.execute(
        "SELECT COUNT(*) FROM _esoph_union WHERE final_positive"
    ).fetchone()[0]
    neg_only = con.execute(
        "SELECT COUNT(*) FROM _esoph_union WHERE llm_negated_only AND NOT final_positive"
    ).fetchone()[0]
    log(f"    union: positive RIDs={pos} negated-only RIDs={neg_only}")
    log_metric(con, "merged", "_esoph_union", None, "positive_rids", pos)
    log_metric(con, "merged", "_esoph_union", None, "negated_only_rids", neg_only)

    log("  Step 5: build FALSE set = patients with op-note evidence in any source"
        " but no positive esophag mention")
    con.execute("DROP TABLE IF EXISTS _opnote_evidence_rids")
    con.execute("""
        CREATE TEMP TABLE _opnote_evidence_rids AS
        SELECT DISTINCT CAST(research_id AS VARCHAR) AS research_id
          FROM main.note_entities_operative_detail
        UNION
        SELECT DISTINCT CAST(research_id AS VARCHAR)
          FROM main.note_entities_llm_airway_invasion
         WHERE result_json IS NOT NULL
        UNION
        SELECT DISTINCT CAST(research_id AS VARCHAR)
          FROM main.operative_episode_detail_v2
    """)
    n_op_evidence = con.execute(
        "SELECT COUNT(*) FROM _opnote_evidence_rids"
    ).fetchone()[0]
    log(f"    RIDs with any op-note evidence: {n_op_evidence}")

    if not args.commit:
        log("  (dry-run) — no UPDATE; skipping companion-column add and CPM write")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run) re-run with --commit to apply.")
        return

    log("  Step 6: add Constraint-7 companion columns if missing")
    for col, dtype in [
        ("op_esophageal_inv_first_date", "DATE"),
        ("op_esophageal_inv_first_source_note_ref", "VARCHAR"),
        ("op_esophageal_inv_first_evidence_text", "VARCHAR"),
        ("op_esophageal_inv_source_table", "VARCHAR"),
        ("op_esophageal_inv_n_notes_documenting", "INTEGER"),
    ]:
        exists = con.execute(f"""
            SELECT COUNT(*) FROM information_schema.columns
             WHERE table_name='canonical_patient_master' AND column_name='{col}'
        """).fetchone()[0]
        if exists == 0:
            con.execute(
                f'ALTER TABLE main.canonical_patient_master ADD COLUMN "{col}" {dtype}'
            )
            log(f"    added CPM column: {col} {dtype}")

    log("  Step 7: UPDATE CPM op_esophageal_inv_any TRUE for positive RIDs")
    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET op_esophageal_inv_any = TRUE,
               op_esophageal_inv_first_date = u.first_positive_date,
               op_esophageal_inv_first_source_note_ref = u.first_positive_note_ref,
               op_esophageal_inv_source_table = u.source_table,
               op_esophageal_inv_n_notes_documenting = CAST(u.n_notes_documenting AS INTEGER)
          FROM _esoph_union u
         WHERE CAST(c.research_id AS VARCHAR) = u.research_id
           AND u.final_positive
    """)
    n_true = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master
         WHERE op_esophageal_inv_any = TRUE
    """).fetchone()[0]
    log(f"    set TRUE: {n_true}")

    log("  Step 8: UPDATE CPM op_esophageal_inv_any FALSE for op-note evidence "
        "without positive esoph mention")
    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET op_esophageal_inv_any = FALSE
          FROM _opnote_evidence_rids r
         WHERE CAST(c.research_id AS VARCHAR) = r.research_id
           AND c.op_esophageal_inv_any IS NULL
    """)

    log("  Step 9: sync operative_episode_detail_v2.esophageal_involvement_flag")
    # TRUE per episode where the airway-invasion LLM source note_row_id is in
    # this episode's date window, OR an operative-entity esophag entity falls
    # within the episode bounds. FALSE per episode otherwise (only when episode
    # has any op-note evidence). NULL only if episode has no evidence at all.
    con.execute("DROP TABLE IF EXISTS _episode_esoph")
    con.execute("""
        CREATE TEMP TABLE _episode_esoph AS
        WITH true_per_episode AS (
            SELECT DISTINCT ep.surgery_episode_id
              FROM main.operative_episode_detail_v2 ep
              JOIN main.note_entities_llm_airway_invasion lai
                ON CAST(lai.research_id AS VARCHAR) = CAST(ep.research_id AS VARCHAR)
               AND TRY_CAST(lai.note_date AS DATE)
                   BETWEEN COALESCE(TRY_CAST(ep.resolved_surgery_date AS DATE),
                                    CAST(ep.surgery_date_native AS DATE)) - 7
                       AND COALESCE(TRY_CAST(ep.resolved_surgery_date AS DATE),
                                    CAST(ep.surgery_date_native AS DATE)) + 30
              JOIN _esoph_llm el ON el.research_id = CAST(ep.research_id AS VARCHAR)
             WHERE el.has_positive = TRUE
               AND CAST(lai.note_row_id AS VARCHAR) = el.first_positive_note_ref
            UNION
            SELECT DISTINCT ep.surgery_episode_id
              FROM main.operative_episode_detail_v2 ep
              JOIN _esoph_op eo
                ON eo.research_id = CAST(ep.research_id AS VARCHAR)
               AND eo.evidence_date
                   BETWEEN COALESCE(TRY_CAST(ep.resolved_surgery_date AS DATE),
                                    CAST(ep.surgery_date_native AS DATE)) - 7
                       AND COALESCE(TRY_CAST(ep.resolved_surgery_date AS DATE),
                                    CAST(ep.surgery_date_native AS DATE)) + 30
        )
        SELECT surgery_episode_id, TRUE AS is_true FROM true_per_episode
    """)
    con.execute("""
        UPDATE main.operative_episode_detail_v2 AS ep
           SET esophageal_involvement_flag = TRUE
          FROM _episode_esoph t
         WHERE ep.surgery_episode_id = t.surgery_episode_id
    """)
    # FALSE for any episode whose patient has any op-note evidence and we did
    # not set TRUE, but only where currently NULL.
    con.execute("""
        UPDATE main.operative_episode_detail_v2 AS ep
           SET esophageal_involvement_flag = FALSE
          FROM _opnote_evidence_rids r
         WHERE CAST(ep.research_id AS VARCHAR) = r.research_id
           AND ep.esophageal_involvement_flag IS NULL
    """)

    snapshot_state(con, "post")
    cpm_invariants(con, "post")
    log("=" * 72)
    log("Script 342 complete.")


if __name__ == "__main__":
    main()
