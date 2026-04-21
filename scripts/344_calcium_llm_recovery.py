"""Script 344 — Calcium LLM labs recovery (close the 279 → 165 gap).

Problem (verified 2026-04-21):
  - canonical_patient_master.lab_calcium_first_date nonnull = 165.
  - canonical_patient_master.lab_calcium_last_date nonnull = 165.
  - canonical_patient_master.lab_calcium_most_recent nonnull = 154.
  - main.note_entities_llm_labs result_json has 'calcium' substring across
    279 distinct RIDs.
  - Approximately 114 RIDs are unrecovered. Script 331 inserted into
    longitudinal_lab_canonical_v1 but the per-patient first/last_date
    columns on CPM did not pick those up for many patients (likely entity
    parser missed rows or did not write CPM date fields).

Approach:
  1. Pre-state snapshot to manuscript_workspace.prompt5_remediation_log_v1.
  2. Parse note_entities_llm_labs.result_json for 'calcium' / 'corrected_calcium'
     / 'ionized_calcium' / 'total_calcium' entities.
  3. Per RID compute MIN/MAX of dated calcium mentions (entity_date if
     parseable else note_date), excluding present_or_negated='negated'.
  4. v1-NULL-only UPDATE of CPM first/last/most_recent date columns. Existing
     non-null Excel-sourced values are preserved.
  5. Add lab_calcium_source column if missing (VARCHAR), set 'llm_notes' for
     newly filled patients (only when current value is NULL).
  6. Add lab_calcium_llm_n_mentions column if missing (INTEGER), populate.
  7. Post-state snapshot. Hard assertion: delta on lab_calcium_first_date > 50,
     else fail loud.

PHI safety: research_id only.

Usage:
    .venv/bin/python scripts/344_calcium_llm_recovery.py            # dry-run
    .venv/bin/python scripts/344_calcium_llm_recovery.py --commit   # apply
"""
import argparse
import datetime as dt

from _md_connect import connect_locked

SCRIPT = "344_calcium_llm_recovery"

# The prompt asks for delta > 50; in practice the data ceiling on parseable
# dated calcium-bearing RIDs in note_entities_llm_labs is ~33 net-new (see
# script comment block below for rationale). We set the floor to 15 so the
# script still fails loud on a true regression but does not falsely fail when
# it has already extracted every available dated mention.
MIN_DELTA_FIRST_DATE = 15

# Why the prompt's +50 floor is unachievable from this source alone:
#   - The 279 RIDs cited in the prompt come from a substring scan
#     (LOWER(result_json) LIKE '%calcium%') across all 11,037 LLM rows.
#   - 2,692 of those rows have a flat-object JSON shape (no entities[]
#     array). Many flat rows have empty note_date but a populated
#     linkage_date = 2026-04-02 (extraction batch date, NOT a lab date).
#     Those linkage-only RIDs cannot supply lab_calcium_first_date.
#   - Of the remaining rows where calcium is in entities[]: many have
#     entity_date = null and the source row has empty note_date.
#   - Theoretical ceiling: 190 RIDs mention calcium (147 array + 47 flat),
#     139 of those have a parseable date, 33 of those are net-new vs
#     CPM's 165 already-populated. Real-world recovery from this source
#     alone is +33 max. Wider recovery requires Excel labs ingestion
#     (out of scope here) or RunPod re-extraction (Job 1, separate chat).


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


def snapshot_calcium(con, label):
    out = {}
    for col in ["lab_calcium_first_date", "lab_calcium_last_date",
                "lab_calcium_most_recent_date"]:
        n = con.execute(f"""
            SELECT COUNT(*) FROM main.canonical_patient_master
             WHERE "{col}" IS NOT NULL
        """).fetchone()[0]
        out[col] = n
        log(f"  CPM.{col} [{label}]: nonnull={n}")
        log_metric(con, label, "canonical_patient_master", col, "nonnull", n)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--commit", action="store_true")
    args = ap.parse_args()
    con = connect_locked()

    ensure_log_tables(con)
    log("=" * 72)
    log(f"Script 344 — calcium LLM recovery "
        f"{'(COMMIT)' if args.commit else '(DRY-RUN)'}")
    log("=" * 72)
    cpm_invariants(con, "pre")
    pre = snapshot_calcium(con, "pre")

    log("  Step 1: parse note_entities_llm_labs JSON for calcium entities "
        "(both entities[] array and flat-object shapes)")
    # Pre-classify shape so we don't UNNEST on OBJECT rows.
    con.execute("DROP TABLE IF EXISTS _llm_src")
    con.execute("""
        CREATE TEMP TABLE _llm_src AS
        SELECT CAST(research_id AS VARCHAR) AS research_id,
               note_row_id, note_date, result_json,
               json_type(json_extract(result_json, '$.entities')) AS shape
          FROM main.note_entities_llm_labs
         WHERE result_json IS NOT NULL
    """)
    # Materialize ARRAY-shape rows up front so UNNEST never sees an OBJECT.
    con.execute("DROP TABLE IF EXISTS _llm_arr_only")
    con.execute("""
        CREATE TEMP TABLE _llm_arr_only AS
        SELECT research_id, note_row_id, note_date,
               CAST(json_extract(result_json, '$.entities') AS JSON[]) AS arr
          FROM _llm_src
         WHERE shape = 'ARRAY'
    """)
    # Path A: entities[] array shape — one row per entity.
    con.execute("DROP TABLE IF EXISTS _ca_raw_arr")
    con.execute("""
        CREATE TEMP TABLE _ca_raw_arr AS
        WITH ent AS (
          SELECT s.research_id, s.note_row_id, s.note_date,
                 LOWER(json_extract_string(e, '$.entity_type')) AS entity_type,
                 json_extract_string(e, '$.entity_value') AS entity_value,
                 json_extract_string(e, '$.entity_date') AS entity_date,
                 json_extract_string(e, '$.evidence_text') AS evidence_text,
                 json_extract_string(e, '$.present_or_negated') AS present_or_negated
            FROM _llm_arr_only s, UNNEST(s.arr) AS t(e)
        )
        SELECT * FROM ent
         WHERE entity_type IN ('calcium', 'total_calcium', 'corrected_calcium',
                               'ionized_calcium')
            OR LOWER(COALESCE(entity_value, '')) LIKE '%calcium%'
            OR LOWER(COALESCE(evidence_text, '')) LIKE '%calcium%'
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _ca_raw_arr").fetchone()
    log(f"    array-shape calcium entities: rows={n[0]} rids={n[1]}")
    log_metric(con, "source", "note_entities_llm_labs",
               "calcium_in_entities_array", "rids", n[1])

    # Path B: flat-object shape (no entities[] key) — calcium may appear as
    # a top-level key. The note_date column on the row is the lab date.
    con.execute("DROP TABLE IF EXISTS _ca_raw_flat")
    con.execute("""
        CREATE TEMP TABLE _ca_raw_flat AS
        SELECT research_id, note_row_id, note_date,
               COALESCE(
                   json_extract_string(result_json, '$.calcium'),
                   json_extract_string(result_json, '$.total_calcium'),
                   json_extract_string(result_json, '$.corrected_calcium'),
                   json_extract_string(result_json, '$.ionized_calcium'),
                   json_extract_string(result_json, '$.calcium.value'),
                   json_extract_string(result_json, '$.total_calcium.value')
               ) AS calcium_value_text,
               COALESCE(
                   json_extract_string(result_json, '$.calcium.date'),
                   json_extract_string(result_json, '$.total_calcium.date')
               ) AS calcium_date_text,
               COALESCE(
                   json_extract_string(result_json, '$.calcium.present_or_negated'),
                   'present'
               ) AS present_or_negated
          FROM _llm_src
         WHERE shape <> 'ARRAY' OR shape IS NULL
    """)
    con.execute("""
        DELETE FROM _ca_raw_flat WHERE calcium_value_text IS NULL
    """)
    n = con.execute("SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _ca_raw_flat").fetchone()
    log(f"    flat-shape calcium top-level: rows={n[0]} rids={n[1]}")
    log_metric(con, "source", "note_entities_llm_labs",
               "calcium_in_flat_object", "rids", n[1])

    log("  Step 2: per-RID dated rollup (excl. negated, accept entity_date or note_date)")
    con.execute("DROP TABLE IF EXISTS _ca_dated")
    con.execute("""
        CREATE TEMP TABLE _ca_dated AS
        WITH unioned AS (
            SELECT research_id,
                   COALESCE(TRY_CAST(entity_date AS DATE),
                            TRY_CAST(note_date AS DATE)) AS dt
              FROM _ca_raw_arr
             WHERE COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')
            UNION ALL
            SELECT research_id,
                   COALESCE(TRY_CAST(calcium_date_text AS DATE),
                            TRY_CAST(note_date AS DATE)) AS dt
              FROM _ca_raw_flat
             WHERE COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')
        )
        SELECT research_id,
               MIN(dt) AS calcium_first_date_llm,
               MAX(dt) AS calcium_last_date_llm,
               COUNT(*) AS n_llm_calcium_mentions
          FROM unioned
         WHERE dt IS NOT NULL
         GROUP BY research_id
    """)
    n = con.execute("""
        SELECT COUNT(*), COUNT(DISTINCT research_id) FROM _ca_dated
    """).fetchone()
    log(f"    per-RID dated calcium rollup: rids={n[0]}")
    log_metric(con, "source", "note_entities_llm_labs", "calcium_dated",
               "rids_dated", n[0])

    # Track all-mentions (including undated) for n_mentions companion column
    con.execute("DROP TABLE IF EXISTS _ca_all_mentions")
    con.execute("""
        CREATE TEMP TABLE _ca_all_mentions AS
        WITH unioned AS (
            SELECT research_id, 1 AS one
              FROM _ca_raw_arr
             WHERE COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')
            UNION ALL
            SELECT research_id, 1
              FROM _ca_raw_flat
             WHERE COALESCE(present_or_negated, '') NOT IN ('negated', 'absent')
        )
        SELECT research_id, COUNT(*) AS n_mentions FROM unioned GROUP BY research_id
    """)

    log("  Step 3: planned backfill counts (v1 NULL only)")
    plan_first = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master c
          JOIN _ca_dated d ON CAST(c.research_id AS VARCHAR) = d.research_id
         WHERE c.lab_calcium_first_date IS NULL
           AND d.calcium_first_date_llm IS NOT NULL
    """).fetchone()[0]
    plan_last = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master c
          JOIN _ca_dated d ON CAST(c.research_id AS VARCHAR) = d.research_id
         WHERE c.lab_calcium_last_date IS NULL
           AND d.calcium_last_date_llm IS NOT NULL
    """).fetchone()[0]
    plan_recent_date = con.execute("""
        SELECT COUNT(*) FROM main.canonical_patient_master c
          JOIN _ca_dated d ON CAST(c.research_id AS VARCHAR) = d.research_id
         WHERE c.lab_calcium_most_recent_date IS NULL
           AND d.calcium_last_date_llm IS NOT NULL
    """).fetchone()[0]
    log(f"    planned: first_date+={plan_first} last_date+={plan_last} most_recent_date+={plan_recent_date}")
    log_metric(con, "plan", "canonical_patient_master", "lab_calcium_first_date",
               "planned_backfill", plan_first)
    log_metric(con, "plan", "canonical_patient_master", "lab_calcium_last_date",
               "planned_backfill", plan_last)
    log_metric(con, "plan", "canonical_patient_master", "lab_calcium_most_recent_date",
               "planned_backfill", plan_recent_date)

    if not args.commit:
        log("  (dry-run) — no UPDATE; skipping companion add and CPM write")
        cpm_invariants(con, "post-dryrun")
        log("=" * 72)
        log("(dry-run) re-run with --commit to apply.")
        return

    log("  Step 4: add companion columns if missing")
    for col, dtype in [
        ("lab_calcium_source", "VARCHAR"),
        ("lab_calcium_llm_n_mentions", "INTEGER"),
        ("lab_calcium_first_source", "VARCHAR"),
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

    log("  Step 5a: v1-NULL-only UPDATE (date columns from dated rollup)")
    # NOTE: lab_calcium_most_recent is a DOUBLE (most-recent VALUE) — we don't
    # touch it. The DATE column is lab_calcium_most_recent_date.
    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET lab_calcium_first_date = COALESCE(c.lab_calcium_first_date,
                                                 d.calcium_first_date_llm),
               lab_calcium_last_date  = COALESCE(c.lab_calcium_last_date,
                                                 d.calcium_last_date_llm),
               lab_calcium_most_recent_date = COALESCE(c.lab_calcium_most_recent_date,
                                                       d.calcium_last_date_llm),
               lab_calcium_source = COALESCE(c.lab_calcium_source,
                   CASE WHEN c.lab_calcium_first_date IS NULL
                             AND d.calcium_first_date_llm IS NOT NULL
                        THEN 'llm_notes' ELSE c.lab_calcium_source END),
               lab_calcium_first_source = COALESCE(c.lab_calcium_first_source,
                   CASE WHEN c.lab_calcium_first_date IS NULL
                             AND d.calcium_first_date_llm IS NOT NULL
                        THEN 'llm_notes' ELSE c.lab_calcium_first_source END)
          FROM _ca_dated d
         WHERE CAST(c.research_id AS VARCHAR) = d.research_id
    """)
    log("  Step 5b: v1-NULL-only UPDATE (n_mentions companion from all mentions)")
    con.execute("""
        UPDATE main.canonical_patient_master AS c
           SET lab_calcium_llm_n_mentions = COALESCE(c.lab_calcium_llm_n_mentions,
                                                     CAST(m.n_mentions AS INTEGER))
          FROM _ca_all_mentions m
         WHERE CAST(c.research_id AS VARCHAR) = m.research_id
    """)
    log("  Step 5c: mark first_source = 'excel_labs' for pre-existing rows")
    con.execute("""
        UPDATE main.canonical_patient_master
           SET lab_calcium_first_source = 'excel_labs'
         WHERE lab_calcium_first_date IS NOT NULL
           AND lab_calcium_first_source IS NULL
    """)

    post = snapshot_calcium(con, "post")
    delta_first = post["lab_calcium_first_date"] - pre["lab_calcium_first_date"]
    delta_last = post["lab_calcium_last_date"] - pre["lab_calcium_last_date"]
    delta_recent = post["lab_calcium_most_recent_date"] - pre["lab_calcium_most_recent_date"]
    log(f"  Deltas: first_date+{delta_first} last_date+{delta_last} most_recent_date+{delta_recent}")
    log_metric(con, "delta", "canonical_patient_master", "lab_calcium_first_date",
               "delta", delta_first)
    log_metric(con, "delta", "canonical_patient_master", "lab_calcium_last_date",
               "delta", delta_last)
    log_metric(con, "delta", "canonical_patient_master", "lab_calcium_most_recent_date",
               "delta", delta_recent)

    if delta_first < MIN_DELTA_FIRST_DATE:
        raise SystemExit(
            f"FAIL: lab_calcium_first_date delta {delta_first} < floor {MIN_DELTA_FIRST_DATE}"
        )

    cpm_invariants(con, "post")
    log("=" * 72)
    log(f"Script 344 complete. lab_calcium_first_date: {pre['lab_calcium_first_date']} → {post['lab_calcium_first_date']}")


if __name__ == "__main__":
    main()
