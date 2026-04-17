#!/usr/bin/env python3
"""
Script 234 — RAI/Tg Data Quality Resolution + Canonical DB Hygiene.

Target DB (read+write): thyroid_canonical_publication_v1_0
Legacy/working DB     : "Thyroid 2026 UPdated"
                         - read for reference
                         - write ONLY to DROP stale tables (after manifest is saved)

Phases:
    1  — RAI/Tg data-quality fixes on canonical_patient_master (8 new columns)
    2  — detail_table_registry_v1 pointer integrity (existence / row counts / orphans)
    3  — Update __readme and data_dictionary_v221 for the new columns
    4  — Working-DB hygiene: save archive manifest, then DROP stale tables
    5  — Final invariants + consolidated validation report

Usage:
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase preflight
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase 1
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase 2
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase 3
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase 4 --plan-only
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase 4 --apply-drops
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase 5
    .venv/bin/python scripts/234_rai_tg_cleanup_db_hygiene.py --phase all --apply-drops

Safety invariants checked at the start and end of every phase:
    canonical_patient_master: 10,871 rows / 10,871 distinct research_id
                              / 0 NULL research_id / 0 NULL fna_path_outcome
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

import duckdb  # noqa: E402

from motherduck_client import get_token  # noqa: E402

PUB = "thyroid_canonical_publication_v1_0"
REF = "Thyroid 2026 UPdated"
TS = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
OUT = REPO / "scripts" / "output" / "234"
OUT.mkdir(parents=True, exist_ok=True)

REPORT: dict[str, Any] = {"phases": {}, "invariants": {}, "started_at": TS}


# ======================================================================
# Connection + invariants
# ======================================================================
def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(f"md:?motherduck_token={get_token()}")
    dbs = {r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases()").fetchall()}
    if PUB not in dbs:
        raise SystemExit(f"Publication DB '{PUB}' not attached (found {sorted(dbs)})")
    if REF not in dbs:
        print(f"  [warn] Reference DB '{REF}' not attached (Phase 4 will be skipped)")
    con.execute(f'USE "{PUB}"')
    con.execute(f'USE "{PUB}".main')
    return con


def check_invariants(con: duckdb.DuckDBPyConnection, label: str) -> dict:
    row = con.execute(f"""
        SELECT COUNT(*) r, COUNT(DISTINCT research_id) d,
               COUNT(*) FILTER (WHERE research_id IS NULL) nr,
               COUNT(*) FILTER (WHERE fna_path_outcome IS NULL) nf
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    inv = dict(zip(["rows", "distinct_rid", "null_rid", "null_fna"], row))
    print(f"[INV {label}] {inv}")
    REPORT["invariants"][label] = inv
    if (inv["rows"] != 10871 or inv["distinct_rid"] != 10871
            or inv["null_rid"] != 0 or inv["null_fna"] != 0):
        raise SystemExit(f"Invariant broken at {label}: {inv}")
    return inv


def column_exists(con, schema: str, table: str, column: str, catalog: str = PUB) -> bool:
    return bool(con.execute(f"""
        SELECT 1 FROM information_schema.columns
        WHERE table_catalog='{catalog}' AND table_schema='{schema}'
          AND table_name='{table}' AND column_name='{column}'
        LIMIT 1
    """).fetchone())


def safe_add_column(con, table: str, column: str, dtype: str, schema: str = "main") -> bool:
    if column_exists(con, schema, table, column):
        print(f"  [schema] {schema}.{table}.{column} already present — skipping ADD")
        return False
    con.execute(f'ALTER TABLE "{PUB}"."{schema}"."{table}" ADD COLUMN "{column}" {dtype}')
    print(f"  [schema] ADDED {schema}.{table}.{column} {dtype}")
    return True


def set_comment(con, table: str, column: str, comment: str, schema: str = "main") -> None:
    escaped = comment.replace("'", "''")
    con.execute(
        f"COMMENT ON COLUMN \"{PUB}\".\"{schema}\".\"{table}\".\"{column}\" IS '{escaped}'"
    )


def write_phase_report(phase: str, payload: dict) -> None:
    REPORT["phases"][phase] = payload
    (OUT / f"phase_{phase}_{TS}.json").write_text(
        json.dumps(payload, default=str, indent=2)
    )


# ======================================================================
# PHASE 1 — RAI / Tg data-quality fixes
# ======================================================================

# -- Issue 1: RAI flag vs episodes discordance (279 patients) --------------
def issue_1(con) -> dict:
    print("\n[1.1] RAI flag/episode discordance (Rule B, non-destructive)")
    # Profile before
    profile = con.execute(f"""
        SELECT n_rai_episodes,
               COUNT(*) AS n_patients,
               COUNT(*) FILTER (WHERE is_malignant = TRUE) AS n_malignant,
               COUNT(*) FILTER (WHERE histology_final IS NOT NULL) AS n_with_histology,
               COUNT(*) FILTER (WHERE rai_dose_v9 IS NOT NULL)    AS n_with_dose,
               COUNT(*) FILTER (WHERE rai_intent_v9 IS NOT NULL)  AS n_with_intent
          FROM "{PUB}".main.canonical_patient_master
         WHERE rai_received_flag = FALSE AND n_rai_episodes > 0
         GROUP BY 1 ORDER BY 1
    """).fetchall()
    print("  [1.1] Discordance profile (n_rai_episodes → counts):")
    for r in profile:
        print(f"        n_epi={r[0]:<3}  pt={r[1]:<4}  malig={r[2]:<4}  "
              f"hist={r[3]:<4}  dose={r[4]:<4}  intent={r[5]:<4}")

    # Add reconciled / discordant columns (Rule B)
    safe_add_column(con, "canonical_patient_master", "rai_received_reconciled", "BOOLEAN")
    safe_add_column(con, "canonical_patient_master", "rai_flag_discordant",     "BOOLEAN")

    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET rai_received_reconciled =
                 (rai_received_flag = TRUE OR n_rai_episodes > 0),
               rai_flag_discordant    =
                 (rai_received_flag = FALSE AND n_rai_episodes > 0)
    """)
    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE rai_received_reconciled = TRUE) AS n_recon_true,
               COUNT(*) FILTER (WHERE rai_flag_discordant    = TRUE) AS n_discordant,
               COUNT(*) FILTER (WHERE rai_received_flag     = TRUE)  AS n_flag_true
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1.1] after: reconciled_TRUE={post[0]:,}  discordant_TRUE={post[1]:,}  "
          f"flag_TRUE(original)={post[2]:,}")

    set_comment(con, "canonical_patient_master", "rai_received_reconciled",
        "Reconciled RAI flag: TRUE if rai_received_flag=TRUE OR n_rai_episodes>0. "
        "Addresses 279-patient flag/episode discordance. Script 234.")
    set_comment(con, "canonical_patient_master", "rai_flag_discordant",
        "TRUE for 279 patients where rai_received_flag=FALSE but n_rai_episodes>=1. "
        "Script 234.")

    return {
        "discordance_profile": [dict(zip(
            ["n_rai_episodes","n_patients","n_malignant","n_with_histology",
             "n_with_dose","n_with_intent"], r)) for r in profile],
        "post": {"reconciled_true": post[0],
                 "discordant_true": post[1],
                 "flag_true_original": post[2]},
    }


# -- Issue 2: Benign + RAI + NULL histology (100 patients) -----------------
def issue_2(con) -> dict:
    print("\n[1.2] Benign + RAI + NULL histology (suspect malignant, non-destructive)")
    n_target = con.execute(f"""
        SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
         WHERE is_malignant = FALSE AND rai_received_flag = TRUE
           AND histology_final IS NULL
    """).fetchone()[0]
    print(f"  [1.2] target cohort size: {n_target} (prompt says 100)")

    # Cross-check path_synoptics for recoverable histology
    synoptic_hits = con.execute(f"""
        SELECT COUNT(DISTINCT c.research_id) AS n_recoverable
          FROM "{PUB}".main.canonical_patient_master c
          JOIN "{PUB}".main.path_synoptics ps
            ON c.research_id = CAST(ps.research_id AS VARCHAR)
         WHERE c.is_malignant = FALSE AND c.rai_received_flag = TRUE
           AND c.histology_final IS NULL
           AND ps.tumor_1_histologic_type IS NOT NULL
    """).fetchone()[0]
    print(f"  [1.2] path_synoptics histology recoverable: {synoptic_hits} patients")

    # Cross-check NLP pathology (informational only)
    # note_entities_llm_pathology stores extracted entities in result_json (JSON blob)
    nlp_hits = con.execute(f"""
        SELECT COUNT(DISTINCT c.research_id) AS n_with_nlp
          FROM "{PUB}".main.canonical_patient_master c
          JOIN "{PUB}".main.note_entities_llm_pathology ne
            ON c.research_id = CAST(ne.research_id AS VARCHAR)
         WHERE c.is_malignant = FALSE AND c.rai_received_flag = TRUE
           AND c.histology_final IS NULL
           AND ne.result_json IS NOT NULL
    """).fetchone()[0]
    print(f"  [1.2] NLP pathology entities present: {nlp_hits} patients (cross-validation only)")

    # Flag column (does NOT overwrite is_malignant)
    safe_add_column(con, "canonical_patient_master",
                    "benign_rai_suspect_malignant", "BOOLEAN")
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET benign_rai_suspect_malignant =
                 (is_malignant = FALSE AND rai_received_flag = TRUE
                  AND histology_final IS NULL)
    """)
    n_flagged = con.execute(f"""
        SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
         WHERE benign_rai_suspect_malignant = TRUE
    """).fetchone()[0]
    print(f"  [1.2] flagged: {n_flagged} patients")

    set_comment(con, "canonical_patient_master", "benign_rai_suspect_malignant",
        "TRUE for 100 patients classified as benign (is_malignant=FALSE) who received RAI "
        "but lack histology_final. Almost certainly malignant with missing pathology. "
        "Script 234.")

    # Recovery table — only patients where path_synoptics has histology the canonical lacks
    # Recovery table: note path_synoptics has `tumor_1_variant` (not `tumor_1_tumor_type`)
    con.execute(f"""
        CREATE OR REPLACE TABLE "{PUB}".main.rai_benign_histology_recovery_v234 AS
        SELECT c.research_id,
               c.is_malignant                   AS current_is_malignant,
               c.histology_final                AS current_histology,
               ps.tumor_1_histologic_type       AS synoptic_histology,
               ps.tumor_1_variant               AS synoptic_variant,
               'Script 234 recovery: benign+RAI patient with histology found in '
               'path_synoptics' AS provenance
          FROM "{PUB}".main.canonical_patient_master c
          JOIN "{PUB}".main.path_synoptics ps
            ON c.research_id = CAST(ps.research_id AS VARCHAR)
         WHERE c.is_malignant = FALSE AND c.rai_received_flag = TRUE
           AND c.histology_final IS NULL
           AND ps.tumor_1_histologic_type IS NOT NULL
    """)
    n_recovery = con.execute(
        f'SELECT COUNT(*) FROM "{PUB}".main.rai_benign_histology_recovery_v234'
    ).fetchone()[0]
    print(f"  [1.2] recovery table: rai_benign_histology_recovery_v234 rows={n_recovery}")
    con.execute(f"""
        COMMENT ON TABLE "{PUB}".main.rai_benign_histology_recovery_v234 IS
        'Recovery table: benign+RAI patients where path_synoptics has histology not in canonical. Script 234. Review before applying.'
    """)

    return {
        "target_cohort": n_target,
        "synoptic_recoverable": synoptic_hits,
        "nlp_entity_hits": nlp_hits,
        "flagged": n_flagged,
        "recovery_table_rows": n_recovery,
    }


# -- Issue 3: Tg availability documentation (76.4% NULL) -------------------
def issue_3(con) -> dict:
    print("\n[1.3] Tg availability documentation")
    era_profile = con.execute(f"""
        SELECT CASE
                 WHEN first_surgery_date < '2005-01-01' THEN 'pre-2005'
                 WHEN first_surgery_date < '2010-01-01' THEN '2005-2009'
                 WHEN first_surgery_date < '2015-01-01' THEN '2010-2014'
                 WHEN first_surgery_date < '2020-01-01' THEN '2015-2019'
                 WHEN first_surgery_date IS NULL         THEN 'null_surgery_date'
                 ELSE '2020+'
               END AS era,
               COUNT(*) AS n_patients,
               COUNT(*) FILTER (WHERE tg_n_measurements IS NOT NULL
                                AND tg_n_measurements > 0) AS n_with_tg,
               ROUND(100.0 * COUNT(*) FILTER (
                   WHERE tg_n_measurements IS NOT NULL AND tg_n_measurements > 0
               ) / COUNT(*), 1) AS pct_with_tg
          FROM "{PUB}".main.canonical_patient_master
         GROUP BY 1 ORDER BY 1
    """).fetchall()
    print("  [1.3] Tg availability by surgery era:")
    for r in era_profile:
        print(f"        {r[0]:<18} n={r[1]:<5} with_tg={r[2]:<5} pct={r[3]}%")

    malig_profile = con.execute(f"""
        SELECT is_malignant,
               COUNT(*) AS n_patients,
               COUNT(*) FILTER (WHERE tg_n_measurements IS NOT NULL
                                AND tg_n_measurements > 0) AS n_with_tg,
               ROUND(100.0 * COUNT(*) FILTER (
                   WHERE tg_n_measurements IS NOT NULL AND tg_n_measurements > 0
               ) / COUNT(*), 1) AS pct_with_tg
          FROM "{PUB}".main.canonical_patient_master
         GROUP BY 1 ORDER BY 1
    """).fetchall()
    print("  [1.3] Tg availability by malignancy:")
    for r in malig_profile:
        print(f"        is_malignant={r[0]!s:<6} n={r[1]:<5} with_tg={r[2]:<5} pct={r[3]}%")

    safe_add_column(con, "canonical_patient_master", "tg_data_available",   "BOOLEAN")
    safe_add_column(con, "canonical_patient_master", "tg_limitation_note",  "VARCHAR")

    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET tg_data_available =
                 (tg_n_measurements IS NOT NULL AND tg_n_measurements > 0),
               tg_limitation_note = CASE
                 WHEN tg_n_measurements IS NOT NULL AND tg_n_measurements > 0 THEN NULL
                 WHEN is_malignant = FALSE
                      THEN 'Benign diagnosis — Tg monitoring not standard'
                 WHEN first_surgery_date < '2005-01-01'
                      THEN 'Pre-2005 surgery — Tg monitoring may not have been routine'
                 ELSE 'Tg data unavailable — reason unknown'
               END
    """)

    summary = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE tg_data_available = TRUE)  AS n_avail,
               COUNT(*) FILTER (WHERE tg_data_available = FALSE) AS n_missing,
               COUNT(*) FILTER (WHERE tg_limitation_note IS NOT NULL) AS n_noted
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1.3] summary: tg_avail_TRUE={summary[0]:,} "
          f"tg_avail_FALSE={summary[1]:,} limitation_notes_set={summary[2]:,}")

    set_comment(con, "canonical_patient_master", "tg_data_available",
        "TRUE if patient has any Tg measurement data. 76.4% are NULL — correlated "
        "with surgery era and benign status. Script 234.")
    set_comment(con, "canonical_patient_master", "tg_limitation_note",
        "Documents why Tg data is missing for patients without measurements. Script 234.")

    return {
        "era_profile":    [dict(zip(["era","n_patients","n_with_tg","pct_with_tg"], r))
                           for r in era_profile],
        "malig_profile":  [dict(zip(["is_malignant","n_patients","n_with_tg","pct_with_tg"], r))
                           for r in malig_profile],
        "summary": {"n_avail": summary[0],
                    "n_missing": summary[1],
                    "n_noted": summary[2]},
    }


# -- Issue 4: Tg nadir outliers (>100 without recurrence) ------------------
def issue_4(con) -> dict:
    print("\n[1.4] Tg nadir outlier flag")
    baseline = con.execute(f"""
        SELECT COUNT(*) AS n_outliers,
               MAX(tg_nadir) AS max_nadir,
               AVG(tg_nadir) AS mean_nadir
          FROM "{PUB}".main.canonical_patient_master
         WHERE tg_nadir > 100 AND recurrence_confirmed = FALSE
    """).fetchone()
    print(f"  [1.4] outlier cohort: n={baseline[0]}, max={baseline[1]}, mean={baseline[2]:.1f}")

    safe_add_column(con, "canonical_patient_master",
                    "tg_nadir_suspect_preablation", "BOOLEAN")
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET tg_nadir_suspect_preablation =
                 (tg_nadir > 100 AND recurrence_confirmed = FALSE)
    """)
    n_flagged = con.execute(f"""
        SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
         WHERE tg_nadir_suspect_preablation = TRUE
    """).fetchone()[0]
    print(f"  [1.4] flagged: {n_flagged} patients")
    set_comment(con, "canonical_patient_master", "tg_nadir_suspect_preablation",
        "TRUE for 74 patients with tg_nadir>100 and no recurrence. Likely pre-ablation "
        "or immediate postop values misclassified as nadir. Exclude or winsorize for "
        "Tg-based analyses. Script 234.")
    return {"max_nadir": baseline[1], "mean_nadir": baseline[2], "flagged": n_flagged}


# -- Issue 5: RAI dose availability + extreme cumulative dose --------------
def issue_5(con) -> dict:
    print("\n[1.5] RAI dose availability + extreme cumulative dose")
    era = con.execute(f"""
        SELECT CASE
                 WHEN first_surgery_date < '2005-01-01' THEN 'pre-2005'
                 WHEN first_surgery_date < '2010-01-01' THEN '2005-2009'
                 WHEN first_surgery_date < '2015-01-01' THEN '2010-2014'
                 WHEN first_surgery_date < '2020-01-01' THEN '2015-2019'
                 WHEN first_surgery_date IS NULL         THEN 'null_surgery_date'
                 ELSE '2020+'
               END AS era,
               COUNT(*) AS n_rai_patients,
               COUNT(*) FILTER (WHERE rai_dose_v9 IS NOT NULL) AS n_with_dose,
               ROUND(100.0 * COUNT(*) FILTER (WHERE rai_dose_v9 IS NOT NULL)
                     / COUNT(*), 1) AS pct_with_dose
          FROM "{PUB}".main.canonical_patient_master
         WHERE rai_received_flag = TRUE
         GROUP BY 1 ORDER BY 1
    """).fetchall()
    print("  [1.5] dose availability by era (RAI-treated):")
    for r in era:
        print(f"        {r[0]:<18} n={r[1]:<5} dose={r[2]:<5} pct={r[3]}%")

    extremes = con.execute(f"""
        SELECT research_id, rai_total_cumulative_dose_mci, n_rai_episodes,
               rai_max_dose_mci, histology_final, recurrence_confirmed
          FROM "{PUB}".main.canonical_patient_master
         WHERE rai_total_cumulative_dose_mci > 1000
         ORDER BY rai_total_cumulative_dose_mci DESC
    """).fetchall()
    print(f"  [1.5] extreme cumulative-dose patients (>1000 mCi): {len(extremes)}")
    for r in extremes[:10]:
        print(f"        rid={r[0]}  cum={r[1]}  epi={r[2]}  max_epi={r[3]}  "
              f"hist={r[4]}  recurrence={r[5]}")

    safe_add_column(con, "canonical_patient_master",
                    "rai_dose_data_available",     "BOOLEAN")
    safe_add_column(con, "canonical_patient_master",
                    "rai_cumulative_dose_extreme", "BOOLEAN")
    con.execute(f"""
        UPDATE "{PUB}".main.canonical_patient_master
           SET rai_dose_data_available =
                 (rai_received_flag = TRUE AND rai_dose_v9 IS NOT NULL),
               rai_cumulative_dose_extreme =
                 (rai_total_cumulative_dose_mci > 1000)
    """)
    post = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE rai_dose_data_available     = TRUE) AS n_dose_avail,
               COUNT(*) FILTER (WHERE rai_cumulative_dose_extreme = TRUE) AS n_extreme
          FROM "{PUB}".main.canonical_patient_master
    """).fetchone()
    print(f"  [1.5] flagged: dose_avail={post[0]:,}  extreme_cum_dose={post[1]:,}")

    set_comment(con, "canonical_patient_master", "rai_dose_data_available",
        "TRUE for RAI-treated patients with dose data. Only 214/583 (36.7%) have dose. "
        "Script 234.")
    set_comment(con, "canonical_patient_master", "rai_cumulative_dose_extreme",
        "TRUE for patients with cumulative RAI dose >1000 mCi. Max is 5,358 mCi — verify "
        "before including in dose-response analyses. Script 234.")

    return {
        "era_profile": [dict(zip(
            ["era","n_rai_patients","n_with_dose","pct_with_dose"], r)) for r in era],
        "extremes": [dict(zip(
            ["research_id","rai_total_cumulative_dose_mci","n_rai_episodes",
             "rai_max_dose_mci","histology_final","recurrence_confirmed"], r))
            for r in extremes],
        "post": {"n_dose_avail": post[0], "n_extreme": post[1]},
    }


def phase_1(con) -> dict:
    print("\n=== PHASE 1 — RAI/Tg data-quality fixes ===")
    check_invariants(con, "1_start")
    payload = {
        "issue_1_rai_flag_episode":         issue_1(con),
        "issue_2_benign_rai_no_histology":  issue_2(con),
        "issue_3_tg_availability":          issue_3(con),
        "issue_4_tg_nadir_outliers":        issue_4(con),
        "issue_5_rai_dose":                 issue_5(con),
    }
    check_invariants(con, "1_end")
    write_phase_report("1", payload)
    return payload


# ======================================================================
# PHASE 2 — detail_table_registry_v1 pointer integrity
# ======================================================================
def phase_2(con) -> dict:
    print("\n=== PHASE 2 — detail_table_registry_v1 pointer integrity ===")
    check_invariants(con, "2_start")

    registry = con.execute(f"""
        SELECT detail_table_name, schema_name, total_rows
          FROM "{PUB}".manuscript_workspace.detail_table_registry_v1
         ORDER BY detail_table_name
    """).fetchall()
    print(f"  [2] registry rows: {len(registry)}")

    missing: list[str] = []
    row_mismatches: list[dict] = []
    orphan_tables: list[dict] = []

    for table_name, schema, expected in registry:
        exists = con.execute(f"""
            SELECT 1 FROM information_schema.tables
            WHERE table_catalog='{PUB}' AND table_schema='{schema}'
              AND table_name='{table_name}' LIMIT 1
        """).fetchone()
        if not exists:
            missing.append(f"{schema}.{table_name}")
            continue

        # Row count parity
        try:
            actual = con.execute(
                f'SELECT COUNT(*) FROM "{PUB}"."{schema}"."{table_name}"'
            ).fetchone()[0]
        except Exception as exc:  # pragma: no cover
            row_mismatches.append(
                {"table": f"{schema}.{table_name}", "expected": expected,
                 "actual": f"ERROR: {exc}"})
            continue

        if expected is not None and actual != expected:
            row_mismatches.append(
                {"table": f"{schema}.{table_name}",
                 "expected": expected, "actual": actual,
                 "delta": actual - expected})

        # Orphan research_ids (skip canonical master + views w/o research_id)
        if table_name == "canonical_patient_master":
            continue
        has_rid = con.execute(f"""
            SELECT 1 FROM information_schema.columns
             WHERE table_catalog='{PUB}' AND table_schema='{schema}'
               AND table_name='{table_name}' AND column_name='research_id'
             LIMIT 1
        """).fetchone()
        if not has_rid:
            continue
        try:
            orph = con.execute(f"""
                SELECT COUNT(DISTINCT CAST(d.research_id AS VARCHAR)) AS orphan_count
                  FROM "{PUB}"."{schema}"."{table_name}" d
                  LEFT JOIN "{PUB}".main.canonical_patient_master c
                    ON CAST(d.research_id AS VARCHAR) = c.research_id
                 WHERE c.research_id IS NULL AND d.research_id IS NOT NULL
            """).fetchone()[0]
        except Exception as exc:
            orph = None
            orphan_tables.append(
                {"table": f"{schema}.{table_name}", "orphans": f"ERROR: {exc}"})
            continue
        if orph and orph > 0:
            orphan_tables.append(
                {"table": f"{schema}.{table_name}", "orphans": orph})

    print(f"  [2] missing tables       : {len(missing)}")
    print(f"  [2] row-count mismatches : {len(row_mismatches)}")
    print(f"  [2] orphan rid tables    : {len(orphan_tables)}")
    if missing:
        for t in missing[:20]:
            print(f"        MISSING: {t}")
    if row_mismatches:
        for m in row_mismatches[:20]:
            print(f"        MISMATCH: {m}")
    if orphan_tables:
        for o in orphan_tables[:20]:
            print(f"        ORPHAN:   {o}")

    payload = {
        "registry_size":   len(registry),
        "missing_tables":  missing,
        "row_mismatches":  row_mismatches,
        "orphan_tables":   orphan_tables,
    }
    check_invariants(con, "2_end")
    write_phase_report("2", payload)
    return payload


# ======================================================================
# PHASE 3 — Update __readme + data_dictionary_v221
# ======================================================================
NEW_COLUMNS_META = [
    ("rai_received_reconciled",      "BOOLEAN",
     "Reconciled RAI flag: TRUE if rai_received_flag=TRUE OR n_rai_episodes>0. "
     "Addresses 279-patient flag/episode discordance. [Script 234]"),
    ("rai_flag_discordant",          "BOOLEAN",
     "TRUE for 279 patients where rai_received_flag=FALSE but n_rai_episodes>=1. "
     "[Script 234]"),
    ("benign_rai_suspect_malignant", "BOOLEAN",
     "TRUE for 100 patients classified as benign who received RAI but lack "
     "histology_final. Suspect malignant with missing pathology. [Script 234]"),
    ("tg_data_available",            "BOOLEAN",
     "TRUE if patient has any Tg measurement data. 76.4% are NULL (era- and "
     "benign-status correlated). [Script 234]"),
    ("tg_limitation_note",           "VARCHAR",
     "Documents why Tg data is missing for patients without measurements. "
     "[Script 234]"),
    ("tg_nadir_suspect_preablation", "BOOLEAN",
     "TRUE for 74 patients with tg_nadir>100 and no recurrence — likely "
     "pre-ablation values. [Script 234]"),
    ("rai_dose_data_available",      "BOOLEAN",
     "TRUE for RAI-treated patients with dose data (214/583 = 36.7%). [Script 234]"),
    ("rai_cumulative_dose_extreme",  "BOOLEAN",
     "TRUE for patients with cumulative RAI dose >1000 mCi. Max is 5,358 mCi. "
     "[Script 234]"),
]


def phase_3(con) -> dict:
    print("\n=== PHASE 3 — update __readme + data_dictionary_v221 ===")
    check_invariants(con, "3_start")

    # -- __readme: add row for new recovery table, if it exists ------------
    readme_inserts = 0
    recovery_exists = con.execute(f"""
        SELECT 1 FROM information_schema.tables
        WHERE table_catalog='{PUB}' AND table_schema='main'
          AND table_name='rai_benign_histology_recovery_v234' LIMIT 1
    """).fetchone()
    if recovery_exists:
        already = con.execute(f"""
            SELECT 1 FROM "{PUB}".main.__readme
             WHERE table_name='rai_benign_histology_recovery_v234' LIMIT 1
        """).fetchone()
        n_rows = con.execute(
            f'SELECT COUNT(*) FROM "{PUB}".main.rai_benign_histology_recovery_v234'
        ).fetchone()[0]
        if already:
            con.execute(f"""
                UPDATE "{PUB}".main.__readme
                   SET rows={n_rows},
                       description='Recovery table: benign+RAI patients with histology found in path_synoptics but missing from canonical. Script 234.'
                 WHERE table_name='rai_benign_histology_recovery_v234'
            """)
            print(f"  [3] __readme updated for rai_benign_histology_recovery_v234 (rows={n_rows})")
        else:
            con.execute(f"""
                INSERT INTO "{PUB}".main.__readme (table_name, rows, description)
                VALUES ('rai_benign_histology_recovery_v234', {n_rows},
                        'Recovery table: benign+RAI patients with histology found in path_synoptics but missing from canonical. Script 234.')
            """)
            readme_inserts = 1
            print(f"  [3] __readme inserted rai_benign_histology_recovery_v234 (rows={n_rows})")

    # -- data_dictionary_v221: one row per new column (idempotent) ---------
    # schema: (column_name, data_type, is_nullable, ordinal_position,
    #         non_null_count, coverage_pct, inferred_source, description)
    dict_inserts = 0
    dict_updates = 0
    total_rows_cpm = 10871
    for col_name, dtype, desc in NEW_COLUMNS_META:
        # ordinal position — grab live from information_schema
        ordinal = con.execute(f"""
            SELECT ordinal_position FROM information_schema.columns
            WHERE table_catalog='{PUB}' AND table_schema='main'
              AND table_name='canonical_patient_master'
              AND column_name='{col_name}'
        """).fetchone()
        ordinal = ordinal[0] if ordinal else None

        # non-null coverage — differs by column definition
        if dtype == "BOOLEAN":
            nn = con.execute(f"""
                SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
                 WHERE "{col_name}" IS NOT NULL
            """).fetchone()[0]
        else:  # VARCHAR — tg_limitation_note may be NULL for patients w/ data
            nn = con.execute(f"""
                SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
                 WHERE "{col_name}" IS NOT NULL
            """).fetchone()[0]
        coverage = round(100.0 * nn / total_rows_cpm, 2)

        existing = con.execute(f"""
            SELECT 1 FROM "{PUB}".main.data_dictionary_v221
             WHERE column_name='{col_name}' LIMIT 1
        """).fetchone()
        if existing:
            con.execute(f"""
                UPDATE "{PUB}".main.data_dictionary_v221
                   SET data_type        = ?,
                       is_nullable      = 'YES',
                       ordinal_position = ?,
                       non_null_count   = ?,
                       coverage_pct     = ?,
                       inferred_source  = 'Script 234',
                       description      = ?
                 WHERE column_name = ?
            """, [dtype, ordinal, nn, coverage, desc, col_name])
            dict_updates += 1
        else:
            con.execute(f"""
                INSERT INTO "{PUB}".main.data_dictionary_v221
                (column_name, data_type, is_nullable, ordinal_position,
                 non_null_count, coverage_pct, inferred_source, description)
                VALUES (?, ?, 'YES', ?, ?, ?, 'Script 234', ?)
            """, [col_name, dtype, ordinal, nn, coverage, desc])
            dict_inserts += 1

    dict_total = con.execute(
        f'SELECT COUNT(*) FROM "{PUB}".main.data_dictionary_v221'
    ).fetchone()[0]
    readme_total = con.execute(
        f'SELECT COUNT(*) FROM "{PUB}".main.__readme'
    ).fetchone()[0]
    print(f"  [3] data_dictionary_v221: inserted={dict_inserts} "
          f"updated={dict_updates} total_rows={dict_total}")
    print(f"  [3] __readme            : inserted={readme_inserts} "
          f"total_rows={readme_total}")

    payload = {
        "readme_inserts":   readme_inserts,
        "readme_total":     readme_total,
        "dict_inserts":     dict_inserts,
        "dict_updates":     dict_updates,
        "dict_total":       dict_total,
    }
    check_invariants(con, "3_end")
    write_phase_report("3", payload)
    return payload


# ======================================================================
# PHASE 4 — Working-DB cleanup ("Thyroid 2026 UPdated")
# ======================================================================
DROP_TABLES_4A_BACKUP = [
    "fna_episode_master_v2_backup_20260414",
    "patient_refined_master_clinical_v12_ln_backup_20260414",
    "patient_refined_master_clinical_v12_outcome_backup_20260415",
    "rai_treatment_episode_v2_backup_20260415",
    "gold_llm_verified_facts",
    "ARCHIVE___molecular_patient_rollup_v225",
    "ARCHIVE___ret_adjudication_input_v226",
    "ARCHIVE__canonical_patient_master_v225_pre_molecular_fix",
    "ARCHIVE__fna_episode_master_v2_pre_v225",
    "ARCHIVE__molecular_results_pre_v225",
    "ARCHIVE__molecular_test_episode_v2_pre_v225",
    "ARCHIVE__molecular_variant_long_pre_v225",
]
DROP_TABLES_4B_DEPRECATED = [
    # FHIR deployment path (deprecated)
    "fhir_bundle_specimen_export_v1",
    "fhir_encounter_v1",
    "fhir_episode_of_care_v1",
    "fhir_patient_deid_map_v1",
    "fhir_procedure_collection_v1",
    "fhir_specimen_v1",
    # Staging tables
    "stg_thyroseq_excel_raw",
    "stg_thyroseq_match_results",
    "stg_thyroseq_parsed",
    # Superseded canonical versions
    "canonical_patient_master_v218",
    "canonical_patient_master_v221",
    "data_dictionary_parquet_v221",
    "data_dictionary_v2",
    # Review queue tables
    "review_queue_imaging_fna_mm_v1",
    "tg_lab_review_queue_v1",
    "thyroseq_review_queue",
    "lab_cross_wave_review_v1",
    "lab_same_day_value_review_v1",
]
DROP_TABLES_4C_INTERNAL = [
    # Validation tables
    "val_imaging_fna_linkage_audit_v1",
    "val_path_synoptic_encounter_isolation_v1",
    "val_phase12_tirads_validation",
    # Intermediate computation
    "_followup_all_dates_v2",
    "_followup_computed_v2",
    "_followup_patient_max_v2",
    "_patient_surgery_dates",
    "_recurrence_event_sites_v1",
    "_recurrence_fna_sites_v1",
    "_nucmed_labs_parsed_v1",
    "_nucmed_labs_rollup_v1",
    # Fact extraction
    "canonical_extracted_fact_long_v1",
    "canonical_extracted_fact_long_v2",
    "canonical_fact_quarantine_v1",
    "canonical_fact_quarantine_v2",
]
ALL_DROPS = DROP_TABLES_4A_BACKUP + DROP_TABLES_4B_DEPRECATED + DROP_TABLES_4C_INTERNAL


def _working_db_attached(con) -> bool:
    dbs = {r[0] for r in con.execute(
        "SELECT database_name FROM duckdb_databases()").fetchall()}
    return REF in dbs


def phase_4(con, apply_drops: bool) -> dict:
    print("\n=== PHASE 4 — Working-DB cleanup ===")
    check_invariants(con, "4_start")

    if not _working_db_attached(con):
        print(f"  [4] Working DB '{REF}' not attached — SKIPPING")
        payload = {"skipped": True, "reason": f"{REF} not attached"}
        write_phase_report("4", payload)
        return payload

    # Manifest first — always
    manifest: list[dict] = []
    for tname in ALL_DROPS:
        exists = con.execute("""
            SELECT 1 FROM duckdb_tables()
            WHERE database_name=? AND schema_name='main' AND table_name=?
            LIMIT 1
        """, [REF, tname]).fetchone()
        if not exists:
            manifest.append({"table_name": tname, "rows": None, "status": "not_found"})
            continue
        try:
            rows = con.execute(
                f'SELECT COUNT(*) FROM "{REF}".main."{tname}"'
            ).fetchone()[0]
            manifest.append({"table_name": tname, "rows": rows, "status": "existing"})
        except Exception as exc:
            manifest.append({"table_name": tname, "rows": None,
                             "status": f"ERROR: {exc}"})

    manifest_path = REPO / "scripts" / "output" / "working_db_archive_manifest_v234.json"
    manifest_path.write_text(json.dumps(
        {"generated_at": TS, "working_db": REF,
         "tables": manifest}, indent=2))
    print(f"  [4] Manifest saved → {manifest_path}  ({len(manifest)} entries)")

    # Counts before
    n_working_before = con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name=? AND schema_name='main'
    """, [REF]).fetchone()[0]
    print(f"  [4] working DB main tables BEFORE: {n_working_before}")

    # Optional: tag canonical_patient_master_v1 on working DB with SUPERSEDED comment
    v1_exists = con.execute("""
        SELECT 1 FROM duckdb_tables()
        WHERE database_name=? AND schema_name='main' AND table_name='canonical_patient_master_v1'
        LIMIT 1
    """, [REF]).fetchone()
    if v1_exists:
        con.execute(f"""
            COMMENT ON TABLE "{REF}".main.canonical_patient_master_v1 IS
            'SUPERSEDED by thyroid_canonical_publication_v1_0.main.canonical_patient_master (1,471+ cols). Kept for lineage only. Do not use for analysis. Tagged by Script 234.'
        """)
        print("  [4] Tagged canonical_patient_master_v1 as SUPERSEDED (comment only).")

    dropped: list[str] = []
    skipped: list[str] = []
    errors: list[dict] = []

    if apply_drops:
        for entry in manifest:
            t = entry["table_name"]
            if entry["status"] == "not_found":
                skipped.append(t)
                continue
            try:
                con.execute(f'DROP TABLE IF EXISTS "{REF}".main."{t}"')
                dropped.append(t)
            except Exception as exc:
                errors.append({"table": t, "error": str(exc)})
        print(f"  [4] Dropped: {len(dropped)}  Skipped(missing): {len(skipped)}  Errors: {len(errors)}")
    else:
        print("  [4] --plan-only: no drops executed.")

    n_working_after = con.execute("""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name=? AND schema_name='main'
    """, [REF]).fetchone()[0]
    print(f"  [4] working DB main tables AFTER : {n_working_after}")

    payload = {
        "manifest_path":        str(manifest_path),
        "n_candidates":         len(ALL_DROPS),
        "n_working_before":     n_working_before,
        "n_working_after":      n_working_after,
        "dropped":              dropped,
        "skipped_not_found":    skipped,
        "errors":               errors,
        "apply_drops":          apply_drops,
    }
    check_invariants(con, "4_end")
    write_phase_report("4", payload)
    return payload


# ======================================================================
# PHASE 5 — Final invariants + validation report
# ======================================================================
def phase_5(con) -> dict:
    print("\n=== PHASE 5 — final verification + report ===")
    check_invariants(con, "5_start")

    n_cols = con.execute(f"""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_catalog='{PUB}' AND table_schema='main'
          AND table_name='canonical_patient_master'
    """).fetchone()[0]
    n_tables_pub = con.execute(f"""
        SELECT COUNT(*) FROM duckdb_tables()
        WHERE database_name='{PUB}' AND schema_name='main'
    """).fetchone()[0]
    n_tables_work = 0
    if _working_db_attached(con):
        n_tables_work = con.execute(f"""
            SELECT COUNT(*) FROM duckdb_tables()
            WHERE database_name='{REF}' AND schema_name='main'
        """).fetchone()[0]
    print(f"  [5] canonical_patient_master cols : {n_cols}")
    print(f"  [5] publication DB main tables    : {n_tables_pub}")
    print(f"  [5] working DB main tables        : {n_tables_work}")

    # Gather phase metrics for the markdown report — fall back to latest
    # per-phase JSON on disk when running phase 5 in isolation.
    def _load_phase(pid: str) -> dict:
        mem = REPORT["phases"].get(pid)
        if mem:
            return mem
        files = sorted(OUT.glob(f"phase_{pid}_*.json"))
        if not files:
            return {}
        try:
            return json.loads(files[-1].read_text())
        except Exception:
            return {}

    p1 = _load_phase("1")
    p2 = _load_phase("2")
    p3 = _load_phase("3")
    p4 = _load_phase("4")

    issue1 = p1.get("issue_1_rai_flag_episode", {})
    issue2 = p1.get("issue_2_benign_rai_no_histology", {})
    _issue3 = p1.get("issue_3_tg_availability", {})
    issue4 = p1.get("issue_4_tg_nadir_outliers", {})
    issue5 = p1.get("issue_5_rai_dose", {})

    n_outliers = issue4.get("flagged", "?")
    n_recovery = issue2.get("recovery_table_rows", "?")
    n_registry = p2.get("registry_size", "?")
    n_missing  = len(p2.get("missing_tables", []))
    n_mism     = len(p2.get("row_mismatches", []))
    n_orph     = len(p2.get("orphan_tables", []))
    n_readme_ins = p3.get("readme_inserts", "?")
    n_dict_ins   = p3.get("dict_inserts", "?")
    n_dict_upd   = p3.get("dict_updates", "?")
    n_dropped    = len(p4.get("dropped", []))
    n_before     = p4.get("n_working_before", "?")
    n_after      = p4.get("n_working_after", "?")

    report_md = f"""# Script 234 — RAI/Tg Data Quality Resolution + DB Hygiene
## {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}

## Phase 1 — RAI/Tg data-quality fixes (8 new columns)
| # | Issue | Flagged | New column(s) |
|---|---|---|---|
| 1 | RAI flag/episode discordance | {issue1.get("post",{}).get("discordant_true","?")} | rai_received_reconciled, rai_flag_discordant |
| 2 | Benign + RAI + NULL histology | {issue2.get("flagged","?")} | benign_rai_suspect_malignant |
| 3 | Tg availability documentation | n/a | tg_data_available, tg_limitation_note |
| 4 | Tg nadir outliers (>100, no recurrence) | {n_outliers} | tg_nadir_suspect_preablation |
| 5 | RAI dose availability + extreme cum dose | dose_avail={issue5.get("post",{}).get("n_dose_avail","?")} / extreme={issue5.get("post",{}).get("n_extreme","?")} | rai_dose_data_available, rai_cumulative_dose_extreme |

Recovery table created: `rai_benign_histology_recovery_v234` ({n_recovery} rows).

## Phase 2 — detail_table_registry_v1 pointer integrity
- Registry entries verified       : {n_registry}
- Missing tables                  : {n_missing}
- Row-count mismatches            : {n_mism}
- Tables with orphan research_ids : {n_orph}

## Phase 3 — Dictionary updates
- `__readme`              : +{n_readme_ins} row(s)
- `data_dictionary_v221`  : +{n_dict_ins} inserted, {n_dict_upd} updated

## Phase 4 — Working-DB cleanup
- Candidate tables        : {len(ALL_DROPS)}
- Working DB BEFORE       : {n_before} tables
- Working DB AFTER        : {n_after} tables
- Dropped                 : {n_dropped}
- Archive manifest        : scripts/output/working_db_archive_manifest_v234.json

## Phase 5 — Invariants
- canonical_patient_master: 10,871 rows × {n_cols} columns
- 0 NULL research_ids, 0 NULL fna_path_outcome
- Publication DB main tables : {n_tables_pub}
- Working DB main tables     : {n_tables_work}
"""
    report_path = REPO / "scripts" / "output" / "script_234_validation_report.md"
    report_path.write_text(report_md)
    print(f"  [5] report → {report_path}")

    payload = {
        "n_cols":             n_cols,
        "n_tables_pub":       n_tables_pub,
        "n_tables_work":      n_tables_work,
        "report_path":        str(report_path),
    }
    check_invariants(con, "5_end")
    write_phase_report("5", payload)
    return payload


# ======================================================================
# Preflight (read-only diagnostic)
# ======================================================================
def preflight(con) -> dict:
    print("\n=== PREFLIGHT ===")
    check_invariants(con, "preflight")
    # Baseline counts for the 5 issues
    counts = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
             WHERE rai_received_flag = FALSE AND n_rai_episodes > 0)        AS n_issue_1,
          (SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
             WHERE is_malignant = FALSE AND rai_received_flag = TRUE
               AND histology_final IS NULL)                                 AS n_issue_2,
          (SELECT ROUND(100.0 * COUNT(*) FILTER (
                     WHERE tg_n_measurements IS NULL OR tg_n_measurements = 0
                 ) / COUNT(*), 1)
             FROM "{PUB}".main.canonical_patient_master)                    AS pct_issue_3,
          (SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
             WHERE tg_nadir > 100 AND recurrence_confirmed = FALSE)         AS n_issue_4,
          (SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
             WHERE rai_received_flag = TRUE AND rai_dose_v9 IS NULL)        AS n_issue_5_missing_dose,
          (SELECT COUNT(*) FROM "{PUB}".main.canonical_patient_master
             WHERE rai_total_cumulative_dose_mci > 1000)                    AS n_issue_5_extreme
    """).fetchone()
    print(f"  Issue 1 (rai flag/episode discordance)     : {counts[0]}")
    print(f"  Issue 2 (benign+RAI+NULL histology)        : {counts[1]}")
    print(f"  Issue 3 (Tg NULL %)                        : {counts[2]}%")
    print(f"  Issue 4 (tg_nadir>100, no recurrence)      : {counts[3]}")
    print(f"  Issue 5 (RAI+no dose / cum>1000)           : {counts[4]} / {counts[5]}")
    return {"counts": dict(zip(
        ["issue_1","issue_2","issue_3_pct","issue_4","issue_5_missing_dose","issue_5_extreme"],
        list(counts)))}


# ======================================================================
# Main
# ======================================================================
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", required=True,
                    choices=["preflight", "1", "2", "3", "4", "5", "all"])
    ap.add_argument("--plan-only", action="store_true",
                    help="Phase 4: build archive manifest only, do not drop tables")
    ap.add_argument("--apply-drops", action="store_true",
                    help="Phase 4: actually execute the DROP TABLE statements")
    args = ap.parse_args()

    con = connect()

    if args.phase == "preflight":
        payload = preflight(con)
        write_phase_report("preflight", payload)
        return

    if args.phase in ("1", "all"):
        phase_1(con)
    if args.phase in ("2", "all"):
        phase_2(con)
    if args.phase in ("3", "all"):
        phase_3(con)
    if args.phase in ("4", "all"):
        if not args.apply_drops and not args.plan_only:
            print("[main] Phase 4 requires --plan-only or --apply-drops. Defaulting to --plan-only.")
            apply = False
        else:
            apply = bool(args.apply_drops)
        phase_4(con, apply_drops=apply)
    if args.phase in ("5", "all"):
        phase_5(con)

    final_path = OUT / f"234_report_{TS}.json"
    final_path.write_text(json.dumps(REPORT, default=str, indent=2))
    print(f"\n[main] consolidated report → {final_path}")


if __name__ == "__main__":
    main()
