"""
Script 313b — Tier 2 completeness invariant check.

Verifies every note_entities_llm_* table has a corresponding *_event_v1 table.
Errors out loudly listing gaps if any are found.

Usage:
    python 313b_tier2_completeness_invariant.py
"""
from __future__ import annotations

import datetime as dt

from _md_connect import connect_locked

SCRIPT = "313b_tier2_completeness_invariant"

DOMAIN_TO_EVENT_TABLE = {
    "note_entities_llm_frozen_section_detail": "frozen_section_event_v1",
    "note_entities_llm_vascular_invasion": "vascular_invasion_event_v1",
    "note_entities_llm_airway_invasion": "airway_invasion_event_v1",
    "note_entities_llm_parathyroid_detail": "parathyroid_detail_event_v1",
    "note_entities_llm_past_surgical_hx": "past_surgical_hx_event_v1",
    "note_entities_llm_past_medical_hx": "past_medical_hx_event_v1",
    "note_entities_llm_functional_outcomes": "functional_outcomes_event_v1",
    "note_entities_llm_physical_exam": "physical_exam_event_v1",
    "note_entities_llm_presenting_symptoms": "presenting_symptoms_event_v1",
    "note_entities_llm_rad_treatment": "rad_treatment_event_v1",
    "note_entities_llm_patient_decision_adherence": "patient_decision_adherence_event_v1",
    "note_entities_llm_dynamic_risk_response": "dynamic_risk_response_event_v1",
    # Domains with pre-existing Tier 2 or integrated into canonical masters:
    "note_entities_llm_synoptic_pathology_enrichment": "synoptic_tumor_long_v1",
    "note_entities_llm_tirads_granular": "canonical_us_nodule_master_v1",
    "note_entities_llm_us_nodule_dynamics": "canonical_us_nodule_master_v1",
    "note_entities_llm_rai_detailed": "rai_treatment_episode_v2",
    "note_entities_llm_cervical_ln_detail": "ln_master_rollup_v1",
    "note_entities_llm_labs": "longitudinal_lab_canonical_v1",
    "note_entities_llm_recurrence": "canonical_recurrence_v1",
    "note_entities_llm_tg_kinetics": "longitudinal_lab_canonical_v1",
    "note_entities_llm_imaging": "canonical_us_nodule_master_v1",
    "note_entities_llm_pathology": "synoptic_tumor_long_v1",
    "note_entities_llm_survival_followup": "canonical_patient_master",
}


def log(msg):
    ts = dt.datetime.utcnow().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def main():
    con = connect_locked()

    log("=" * 72)
    log("Script 313b — Tier 2 completeness invariant check")
    log("=" * 72)

    # Get all note_entities_llm_* tables
    llm_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name LIKE 'note_entities_llm_%'
          AND table_name NOT LIKE '%broken%'
          AND table_name NOT LIKE '%_pre%'
        ORDER BY table_name
    """).fetchall()
    llm_names = [r[0] for r in llm_tables]

    # Get all existing tables in main
    all_tables = con.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'main'
    """).fetchall()
    all_table_names = {r[0] for r in all_tables}

    gaps = []
    results = []

    for llm_tbl in llm_names:
        expected = DOMAIN_TO_EVENT_TABLE.get(llm_tbl)
        if expected is None:
            gaps.append(f"  {llm_tbl}: NO MAPPING DEFINED")
            results.append((llm_tbl, "NO_MAPPING", False))
            continue

        exists = expected in all_table_names
        status = "OK" if exists else "MISSING"
        results.append((llm_tbl, expected, exists))
        log(f"  {llm_tbl:55s} -> {expected:40s} [{status}]")
        if not exists:
            gaps.append(f"  {llm_tbl}: expected {expected} MISSING")

    # Write completeness table
    con.execute("""
        CREATE OR REPLACE TABLE manuscript_workspace.tier2_completeness_v1 (
            llm_source VARCHAR,
            expected_tier2_table VARCHAR,
            has_tier2_event_table BOOLEAN,
            checked_at TIMESTAMP
        )
    """)
    for llm_tbl, expected, exists in results:
        con.execute("""
            INSERT INTO manuscript_workspace.tier2_completeness_v1 VALUES (?, ?, ?, ?)
        """, [llm_tbl, str(expected), exists, dt.datetime.utcnow()])

    n_total = len(results)
    n_ok = sum(1 for _, _, ok in results if ok)
    log(f"")
    log(f"  Completeness: {n_ok}/{n_total} domains have Tier 2 tables")

    if gaps:
        log(f"")
        log(f"  GAPS FOUND ({len(gaps)}):")
        for g in gaps:
            log(g)
        raise SystemExit(
            f"Tier 2 completeness invariant FAILED: {len(gaps)} gaps. "
            f"See above for details."
        )

    log(f"")
    log(f"  INVARIANT PASSED: all {n_total} LLM domains have Tier 2 coverage.")
    log("=" * 72)


if __name__ == "__main__":
    main()
