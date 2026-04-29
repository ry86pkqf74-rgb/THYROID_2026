# Cursor Prompt — mig_177 mig_154 Invasion Family PM-vs-Events Reconcile (12 vasc + 11 capsular/lvi/pni cols)

**Lane:** 66 / mig_177
**Batch_id:** `mig_177_mig154_invasion_family_reconcile_20260429`
**Generated:** 2026-04-29
**Type:** Read-only profile + reconciliation proposal. **No data writes.** Logan ratifies before any apply.

---

## §0 Why this lane exists

The invasion family on PM is split across **4 CF tags** (Cowork live 2026-04-29):

| CF tag | Cols affected | Family |
|---|---:|---|
| `CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT` | 12 | vascular invasion (vasc_*, vascular_*, vi_*) |
| `CF-mig154-PM-CAPSULAR-VS-EVENT-PRESENT` | 4 | capsular invasion |
| `CF-mig154-PM-LVI-VS-EVENT-PRESENT` | 3 | lymphovascular invasion |
| `CF-mig154-PM-PNIANY-VS-EVENT-PRESENT` | 4 | perineural invasion |

**Total: 23 PM cols** that have a corresponding event-grain canonical (`canonical_invasion_events_v1`) where the event-level "present" assertion may diverge from the PM's legacy patient-rollup logic.

Cowork live verified 2026-04-29 — full col list:

**Vascular invasion (12):** `vasc_confidence_final_v13`, `vasc_grade`, `vasc_grade_final_v13`, `vasc_source_final_v13`, `vasc_vessel_count_v13`, `vascular_invasion_final`, `vascular_invasion_grade`, `vascular_vessel_count`, `vascular_who_2022_grade`, `vi_any_present_path`, `vi_ordinal_worst`, `vi_vessels_max`

**Capsular invasion (4):** `capsular_any_present_path`, `capsular_invasion_refined`, `capsular_invasion_v6`, `capsular_ordinal_worst`

**LVI (3):** `lvi_any_present_path`, `lvi_grade`, `lvi_ordinal_worst`

**PNI (4):** `perineural_invasion`, `pni_any_present_path`, `pni_positive`, `pni_refined_v6`

This lane reconciles the 23 PM cols against `canonical_invasion_events_v1` event-grain truth. Per `feedback_findings_vs_staging.md`: "findings are primary and staging follows findings; pathologist's stage assertion does not override the actual finding." So the events grain is generally truth, but PM cols may carry different semantics by design (e.g., `_ordinal_worst`, `_grade_final`).

## §1 Governance posture

- Read-only profile. No `query_rw`.
- Output: profile report + reconciliation proposal + commented probe SQL.
- Logan ratifies the resolution per family before any apply.

## §2 Required pre-flight probes

```sql
-- §2a Confirm canonical_invasion_events_v1 exists + col list
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_invasion_events_v1'
ORDER BY column_name;

-- §2b Patient-rollup for invasion events (if a rollup exists)
SELECT column_name FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_invasion_patient_rollup_v1'
ORDER BY column_name;

-- §2c PM vs events 2x2 reconcile per family (one example: vi_any_present_path)
WITH events_present AS (
  SELECT DISTINCT research_id
  FROM main.canonical_invasion_events_v1
  WHERE invasion_subtype = 'vascular_invasion' AND finding_status = 'present'
),
pm AS (
  SELECT research_id, vi_any_present_path
  FROM main.canonical_patient_master
)
SELECT
  SUM(CASE WHEN pm.vi_any_present_path = TRUE AND ev.research_id IS NOT NULL THEN 1 ELSE 0 END) AS pm_t_ev_t,
  SUM(CASE WHEN pm.vi_any_present_path = TRUE AND ev.research_id IS NULL THEN 1 ELSE 0 END) AS pm_t_ev_f,
  SUM(CASE WHEN pm.vi_any_present_path = FALSE AND ev.research_id IS NOT NULL THEN 1 ELSE 0 END) AS pm_f_ev_t,
  SUM(CASE WHEN pm.vi_any_present_path = FALSE AND ev.research_id IS NULL THEN 1 ELSE 0 END) AS pm_f_ev_f,
  SUM(CASE WHEN pm.vi_any_present_path IS NULL THEN 1 ELSE 0 END) AS pm_null
FROM pm LEFT JOIN events_present ev USING (research_id);
-- Repeat for capsular_any_present_path, lvi_any_present_path, pni_any_present_path.
-- Adjust events filter per invasion_subtype.

-- §2d _ordinal_worst / _grade_final cols vs events grade aggregation
-- (e.g., vi_ordinal_worst should = MAX(ordinal_grade) over events for that pt)
SELECT
  pm.research_id,
  pm.vi_ordinal_worst AS pm_ordinal_worst,
  ev.event_max_ordinal AS event_max_ordinal,
  CASE WHEN pm.vi_ordinal_worst = ev.event_max_ordinal THEN 'match' ELSE 'mismatch' END AS reconcile_status
FROM main.canonical_patient_master pm
LEFT JOIN (
  SELECT research_id, MAX(invasion_ordinal_grade) AS event_max_ordinal
  FROM main.canonical_invasion_events_v1
  WHERE invasion_subtype = 'vascular_invasion'
  GROUP BY research_id
) ev USING (research_id)
WHERE pm.vi_ordinal_worst IS NOT NULL OR ev.event_max_ordinal IS NOT NULL
LIMIT 50;

-- §2e Cohort coverage: how many PM patients have ANY invasion events?
SELECT COUNT(DISTINCT research_id) AS n_with_invasion_events
FROM main.canonical_invasion_events_v1;
```

## §3 Per-family reconcile + decision

For each of the 4 families (vasc, capsular, lvi, pni), agent provides:

1. **Live 2x2** (PM_T/F vs events_T/F).
2. **Mismatch direction** (PM-only positive vs events-only positive).
3. **Source-of-truth call**: per `feedback_findings_vs_staging.md`, events should be primary unless there's a documented reason. Recommend events-as-truth unless evidence says otherwise.
4. **Decision options:**
   - **D1**: Drop PM legacy cols (rename to `_legacy_raw`); re-derive from events.
   - **D2**: Keep both; add `_pm_vs_event_reconcile_status` audit col on PM; document drift.
   - **D3**: Per-col case-by-case (some PM cols are actually different semantics, e.g., `_grade_final_v13` is not a simple events agg).

5. **Recommendation** with rationale.

## §4 Logan-decision package

For each family, Logan ratifies one of D1/D2/D3, plus a per-col override list when D3.

Cross-cutting questions Logan must also answer:
- Should `*_any_present_path` cols always be re-derived from events via `EXISTS finding_status='present'` semantics?
- Should `*_ordinal_worst` cols always be re-derived from events via `MAX(invasion_ordinal_grade)`?
- Should `*_grade_final_v*` / `*_confidence_final_v*` versioned cols be left alone (they encode build-version-specific scoring logic)?

## §5 Apply skeleton (for mig_177b later, NOT this lane)

```sql
-- mig_177b Section A: pre-snapshot per family
-- mig_177b Section B: re-derive cols per Logan-ratified D1/D2/D3 plan
-- mig_177b Section C: registry resync (notes appendix recording the decision)
-- mig_177b Section D: post-state verify (PM vs events 2x2 should be clean per family)
```

DO NOT author this in mig_177 — wait for Logan's per-family decisions.

## §6 Required CFs

- `CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT` (12 cols) → STAYS OPEN until Logan ratifies; closes via mig_177b
- `CF-mig154-PM-CAPSULAR-VS-EVENT-PRESENT` (4 cols) → same
- `CF-mig154-PM-LVI-VS-EVENT-PRESENT` (3 cols) → same
- `CF-mig154-PM-PNIANY-VS-EVENT-PRESENT` (4 cols) → same
- `CF-mig177-RECOMMENDED-DECISION-<family>-<D1..D3>` (informational; agent's per-family recommendation)
- `CF-mig177-VERSIONED-COL-PRESERVE` (informational; identifies cols with `_v<N>` suffixes that encode build-version-specific scoring and should NOT be auto-rederived)

## §7 Files + Git workflow

- `qc_framework_v1/reports/mig_177_mig154_invasion_family_reconcile_20260429.md` — per-family 2x2 + decision package
- `qc_framework_v1/migrations/177_invasion_family_reconcile_probes_20260429.sql` — commented probe SQL Logan can replay
- Commit: `qc: mig_177 mig_154 invasion family PM-vs-events reconcile (read-only)`
- Push.

## §8 Out of scope

- Do NOT apply any UPDATE.
- Do NOT touch ETE invasion (separate canonical: `canonical_ete_event_resolved_v1`).
- Do NOT touch tracheal/airway/esophageal invasion (separate canonical: `canonical_esophageal_invasion_events_v1`, etc.).
- Do NOT modify `canonical_invasion_events_v1` itself — events grain is the source of truth.
- Do NOT touch any path_malignant cols outside the 23 listed.

## §9 Apply governance

Read-only lane. Agent ships profile + decision package only.

Per `feedback_findings_vs_staging.md`: events grain is generally primary; PM legacy cols are usually rollups that should be re-derived. Watch for exceptions (versioned scoring cols that intentionally don't equal a simple events agg).

Per AGENTS governance: agent ships profile only. **No `query_rw` from agent.**
