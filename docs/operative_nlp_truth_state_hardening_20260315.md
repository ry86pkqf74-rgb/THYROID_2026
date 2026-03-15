# Operative NLP Truth-State Hardening Report

**Date**: 2026-03-15  
**Script**: `scripts/104_operative_truth_state_hardening.py`  
**Scope**: Prevent manuscript and analysis layers from misrepresenting unknown operative details as confirmed negatives

---

## 1. Problem Statement

Script 22 (`22_canonical_episodes_v2.py`) creates `operative_episode_detail_v2` with 10 hardcoded `FALSE AS <field>` defaults for operative NLP boolean fields. The COALESCE-based UPDATE that follows (`COALESCE(nlp_value, old_FALSE)`) cannot overwrite these because FALSE is non-NULL, causing NLP-confirmed unknowns to remain as FALSE. Script 86 compounds this by using `BOOL_OR(COALESCE(field, FALSE))` in patient-level aggregation and `COALESCE(o.field, FALSE)` in episode propagation, converting any surviving NULL back to FALSE.

**Impact**: In the pre-hardening state, all 10 operative boolean fields had **zero NULL values** across 9,371 operative episodes. Every unknown was stored as FALSE (a confirmed negative), which is semantically incorrect and misleading for manuscript analyses.

---

## 2. Root Cause Chain

| Step | Script | Defect | Effect |
|------|--------|--------|--------|
| 1 | 22 (line 643–656) | `FALSE AS rln_monitoring_flag, ...` | All rows start as FALSE |
| 2 | 22 (line 1022–1032) | `COALESCE(e.field, o.field)` | NULL NLP result → keeps old FALSE |
| 3 | 86 (line 244–253) | `COALESCE(o.field, FALSE)` in episode UPDATE | NULL → FALSE during episode propagation |
| 4 | 86 (line 147–170) | `BOOL_OR(COALESCE(field, FALSE))` for patient agg | NULL → FALSE → aggregates as FALSE |

**Exception**: Script 94 already used bare `BOOL_OR(field)` without COALESCE — this was correct.

---

## 3. BEFORE State (Live MotherDuck, 2026-03-15)

### operative_episode_detail_v2 (9,371 rows)

| Field | TRUE | FALSE | NULL |
|-------|-----:|------:|-----:|
| rln_monitoring_flag | 1,702 | 7,669 | 0 |
| parathyroid_autograft_flag | 40 | 9,331 | 0 |
| gross_ete_flag | 22 | 9,349 | 0 |
| local_invasion_flag | 25 | 9,346 | 0 |
| tracheal_involvement_flag | 9 | 9,362 | 0 |
| esophageal_involvement_flag | 0 | 9,371 | 0 |
| strap_muscle_involvement_flag | 186 | 9,185 | 0 |
| reoperative_field_flag | 46 | 9,325 | 0 |
| drain_flag | 169 | 9,202 | 0 |
| parathyroid_resection_flag | 0 | 9,371 | 0 |
| frozen_section_flag | 0 | 0 | 9,371 |
| berry_ligament_flag | 0 | 0 | 9,371 |

### patient_analysis_resolved_v1 op_* (10,871 rows)

| Field | TRUE | FALSE | NULL |
|-------|-----:|------:|-----:|
| op_rln_monitoring_any | 1,701 | 7,032 | 2,138 |
| op_drain_placed_any | 169 | 8,564 | 2,138 |
| op_strap_muscle_any | 186 | 8,547 | 2,138 |
| op_reoperative_any | 46 | 8,687 | 2,138 |
| op_parathyroid_autograft_any | 40 | 0 | 10,831 |
| op_local_invasion_any | 25 | 8,708 | 2,138 |
| op_tracheal_inv_any | 9 | 8,724 | 2,138 |
| op_esophageal_inv_any | 0 | 8,733 | 2,138 |
| op_intraop_gross_ete_any | 22 | 8,711 | 2,138 |

The 2,138 NULLs represent patients with no surgery record (correct). All other 8,733 patients had FALSE — **incorrect** for non-confirmed fields.

---

## 4. Fix Applied

### Phase B: Recode operative_episode_detail_v2

```sql
-- For each of the 10 hardcoded-FALSE fields:
UPDATE operative_episode_detail_v2
SET <field> = NULL
WHERE <field> IS NOT TRUE
```

Semantic rule: **TRUE (NLP-confirmed positive) → preserved; everything else → NULL (unknown)**.

This is correct because:
- The V2 OperativeDetailExtractor only fires regex patterns on positive findings
- "Not found" ≠ "confirmed absent" — absence of evidence ≠ evidence of absence
- The extractor does not systematically assert "this finding is definitively absent"

### Phase C–E: Re-propagation (stripped COALESCE)

- Episode layer: bare `o.<field>` (no `COALESCE(o.<field>, FALSE)`)
- Patient layer: `BOOL_OR(<field>)` (no `BOOL_OR(COALESCE(<field>, FALSE))`)
- Manuscript layer: direct copy from patient layer

### Script 86 Inline Fix

Two defects permanently corrected with comments:

1. **PATIENT_OP_FIELDS** (line 147–170): `BOOL_OR(COALESCE(field, FALSE))` → `BOOL_OR(field)`
2. **UPDATE_EPISODE_SQL** (line 244–253): `COALESCE(o.field, FALSE)` → `o.field`

---

## 5. AFTER State

### operative_episode_detail_v2 (9,371 rows)

| Field | TRUE | FALSE | NULL | FALSE→NULL |
|-------|-----:|------:|-----:|---:|
| rln_monitoring_flag | 1,702 | 0 | 7,669 | 7,669 |
| parathyroid_autograft_flag | 40 | 0 | 9,331 | 9,331 |
| gross_ete_flag | 22 | 0 | 9,349 | 9,349 |
| local_invasion_flag | 25 | 0 | 9,346 | 9,346 |
| tracheal_involvement_flag | 9 | 0 | 9,362 | 9,362 |
| esophageal_involvement_flag | 0 | 0 | 9,371 | 9,371 |
| strap_muscle_involvement_flag | 186 | 0 | 9,185 | 9,185 |
| reoperative_field_flag | 46 | 0 | 9,325 | 9,325 |
| drain_flag | 169 | 0 | 9,202 | 9,202 |
| parathyroid_resection_flag | 0 | 0 | 9,371 | 9,371 |

**Total FALSE→NULL cells**: 88,511 (across 10 fields × 9,371 rows, minus TRUE)

### patient_analysis_resolved_v1 op_* (10,871 rows)

All 9 op_* boolean fields now carry NULL for patients whose operative notes lacked NLP-confirmed evidence. Only TRUE values (NLP-confirmed) and NULL values (unknown/no-surgery) exist.

### Bonus Fix Discovered

`rln_monitoring_flag` and `drain_flag` had **TRUE=0** in `episode_analysis_resolved_v1` BEFORE hardening, despite TRUE>0 in OED. The old COALESCE propagation was masking these TRUE values. After hardening, these fields correctly propagate.

---

## 6. Scoring System Impact

`gross_ete_flag` is consumed by scoring scripts (51, 51b) for AJCC8 T3b staging:
```sql
COALESCE(pt.gross_ete, FALSE)  -- in scoring scripts
```

After hardening, unknown ETE → NULL → `COALESCE(NULL, FALSE)` → no T3b upstaging. This is **correct behavior** — unknown ETE should NOT trigger staging changes. The scoring scripts' own COALESCE is an appropriate safety guard at the *consumption* layer (unlike the *storage* layer where it was harmful).

---

## 7. Validation Tables

| Table | Rows | Purpose |
|-------|-----:|---------|
| `val_operative_truth_state_v1` | 39 | Per-field AFTER distributions across 4 tables |
| `val_operative_truth_state_delta_v1` | 39 | Before/after comparison with change counts |

---

## 8. Files Modified

| File | Change |
|------|--------|
| `scripts/104_operative_truth_state_hardening.py` | **NEW** — 8-phase hardening script |
| `scripts/86_operative_nlp_final_sync.py` | **FIX** — removed COALESCE wrappers from PATIENT_OP_FIELDS and UPDATE_EPISODE_SQL |

---

## 9. Exports

```
exports/operative_nlp_truth_state_hardening_YYYYMMDD_HHMM/
├── val_operative_truth_state_v1.csv
├── val_operative_truth_state_v1.parquet
├── val_operative_truth_state_delta_v1.csv
├── val_operative_truth_state_delta_v1.parquet
└── manifest.json
```

---

## 10. Principle Established

**Tri-state truth semantics** for operative NLP boolean fields:
- **TRUE** = NLP-confirmed positive finding
- **NULL** = Unknown (NLP found no evidence, or NLP never processed this patient's notes)
- **FALSE** = Reserved for explicit NLP negation (not currently used; extractor does not track negation separately)

Downstream consumers **must** use `COALESCE(field, FALSE)` only at the *display/scoring* boundary, never at the *storage/propagation* boundary.
