# mig_177 — mig_154 Invasion Family PM-vs-Events Reconciliation Decision Package

**Date:** 2026-04-29  
**Lane:** 66 / mig_177  
**Batch:** `mig_177_mig154_invasion_family_reconcile_20260429`  
**Posture:** read-only MotherDuck profile and Logan decision package; no data writes.  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Target tables:** `main.canonical_patient_master`, `main.canonical_invasion_events_v1`, `main.canonical_invasion_patient_rollup_v1`  
**Replay SQL:** `qc_framework_v1/migrations/177_invasion_family_reconcile_probes_20260429.sql`

## Executive summary

The live MotherDuck profile confirms the mig_154 invasion-family carry-forwards across all **23 canonical_patient_master (PM) columns**. All 23 columns are present on PM and registered as `verified` under `mig_154_patient_master_pathology_invasion_cluster_20260429`, with open CF tags for vascular invasion, capsular invasion, LVI, and PNI.

`main.canonical_invasion_events_v1` is populated (**51,751 rows / 10,871 patients**) and is the correct event-grain source for strict finding assertions. However, its live schema differs from the prompt's draft examples:

- the event discriminator is `invasion_type`, not `invasion_subtype`;
- the table has no `invasion_ordinal_grade`, vessel-count, or grade-final columns;
- grade/count/versioned PM fields therefore cannot be replayed as a simple `MAX(invasion_ordinal_grade)` aggregation from the current events table.

The direct PM `*_any_present_path` booleans are not clean event-present rollups. Mismatch direction is family-specific but generally shows large PM-only-positive legacy burden:

| Family | PM boolean | Event type | PM=T / Event=T | PM=T / Event=F | PM=F / Event=T | PM=NULL / Event=T | Strict event-present patients | Direct mismatch summary |
|---|---|---|---:|---:|---:|---:|---:|---|
| Vascular | `vi_any_present_path` | `vascular_microscopic` | 1,061 | 2,637 | 26 | 22 | 1,109 | 2,637 PM-only positives; 48 event-only positives |
| Capsular | `capsular_any_present_path` | `capsular` | 795 | 314 | 33 | 117 | 945 | 314 PM-only positives; 150 event-only positives |
| LVI | `lvi_any_present_path` | `lymphatic_microscopic` | 778 | 2,614 | 2 | 0 | 780 | 2,614 PM-only positives; 2 event-only positives |
| PNI | `pni_any_present_path` | `perineural` | 107 | 1,383 | 1 | 14 | 122 | 1,383 PM-only positives; 15 event-only positives |

The existing `canonical_invasion_patient_rollup_v1` bridge is much closer to the event-present truth for these four axes, but it is not perfectly identical for every family:

| Family | Rollup column | Rollup=T / Event=T | Rollup=T / Event=F | Rollup=F / Event=T | Notes on rollup-only positives |
|---|---|---:|---:|---:|---|
| Vascular | `any_vascular_microscopic_anywhere` | 1,109 | 5 | 0 | 5 synoptic-path rows with only `absent` event status |
| Capsular | `any_capsular_anywhere` | 945 | 0 | 0 | Exact match to strict event-present patients |
| LVI | `any_lymphatic_microscopic_anywhere` | 780 | 120 | 0 | 91 with no event rows, 27 absent-only, 2 indeterminate-only |
| PNI | `any_perineural_anywhere` | 122 | 2 | 0 | 2 with no event rows |

**Recommendation:** ratify **D3 (per-column case-by-case)** for each family, with a consistent sub-rule:

1. For `*_any_present_path` patient booleans, use strict event-present semantics in mig_177b: `EXISTS canonical_invasion_events_v1 WHERE finding_status='present'` for the corresponding `invasion_type`. Preserve the current PM boolean values as legacy/audit values or add reconcile-status columns before overwriting.
2. For grade, ordinal, vessel-count, confidence, source, and versioned fields, do **not** auto-rederive in mig_177b from the current event table because the event schema lacks the required grade/count attributes. Preserve versioned fields and require a separate grade/count lineage design if Logan wants those fields rebuilt.
3. Keep all four mig_154 CFs open until Logan ratifies the D3 sub-rules and mig_177b applies the approved plan.

## Live schema and coverage

### Event table schema facts

`canonical_invasion_events_v1` columns:

| Column | Type |
|---|---|
| `invasion_event_id` | VARCHAR |
| `research_id` | BIGINT |
| `invasion_type` | VARCHAR |
| `finding_status` | VARCHAR |
| `source_modality` | VARCHAR |
| `source_kind` | VARCHAR |
| `source_table` | VARCHAR |
| `source_row_id` | VARCHAR |
| `finding_date` | DATE |
| `linked_surgery_episode_id` | BIGINT |
| `linked_path_malignant_event_id` | BIGINT |
| `linkage_method` | VARCHAR |
| `n_candidate_episodes` | INTEGER |
| `linkage_ambiguous_multi_finding` | BOOLEAN |
| `confidence` | DOUBLE |
| `evidence_span_hash` | VARCHAR |
| `evidence_qualifier` | VARCHAR |
| `extraction_run_id` | VARCHAR |
| `build_script` | VARCHAR |
| `build_ts` | TIMESTAMP |

### Event coverage by relevant family

| Event `invasion_type` | Present rows | Present patients | Suspected patients | Indeterminate patients | Absent patients | Any-status patients |
|---|---:|---:|---:|---:|---:|---:|
| `vascular_microscopic` | 2,883 | 1,109 | 28 | 68 | 3,605 | 4,203 |
| `capsular` | 2,136 | 945 | 22 | 240 | 1,232 | 2,195 |
| `lymphatic_microscopic` | 1,233 | 780 | 2 | 66 | 2,701 | 3,447 |
| `perineural` | 360 | 122 | 0 | 10 | 1,518 | 1,626 |

The event table also contains ETE, soft-tissue, airway, tracheal, and esophageal axes. Those are out of scope for mig_177.

## Registry provenance

All 23 PM columns are registered as verified with `verification_method = derivation_vs_canonical_invasion_events_v1` and `batch_id = mig_154_patient_master_pathology_invasion_cluster_20260429`.

| Family | Columns | Registry note summary |
|---|---|---|
| Vascular | 12 | WHO 2022 ladder + vessel extrema; internal consistency v13 `vasc_*` tier; `CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT` and `CF-mig154-INVASION-FAMILY-LINEAGE` |
| Capsular | 4 | BOOL_OR / ordinal worst from capsular-axis events + path malignant feeder; `CF-mig154-PM-CAPSULAR-VS-EVENT-PRESENT` |
| LVI | 3 | Lymphatic-microscopic axis distinct from vascular VI union; `CF-mig154-PM-LVI-VS-EVENT-PRESENT`; near-uniform-true caveat noted for `lvi_any_present_path` |
| PNI | 4 | Perineural axis + `pni_refined_v6` cleaning rule; `CF-mig154-PM-PNIANY-VS-EVENT-PRESENT` and wider-than-event caveat |

## PM column completeness and value profile

### Vascular invasion family (12 columns)

| Column | Type | Non-null | Distinct non-null | Top values / notes |
|---|---|---:|---:|---|
| `vasc_confidence_final_v13` | DOUBLE | 3,751 | 5 | 0.75=3,281; 0.95=354; 0.5=56; 1.0=45; 0.9=15 |
| `vasc_grade` | VARCHAR | 10,871 | 5 | blank=7,120; present_ungraded=3,281; focal=243; extensive=171; indeterminate=56 |
| `vasc_grade_final_v13` | VARCHAR | 3,751 | 4 | present_ungraded=3,281; focal=243; extensive=171; indeterminate=56 |
| `vasc_source_final_v13` | VARCHAR | 3,751 | 3 | path_synoptic_text=3,691; path_synoptic_quantify=45; multi_tumor_aggregate=15 |
| `vasc_vessel_count_v13` | DOUBLE | 46 | 6 | 1=20; 2=14; 3=7; 4=2; 6=2; 5=1 |
| `vascular_invasion_final` | VARCHAR | 3,751 | 4 | same distribution as `vasc_grade_final_v13` |
| `vascular_invasion_grade` | VARCHAR | 3,751 | 4 | same distribution as `vasc_grade_final_v13` |
| `vascular_vessel_count` | DOUBLE | 46 | 6 | same distribution as `vasc_vessel_count_v13` |
| `vascular_who_2022_grade` | VARCHAR | 400 | 2 | focal (<4 vessels)=231; extensive (>=4 vessels)=169 |
| `vi_any_present_path` | BOOLEAN | 3,753 | 2 | true=3,698; false=55 |
| `vi_ordinal_worst` | INTEGER | 3,698 | 3 | 2=3,345; 1=185; 3=168 |
| `vi_vessels_max` | DOUBLE | 46 | 6 | same distribution as vessel-count columns |

### Capsular invasion family (4 columns)

| Column | Type | Non-null | Distinct non-null | Top values / notes |
|---|---|---:|---:|---|
| `capsular_any_present_path` | BOOLEAN | 1,286 | 2 | true=1,109; false=177 |
| `capsular_invasion_refined` | VARCHAR | 1,191 | 2 | present=1,126; absent=65 |
| `capsular_invasion_v6` | VARCHAR | 1,227 | 3 | present=1,135; absent=64; indeterminate=28 |
| `capsular_ordinal_worst` | INTEGER | 1,186 | 4 | 2=1,012; 0=77; 3=59; 1=38 |

### LVI family (3 columns)

| Column | Type | Non-null | Distinct non-null | Top values / notes |
|---|---|---:|---:|---|
| `lvi_any_present_path` | BOOLEAN | 3,449 | 2 | true=3,392; false=57 |
| `lvi_grade` | VARCHAR | 3,366 | 16 | x=2,575; present=665; extensive=52; indeterminate=46; focal=6; typo/edge tokens present |
| `lvi_ordinal_worst` | INTEGER | 3,393 | 4 | 2=3,329; 3=58; 1=5; 0=1 |

### PNI family (4 columns)

| Column | Type | Non-null | Distinct non-null | Top values / notes |
|---|---|---:|---:|---|
| `perineural_invasion` | VARCHAR | 1,438 | 6 | x=1,339; present=91; focal=4; indeterminate=2; X=1; c/a=1 |
| `pni_any_present_path` | BOOLEAN | 1,493 | 2 | true=1,490; false=3 |
| `pni_positive` | BOOLEAN | 1,487 | 1 | true=1,487 only; no false values among non-null |
| `pni_refined_v6` | VARCHAR | 1,487 | 3 | present_ungraded=1,480; focal=4; indeterminate=3 |

## Per-family decision package

### 1. Vascular invasion

**Direct PM vs strict event-present:**

| Metric | Count |
|---|---:|
| PM true + event present | 1,061 |
| PM true + no event present | 2,637 |
| PM false + event present | 26 |
| PM false + no event present | 29 |
| PM null + event present | 22 |
| PM null + no event present | 7,096 |

**Bridge rollup vs strict event-present:**

| Metric | Count |
|---|---:|
| Rollup true + event present | 1,109 |
| Rollup true + no event present | 5 |
| Rollup false + event present | 0 |
| Rollup false + no event present | 9,757 |

The direct PM boolean is much wider than strict events. The bridge rollup is near-identical, with 5 rollup-only positives tied to synoptic-path absent-only statuses.

**Recommendation:** **D3**.

- Re-derive `vi_any_present_path` from strict `canonical_invasion_events_v1` present rows or from a repaired patient-rollup that excludes the 5 absent-only rollup positives.
- Preserve versioned and grade/count fields (`vasc_*_v13`, `vascular_*`, `vi_ordinal_worst`, `vi_vessels_max`) until a grade/count event lineage exists. Current events have no ordinal/vessel columns.
- Add `CF-mig177-RECOMMENDED-DECISION-vascular-D3`.

### 2. Capsular invasion

**Direct PM vs strict event-present:**

| Metric | Count |
|---|---:|
| PM true + event present | 795 |
| PM true + no event present | 314 |
| PM false + event present | 33 |
| PM false + no event present | 144 |
| PM null + event present | 117 |
| PM null + no event present | 9,468 |

**Bridge rollup vs strict event-present:** exact match: 945 rollup true/event present, 0 rollup-only positives, 0 event-only positives.

**Recommendation:** **D3**.

- Re-derive `capsular_any_present_path` from strict capsular event-present semantics or directly from `canonical_invasion_patient_rollup_v1.any_capsular_anywhere` because it exactly matches strict events in the live profile.
- Preserve `capsular_invasion_refined`, `capsular_invasion_v6`, and `capsular_ordinal_worst` until Logan decides whether those are legacy path-feeder semantics or should be rebuilt from a future event-grade table.
- Add `CF-mig177-RECOMMENDED-DECISION-capsular-D3`.

### 3. LVI / lymphatic microscopic invasion

**Direct PM vs strict event-present:**

| Metric | Count |
|---|---:|
| PM true + event present | 778 |
| PM true + no event present | 2,614 |
| PM false + event present | 2 |
| PM false + no event present | 55 |
| PM null + event present | 0 |
| PM null + no event present | 7,422 |

**Bridge rollup vs strict event-present:**

| Metric | Count |
|---|---:|
| Rollup true + event present | 780 |
| Rollup true + no event present | 120 |
| Rollup false + event present | 0 |
| Rollup false + no event present | 9,971 |

The 120 rollup-only positives break down as: 91 patients with no matching `lymphatic_microscopic` event rows, 27 absent-only, and 2 indeterminate-only.

**Recommendation:** **D3**.

- Treat strict `lymphatic_microscopic` event-present as the primary event-grain truth for an event-derived LVI-present flag.
- Do not silently overwrite or drop current `lvi_*` fields without Logan approval because the live PM fields are much wider and may encode older generic lymphovascular path-feeder semantics rather than the strict lymphatic-microscopic event axis.
- Preferred mig_177b action: preserve current `lvi_*` values as legacy/audit fields, create or repopulate an explicit event-derived flag, and add reconcile-status counts.
- Add `CF-mig177-RECOMMENDED-DECISION-lvi-D3`.

### 4. PNI / perineural invasion

**Direct PM vs strict event-present:**

| Metric | Count |
|---|---:|
| PM true + event present | 107 |
| PM true + no event present | 1,383 |
| PM false + event present | 1 |
| PM false + no event present | 2 |
| PM null + event present | 14 |
| PM null + no event present | 9,364 |

**Bridge rollup vs strict event-present:**

| Metric | Count |
|---|---:|
| Rollup true + event present | 122 |
| Rollup true + no event present | 2 |
| Rollup false + event present | 0 |
| Rollup false + no event present | 10,747 |

The 2 rollup-only positives have no matching perineural event rows.

**Recommendation:** **D3**.

- Re-derive `pni_any_present_path` and `pni_positive` from strict perineural event-present semantics in mig_177b, preserving old values as legacy/audit fields or adding reconcile status first.
- Preserve `perineural_invasion` and `pni_refined_v6` until Logan ratifies whether `x`/present_ungraded path placeholders should remain legacy evidence or be replaced by event-only truth.
- Add `CF-mig177-RECOMMENDED-DECISION-pni-D3`.

## Cross-cutting Logan questions

### Should `*_any_present_path` columns always be re-derived from events?

**Recommended answer:** Yes for the four strict event axes in mig_177, but use D3 governance rather than blind overwrite. The direct PM booleans have large drift from strict event-present truth. Re-derive from:

```text
EXISTS (
  SELECT 1
  FROM main.canonical_invasion_events_v1 e
  WHERE CAST(e.research_id AS VARCHAR) = CAST(pm.research_id AS VARCHAR)
    AND e.invasion_type = '<family_event_type>'
    AND e.finding_status = 'present'
)
```

Before applying, snapshot the current PM columns to `_legacy_raw` or an audit table and produce a `_pm_vs_event_reconcile_status` column/table so analysts can see the direction of change.

### Should `*_ordinal_worst` columns always be re-derived from event `MAX(invasion_ordinal_grade)`?

**Recommended answer:** Not yet. The live event table has no `invasion_ordinal_grade`. A future grade/count migration can either extend events with ordinal attributes or build a separate source-linked grade rollup. mig_177b should not invent ordinal derivations from `finding_status` alone.

### Should versioned columns be left alone?

**Recommended answer:** Yes. Preserve build-version-specific fields such as `vasc_confidence_final_v13`, `vasc_grade_final_v13`, `vasc_source_final_v13`, `vasc_vessel_count_v13`, `capsular_invasion_v6`, and `pni_refined_v6` unless Logan approves a versioned rebuild from a richer source. Add `CF-mig177-VERSIONED-COL-PRESERVE` as an informational carry-forward.

## Proposed mig_177b apply shape (not authored in this lane)

1. **Pre-snapshot** the 23 PM columns, registry rows, and per-family 2x2 counts.
2. **For boolean present flags**, apply Logan-ratified D3 sub-rule:
   - vascular: strict `event_type='vascular_microscopic' AND finding_status='present'`;
   - capsular: strict `event_type='capsular' AND finding_status='present'`;
   - LVI: strict `event_type='lymphatic_microscopic' AND finding_status='present'` plus an explicit legacy-LVI audit disposition;
   - PNI: strict `event_type='perineural' AND finding_status='present'`.
3. **For grade/count/versioned fields**, preserve as-is or rename to legacy only after a separate reader-impact scan and Logan signoff.
4. **Registry resync**: append the ratified rule and note that event schema lacks ordinal/vessel fields in mig_177.
5. **Post-verify**: strict event-present 2x2 should be clean for columns Logan chooses to rederive.

## Carry-forwards

| CF tag | Disposition after mig_177 |
|---|---|
| `CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT` | Stays open until Logan ratifies and mig_177b applies |
| `CF-mig154-PM-CAPSULAR-VS-EVENT-PRESENT` | Stays open until Logan ratifies and mig_177b applies |
| `CF-mig154-PM-LVI-VS-EVENT-PRESENT` | Stays open until Logan ratifies and mig_177b applies |
| `CF-mig154-PM-PNIANY-VS-EVENT-PRESENT` | Stays open until Logan ratifies and mig_177b applies |
| `CF-mig177-RECOMMENDED-DECISION-vascular-D3` | Informational recommendation |
| `CF-mig177-RECOMMENDED-DECISION-capsular-D3` | Informational recommendation |
| `CF-mig177-RECOMMENDED-DECISION-lvi-D3` | Informational recommendation |
| `CF-mig177-RECOMMENDED-DECISION-pni-D3` | Informational recommendation |
| `CF-mig177-VERSIONED-COL-PRESERVE` | Informational recommendation to preserve versioned build fields |

## Out-of-scope confirmation

No updates, alters, creates, deletes, registry changes, or production data writes were performed. This lane did not touch ETE, tracheal, airway, esophageal, path_malignant columns outside the listed 23, or `canonical_invasion_events_v1` itself.
