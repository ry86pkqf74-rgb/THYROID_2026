# mig_186 — Apply R-D NIFTP + uncertain-malignancy exclusion (RATIFIED)

**Date:** 2026-04-30  
**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Batch:** `mig_186_apply_RD_niftp_exclusion_ratified_20260430`  
**SQL skeleton:** `qc_framework_v1/migrations/186_apply_RD_niftp_exclusion_ratified_20260430.sql`  
**Target DB:** `thyroid_canonical_publication_v1_0`

---

## §1 Logan-ratified R-D rule (verbatim)

**R-D hybrid:** archive affected rows, optionally land them in a new `canonical_path_indeterminate_events_v1` table for queryable provenance, then DELETE them from `canonical_path_malignant_events_v1`. Rebuild `canonical_path_malignant_patient_rollup_v1` after deletion. Open `CF-mig186-WHO-2017-NIFTP-RECLASS` on registry. Apply downstream cascade where needed.

**Affected scope (scoping):**

- 213 NIFTP events / 195 patients  
- 7 uncertain-malignant-potential events / 7 patients  
- **Total: 220 events / 202 patients**

**Patient cohort split:**

- **87 mixed** — another malignant event present; keep patient, drop affected event row(s).  
- **115 edge** — no other path malignant event row but CPM `is_malignant=TRUE`; **keep CPM row**; open `CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION` for spot-check (likely biopsy-only or imaging-only malignancy evidence without a path event).

---

## §2 Pre-flight inventory vs scoping baseline

Live MotherDuck probe (`thyroid_canonical_publication_v1_0`), SELECT-only, 2026-04-30:

| Metric | Scoping baseline | Live MD | Delta |
|--------|------------------|---------|-------|
| Affected events | 220 | **220** | 0 |
| Affected patients | 202 | **202** | 0 |
| CPM rows | 10,871 (invariant) | **10,871** | 0 |
| Mixed patients | 87 | **87** | 0 |
| Edge patients (NIFTP/UMP-only path events) | 115 | **115** | 0 |

**Verdict:** No drift; Path-C may execute preflight in §0 of the SQL file and expect the same counts.

---

## §3 Cascade scan

Summary (detail: `exports/mig186_apply_RD_20260430/cascade_scan.csv`):

| Consumer | Rows (context) | Rebuild? | Action |
|----------|----------------|----------|--------|
| `main.canonical_path_malignant_events_v1` | 220 affected | Yes (DELETE) | Archive §A → DELETE §C |
| `main.canonical_path_indeterminate_events_v1` | 0 → 220 | Yes (CREATE) | §B landing |
| `main.canonical_path_malignant_patient_rollup_v1` | 4,137 total (live); 202 affected rids present | Yes | Pre-snapshot §D1; `CREATE OR REPLACE` §D2 (Script 361 Step 5a; **no** `path_outcome_classification_v1` join — table absent on live MD) |
| `main.canonical_invasion_events_v1` | 1,845 rows for affected patient set | No | Patient + surgery grain; **no** DELETE in this migration |
| `main.canonical_patient_master` | 10,871 | No mutation | CF only: **115** edge patients — do not auto-flip `is_malignant` |
| `views_readable.path_malignant_*_VIEW_v1` | — | No | Thin views over rebuilt tables |
| `manuscript_workspace.detail_table_registry_v1` | — | Append | §E description appendix |
| `main.canonical_column_verification_registry_v1` | — | Update | §F / §G open CF notes + `not_started` |

**Rollup cardinality note:** Live malignant rollup = **4,137** rows (patients with ≥1 malignant path event), not 10,871. After apply, expect **~4,022** rows (4,137 − 115 edge patients who become rollup-absent when only NIFTP/UMP rows were removed).

---

## §4 Affected-event sample (10 rows)

Exported: `exports/mig186_apply_RD_20260430/affected_event_sample.csv`

(min `research_id` sort — includes Atypical Hürthle / NIFTP exemplars)

---

## §5 Expected post-state metrics

- `canonical_path_malignant_events_v1`: row count decreases by **220**.  
- `canonical_path_indeterminate_events_v1`: **220** rows (first apply from this batch).  
- `canonical_path_malignant_patient_rollup_v1`: **~4,022** rows (baseline 4,137 − **115** edge); **87** mixed patients remain with updated aggregates.  
- `canonical_patient_master`: row count **10,871** unchanged; **edge** triage via CF only.  
- Registry: `detail_table_registry_v1` description appendix; `canonical_column_verification_registry_v1` CF opens per SQL §F–§G.  
- `cpm_reconciliation_provenance_v1`: one row `mig186_apply_RD_niftp_exclusion_ratified_20260430`.

---

## §6 Unblocking checklist — Cowork Path-C apply

1. `USE thyroid_canonical_publication_v1_0` (locked search path).  
2. Run SQL §0 probes — confirm **220 / 202** still.  
3. Execute §A → verify archive row count **220**.  
4. Execute §B → indeterminate row count **220**.  
5. Execute §C → malignant events −220.  
6. Execute §D1–§D2 → rollup rebuilt; spot-check **115** former rollup keys dropped.  
7. Execute §E–§H → registry + provenance.  
8. Run §I probes; reconcile rollup ~4,022.  
9. Re-materialize / refresh any **downstream manuscript views** that aggregate path malignant events (outside this file — cascade per workspace conventions).  
10. Carry-forwards: **CF-mig186-WHO-2017-NIFTP-RECLASS**, **CF-mig186-EDGE-NO-MALIGNANT-EVENT-AFTER-EXCLUSION**.

---

*End of report.*
