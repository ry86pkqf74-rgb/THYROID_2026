# mig_98a — Apply Logan decisions: `vocal_cord_paralysis` in `canonical_complications_events_v1`

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)  
**Engagement:** mig_98a (post review workbook `vocal_cord_paralysis_review__mig_98a.xlsx`)  
**Executor:** `qc_framework_v1/scripts/apply_mig_98a_vocal_cord_decisions.py`

## Decisions (Logan)

| Bucket / case | Disposition |
|---------------|-------------|
| NEGATION_RISK (24 rows) | 23 × **FLIP_TO_PRESENT** (`finding_status` indeterminate → present). **1 × ACCEPT** — `research_id` **7306** (mixed present/negated). |
| CF91_ABSORPTION | **5048** — **ADD** synthetic `note_entities_complications`-style row (per-mention granularity; evidence hash only). **`finding_date` = `2016-12-02`** (CT date from deleted invasion row `invasion_event_id=b79ddab49f77d446c6d232e4819ee2b0`, mig_91 — not surgery date). **11862** — **NO_ADD** (mass effect, not VC paralysis). |
| CONFIRMED_POSITIVE (32) | **ACCEPT** (no change). |
| CONFIRMED_NEGATIVE (153) | **ACCEPT** (no change). |
| ONSET_AMBIGUOUS (present + `onset_class = unspecified`) | **AUTO_DERIVE** `onset_class`: same day-bucket rules as Script 364 (intraop / early_postop / late_postop), anchored to **latest surgery on or before `finding_date`** (mig_98a uses no 30-day link cap so long-interval CT/post-op findings classify as `late_postop`; 364’s build caps linkage at 30 days). |
| PERMANENCE_UNKNOWN (49) | **ACCEPT_AS_INDETERMINATE** — no `permanence_class` mutation. |

## Schema carry-forward (this migration)

- **`finding_date_source`** on `main.canonical_complications_events_v1`: distinguishes `entity_date` vs `note_date_fallback` vs `first_surgery_fallback` for `note_entities_complications` rows. **Live `note_entities_complications` was dropped in Script 364 phase 7** — the apply script joins the pre-364 archive `"Thyroid 2026 UPdated".archive_pub_v1_0.note_entities_complications_pre364_20260422_050902` using the same `hash(...)` key as Script 364. Other `source_table` values use structured labels only.

## Cross-migration carry-forwards (not implemented here)

- **CF-VC-PERMANENCE** — link vocal-cord permanence to follow-up laryngoscopy in **mig_99**.
- **CF-COMPLICATION-FINDING-DATE-PROVENANCE** — partially addressed by `finding_date_source`; any finer-grained note-level provenance remains future work.

## Post-apply

1. Rebuild **`canonical_complications_patient_rollup_v1`** (Script **364** step 5, or `--commit --phase 5`).
2. If CPM complication flags disagree with rollup, run **`scripts/364_cpm_feeder_repoint.py --commit`** as a separate governed step.

## Provenance

- One row in **`manuscript_workspace.cpm_reconciliation_provenance_v1`** with `run_id = mig98a_vocal_cord_paralysis_<UTCdate>` (inserted by apply script when `--apply`).
