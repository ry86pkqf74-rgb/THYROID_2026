# mig_98d — Apply Logan-ratified bulk dispositions: `seroma`

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck `.eras` account) **Engagement:** mig_98d (post seroma_review__mig_98d.xlsx; Logan ratified bulk-apply) **Executor:** `qc_framework_v1/scripts/apply_mig_98d_seroma_decisions.py`**Applied:** 2026-04-28

## Disposition rule (Logan-ratified)

Same protocol as mig_98c: trust date-based attribution + structured/REAL signals. No individual review; bulk-apply.

Priority order:

1. `timing_days < 0` → PMH_PREEXISTING
2. `timing_days > 5y` → PMH_NOT_OPERATIVE
3. `timing_days 1–5y` → PMH_POSSIBLY_PRIOR_OP
4. `STRUCTURED_SIGNAL` (phen_confirmed OR refined_v5) + `0–365d` → KEEP_OPERATIVE / KEEP_POSTOP_LATE
5. REAL note-text mention + `0–365d` → KEEP_OPERATIVE / KEEP_POSTOP_LATE
6. `31–365d` (defensive) → KEEP_POSTOP_LATE
7. `0–30d` (template noise, no signal) → DELETE

## Disposition distribution (873 candidates)

DecisionnDELETE744PMH_PREEXISTING79KEEP_OPERATIVE39KEEP_POSTOP_LATE6PMH_NOT_OPERATIVE3PMH_POSSIBLY_PRIOR_OP2

Net: **45 KEEP, 84 PMH, 744 DELETE**.

## Phases (apply script)

1. **Snapshot** seroma rows (2,259 rows) → `archive_pub_v1_0.canonical_complications_events_v1_pre98d_seroma_<ts>`
2. **PMH INSERT** — 84 rows in `canonical_pmh_events_v1`:
   - source_table = `mig_98d_pmh_synthetic`
   - source_note_type = `other_history`
   - finding_value_norm = `seroma`
   - is_preexisting = `TRUE`
   - anchor_source = `mig_98d_classifier_logan_curated`
3. **DELETE** non-keep present (1,328) + suspected (0)
4. **Standardize** evidence_strength on KEEP rows:
   - probable: 44 rows (39 pts — structured/REAL + 0-365d)
   - possible: 4 rows (6 pts — 31-365d defensive keepers)
5. **Rebuild** `canonical_complications_patient_rollup_v1` (Script 364 step_5)
6. **Provenance** `mig98d_seroma_<UTCdate>`

## Post-state

- present_rows: 1,407 → **79** (45 keep pts)
- present_pts: 873 → **45**
- absent_rows: 844 (unchanged)
- canonical_pmh_events_v1 seroma rows: 0 → **84**
- Rollup ever_seroma: definitive=0, probable_or_better=39, any_evidence=45
- Projected analytic prevalence: 45 / 10,871 = 0.41% (close to literature 1-2% lower bound)

## Cross-migration carry-forwards

- **CF-COMPLICATIONS-REGISTRY-FLIP** — 4 of 8 sub-migs done (98a vcp ✓, 98b chyle ✓, 98c voice/nerve ✓, 98d seroma ✓); flips in mig_99 after 98e-h.

## Pattern reuse (for mig_98e hematoma / 98f-g hypoCa pair / 98h mortality)

- Bulk priority-rule disposition is reusable for any complication where structured signal is sparse (&lt;5% of cohort) and template noise dominates.
- evidence_strength standardization tiers: definitive (phen_confirmed+treat_req) / probable (phen_confirmed OR REAL) / possible (defensive only).
- Same PMH row template: `is_preexisting=TRUE`, `source_table='mig_98X_pmh_synthetic'`, anchor_source = mig-specific.
