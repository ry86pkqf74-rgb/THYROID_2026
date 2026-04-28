# mig_98b — Apply Logan decisions: `chyle_leak` in `canonical_complications_events_v1`

**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck `.eras` account) **Engagement:** mig_98b (post review workbook `chyle_leak_real_candidates_v2__mig_98b.xlsx`) **Executor:** `qc_framework_v1/scripts/apply_mig_98b_chyle_leak_decisions.py`**Applied:** 2026-04-28

## Deep-dive findings (drove sharper bucketing)

- 14.5% canonical prevalence (1,576 / 10,871 pts) vs literature 1-3%
- 0 / 1,576 pts have any chyle-management med (octreotide / MCT / TPN / sandostatin) in `canonical_medications_events_v1`
- Note-text classifier on `clinical_notes_long` chyle mentions:
  - 1,559 pts: TEMPLATE only (consent risk list, valsalva intra-op test, negation, prophylactic clipping)
  - 9 pts: UNMARKED — manually verified all are template variants
  - 1 pt: NO chyle text in clinical_notes_long (rid 3587, phenotype-only artifact)
  - 7 pts: REAL-pattern hits (clinical signal: drain output, JP bulb, MCT/low-fat diet, "complicated by", pressure dressing, dietary advance)

## Decisions (Logan)

ridclassifier labelLogan decision8597YES (gold)**YES** — POD1 JP-bulb chyle, pressure dressing, fat-free8333YES**YES** — clinical, clear-liquid x \~30d10949YES**YES** — c/b chyle hospitalization + 4-wk low-fat diet8815BORDERLINE**YES** — fat-free diet (promoted from BORDERLINE)11011INTRA-OP ONLY**YES** — intra-op chyle, tied off (kept as evidence)5673PRIOR-SURGERY HX**PMH** — prior MRND 3/19 c/b chyle leak; move to PMH10376LIKELY FALSE POSITIVE**NO** — consent risk list false positive

## Phases (apply script)

1. **Snapshot** chyle_leak rows pre-apply → `archive_pub_v1_0.canonical_complications_events_v1_pre98b_chyle_<ts>` (4,631 rows)
2. **DELETE** non-keep `present` chyle_leak rows (research_id NOT IN keep+pmh set) → 2,991 rows removed
3. **DELETE** rid 5673 chyle_leak rows (3 rows; moved to PMH)
4. **INSERT** synthetic PMH row in `canonical_pmh_events_v1` for rid 5673:
   - source_table = `mig_98b_pmh_synthetic`
   - source_note_type = `other_history`
   - finding_value_norm = `chyle_leak`
   - finding_date = `2019-03-15` (approx midpoint of "3/19" prior MRND)
   - mention_note_date = `2024-08-21` (when noted in OTHER_HISTORY note)
   - finding_status = `present`, evidence_strength = `definitive`
   - is_preexisting = `TRUE`, anchor_source = `mig_98b_classifier_logan_curated`
5. **Standardize** evidence_strength on the 5 kept rids:
   - rid 8597 (gold) → `definitive` (10 rows)
   - rids 8333, 10949 (clinical sequelae documented) → `probable` (20 rows)
   - rids 8815, 11011 (borderline / intra-op only) → stay `possible`
6. **Rebuild** `canonical_complications_patient_rollup_v1` via Script 364 step_5
7. **Provenance** row in `manuscript_workspace.cpm_reconciliation_provenance_v1`(run_id `mig98b_chyle_leak_<UTCdate>`)

## Post-state assertions (all green)

- present_rows: 3,028 → 34 (only the 5 keep rids)
- present_pts: 1,576 → 5
- absent_rows: 1,603 (unchanged)
- rid_5673 chyle_leak in canonical_complications: 0
- rid_5673 chyle_leak in canonical_pmh_events_v1: 1
- ever_chyle_leak_definitive: 1 pt (8597)
- ever_chyle_leak_probable_or_better: 3 pts (8597, 8333, 10949)
- ever_chyle_leak_any_evidence: 5 pts

## Projected analytic prevalence

5 / 10,871 = 0.05% (well below literature 1-3%; consistent with high-volume center reporting, or a slight under-call given Logan-conservative decisions on 8815 / 11011 borderlines).

## Schema (no schema changes this migration)

- No new columns added; mig_98a's `finding_date_source` carries over.
- canonical_pmh_events_v1 unchanged; one synthetic row inserted using the table's existing schema.

## Cross-migration carry-forwards

- **CF-COMPLICATIONS-REGISTRY-FLIP** — 15 not_started + 4 na cols on canonical_complications_events_v1 stay as-is until mig_99 (after all 8 sub-migs: 98a vocal_cord ✓, 98b chyle_leak ✓, 98c rln_injury, 98d seroma, 98e hematoma, 98f hypoparathyroidism, 98g hypocalcemia_clinical, 98h mortality).
- **CF-CHYLE-PHENOTYPE-MISS** — phenotype tier missed 8333 + 10949 (treatment_required=FALSE); note-text classifier was the source of truth for these. Worth a phenotype-pipeline fix in a future iteration.
