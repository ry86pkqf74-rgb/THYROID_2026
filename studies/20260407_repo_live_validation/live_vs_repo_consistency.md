# Live vs repository consistency

- Audit UTC: 2026-04-07T22:08:41Z
- Credential: **read_write** (class `read_write`)

## README / signoff claims checked
- README (2026-04-07): 119 release-mode PASS WITH WARN; governance blocked on synthetic MRQ until human-reviewed hydrate.
- README: final institutional non-Tg lab wave `final_institutional_20260407` closed via script 127.
- studies/20260407_publication_signoff_live/final_verdict_memo.md: historical MRQ 5620/5622 SYNTHETIC; later deltas said operator saw no synthetic slice — reconcile on live.

## Live evidence (this run)

- **MRQ synthetic/automation filter count:** 0 rows (ILIKE '%SYNTHETIC%' OR '%AUTOMATION_ONLY%' on verification_status).
- **MRQ auto_accepted* vs confirmed_correct:** 11240 vs 4 (README: manuscript governance expects human-reviewed posture).
- **final_institutional longitudinal rows:** 989
- **Broken FHIR refs (v_diag):** 0
- **NULL decision_batch_id in promotion_review_decisions:** 0

## Stale checked-in PASS reports
- Compare this run to `studies/20260407_formalization_validation_release_mode/validation_report.md` (early 20-check PASS — treat as history per README).
- Prefer `studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md` for committed 27-check lineage audit; **re-run 119** if you need same checklist as that artifact.

## Consistency summary
- **HOLD:** governance or data wave signal differs from publication-ready bar — see `live_db_audit.md` blockers.