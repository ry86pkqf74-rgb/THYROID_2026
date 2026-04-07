# Blocker matrix — manuscript sign-off vs live MotherDuck (2026-04-07 reaudit)

Primary evidence: `live_sql_exports/*.csv`, `119_release_validation/validation_report.md`, `review_queue_triage_parent/review_queue_triage_20260407_190119/`.

| Blocker ID | Category | Live signal (2026-04-07) | Blocks automation (`119 --release-mode`)? | Blocks manuscript sign-off? | Notes |
|------------|----------|---------------------------|-------------------------------------------|------------------------------|-------|
| B1 | Specimen / genomic governance | `qa.v_diag_specimen_review_burden_v1`: **9,966** rows `specimen_genomic_link_review` + **`open`**; merge queue **1** `open` | **No** — surfaced as **WARN** only | **Yes** — human/process burden before treating bindings as publication-clean | Matches `119` WARN row; operator must burn down or formally accept policy exception. |
| B2 | Manual review queue semantics | **11,244** MRQ rows; **0** NULL `verification_status`; breakdown dominated by **`auto_accepted_*`** (11,240) vs **`confirmed_correct` (4)** | **No** | **Yes** — not human manuscript adjudication at scale | Two run labels × 5,622 rows (`20260407_tier_policy_review_gate`, `promotion_gate`) per triage `summary.md`. |
| B3 | Promotion package | `qa.promotion_review_decisions`: **3** rows | **No** | **Yes** — thin / non-substantive vs publication promotion | `final_verdict_memo.md` cited 2 rows; live **3** (minor drift; still not a full human package). |
| B4 | Institutional non-Tg lab wave | `longitudinal_lab_canonical_v1` / `longitudinal_lab_deduped_v` analyte groups: **only** `thyroid_tumor_markers` (76,971 / 55,210 rows) | **No** | **Yes** where manuscript requires PTH/Ca/TSH/vitD panel | Aligns with README “non-Tg lab pull **Pending**”. |
| B5 | Molecular contract table | `main.molecular_results`: **0** rows | **No** — `119` skips contract checks when empty | **Partial** — depends on manuscript claims using molecular_results vs canonical facts | Consistent with RELEASE_NOTES / prior `119` behavior. |
| B6 | Release-mode validator | `119`: **26 PASS / 1 WARN / 0 FAIL** | **No** (PASS WITH WARNINGS) | **No** for automation gate; **Yes** for strict “no WARN” bar if org policy requires it | Specimen/FHIR **diagnostics** row **PASS clean** this run; burden row still WARN. |

## Resolved / non-blockers for this audit

- **NULL `verification_status` structural MRQ gate:** **0** pending NULLs — does not block `119`.
- **Specimen/FHIR QA diagnostics aggregate:** **PASS** (`dup`/`orphan`/`broken_fhir_refs` clean per `validation_report.md`) — contrasts with earlier 2026-04-07 snapshot that **FAILED** `broken_fhir_refs` in `studies/20260407_publication_signoff_live/`.

## Exactly one release verdict (manuscript)

**TECHNICALLY PASSING BUT BLOCKED BY HUMAN REVIEW + FINAL LAB WAVE**

Automation is **PASS WITH WARNINGS**; manuscript remains blocked by **human/specimen–genomic governance** (B1, B2, B3) and **missing institutional non-Tg lab wave** (B4).
