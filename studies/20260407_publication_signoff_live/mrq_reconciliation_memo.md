# MRQ reconciliation memo — checked-in claims vs live MotherDuck

## Live triage

- **Bundle:** `exports/review_queue_triage_20260407_103411/` (`scripts/120_review_queue_triage.py --md --md-sa`, no `run_label` filter).
- **Counts:** `qa.manual_review_queue` total **5,622**; **pending (NULL `verification_status`)** **0**; all rows non-null status.

## Live `verification_status` distribution (governance-critical)

| verification_status | n |
|---------------------|--:|
| SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF | 5,620 |
| confirmed_correct | 2 |

**Interpretation:** Almost the entire queue was filled with a **non–manuscript** synthetic status. That satisfies the *structural* release gate (“no NULLs”) but is **not** human manuscript adjudication.

## `qa.promotion_review_decisions`

- **Row count:** 2  
- **decision_batch_id:** NULL on sampled aggregates  

Sparse compared to a real promotion package; cannot stand in for org sign-off records.

## Claim-by-claim matrix (PASS / PARTIAL / FAIL vs **live**)

| Claim | Source | Reconciliation |
|--------|--------|----------------|
| Release-mode PASS | Checked-in `studies/20260407_formalization_validation_release_mode/validation_report.md` (2026-04-07) | **FAIL** live — current run BLOCKED (specimen/FHIR QA diagnostics FAIL). |
| Release-mode BLOCKED (5,622 pending) | Same folder `README.md` | **FAIL** as stated — live shows **0** pending; README is **stale**. |
| NOT READY FOR FINAL SIGN-OFF | `studies/20260407_signoff_memo/signoff_memo.md` | **PARTIAL** — many early blockers resolved; **governance + QA** issues remain (synthetic MRQ, 119 FAIL). |
| Manuscript-ready vs MotherDuck gate | Top-level `README.md` | **PARTIAL** — correctly separates local freeze vs cloud; must cite **live** validation report for current PASS/FAIL. |
| Synthetic MRQ warning | `studies/20260409_final_master_release/EVIDENCE_PACK.md` | **PASS** — **confirmed live** (`SYNTHETIC_…` dominates MRQ). |

**Automation vs humans**

- **Automation:** `119` exercises structural + parity + MRQ null checks + specimen QA hooks; latest run **does not fully pass** (specimen FAIL).  
- **Humans:** Only **2** MRQ rows show `confirmed_correct`; **5,620** are explicitly synthetic-not-for-manuscript. **No** credible human-reviewed adjudication package for manuscript sign-off.  
- **Pending / waived / source-limited:** No pending MRQ rows; “waived” N/A; source limits remain for non-Tg labs (see lab memo).
