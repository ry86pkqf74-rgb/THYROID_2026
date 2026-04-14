# MRQ reconciliation memo — checked-in claims vs live MotherDuck

**Update (2026-04-14):** Live prod (and dev/qa clones) now carry **tier-policy** `verification_status` values (`auto_accepted_*`) rather than `SYNTHETIC_AUTOMATION_ONLY_*`. Evidence: [`../live_state_refresh_20260408_074310/mrq_status_distribution.csv`](../live_state_refresh_20260408_074310/mrq_status_distribution.csv) and [`../manuscript_human_review_release_20260413T170316Z/report.md`](../manuscript_human_review_release_20260413T170316Z/report.md). **`qa.promotion_review_decisions`** has **4** rows with non-null `decision_batch_id` (Check **5b**). Treat the **2026-04-07** §2 table as **historical artifact** only.

**Update (2026-04-08):** A fresh `119 --release-mode` run (see [`../20260407_repo_delta_gap_audit/119_release_mode_rerun/validation_report.md`](../20260407_repo_delta_gap_audit/119_release_mode_rerun/validation_report.md)) reports **no** synthetic-placeholder rows in `qa.manual_review_queue` and **11,244** reviewed / **0** pending. The **triage table** in §2 below is a **2026-04-07** snapshot — keep for audit trail only; reconcile publication language against **live** MotherDuck or the repo-delta study.

**Update (2026-04-07, later session):** `119 --release-mode` **no longer FAIL** on specimen/FHIR diagnostics in the **latest** live run — see [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md). Rows in the claim matrix below that cite “119 FAIL” refer to the **earlier** snapshot in [`validation_report.md`](validation_report.md); interpret **Release-mode PASS** in light of that newer report.

## Live triage

- **Bundle:** `exports/review_queue_triage_20260407_103411/` (`scripts/120_review_queue_triage.py --md --md-sa`, no `run_label` filter).
- **Counts:** `qa.manual_review_queue` total **5,622**; **pending (NULL `verification_status`)** **0**; all rows non-null status.

## Live `verification_status` distribution (governance-critical)

### Current (2026-04-08 live refresh — prod `Thyroid 2026`)

| verification_status | n |
|---------------------|--:|
| auto_accepted_standard | 3,081 |
| auto_accepted_critical_sample_ok | 1,646 |
| auto_accepted_informational | 893 |
| confirmed_correct | 2 |

**Interpretation:** Queue is **tier-policy auto-accepted**, not row-level clinician review. **Check 5b** passes (no synthetic-placeholder *status strings*); manuscript governance may still require human-reviewed CSV + hydrate per [`docs/publication_governance_gate.md`](../../docs/publication_governance_gate.md).

### Historical (2026-04-07 snapshot — do not cite as current)

| verification_status | n |
|---------------------|--:|
| SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF | 5,620 |
| confirmed_correct | 2 |

## `qa.promotion_review_decisions`

- **Row count (2026-04-08 live):** **4** — all rows have non-empty `decision_batch_id` per `119` Check **5b**.  
- **Historical note:** Earlier memo capture showed **2** rows with NULL batch metadata; **superseded** by tier + promotion hardening on live catalog.

## Claim-by-claim matrix (PASS / PARTIAL / FAIL vs **live**)

| Claim | Source | Reconciliation |
|--------|--------|----------------|
| Release-mode PASS | Checked-in `studies/20260407_formalization_validation_release_mode/validation_report.md` (2026-04-07) | **Historical** — that file is **04:47Z**, **20 checks**, **PASS** (predates 27-check suite). **Not** the same artifact as **signoff_live** BLOCKED or **lineage** PASS+WARN. |
| Release-mode BLOCKED (5,622 pending) | Same folder `README.md` | **FAIL** as stated — live shows **0** pending; README is **stale**. |
| NOT READY FOR FINAL SIGN-OFF | `studies/20260407_signoff_memo/signoff_memo.md` | **PARTIAL** — many early blockers resolved; **governance** issues remain (**tier `auto_accepted_*`** vs human-reviewed bar); **119** specimen gate **passed** in later run with **WARN** (not FAIL). |
| Manuscript-ready vs MotherDuck gate | Top-level `README.md` | **PARTIAL** — correctly separates local freeze vs cloud; must cite **live** validation report for current PASS/FAIL. |
| Synthetic MRQ warning | `studies/20260409_final_master_release/EVIDENCE_PACK.md` | **Historical** — predates **`auto_accepted_*`** tier rollout; live uses tier policy (see §2 current table). |

**Automation vs humans**

- **Automation:** `119` exercises structural + parity + MRQ null checks + specimen QA hooks; **earlier** in-folder run had specimen **FAIL**; **later** run **PASS WITH WARN** (see lineage audit `119_release_validation/`).  
- **Humans:** Only **2** MRQ rows show `confirmed_correct`; the remainder are **`auto_accepted_*`** (tier batch), not clinician line-by-line review. **No** credible human-reviewed adjudication package for manuscript sign-off unless policy accepts tiers.  
- **Pending / waived / source-limited:** No pending MRQ rows; “waived” N/A; source limits remain for non-Tg labs (see lab memo).
