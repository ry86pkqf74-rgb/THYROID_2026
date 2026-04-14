# Final verdict memo — publication signoff (live 2026-04-07)

**Update (2026-04-14 UTC):** Live prod MRQ uses **`auto_accepted_*`** tier policy (not `SYNTHETIC_*` placeholder strings). Evidence: [`../live_state_refresh_20260408_074310/mrq_status_distribution.csv`](../live_state_refresh_20260408_074310/mrq_status_distribution.csv). **`qa.promotion_review_decisions`**: **4** rows with `decision_batch_id` (Check 5b). Headline **“blocked by synthetic MRQ”** below is **historical shorthand** for *automation-only / non–row-level clinician review* — current governance question is whether **`auto_accepted_*`** satisfies your manuscript bar (see [`mrq_reconciliation_memo.md`](mrq_reconciliation_memo.md)).

**Update (2026-04-07 UTC):** The **final institutional non-Tg lab wave** (`final_institutional_20260407`) was ingested via `scripts/127_analyst_institutional_lab_append.py`; see [`../20260407_institutional_lab_wave_closeout/`](../20260407_institutional_lab_wave_closeout/) and refreshed master evidence [`../20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md). The **lab-wave** manuscript caveat in the executive summary below is **superseded** for that deliverable; **MRQ / human sign-off** posture must still be verified on live MotherDuck for your promotion.

**Supersession:** Specimen/FHIR **`119` outcome changed later the same day** — see [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) (**`25 PASS / 2 WARN / 0 FAIL`**, `broken_fhir_refs=0`). **Lab-wave** blocker: **superseded** by `final_institutional_20260407` ingest (see banner above). **Governance** blocker (**MRQ / human sign-off**): still **active** — automation passes `119`, but queue remains **tier auto-accept**, not line-by-line adjudication unless policy accepts it. Executive summary below mixes **historical** capture language with later deltas; prefer the banners + [`../../docs/REPO_STATUS.md`](../../docs/REPO_STATUS.md) for the current trichotomy (automation vs governance vs source-limited).

## Executive summary

**Historical snapshot (memo capture time):** MotherDuck **manual_review_queue** rows were **non-null** but **5,620 / 5,622** carried **`SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`**, matching the warning in [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../20260409_final_master_release/EVIDENCE_PACK.md). **`qa.promotion_review_decisions`** was **not** a substantive human promotion package (2 rows). Longitudinal labs were described as **Tg-family waves only** with **no** final institutional non-Tg wave. **Earlier same-day `119 --release-mode`** (in-folder [`validation_report.md`](validation_report.md)) **BLOCKED** on **Specimen/FHIR QA diagnostics** (`broken_fhir_refs=10139`); **later rerun** did **not** reproduce that FAIL (see supersession link above).

**Live delta (2026-04-07 UTC operator run):** `final_institutional_20260407` is ingested; MRQ on production showed **no** synthetic placeholder slice in verification-status rollup (see closeout traceability memo). Reconcile promo decisions and MRQ counts against MotherDuck before manuscript language.

## Exactly one verdict (April 2026, post–live rerun)

**Technically passing — governance HOLD (automation-only MRQ tiers).**

**Meaning:** **Automation** — later same-day `119 --release-mode` = **PASS WITH WARN** (lineage audit folder; specimen/FHIR diagnostics not FAIL). **Governance** — manuscript sign-off **not** complete until policy accepts **`auto_accepted_*`** as sufficient **or** MRQ is hydrated with **human-reviewed** `verification_status`; live catalog no longer uses `SYNTHETIC_*` status strings, but the **review model** is still batch/tier automation, not row-level clinician review by default. **Lab wave** — **not** the current blocker; `final_institutional_20260407` is ingested (see banner + [`../20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md)).

**Historical (memo capture-time trichotomy):** “Technically passing but blocked by human review + final lab wave” applied **before** lab ingest closed; preserved in executive summary for audit trail only.

## What would unlock branch C (high level)

1. Replace synthetic MRQ statuses with **human-reviewed** CSV + hydrate path; populate **real** promotion decisions as appropriate.  
2. Ingest **final institutional** lab CSV for required analytes **via 127** (or equivalent approved path).  
3. Keep **specimen/FHIR QA diagnostics** and **genomic review burden** within policy until `119 --release-mode` is acceptable for your sign-off bar (current later run: **PASS with WARN** — see lineage audit report).  
4. Then: `126_final_master_release.py --md` **without** `--synthetic-fill-mrq-verification`, followed by `119` + `126_release_candidate_motherduck_audit.py` per runbooks.

Commands: see [`commands_run.log`](commands_run.log).
