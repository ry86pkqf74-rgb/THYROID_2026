# Final verdict memo — publication signoff (live 2026-04-07)

**Update (2026-04-07 UTC):** The **final institutional non-Tg lab wave** (`final_institutional_20260407`) was ingested via `scripts/127_analyst_institutional_lab_append.py`; see [`../20260407_institutional_lab_wave_closeout/`](../20260407_institutional_lab_wave_closeout/) and refreshed master evidence [`../20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md). The **lab-wave** manuscript caveat in the executive summary below is **superseded** for that deliverable; **MRQ / human sign-off** posture must still be verified on live MotherDuck for your promotion.

**Supersession:** Specimen/FHIR **`119` outcome changed later the same day** — see [`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md) (**`25 PASS / 2 WARN / 0 FAIL`**, `broken_fhir_refs=0`). This memo’s **governance** blockers (synthetic MRQ, lab wave) are unchanged; **automation** wording below reflects the **earlier** in-folder `validation_report.md` snapshot.

## Executive summary

**Historical snapshot (memo capture time):** MotherDuck **manual_review_queue** rows were **non-null** but **5,620 / 5,622** carried **`SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`**, matching the warning in [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../20260409_final_master_release/EVIDENCE_PACK.md). **`qa.promotion_review_decisions`** was **not** a substantive human promotion package (2 rows). Longitudinal labs were described as **Tg-family waves only** with **no** final institutional non-Tg wave. **Earlier same-day `119 --release-mode`** (in-folder [`validation_report.md`](validation_report.md)) **BLOCKED** on **Specimen/FHIR QA diagnostics** (`broken_fhir_refs=10139`); **later rerun** did **not** reproduce that FAIL (see supersession link above).

**Live delta (2026-04-07 UTC operator run):** `final_institutional_20260407` is ingested; MRQ on production showed **no** synthetic placeholder slice in verification-status rollup (see closeout traceability memo). Reconcile promo decisions and MRQ counts against MotherDuck before manuscript language.

## Exactly one verdict (plan trichotomy)

**At memo capture: technically passing but blocked by human review + final lab wave.**

**Footnote (literal “technical pass” nuance):** For the **timestamped** report in [`validation_report.md`](validation_report.md), **full** `--release-mode` did **not** pass because of the specimen/FHIR **FAIL**. A **later** rerun (lineage audit folder) **passed** that check with **WARN** only. The historical label above reflected **synthetic MRQ + missing institutional chem panel**; **lab panel ingest is now closed** (see banner update). Continue to treat **WARN** / review burden and **MRQ / promotion** governance as blocking **manuscript** sign-off until you accept live MotherDuck posture.

## What would unlock branch C (high level)

1. Replace synthetic MRQ statuses with **human-reviewed** CSV + hydrate path; populate **real** promotion decisions as appropriate.  
2. Ingest **final institutional** lab CSV for required analytes **via 127** (or equivalent approved path).  
3. Keep **specimen/FHIR QA diagnostics** and **genomic review burden** within policy until `119 --release-mode` is acceptable for your sign-off bar (current later run: **PASS with WARN** — see lineage audit report).  
4. Then: `126_final_master_release.py --md` **without** `--synthetic-fill-mrq-verification`, followed by `119` + `126_release_candidate_motherduck_audit.py` per runbooks.

Commands: see [`commands_run.log`](commands_run.log).
