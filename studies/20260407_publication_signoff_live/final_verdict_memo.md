# Final verdict memo — publication signoff (live 2026-04-07)

## Executive summary

MotherDuck **manual_review_queue** rows are **non-null** but **5,620 / 5,622** carry **`SYNTHETIC_AUTOMATION_ONLY_NOT_MANUSCRIPT_SIGNOFF`**, matching the warning in [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../20260409_final_master_release/EVIDENCE_PACK.md). **`qa.promotion_review_decisions`** is **not** a substantive human promotion package (2 rows). Longitudinal labs are **Tg-family waves only**; **no** final institutional non-Tg wave. **`119 --release-mode`** latest run **BLOCKED** on **Specimen/FHIR QA diagnostics** (`broken_fhir_refs=10139`).

## Exactly one verdict (plan trichotomy)

**TECHNICALLY PASSING BUT BLOCKED BY HUMAN REVIEW + FINAL LAB WAVE**

**Footnote (literal “technical pass” nuance):** Strictly speaking, **full** `--release-mode` did **not** pass on this run because of the specimen/FHIR **FAIL**. The label above reflects the **primary manuscript governance gaps** called out in repo intent (synthetic MRQ + missing institutional chem panel). Treat specimen/FHIR QA as an **additional** hard gate before claiming any “all-green” release signoff.

## What would unlock branch C (high level)

1. Replace synthetic MRQ statuses with **human-reviewed** CSV + hydrate path; populate **real** promotion decisions as appropriate.  
2. Ingest **final institutional** lab CSV for required analytes **via 127** (or equivalent approved path).  
3. Remediate **specimen/FHIR QA diagnostics** until `119 --release-mode` is fully PASS.  
4. Then: `126_final_master_release.py --md` **without** `--synthetic-fill-mrq-verification`, followed by `119` + `126_release_candidate_motherduck_audit.py` per runbooks.

Commands: see [`commands_run.log`](commands_run.log).
