# Formalization Validation — Release Mode

**Date:** 2026-04-07  
**Script:** `scripts/119_md_formalization_validate.py`

## Supersession — read this first

This folder holds an **early** checked-in **`validation_report.md`** (04:47 UTC, **20 checks**, verdict **PASS**). The **current** release-mode validator in repo runs **27 checks** (molecular + specimen/FHIR, etc.).

**Authoritative chain for “what does `119 --release-mode` say *now*?”**

1. **Re-run locally** with `--output-dir studies/<your_run>/`, **or**
2. Use the latest committed full run: **[`../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md`](../20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md)** — **25 PASS / 2 WARN / 0 FAIL** (specimen/FHIR diagnostics **WARN** only; `broken_fhir_refs=0`).
3. **Contrast / history:** [`../20260407_publication_signoff_live/validation_report.md`](../20260407_publication_signoff_live/validation_report.md) — **BLOCKED** at 10:33 UTC (`broken_fhir_refs=10139`); folder README explains same-day dual outcomes.

Narrative catalog + lineage contract: [`../20260407_live_truth_and_lineage_contract_audit/live_truth_audit.md`](../20260407_live_truth_and_lineage_contract_audit/live_truth_audit.md).

## Purpose

Validates MotherDuck formalization infrastructure in two modes:

- **Structural (default):** informational WARNs for missing infrastructure  
- **Release (`--release-mode`):** strict FAILs for sign-off infrastructure

## Why this folder is “historical”

- **`validation_report.md` here** = **20 checks**, MRQ **16,866** reviewed — predates the **27-check** suite and later MRQ hydrates (**5,622** rows in live signoff-era audits).  
- **Do not** merge its PASS/COUNTS with later reports without labeling timestamps.

## Files

| File | Description |
|------|-------------|
| [`validation_report.md`](validation_report.md) | Point-in-time release-mode report (**04:47Z**, 20 checks, **PASS**) — preserved for diff/history |

## Old README table (deprecated)

Earlier text in this README mixed claims about “BLOCKED pending queue” with different artifacts; see **Supersession** above. Live governance (synthetic MRQ dominance, lab waves) is documented in [`../20260407_publication_signoff_live/`](../20260407_publication_signoff_live/).
