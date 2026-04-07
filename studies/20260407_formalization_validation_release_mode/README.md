# Formalization Validation — Release Mode

**Date:** 2026-04-07  
**Script:** `scripts/119_md_formalization_validate.py`

## Purpose

Validates MotherDuck formalization infrastructure in two modes:

- **Structural (default):** informational WARNs for missing infrastructure
- **Release (`--release-mode`):** hard FAILs that block sign-off

## Key Results (historical snapshot in this folder)

The checked-in `validation_report.md` here captured a **PASS** for an earlier run (incl. MRQ counts 16,866 — different MRQ snapshot than current live catalog).

**Do not treat the summary table below as current.** Re-run `119 --md --md-sa --release-mode` or read the live audit:

- [`studies/20260407_publication_signoff_live/validation_report.md`](../20260407_publication_signoff_live/validation_report.md)
- [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](../20260407_publication_signoff_live/final_verdict_memo.md)

## Historical table (superseded narrative)

| Mode | Passed | Warned | Failed | Verdict |
|------|--------|--------|--------|---------|
| Structural | 14 | 1 | 0 | PASS |
| Release (old README claim) | 14 | 1 | 1 | BLOCKED (pending review queue) — **stale** |

The “5,622 pending / 0 reviewed” row was incorrect for current MotherDuck state; live triage shows **5,622** rows with **0** NULL `verification_status`, but **5,620** use synthetic automation-only status (not manuscript review).

## Files

| File | Description |
|------|-------------|
| `validation_report.md` | Point-in-time release-mode validation report (historical) |
