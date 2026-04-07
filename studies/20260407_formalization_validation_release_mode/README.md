# Formalization Validation — Release Mode

**Date:** 2026-04-07  
**Script:** `scripts/119_md_formalization_validate.py`

## Purpose

Validates MotherDuck formalization infrastructure in two modes:

- **Structural (default):** informational WARNs for missing infrastructure
- **Release (`--release-mode`):** hard FAILs that block sign-off

## Key Results

| Mode | Passed | Warned | Failed | Verdict |
|------|--------|--------|--------|---------|
| Structural | 14 | 1 | 0 | PASS |
| Release | 14 | 1 | 1 | BLOCKED (pending review queue) |

## Blocker

`qa.manual_review_queue` has 5,622 pending items with 0 reviewed.
Release mode requires all promotable rows to be resolved before sign-off.

## Files

| File | Description |
|------|-------------|
| `validation_report.md` | Release-mode validation report |
