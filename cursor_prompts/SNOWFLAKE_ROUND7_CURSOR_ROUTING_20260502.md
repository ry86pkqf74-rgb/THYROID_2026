# Snowflake Round-7 Findings — Cursor Routing Summary

**Generated:** 2026-05-02 by Cowork.

4 follow-on Cursor prompts after mig_260-265 closed. Run order matters — mig_264b reduces the residual count for mig_266's Bethesda footnote; mig_267 unblocks downstream cohort views.

| # | Mig | Title | Tool | Severity | Run order |
|---|---|---|---|---|---|
| 1 | mig_264b | Bethesda-2 obvious-fix subset (NIFTP/follicular adenoma/negative-FNA-day) | **Cursor Composer** | MED | First |
| 2 | mig_266 | Bulk manuscript footnote update across M032/M037/M044/M025/M027/M004 | **Cursor Composer** | MED | Second (after 264b sets the residual ROM number) |
| 3 | mig_267 | canonical_histology_lookup_v1 SSOT | **Cursor Chat → Composer** | MED | Third |
| 4 | mig_268 | Residual focality drift cleanup (2 rows from mig_261) | **Cursor Composer** | LOW | Anytime (independent) |

## File index (cursor_prompts/)

1. `CURSOR_PROMPT_MIG_264B_BETHESDA2_OBVIOUS_FIX_20260502.md` — applies mig_264 audit findings; ~43 row changes
2. `CURSOR_PROMPT_MIG_266_MANUSCRIPT_FOOTNOTE_BULK_20260502.md` — text-only edit across 5 manuscript drafts; declares 6 round-6 conventions in methods sections
3. `CURSOR_PROMPT_MIG_267_HISTOLOGY_LOOKUP_SSOT_20260502.md` — Chat decision pass + SSOT lookup table + downstream re-point
4. `CURSOR_PROMPT_MIG_268_FOCALITY_RESIDUAL_DRIFT_20260502.md` — 2-row cleanup, 5 SQL statements

## After all 4 land

Cohort numbers will shift slightly:
- Bethesda 2 + IS_MALIGNANT: 385 → ~342 (mig_264b)
- 22 NIFTP + 2 follicular adenoma now `IS_MALIGNANT=FALSE` — total cohort malignancy rate drops from 38.1% to 37.9%
- Histology grouping replaced with SSOT JOIN — no numeric change but consistency across manuscripts

Re-run the affected scripts:
```bash
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/01_export_md_to_parquet.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/02_load_to_snowflake.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/04_build_flat_views.py
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/13_prompt7_tirads_bethesda.py  # post mig_264b
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/09_m037_table1.py              # post mig_267
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/19_m044_table1.py              # post mig_267
SNOWFLAKE_PAT='...' python snowflake_trial/scripts/22_m037_table2_logreg.py        # post mig_267
```

## Optional (not in this batch)

- **mig_269** — `canonical_recurrence_events_v1` SSOT (only worth doing if M044 Cox PH model needs cleaner recurrence input — defer until manuscript priority confirmed)
- **NLP refresh batch** — standalone workstream re-running NLP entity extraction against Social History sub-sections, CAP "Lymph-Vascular Invasion" patterns, and recurrence note text. Closes CF-mig260b/c/d + CF-mig261c/d/e. Big scope, not a single Cursor mig.
