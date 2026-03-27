# MotherDuck follow-up: Excel vs `path_synoptics` LN reconcile failures

**Investigated:** 2026-03-27 (live `thyroid_research_2026`, `path_synoptics`)  
**Context:** `run_excel_vs_motherduck_ln_reconcile.py` returned **FAIL** with 0 discordant cleaned LN on **matched** keys; issues were duplicate-key ambiguity (**593**) and key mismatch (**5012**).

## Summary

| `research_id` | Issue | Root cause (MotherDuck) | Excel agrees? |
|---------------|--------|-------------------------|---------------|
| **593** | Duplicate `(research_id, surgery_date)` with **different** LN pairs | **Two rows**, same `surg_date` `2004-08-10 00:00:00`: `(0, x)` vs `(44;, 3)` | Yes — same two rows in `raw/...synoptic...xlsx` |
| **5012** | One Excel-only and one MD-only join key | **Three** synoptic rows; one row has `surg_date` = **`\t6/2/2017`**. `TRY_CAST(surg_date AS DATE)` → **NULL** in DuckDB; pandas parses → **2017-06-02** | Yes — Excel shows the tab-prefixed US date; third row sorts as 2017-06-02 |

## Evidence queries (reproducible on MotherDuck)

```sql
-- Detail for both patients (no PHI columns)
SELECT research_id,
       CAST(surg_date AS VARCHAR) AS surg_date_raw,
       TRY_CAST(surg_date AS DATE) AS surg_date_date,
       CAST(tumor_1_ln_examined AS VARCHAR) AS ln_ex,
       CAST(tumor_1_ln_involved AS VARCHAR) AS ln_pos
FROM path_synoptics
WHERE research_id IN (593, 5012)
ORDER BY research_id, surg_date_date NULLS LAST, surg_date_raw;

-- 593: two rows same calendar date
SELECT research_id,
       TRY_CAST(surg_date AS DATE) AS d,
       COUNT(*) AS n_rows,
       LIST(DISTINCT CAST(tumor_1_ln_examined AS VARCHAR)) AS distinct_ln_ex,
       LIST(DISTINCT CAST(tumor_1_ln_involved AS VARCHAR)) AS distinct_ln_pos
FROM path_synoptics
WHERE research_id = 593
GROUP BY 1, 2;

-- 5012: date parse — STRPTIME succeeds where TRY_CAST fails
SELECT research_id,
       CAST(surg_date AS VARCHAR) AS sv,
       TRY_CAST(surg_date AS DATE) AS try_cast_date,
       TRY_STRPTIME(TRIM(CAST(surg_date AS VARCHAR)), '%m/%d/%Y')::DATE AS us_date_strptime
FROM path_synoptics
WHERE research_id = 5012;
```

**Population scan (2026-03-27):** rows where `TRY_CAST(surg_date AS DATE)` is null but US-style trim+strptime works:

```sql
SELECT COUNT(*) AS n_fixable_us_date
FROM path_synoptics
WHERE research_id IS NOT NULL
  AND TRY_CAST(surg_date AS DATE) IS NULL
  AND TRY_STRPTIME(TRIM(CAST(surg_date AS VARCHAR)), '%m/%d/%Y') IS NOT NULL;
```

Result: **1** row (`research_id` 5012, value `\t6/2/2017`).

## Recommended actions

### A. `5012` — surgery date normalization (ETL / one-off MotherDuck)

- **Cause:** Leading whitespace + **M/D/YYYY** string not accepted by `TRY_CAST(... AS DATE)` for this column type/value combo.
- **Fix (pipeline):** On ingest or in a canonical view, use e.g. `COALESCE(TRY_CAST(TRIM(CAST(surg_date AS VARCHAR)) AS DATE), TRY_STRPTIME(TRIM(CAST(surg_date AS VARCHAR)), '%m/%d/%Y')::DATE)` (with guards / additional formats as needed).
- **One-off data fix on MotherDuck:** normalize that single cell to an ISO date or timestamp string DuckDB parses reliably, e.g. `2017-06-02` or `2017-06-02 00:00:00`.

**Proposed UPDATE (run only after explicit approval — targets RW database):**

```sql
-- Preview
SELECT research_id, surg_date
FROM path_synoptics
WHERE research_id = 5012
  AND CAST(surg_date AS VARCHAR) LIKE '%6/2/2017%';

-- Apply (example: normalize to DATE-castable string)
-- UPDATE path_synoptics
-- SET surg_date = '2017-06-02'
-- WHERE research_id = 5012
--   AND TRIM(CAST(surg_date AS VARCHAR)) = '6/2/2017';
```

*Do not execute in production without confirmation; align with your ingest ownership and audit trail.*

### B. `593` — duplicate same-day synoptic rows with conflicting LN

- **Cause:** **Not** an Excel-vs-MD drift issue — both sides carry **two** rows for **2004-08-10** with **incompatible** LN fields (0/x vs 44/3). This is a **source data / duplicate specimen row** problem.
- **Fix:** Manual adjudication or a deduplication rule (e.g. prefer row with both LN populated, or tie-break by row order) — must be **documented** and **not** silent if it changes analytics.

### C. Reconcile runner (optional hardening)

- Align Python join key with production truth: for `surgery_date_key`, mirror the same `COALESCE(TRY_CAST(...), STRPTIME(...))` logic used in MotherDuck canonical dates, so Excel pandas and MD SQL agree on edge cases without requiring a DB UPDATE.

## Verdict

- **5012:** **Technical parse/whitespace issue** in MotherDuck storage — **one row**, fixable with trim + US date parse or a one-row UPDATE.  
- **593:** **Duplicate/conflicting structured data** — **review/dedupe**, not LN ETL fidelity.
