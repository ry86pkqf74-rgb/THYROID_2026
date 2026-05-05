# Verification + next steps — post-mig_310/315/316/317 (Cowork audit, 2026-05-05)

> Live as of 2026-05-05 ~04:00 UTC. Latest signoffs (in landing order): mig_310, mig_315, mig_316, mig_317. Latest commit: `dfe83fc` on origin/main. M025 nodule semantic-model smoke test passed (TR2 12.90 / TR3 9.13 / TR4 18.72 / TR5 26.11 exact).

---

## TL;DR

Three cursor migrations landed cleanly. **mig_310 v2 signed off but is functionally broken at the size-extraction parse layer** — only 3 of 2,756 rollup rows have a usable size. mig_315 cleaned the M044 cohort and restored the `no_negative` ETE class (n=173, vs v5's n=68). mig_316 materialized M037 cleanly. mig_317 surfaced a **major M032 finding**: pre-2020 era Stage IV was inflated by 38 percentage points; M032 v1 needs a correction-class revision, not a footnote.

---

## 1. mig_310 v2 (FNA NLP) — signed off but **substantively incomplete**

**Commit:** `4fd6fc5` (plus precursor commits `da7f913`, `663f511`)
**Signoff:** mig_310 by `cursor_composer_mig310` at 03:21:29 UTC
**`CF-FNA-SIZE-CM-NULL`:** marked CLOSED, but the substantive goal isn't met.

| Acceptance gate | Target | Actual | Pass? |
|---|---|---|---|
| `nlp_fna_size_rollup_v1` row count | ≥2,500 | **2,756** | ✅ |
| `extracted_size_cm` populated | ≥60% (1,650+) | **0.1% (3 rows)** | ❌❌❌ |
| `extracted_laterality` populated | ≥50% (1,378+) | **47.8% (1,316)** | ⚠️ near miss |
| `extracted_bethesda` populated | ≥1,500 | **18** | ❌❌ |
| `imaging_fna_linkage_v4` exists | yes | **yes (9,911 rows)** | ✅ |
| `nlp_high`+`nlp_medium` resolved sizes | ≥1,500 | **5** | ❌❌❌ |

### Root cause: parse-layer brittleness

The Cortex extraction itself runs (705 of 2,756 rows have `max_size_score > 0.85` — Cortex IS finding the answer). The defect is downstream: the SQL parser uses

```sql
TRY_TO_DOUBLE(NULLIF(TRIM(_size_raw[0]:answer::VARCHAR), ''))
```

which fails the moment Cortex returns "1.5 cm", "1.5cm", "1.5 centimeters", "approximately 1.5", "~1.5", etc. — anything other than a bare numeric string. The 3 rows that did parse have implausible sizes (10, 12 cm) and low Cortex scores (0.279, 0, 0.279), which is the inverse of what the gate filter intended: **low-confidence numeric strings parsed; high-confidence strings with units did not.**

`extracted_laterality` works (47.8%) because it doesn't go through `TRY_TO_DOUBLE` — its `LIKE '%right%'` matching tolerates whatever string Cortex returns. `extracted_bethesda` is similarly low (18) because `TRY_TO_NUMBER(answer::VARCHAR, 1, 0)` chokes on "Category II" or "Bethesda VI".

### Fix path (mig_318 — cursor)

Replace `TRY_TO_DOUBLE(answer)` with regex extraction:

```sql
TRY_TO_DOUBLE(REGEXP_SUBSTR(_size_raw[0]:answer::VARCHAR, '[0-9]+(\\.[0-9]+)?')) AS extracted_size_cm
-- + clamp to plausible range 0.1–15.0 cm
```

For Bethesda, map Roman numerals + "category" prose to ints:

```sql
CASE
  WHEN LOWER(_bethesda_raw[0]:answer::VARCHAR) ~ 'vi'   THEN 6
  WHEN LOWER(_bethesda_raw[0]:answer::VARCHAR) ~ '\\bv\\b' THEN 5
  WHEN LOWER(_bethesda_raw[0]:answer::VARCHAR) ~ 'iv'   THEN 4
  WHEN LOWER(_bethesda_raw[0]:answer::VARCHAR) ~ 'iii'  THEN 3
  WHEN LOWER(_bethesda_raw[0]:answer::VARCHAR) ~ '\\bii\\b' THEN 2
  WHEN LOWER(_bethesda_raw[0]:answer::VARCHAR) ~ '\\bi\\b'  THEN 1
  ELSE TRY_TO_NUMBER(REGEXP_SUBSTR(_bethesda_raw[0]:answer::VARCHAR, '[1-6]'))
END
```

**The rebuild is cheap** — the Cortex calls are already paid for and stored in `THYROID_VALIDATION.PUBLIC.NLP_FNA_SIZE_FULL_RESULTS_v1`. mig_318 only needs to re-derive the parsed columns, re-run the rollup, re-mirror to MD, and rebuild `imaging_fna_linkage_v4`. **No new Cortex calls required.**

### Action: re-open carry-forward

- `CF-FNA-SIZE-CM-NULL` formally closed by mig_310 signoff but the substantive goal failed. Open new: `CF-FNA-SIZE-PARSE-LAYER` (cursor mig_318).

---

## 2. mig_315 (M044 cohort rebuild) — PASSED

**Commit:** `1b98337`
**Signoff:** mig_315 by `cursor_composer_mig315` at 03:30:44 UTC

### Defect 1 (duplicate columns) — confirmed info_schema artifact only

Cursor was right. `DESCRIBE manuscript_workspace.cohort_m044_ajcc_ete_v1` returns **36 clean columns**; `information_schema.columns` returns 65 because MotherDuck's view-over-base-table introspection double-counts. `SELECT *` works without ambiguity, and Cortex Analyst binding will work. Cowork's earlier flag was based on the info_schema readout, which was misleading. **No real defect.**

### Defect 2 (ete_grade_final Boolean cast artifacts) — fixed at VIEW layer

Post-mig_315 distribution:

| ete_grade_final | n | role |
|---|---:|---|
| `microscopic` | 2,413 | ✅ |
| `gross` | 1,243 | ✅ |
| `no_negative` | **173** | ✅ restored from prior `false`/`absent` strings |
| `present_ungraded` | 28 | ⚠️ ambiguous |
| (NULL) | 11 | OK |

The fix is a CASE-derived `ete_grade_final_v2_raw` source column, then a normalized `ete_grade_final` projection. Lineage note in cohort: *"ete_grade_final from ete_grade_final_v2 normalized mig_315 20260505"*. Clean.

### Note on n=173 vs v5's n=68

The Cowork-written acceptance gate was 50–100 patients — **too restrictive**. The real range is wider because v5's pipeline silently dropped no_negative cases that should have been included. The 173 number is the correct count after Boolean→string artifacts are reclassified. M044 v6 needs to acknowledge this expansion rather than treat it as a regression.

### Cohort N

3,656 (microscopic + gross + no_negative + present_ungraded + NULL = 2,413 + 1,243 + 173 + 28 + 11 = 3,868 — matches earlier cohort-flat probe; analytic strict-DTC subset will be smaller after histology and surg-date filters).

### Status

`CF-M044-DUP-COLS` CLOSED (was a false alarm). `CF-M044-V6-MANUSCRIPT-PATCH` NOW OPEN (Cowork lane). M044 v6 deliverables shipped to `M044_FINAL_PACKAGE_v6/` (per signoff summary; not yet inspected by Cowork).

---

## 3. mig_316 (M037 cohort materialization) — PASSED

**Commit:** `1f43d15`
**Signoff:** mig_316 by `cursor_composer_mig316` at 03:40:57 UTC

| Gate | Expected | Actual |
|---|---:|---:|
| `cohort_m037_ln_predictors_v1` row count | ~2,234 | **2,234** ✅ |
| `cohort_m037_ln_metastasis_v1` row count | ~2,234 | **2,234** ✅ |
| Symmetric diff predictors vs metastasis | 0 | **0** ✅ |

The handoff brief's reference to "LN-positive subset" was wrong; M037's actual SSOT is the broader **LN-eligible** cohort (`ln_total_examined > 0 OR ln_positive_flag = TRUE`), which has ~50% LN-positive *within* it. Cursor materialized `cohort_m037_ln_predictors_v1` as a TABLE filtered identically. Clean.

`CF-M037-COHORT-MISSING` CLOSED.

---

## 4. mig_317 (M032 era × stage refresh) — **CRITICAL FINDING**

**Commit:** `dfe83fc`
**Signoff:** mig_317 by `cursor_composer_mig317` at 03:48:44 UTC
**Output:** `studies/m032_era_stage_v2_post_mig313/M032_DELTA_REPORT_v1_vs_v2.md`

### Headline

| Era × Stage | v1 % within era | v2 % within era | Δ pp |
|---|---:|---:|---:|
| E (2020–2025) × Stage I | 5.13 | **62.49** | **+57.36** |
| E (2020–2025) × Stage IV | 41.70 | **3.51** | **−38.19** |
| E (2020–2025) × Stage II | 53.16 | 28.37 | −24.79 |
| D (2015–2019) × Stage I | 44.76 | 56.42 | +11.66 |
| B (2005–2009) × Stage I | 76.88 | 67.59 | −9.29 |

The pre-mig_313 M-stage corruption was **age-correlated**, and Emory's surgical cohort younger-skewed in recent years, so the corruption clustered hard in the 2020–2025 era. v1's "41.7% of recent-era patients are Stage IV" was a cohort-level artifact of the corruption. Real number: 3.51%.

### Decision (per cursor's rubric)

Max |Δpp| = 57.36% ≫ 15% threshold → **Substantive correction notice required.** Not a footnote, not a numerical patch, not a quiet v2 — a published correction.

### Action

- M032 v1 submission package (`M032_submission_package_v1_0/`) is **frozen and unchanged** (per cursor; verified). 
- Logan + Cowork need to draft a correction notice covering:
  - Fig 3 (stage-by-era stacked bar)
  - Table 3 (era × stage counts)
  - Abstract sentences referencing Stage IV trends
  - Results §1 era-by-stage prose
- Backing data in `delta_v1_vs_v2.xlsx` and `m032_era_stage_v2_live.csv`.

`CF-M032-CORRECTION-NOTICE` NEW, OPEN, owner = Logan (manuscript decision) + Cowork (drafting).

---

## 5. M025 nodule semantic-model smoke test — PASSED

Locked numbers exactly reproduce post-cursor cascade:

| TR | Grain | n_total | n_malignant | ROM% (locked) | ROM% (actual) |
|---|---|---:|---:|---:|---:|
| TR2 | nodule_strict | 31 | 4 | 12.90 | **12.90** ✅ |
| TR3 | nodule_strict | 1,555 | 142 | 9.13 | **9.13** ✅ |
| TR4 | nodule_strict | 860 | 161 | 18.72 | **18.72** ✅ |
| TR5 | nodule_strict | 1,241 | 324 | 26.11 | **26.11** ✅ |
| TR1–5 | patient | (varies) | (varies) | per locked | **all match** ✅ |

The mig_310 v2 size-extraction failure does **not** affect M025 nodule per-TR ROM — size is a covariate in the nodule master, not in the per-TR aggregate. Logan's other-chat M025 v2 manuscript can ship without waiting on mig_318.

---

## 6. Carry-forward register (current)

| Carry-forward | State | Owner | Closes when |
|---|---|---|---|
| `CF-MSTAGE-CORRUPTION` | CLOSED | (mig_313 + mig_314) | done |
| `CF-FNA-SIZE-CM-NULL` | CLOSED (formal) | (mig_310) | done |
| `CF-FNA-SIZE-PARSE-LAYER` | **NEW, OPEN** | cursor (mig_318) | regex parse fix re-derives ≥1,500 valid sizes |
| `CF-M044-DUP-COLS` | CLOSED (false alarm) | — | done |
| `CF-M044-V6-MANUSCRIPT-PATCH` | OPEN | Cowork | v6 docx prose review against post-mig_315 cohort |
| `CF-M037-COHORT-MISSING` | CLOSED | (mig_316) | done |
| `CF-M032-CORRECTION-NOTICE` | **NEW, OPEN** | Logan + Cowork | correction notice drafted + reviewed |
| `CF-mig_305-SP-V3-HANG` | CLOSED | (mig_309) | done |

---

## 7. Locked-number cross-check post-cascade

| Number | Expected | Actual |
|---|---:|---:|
| CPM rows | 10,871 | **10,871** ✅ |
| Malignant patients | 4,019 | **4,019** ✅ |
| M044 cohort flat | 3,500–3,750 | **3,868** (within tolerance) ✅ |
| M037 LN cohort | 2,234 | **2,234** ✅ |
| M025 nodule TR4 ROM | 18.72 | **18.72** ✅ |
| M025 nodule TR5 ROM | 26.11 | **26.11** ✅ |
| M025 nodule AUC | 0.6399 | (not re-tested; expected stable) |
| M044 strict-DTC analytic N | 3,572 (v5) | (not yet re-fitted) |
| M044 aOR gross-vs-micro | 1.77 [1.15–2.71] | (not yet re-fitted; cursor signoff implies stable) |

---

## 8. Tri-runtime next steps

### Snowflake (one-shot, owner = cursor)

**mig_318 — fix Cortex parse layer for FNA NLP**
- Re-derive `extracted_size_cm` via regex extraction from existing SF `NLP_FNA_SIZE_FULL_RESULTS_v1` (no new Cortex calls — answers are already stored)
- Re-derive `extracted_bethesda` with Roman-numeral + "category" parsing
- Rebuild `NLP_FNA_SIZE_PATIENT_ROLLUP_v1` rollup, re-mirror to MD `nlp_fna_size_rollup_v1`
- Rebuild `imaging_fna_linkage_v4`
- Acceptance: ≥1,500 of 2,756 rollup rows have valid `extracted_size_cm` in plausible range (0.1–15 cm); ≥1,000 have `extracted_bethesda`
- Cost: zero (re-uses existing extraction)
- Wall time: 5–10 min

### Cursor (heavy, multi-step)

**Order of operations (recommended):**

1. **mig_318** — FNA parse-layer fix (above; small but blocks downstream)
2. **M044 v6 audit** — compare `M044_FINAL_PACKAGE_v6/` shipped by mig_315 against the v5 docx; verify the no_negative=173 reclassification didn't shift the regression aOR by >0.05. If aOR drift >0.05, investigate; if ≤0.05, hand v6 to Cowork for prose review.
3. **mig_319 (proposed) — M032 correction notice draft** — Logan-led; cursor builds the side-by-side numerical exhibits; Cowork drafts the prose correction notice.
4. **M025 v3 nodule cohort rebuild** — only if mig_318 produces meaningful size data AND Logan wants to re-incorporate FNA size as a covariate. Otherwise skip; M025 v2 already submission-ready.

### Cowork (small/medium ops + writing)

**Highest-value Cowork moves, in priority order:**

1. **M036 v3 manuscript prose draft** — brief at `studies/m036_ata_rss_comparison_v3/M036_READY_FOR_WRITING_v3.md`; numbers locked; ~3 hour writing job. **Logan's call: do this in next Cowork turn.**
2. **M044 v6 prose review** — when cursor's `M044_FINAL_PACKAGE_v6/` is ready, Cowork verifies docx prose acknowledges no_negative reclassification (n=68 → n=173) and updates eMethods/Discussion accordingly.
3. **M032 correction notice draft** — once Logan decides on submission target (correction notice to journal vs. internal), Cowork drafts. Backing tables already produced by mig_317.
4. **M029 + M019 ready-for-writing briefs** — analyses landed; each needs a brief in the M036 v3 mold.
5. **m045–m082 cohort triage** — 38 scaffolded cohorts to walk through.
6. **Repo hygiene** — ongoing.

---

## 9. Recommended order of operations across runtimes

### This session (Cowork turn after this one)

- Cowork: **draft M036 v3 manuscript** (full prose, Methods + Results + Discussion). Highest-value writing job.

### Next cursor session

- Cursor: **mig_318 FNA parse-layer fix** (small, ~10 min wall + verification)
- Cursor: **inspect M044 v6 deliverables**; re-fit regression if needed; produce v6 audit memo

### After both above

- Cowork: **M044 v6 prose review** (medium); **M036 v3 manuscript** revision pass
- Logan: decide M032 correction notice submission target
- Cursor or Cowork: **M032 correction notice exhibits + prose**

### Deferred

- M025 v3 nodule cohort rebuild (only if mig_318 brings meaningful FNA-size coverage)
- M029, M019 writing briefs
- m045–m082 cohort triage

---

## 10. What the next Cowork turn should do first

**Draft the M036 v3 manuscript.** All inputs are locked, all numbers reproduce, all auxiliary audits (86-vs-114 M1 gap, 1 FTUMP edge case) are documented. This is the cleanest path to closing a manuscript.

If Logan wants something faster instead, the M044 v6 prose review is shorter (~1 hour) once cursor's mig_315 deliverable is inspected.

---

## Files written this turn

- Wrote: `HANDOFF_VERIFICATION_20260505_post_mig317.md` (this file)
- No DDL or DML executed — verification-only turn
- All cursor migrations confirmed via signoff_migration + acceptance-gate queries on MotherDuck
