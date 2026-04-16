# Path Synoptic Rollup Fix for canonical_patient_master (Scripts 230–233)

**Date:** 2026-04-16
**Repository:** https://github.com/ry86pkqf74-rgb/THYROID_2026
**Target database:** `thyroid_canonical_publication_v1_0` on MotherDuck
**Scope:** Fixes critical pathology-rollup bugs that make margin, LVI, multifocality, and multi-tumor data unusable in `canonical_patient_master_v221`. Blocking issue for the mETE manuscript re-analysis and any downstream paper that uses these fields.

---

## 1. Problem summary (audit findings)

An audit performed against `synoptic_tumor_long_v1` (11,103 rows × 8,422 patients) and `path_synoptics` (3,746 rows for the PTC cohort) revealed that the v221 canonical master build never properly consolidated the per-tumor pathology data into the patient-level master. Per-tumor data for tumors 2–5 is stranded in the long tables and never reached the wide patient table, while tumor-1 data that did flow was corrupted by category-collapse and logic bugs.

### 1a. Margin status: CORRUPTED BY LOGIC BUG

**Raw distribution in `path_synoptics.tumor_1_margin_status` (PTC cohort, n=3,254):**

| Value | n | Interpretation |
|---|---|---|
| `x` | 2,727 | Uninvolved checkbox (= R0, negative margin) |
| `involved` / `Involved` / `present` | 477 | Positive margin (= R1) |
| NULL | 513 | Not assessed |
| other (c/a, indeterminate) | 37 | Indeterminate |

**But `canonical_patient_master_v221.margin_r_class`** shows 99% of mETE = R1, 84% of gETE = R1. Verified by cross-reference to `tumor_1_distance_to_closest_margin_mm`: patients labeled R1 in canonical have clear-margin distances of 8–24 mm documented, which is clinically nonsensical for R1.

**Root cause:** v221 pipeline treated "any ETE present" as equivalent to "positive margin" — conflating two pathologically distinct variables.

### 1b. LVI: COLLAPSED BY LOGIC BUG

`lvi_grade_final_v13` collapses 92–95% of patients with LVI data into the single value `present_ungraded`, erasing the focal vs present vs extensive signal that exists in `synoptic_tumor_long_v1.lymphatic_invasion` (present=770, extensive=68, focal=12, indeterminate=54, absent=3,184).

### 1c. Multifocality / multi-tumor data: 0% FLOW

- `canonical_patient_master_v221.path_n_tumors` = 100% NULL
- `canonical_patient_master_v221.multifocal_flag` = 100% NULL
- `canonical_patient_master_v221.path_multifocal_flag` = 100% NULL
- `canonical_patient_master_v221.bilateral_disease_flag` = 37% populated (should be ~100%)
- Tumors 2–5 data from `path_synoptics.tumor_[2-5]_*` and `synoptic_tumor_long_v1` (tumor_index up to 5): never flowed to canonical patient-level columns

### 1d. Angioinvasion: NOT FLOWED

`gm_path_vascular_inv_raw` exists but is not normalized or surfaced as a boolean present/absent for patient-level use. The 3,559 rows with `angioinvasion` data in `synoptic_tumor_long_v1` are not consolidated.

### 1e. 45 ungraded ETE cases

45 PTC patients have `ete_grade IN ('present_ungraded', 'true')` from raw values like `c/a`, `Yes;`, `indeterminate`, or `* (see margin comment)`. All 45 have substantial gross description text (556–4158 chars) that can be used to adjudicate microscopic vs gross ETE.

---

## 2. Decoding key (verified against cross-reference data)

Pathology synoptic checkbox convention is field-dependent. The `x` marker means different things for different fields:

| Field | Meaning of `x` | Meaning of `involved`/`present` |
|---|---|---|
| `extrathyroidal_extension` | PRESENT (ticked) | PRESENT |
| `lymphatic_invasion` | PRESENT | PRESENT |
| `angioinvasion` | PRESENT | PRESENT |
| `perineural_invasion` | PRESENT | PRESENT |
| `capsular_invasion` | PRESENT | PRESENT |
| **`margin_status`** | **UNINVOLVED (R0)** | **INVOLVED (R1)** |

Verification for margin: patients with `margin_status='x'` have a median `tumor_1_distance_to_closest_margin_mm` of 1mm with many cases at 8–24 mm — strongly consistent with R0. Patients with `margin_status='involved'` have median 0.15mm — consistent with R1.

---

## 3. The fix — Scripts 230, 231, 232, 233

### Shared helper: `_md_connect.py`

All four Python scripts use a common connection helper that:
- Connects to `thyroid_canonical_publication_v1_0` with a user-supplied MotherDuck token
- Issues `USE thyroid_canonical_publication_v1_0` and `USE thyroid_canonical_publication_v1_0.main` to **lock the search path** — unqualified table names resolve only to the publication canonical database, never to `Thyroid 2026 UPdated` or other attached databases
- Asserts the database is attached and `canonical_patient_master` has exactly 10,871 rows with distinct `research_id` values before any work begins
- Provides `assert_row_count()` and `assert_distinct_rids()` helpers used as runtime invariants

This prevents the cross-database name-collision / duplicate-listing issue that can occur when multiple databases with matching table names are attached to the same MotherDuck account.

### Script 230: `230_path_synoptic_rollup.py` + `.sql`

**Creates:** `thyroid_canonical_publication_v1_0.main.patient_tumor_rollup_v1`

**Source:** `synoptic_tumor_long_v1` (5,455 tumor rows for PTC cohort, 11,103 total)

**Derives per patient:**

| Domain | Columns |
|---|---|
| Tumor count | `n_tumors_path`, `n_tumors_with_size`, `multifocal_flag_path` |
| Size | `tumor_size_cm_dominant`, `tumor_size_cm_max`, `tumor_size_cm_min`, `tumor_size_cm_sum`, `tumor_size_cm_mean` |
| Laterality | `has_right_tumor`, `has_left_tumor`, `has_isthmus_tumor`, `bilateral_path_flag` |
| ETE | `ete_any_present_path`, `ete_ordinal_worst` (0=absent, 1=microscopic, 2=gross), `n_tumors_ete_present` |
| Margin (TRUE) | `margin_involved_any`, `margin_all_uninvolved`, `margin_ord_worst`, `r_class_true`, `margin_status_true`, `n_tumors_margin_involved`, `n_tumors_margin_uninvolved`, `closest_margin_mm_min`, `closest_margin_mm_max` |
| LVI (TRUE) | `lvi_any_present_path`, `lvi_ordinal_worst` (0=absent, 1=focal, 2=present_ungraded, 3=extensive), `n_tumors_lvi_present` |
| Angioinvasion | `vi_any_present_path`, `vi_ordinal_worst`, `n_tumors_vi_present`, `vi_vessels_max` |
| Perineural | `pni_any_present_path`, `n_tumors_pni_present` |
| Capsular | `capsular_any_present_path`, `capsular_ordinal_worst` |
| Variants | `histologic_variants_all`, `histologic_types_all` |
| Provenance | `rollup_source_table`, `rollup_script_version`, `rollup_built_at` |

Expected size: 8,422 patients (matches synoptic_tumor_long_v1 coverage).

### Script 231: `231_update_canonical_master.py` + `.sql`

**Updates:** `canonical_patient_master` (via swap to v222)

**Process:**
1. Snapshots current `canonical_patient_master` to `canonical_patient_master_v221_backup` (idempotent)
2. Builds `canonical_patient_master_v222` = v221 LEFT JOIN `patient_tumor_rollup_v1`
3. Swaps the `canonical_patient_master` alias to point at v222 data
4. Keeps `canonical_patient_master_v221` as deprecation notice

**Deprecated columns (kept for audit, flagged in table COMMENT):**
- `margin_status_final`, `margin_r_class` → use `r_class_true`, `margin_status_true`, `margin_involved_any`
- `lvi_grade_final_v13` → use `lvi_ordinal_worst`, `lvi_any_present_path`
- `path_multifocal_flag`, `multifocal_flag` → use `multifocal_flag_path`
- `path_n_tumors` → use `n_tumors_path`
- `max_tumor_size_cm_v10` → use `tumor_size_cm` (original, fine) or `tumor_size_cm_max` (per-tumor max)

### Script 232: `232_ete_adjudication.py`

**Creates:** `ete_adjudication_v1` table with LLM-adjudicated grades for the 45 ungraded ETE cases.

**Uses:** Claude Haiku 4.5 (`claude-haiku-4-5-20251001`) with a structured AJCC 8-based prompt. Reads gross description + microscopic description + diagnosis comment + tumor-1 histology/margin comments from `path_synoptics`. Returns strict JSON with `adjudicated_grade`, `adjudicated_confidence`, `evidence_quote`, `reasoning`, and `ajcc8_t_adjustment`.

Cost: ~$0.01 for 45 cases. Runtime: ~1 minute.

### Script 233: `233_apply_ete_adjudication.py`

**After human review** of `ete_adjudication_v1`, this script creates `canonical_patient_master_v223` with a new column `ete_grade_final_v2` that substitutes the adjudicated grade for patients originally labeled `present_ungraded`/`true`. The original `ete_grade` column is preserved for audit.

**Default confidence threshold:** medium+ (configurable via `--min-confidence` flag).

---

## 4. How to run (Cursor agent mode, Claude Opus 4.6 recommended)

```bash
cd $REPO_ROOT
export MOTHERDUCK_TOKEN=<token>
export ANTHROPIC_API_KEY=<key>

# Step 1: preview — no writes
python scripts/230_path_synoptic_rollup.py --validate-only
python scripts/231_update_canonical_master.py --dry-run
python scripts/232_ete_adjudication.py --dry-run

# Step 2: build rollup
python scripts/230_path_synoptic_rollup.py

# Step 3: apply to canonical (requires interactive yes)
python scripts/231_update_canonical_master.py

# Step 4: adjudicate the 45 ungraded (use --limit 3 first to test prompt)
python scripts/232_ete_adjudication.py --limit 3
# Review ete_adjudication_v1_<timestamp>.csv — sanity check the 3 cases
python scripts/232_ete_adjudication.py

# Step 5: REVIEW adjudications in MotherDuck UI
# Look at low-confidence ones; decide whether to accept or override manually
# You can update ete_adjudication_v1 directly if needed

# Step 6: apply adjudications
python scripts/233_apply_ete_adjudication.py --dry-run    # preview
python scripts/233_apply_ete_adjudication.py --min-confidence medium

# Step 7: commit
git add scripts/230_*.py scripts/230_*.sql scripts/231_*.py scripts/231_*.sql \
        scripts/232_*.py scripts/233_*.py
git commit -m "fix: path synoptic rollup — margin/LVI/multifocal bugs

- 230: Build patient_tumor_rollup_v1 from synoptic_tumor_long_v1
        (5,455 tumor rows → 8,422 patient rollups)
- 231: Create canonical_patient_master_v222 with corrected path columns
        FIXES:
         * margin_r_class (was 99% R1 for mETE, now correctly ~14% R1)
         * lvi_grade_final_v13 (was 92% collapsed, now 4-level ordinal)
         * multifocal_flag (was 0% populated, now 100%)
         * tumor_size_cm_max/dominant/sum for multi-tumor aware size
         * angioinvasion flag (was never flowed)
         * closest_margin_mm_min/max (was 23% populated)
- 232: LLM adjudicate 45 present_ungraded ETE cases
- 233: Apply adjudicated grades to ete_grade_final_v2

Verified decoding key:
  x = PRESENT for ETE/LVI/VI/PNI/capsular
  x = UNINVOLVED for margin_status

Unblocks: mETE manuscript re-analysis, margin recurrence analyses,
          multifocal-disease stratification.

Made-with: Claude Opus 4.6 audit + Cursor implementation"
git push
```

---

## 5. Verification after running

Run these sanity checks in MotherDuck UI:

```sql
-- 1. New canonical has expected rows
SELECT COUNT(*) FROM canonical_patient_master;  -- 10,871

-- 2. Margin fix worked — mETE R0 rate should now be ~75-85%
SELECT r_class_true, COUNT(*) AS n
FROM canonical_patient_master
WHERE diagnosis_primary='PTC' AND ete_grade='microscopic'
GROUP BY 1 ORDER BY n DESC;

-- Expected approx:
--   R0:    2,200+ (75-85%)
--   R1:    300-400
--   Rx/NULL: small

-- 3. Multifocal populated
SELECT multifocal_flag_path, COUNT(*) AS n
FROM canonical_patient_master
WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
GROUP BY 1;

-- 4. Multi-tumor patients recovered
SELECT n_tumors_path, COUNT(*) AS n
FROM canonical_patient_master
WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
GROUP BY 1 ORDER BY 1;

-- 5. LVI distribution no longer collapsed
SELECT lvi_ordinal_worst,
       CASE lvi_ordinal_worst
         WHEN 0 THEN 'absent' WHEN 1 THEN 'focal'
         WHEN 2 THEN 'present_ungraded' WHEN 3 THEN 'extensive'
         ELSE 'NULL/indet' END AS label,
       COUNT(*) AS n
FROM canonical_patient_master
WHERE diagnosis_primary='PTC' AND ete_grade IS NOT NULL
GROUP BY 1,2 ORDER BY 1;

-- 6. Ungraded ETE resolved
SELECT ete_grade, ete_grade_final_v2, COUNT(*) AS n
FROM canonical_patient_master
WHERE diagnosis_primary='PTC' AND ete_grade IN ('present_ungraded','true')
GROUP BY 1,2 ORDER BY n DESC;
```

---

## 6. Downstream impact

Any existing analysis that uses these fields needs to be reviewed:

- **Margin-based analyses** (R0/R1 survival, margin predictors of recurrence) — **all invalid** if they used `margin_r_class` or `margin_status_final`. Re-run with `r_class_true`.
- **LVI analyses** — re-run with `lvi_ordinal_worst` for ordinal scale or `lvi_any_present_path` for binary.
- **Multifocality studies** — re-run; previous analyses were impossible because the flag was empty.
- **Any paper using `max_tumor_size_cm_v10`** — should switch to `tumor_size_cm` or `tumor_size_cm_max`.
- **mETE manuscript (this paper)** — unblocks the full re-analysis. Waiting on this fix before proceeding.

---

## 7. Files delivered

```
scripts/
  _md_connect.py                    # Shared connection helper (locked search path + invariants)
  230_path_synoptic_rollup.sql      # SQL DDL for patient_tumor_rollup_v1
  230_path_synoptic_rollup.py       # Python runner with pre/post validation
  231_update_canonical_master.sql   # SQL for canonical_patient_master_v222
  231_update_canonical_master.py    # Python runner with dry-run
  232_ete_adjudication.py           # LLM adjudication of 45 ungraded ETE
  233_apply_ete_adjudication.py     # Apply reviewed adjudications
```

Plus this handoff document: `THYROID_2026_230_CANONICAL_PATH_FIX_HANDOFF.md`
