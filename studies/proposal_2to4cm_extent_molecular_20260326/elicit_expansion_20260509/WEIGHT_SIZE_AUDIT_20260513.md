# Thyroid weight / size / parathyroid weight — canonical-layer coverage audit

**Date:** 2026-05-13
**Requested by:** Logan ("confirm that thyroid weight (right/left and total where given), thyroid size by lobe (largest dimension + all 3 sizes + volume), and parathyroid gland weight where given are all fully parsed in the master canonical")

## TL;DR

| What | Status | Action needed |
|---|---|---|
| Thyroid weight — overall (total or specimen_combined) | ✅ **89%** of surgical patients | None |
| Thyroid weight — by lobe (right/left) | ⚠️ **36–37%** per lobe (often only one lobe sent at surgery) | None for the manuscript; data-limited |
| Thyroid 3D size from US (length/width/depth/volume by lobe) | ✅ **Fully parsed**; 30% of surgical patients have US dimensions | None |
| Thyroid 3D size from PATHOLOGY (`thyroid_sizes.rl_formatted` etc.) | ⚠️ **Strings parsed but not 3D-decomposed.** 99% of strings match `# × # × # cm` pattern. The 3 dimensions are stuck in a STRING; only the volume scalar is exposed. | **Parser gap — Cursor fix** |
| Total thyroid volume from PATHOLOGY | ⚠️ Only **8%** of rows have `total_formatted` populated (915/11,675); `rl_volume_cm3` covers 40% | Compute-side fix; deferred unless co-author wants per-patient totals |
| **Parathyroid weight** | ❌ **NO STRUCTURED COLUMN.** Only mentioned in LLM `evidence_quote` / `parathyroid_pathology` text for ~72 surgical patients (2.4%). | **Real gap — Cursor fix** |

## Detailed table coverage

### `pub_canonical.thyroid_weights` (n=10,001 patients)

| Column | n populated | % |
|---|---:|---:|
| `right_lobe` (FLOAT) | 3,622 | 36% |
| `left_lobe` (FLOAT) | 3,693 | 37% |
| `isthmus` (FLOAT) | 102 | 1% |
| `total_weight` (FLOAT) | 5,433 | 54% |
| `specimen_weight_combined` (FLOAT) | **9,626** | **96%** |
| Both lobes (right AND left) | 1,561 | 16% |
| Any weight populated | 9,626 | 96% |

**Surgical-cohort coverage:** 7,468 / 8,368 surgical patients (89%) have at least one weight value.

The dominant column is `specimen_weight_combined` (96% populated). The per-lobe split is sparser because in many lobectomies only one specimen is sent and in many totals the surgeon submits the gland as a single piece. **For the manuscript this is fine** — `specimen_weight_combined` works as a single weight covariate; per-lobe weight is a sub-analysis only available for ~16% of patients.

### `pub_canonical.thyroid_sizes` (n=11,675 / 11,671 distinct patients — PATHOLOGY-side)

| Column | n populated | % | Notes |
|---|---:|---:|---|
| `rl_formatted` (STRING) | 4,690 | 40% | Pattern `# × # × # cm` in 99.4% (4,664/4,690) — **3D in text** |
| `ll_formatted` (STRING) | 4,686 | 40% | Same pattern |
| `isthmus_formatted` (STRING) | 2,965 | 25% |  |
| `total_formatted` (STRING) | 915 | 8% |  |
| `rl_volume_cm3` (FLOAT) | 4,672 | 40% | scalar volume parsed |
| `ll_volume_cm3` (FLOAT) | 4,666 | 40% |  |
| `total_volume_cm3` (FLOAT) | 914 | 8% |  |
| `surg_date` | STRING type, not date — should be DATE | — | Minor schema fix |

**Surgical-cohort coverage:** 4,593 / 8,368 (55%) have any pathology-side thyroid size.

**The gap:** `rl_formatted` is `"5.2 x 1.8 x 2.0 cm"` style strings — the 3 dimensions are in the data but not exposed as separate `rl_length_cm_path`, `rl_width_cm_path`, `rl_depth_cm_path` numeric columns. If you want to do **largest-dimension** analyses on the pathology side (analogous to `imaging_nodule_size_cm` on the imaging side), you need the parsed components. **The structured `# × # × # cm` pattern means this is a 30-line Cursor fix** with no LLM cost — a regex pulls out the three floats.

### `pub_canonical.canonical_us_thyroid_gland_v2` (n=13,578 / 10,859 distinct patients — ULTRASOUND-side)

| Column | n populated | % |
|---|---:|---:|
| `rl_length_cm` / `rl_width_cm` / `rl_depth_cm` / `rl_volume_ml` | 6,793 each | 50% |
| `ll_length_cm` / `ll_width_cm` / `ll_depth_cm` / `ll_volume_ml` | 6,793 each | 50% |
| Full RL 3D (length AND width AND depth) | 6,793 | 50% |
| `total_thyroid_volume_ml` | 6,793 | 50% |
| `isthmus_thickness_mm` | — | — (single dim only; that's correct, isthmus is reported as thickness) |

**Surgical-cohort coverage:** 2,491 / 8,368 (30%) of surgical patients have US lobe dimensions. (Many surgical patients don't have a thyroid US in this institution's data; those who do have COMPLETE 3D parsing.)

**This is the well-parsed table.** No gap. Use this for any imaging-side size analysis.

### `pub_canonical.canonical_parathyroid_events_v1` (n=8,697 / 4,443 distinct patients)

| Column | n populated | % |
|---|---:|---:|
| `glands_identified_count` | 2,988 | 34% |
| `glands_autotransplanted` | 348 | 4% |
| `parathyroid_pathology` (text) | 3,885 | 45% |
| `intact_pth_value_ngL` | 578 | 7% |
| `evidence_quote` with weight keyword (mg/gram/gm) | 374 | 4% |
| `reasoning` with weight keyword | 153 | 2% |
| `parathyroid_pathology` with weight keyword | 0 | 0% |

**Surgical-cohort coverage:** 3,020 / 8,368 (36%) have a parathyroid event; only **72 (2.4%)** of surgical patients have any weight text signal in the LLM extraction. The structured weight column **does not exist**.

**The gap:** There is no `parathyroid_weight_mg` column on `canonical_parathyroid_events_v1`. Where weight is documented (incidental parathyroidectomy or planned parathyroidectomy with a recorded path-spec weight), the value is buried in `evidence_quote` or `reasoning` LLM-extracted text. ~72 surgical patients (likely the M084-relevant subset of parathyroid surgeries with thyroid co-pathology) have weight mentions. A regex extraction over the existing LLM evidence text would surface these into a new `parathyroid_weight_mg` numeric column.

## What's already good

- **Thyroid weight overall**: 89% surgical-cohort coverage via `specimen_weight_combined`. Usable for the manuscript.
- **US 3D parsing**: complete on `canonical_us_thyroid_gland_v2` for the patients who have US.
- **Total thyroid volume from US**: 6,793 rows.

## What needs Cursor

Both gaps are small, structured, and additive. Combined estimated effort: ~1 hour of Cursor work + a skill version bump to **v2.3.1 (patch)**.

### Gap 1: Parse `thyroid_sizes.rl_formatted` / `ll_formatted` / `total_formatted` into 3D columns

Add columns:
- `rl_length_cm_path`, `rl_width_cm_path`, `rl_depth_cm_path` (FLOAT)
- `ll_length_cm_path`, `ll_width_cm_path`, `ll_depth_cm_path` (FLOAT)
- `total_length_cm_path`, `total_width_cm_path`, `total_depth_cm_path` (FLOAT)
- `rl_largest_dim_cm_path`, `ll_largest_dim_cm_path` (computed: max of the three)

Regex for the standard `# × # × # cm` pattern:
```python
import re
DIM3_RX = re.compile(r'(\d+(?:\.\d+)?)\s*[x×]\s*(\d+(?:\.\d+)?)\s*[x×]\s*(\d+(?:\.\d+)?)\s*cm', re.I)
```
Run on `rl_formatted` / `ll_formatted` / `total_formatted`. Expected yield based on pattern audit: 4,664/4,690 = 99% successful extraction on `rl_formatted`.

Edge cases:
- `# cm` (1D only): 18 rows on rl_formatted — set length only, leave width/depth NULL
- `# × # cm` (2D only): 6 rows — set length+width, leave depth NULL
- `-# × # × # cm` (leading minus, likely OCR artifact): 2 rows — manually inspect or set NULL

### Gap 2: Extract `parathyroid_weight_mg` from LLM evidence text

Add column `parathyroid_weight_mg` (FLOAT) on `canonical_parathyroid_events_v1`.

Regex search over `evidence_quote` + `reasoning` + `parathyroid_pathology` concatenated:
```python
WEIGHT_RX = re.compile(r'(?:weight[s]?(?:\s*of)?\s*[:\-]?\s*|=\s*)(\d+(?:\.\d+)?)\s*(mg|gm|g\b|gram[s]?)', re.I)
```
Convert `g`/`gm`/`gram` → multiply by 1000 to normalize to mg.

Expected yield: ~150 rows globally; ~72 in the surgical cohort. Small enough to manually review the regex hits before committing.

## Manuscript impact

- **None for EXT2-4 v3** as currently scoped. The Table 1 cohort characteristics use only `specimen_weight_combined` and `surg_total_thyroidectomy`/`surg_hemithyroidectomy` — no need for per-lobe weight or pathology-side 3D dimensions.
- **Future analyses** that would need these fixes:
  - Substernal goiter studies (need 3D dimensions to define substernal extension by depth)
  - Tumor-density / weight-per-volume analyses (need both weight and 3D)
  - M084 parathyroid manuscript (parathyroid weight is clinically interesting)
- **Recommendation:** schedule both fixes as a single Cursor session before the next manuscript that needs either; not blocking for EXT2-4 v3.

## Outputs

- `WEIGHT_SIZE_AUDIT_20260513.md` (this file)
- `CURSOR_PROMPT_thyroid_size_3D_and_parathyroid_weight.md` (handoff)

---

## Post-apply addendum — 2026-05-14

**Cursor wrote `scripts/mig_326_thyroid_3d_parathyroid_weight_bq.py` and Cowork ran `--apply` via Desktop Commander** (run_id `mig_326_20260514_c7a1ec0e`). Snapshots created at `pub_archive.thyroid_sizes_pre_3d_parse_20260514` and `pub_archive.canonical_parathyroid_events_v1_pre_weight_extract_20260514`.

### Phase 1 verification — thyroid_sizes pathology-side 3D parsing — ✅ PASSED

| Field | Source rows | Parsed | Parse frac |
|---|---:|---:|---:|
| `rl_length_cm_path` | 4,690 | 4,666 | **99.5%** |
| `ll_length_cm_path` | 4,686 | 4,660 | **99.5%** |
| `total_length_cm_path` | 915 | 914 | **99.9%** |

All exceed the 97% acceptance gate. 42 distinct `dim_parse_status` values, covering 3D-parsed, 2D-only, 1D-only, empty, and unparseable edge cases.

### Phase 2 verification — parathyroid_weight_mg — ⚠️ PARTIAL PASS

| Metric | Value | Gate | Status |
|---|---:|---|---|
| Rows with `parathyroid_weight_mg` (global) | 16 | ≥ 50 | ⚠️ Below gate |
| Rows in surgical cohort | 16 | ≥ 30 | ⚠️ Below gate |
| Median weight | 561 mg | 200–800 mg | ✅ In range |
| Min weight | 10 mg | ≥ 20 mg | ⚠️ Below floor (flagged) |
| Max weight | 19,600 mg | ≤ 50,000 mg | ✅ In range |

The keyword-proximity regex was conservative on purpose (require `weight|wt|weighed|weighing` within 30 chars of the number) — that yielded only 16 hits vs the ≥50 target. The median of 561 mg is consistent with a clinically reasonable distribution (normal parathyroid 30–60 mg; adenomas 100–10,000+ mg), so the values that DID extract look real. The min of 10 mg is below the 20 mg sanity floor and should be manually inspected; it could be a small specimen fragment or a unit-conversion edge case.

### Recommended follow-up

If M084 (parathyroid manuscript) needs more rows:
1. **Manually inspect the 16 extracted values** for unit-conversion and small-specimen edge cases (export `parathyroid_event_id`, `parathyroid_weight_mg`, evidence keys — keep CSV off git, internal-share only).
2. Either accept the 16 rows as-is (clinically informative, conservative regex), OR relax the keyword-proximity rule to widen the window (50 chars) and re-run, which might yield 30–50 more hits at the cost of additional false-positive risk.

### Manuscript impact

- **EXT2-4 v3**: nil. Table 1 uses `specimen_weight_combined` and `surg_total_thyroidectomy`/`surg_hemithyroidectomy` only. No rebuild needed.
- **M084 (parathyroid adenoma)**: directly benefits. The new `parathyroid_weight_mg` column is now available as a covariate; n=16 with weight data may be enough for descriptive statistics but is small for inferential modeling.
- **Future substernal-goiter analyses**: directly benefit from `rl_largest_dim_cm_path` / `ll_largest_dim_cm_path` / `rl_depth_cm_path` (substernal extension is often defined by inferior pole position relative to suprasternal notch — a depth/length question).

### Audit trail

- DFL row (pre-edit): `recxfnbbriG3rj4iB` (filed by Cursor)
- MFL row (post-edit): `MFL-20260514-EXT2-4-WEIGHT-SIZE-EXTENSION` (`recslFSEwfVWyzv4P`) — linked to both EXT2-4 (`rec1GJyrmKdKxjlaY`) and M084 (`recx6Jr6WFtF2hZxb`)
- Skill version: `thyroid-integration` bumped to **v2.3.1** by Cursor
- Snapshots: `pub_archive.thyroid_sizes_pre_3d_parse_20260514`, `pub_archive.canonical_parathyroid_events_v1_pre_weight_extract_20260514`
