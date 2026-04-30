# mig_215 investigation — US nodule size outliers + ACR points=1 band (2026-04-30)

## Rid 8931 (`size_cm_max = 48`, 20 rows)

- **2016-03-25 exam:** nodules have normal `length_mm` / `width_mm` / `height_mm`; `size_cm_max` ~0.83–1.91 cm — no issue.
- **2017-03-21 and 2019-08-29 exams:** `length_mm`/`width_mm`/`height_mm` all NULL; `size_cm_max` stuck at **48.0** on every nodule row (multi-nodule shell rows from `imaging_nodule_master_v1`).
- **`extracted_size_cm`** (LLM / text path) carries plausible nodule sizes (**2.4–5.0 cm**) on 5 rows where both 48 and extracted are present.
- **Conclusion:** 48 is not interpretable as cm (no supporting mm axes); it is almost certainly **wrong placeholder or non-nodule field scaled into `size_cm_max`**. Where `extracted_size_cm` exists, **replace `size_cm_max` with `extracted_size_cm`**. Where it does not, **NULL `size_cm_max` and set `is_size_outlier_quarantine = TRUE`** so manuscripts can exclude or stratify.

## Rid 8613 (`size_cm_max = 21`, 1 row)

- No mm axes, no `extracted_size_cm`. Single nodule row.
- **Conclusion:** **21 mm mis-recorded as cm** → corrective **`size_cm_max = 2.1`**.

## ACR 2017 band violations (23 rows)

- **`acr2017_tirads_points = 1`** with **`acr2017_tirads_category` in (TR1, TR2, TR3)**.
- ACR 2017 point totals map to categories as: 0→TR1, 2→TR2, 3→TR3, 4–6→TR4, ≥7→TR5 — **no category for total = 1**.
- Distribution: 21× TR2, 1× TR3, 1× TR1 (legacy `tirads_level_2017` disagreed with recomputed feature sum).
- **Fix:** Set **`acr2017_tirads_category = NULL`** for these rows; recompute **`acr2017_vs_updated_concordant`** for the affected key set.

## Post-apply checks

- Rows with `size_cm_max > 20`: expect **0** in publication DB.
- Rows with `acr2017_tirads_points = 1` and non-null category: expect **0**.
