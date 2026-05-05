# Cortex Analyst NL verification (M048 v3)

Run these after semantic model bind (optional `m048_v3_covariates_semantic_model.yaml`).

1. What proportion of patients of each race had any FNA performed before surgery?
2. What is the median Bethesda category by race?
3. In Black patients with TR5 max category, what is the mean tumor size at pathology?
4. How many race × TR4 cells have at least 10 malignant patients in the v3 disparity-direction table?
5. Among genetics-tested patients only, what is the per-race patient AUC? (cross-check sensitivity arm C CSV)

**Expected anchors (refresh from `m048_v3_run_snapshot.json` and QA CSVs after each run).**
