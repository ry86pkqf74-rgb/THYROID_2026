# Cursor prompt — M048 Racial Disparities in TI-RADS Performance

**Scope (READ THIS FIRST).** Cursor's job here is **data cleaning, statistical
analysis, table production, figure rendering, and verification (including
Cortex Analyst NL Q&A) ONLY**. **DO NOT** write the manuscript, abstract,
introduction, discussion, or any narrative prose. The manuscript will be
authored separately in a Cowork/Grok/ChatGPT chat using the verified outputs
this prompt produces.

**Repo:** `THYROID_2026`
**Author of prompt:** Logan D. Glosser (via Cowork session 2026-05-05)
**Database:** `thyroid_canonical_publication_v1_0` @ release tag `pub_v1_1`
**Builds on:** M025 v2 submission package (mig_307 / mig_307b)
**Status of inputs:** READY — reuses `m025_analytic_master_patient_v1` and
`m025_analytic_master_nodule_v1`; no new canonical work.

---

## Goal

Produce a clean, fully-verified analytic dataset for **M048 — Racial
Disparities in ACR TI-RADS Performance** in our 25-year operative thyroid
cohort. Race composition: 45.5% Black, 40.9% White, 6.0% Asian — uniquely
diverse for the published TI-RADS literature. The paper will mirror M025's
patient-grain + nodule-grain framework, stratified by race.

The deliverable is a study folder under
`studies/m048_racial_disparities_tirads/` parallel in structure to
`studies/m025_tirads_performance/`, plus a tables/figures package that a
human writer can pull from. **No DOCX manuscript output, no prose.**

---

## Pre-specified analytic plan (do NOT modify without senior-author sign-off)

### Cohort and strata
- Reuse the M025 patient analytic master (n=3,375) and nodule analytic master
  (n=37,438; n=3,687 strict-eligible).
- Race strata (primary inferential): **Black**, **White**, **Asian**.
- Race strata (descriptive only, collapsed): **Other** (NHPI / AI/AN / Other),
  **Unknown** (Unknown or Not Reported / NULL).

### Predictor and outcome (unchanged from M025)
- Patient grain: `max_tirads_category_ever` ∈ {TR1..TR5}; outcome
  `is_malignant`.
- Nodule grain: `acr2017_tirads_category` ∈ {TR1..TR5} on
  `analytic_eligible_strict_acr_pernodule = TRUE`; outcome
  `nodule_path_proven_malignant`.

### Pre-specified primary analyses
1. **Per-race AUC** at patient and nodule grain (closed-form Mann–Whitney
   with tie correction).
2. **Per-race per-TR ROM** at patient and nodule grain with Wilson 95% CIs.
3. **Per-race threshold metrics** (sens/spec/PPV/NPV) at TR≥TR3, TR≥TR4,
   TR≥TR5 with Wilson 95% CIs.
4. **Per-race per-feature score distribution** (composition / echogenicity /
   shape / margin / foci): chi-square test of independence between race and
   each feature's discrete score, with Bonferroni correction (α = 0.05 / 5).
5. **Per-race FNA-eligibility audit** stratifying the M025 1,553 / 472
   numerators and denominators by race.

### Pre-specified secondary analyses
6. **Patient × nodule grain inflation by race**: TR4 and TR5 inflation
   (patient ROM − nodule ROM, percentage points) per race.
7. **Per-race Bethesda × TR cross-tabulation** (descriptive; supplementary
   heatmap).
8. **Sensitivity arm S048-A**: pool `Other` race stratum as a single
   comparator to bound small-cell inference.

### Statistical conventions (DO NOT change)
- Wilson 95% CIs on all proportions (sens, spec, PPV, NPV, ROM).
- AUC via closed-form rank Mann–Whitney equivalent (matches M025).
- Bootstrap 1,000 replicates × race × grain for AUC 95% CIs (stratified
  bootstrap on each race subset; mirrors M025 bootstrap).
- Chi-square with Yates correction for race × feature score tests.
- Report all p-values with effect sizes; do NOT chase significance.

---

## Files to produce

```
studies/m048_racial_disparities_tirads/
  ├── M048_motherduck_queries.sql          (already present — driver)
  ├── m048_run_snapshot.json               (run metadata: timestamp, db tag,
  │                                         row counts, git sha, mig_id)
  ├── m048_qa_gates.csv                    (pass/fail vs M025 totals)
  ├── m048_diagnostic_performance.csv      (per-grain × per-race; sens/spec/
  │                                         PPV/NPV with Wilson CIs)
  ├── m048_rom_by_race_x_tr.csv            (long format; Wilson CIs)
  ├── m048_auc_by_race.csv                 (with bootstrap 95% CIs)
  ├── m048_threshold_metrics.csv           (Wilson CIs)
  ├── m048_feature_distribution.csv        (raw counts + chi-sq results +
  │                                         Bonferroni-adjusted p)
  ├── m048_fna_compliance_by_race.csv
  ├── m048_bethesda_x_race_x_tr.csv
  ├── m048_inflation_by_race.csv           (TR4/TR5 patient–nodule pp + CIs)
  ├── m048_handoff_README.md               (numbers table for the writing
  │                                         chat — see template below)
  └── verification/
      ├── m025_reconciliation.csv          (per-race totals match M025
      │                                     Table_7 to 0 patients)
      ├── cortex_smoke_tests.md            (NL queries + returned SQL +
      │                                     reconciliation against CSVs)
      └── independent_recompute.py         (re-derives 5 headline numbers
                                            from raw m025_analytic_master
                                            rows; must match CSV outputs)

M048_submission_package/figures/         (PNG + vector PDF, 300 dpi)
  ├── Figure_1_Cohort_Flow_by_Race.{png,pdf}
  ├── Figure_2_ROC_by_Race.{png,pdf}            (overlaid; one curve per race)
  ├── Figure_3_ROM_by_Race_Patient.{png,pdf}    (paired bars by race × TR)
  ├── Figure_3b_ROM_by_Race_Nodule.{png,pdf}    (paired bars by race × TR)
  ├── Figure_4_Inflation_by_Race.{png,pdf}      (forest by race × TR)
  ├── Figure_5_Feature_Distribution.{png,pdf}   (5 small multiples)
  └── Figure_S1_Bethesda_x_Race_x_TR.{png,pdf}  (heatmap, faceted by race)

# DO NOT produce in this run:
# - Any DOCX manuscript file
# - Cover letter, suggested reviewers, highlights
# - Abstract or any narrative prose
# - References / Vancouver bibliography
# Those will be authored by a human in a separate manuscript-writing chat.
```

---

## Implementation steps

### Step 1 — Run pre-specified SQL on MotherDuck

Execute `studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql`
against `thyroid_canonical_publication_v1_0`. After each `CREATE OR REPLACE
TABLE`, dump to CSV.

```python
import duckdb, pandas as pd, os, json, hashlib, subprocess
from datetime import datetime, timezone
from motherduck_client import get_token  # repo root

DB = "thyroid_canonical_publication_v1_0"
con = duckdb.connect(f"md:{DB}?motherduck_token={get_token()}")
con.execute("USE thyroid_canonical_publication_v1_0;")
con.execute("SET schema = 'manuscript_workspace';")

# ... execute SQL file, dump tables ...
```

### Step 2 — Compute Wilson CIs and bootstrap AUC CIs in Python

Reuse `M025_FINAL_PACKAGE/m025_sensitivity_lib.py` Wilson + bootstrap helpers.
Stratify bootstrap on race × grain, 1,000 replicates, 95% percentile CIs.

### Step 3 — Statistical tests

For per-feature score distribution (Section 5 in the SQL):
- Pivot `m048_feature_distribution.csv` to wide.
- For each of the 5 features, run `scipy.stats.chi2_contingency` on the
  (race × score) contingency.
- Apply Bonferroni correction (α = 0.05 / 5 = 0.01).
- Record raw chi-sq, df, raw p, Bonferroni-adjusted p, and Cramér's V effect
  size.

### Step 4 — Cortex Analyst NL verification

The M025 nodule-level Cortex Analyst semantic model was bound in mig_311
(see `CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md`). Use it to independently
verify the M048 numbers via natural-language queries.

For each of these NL prompts, capture: the question, the SQL Cortex
generated, the result, and the corresponding CSV cell that should match.
Save to `verification/cortex_smoke_tests.md`.

```
1. "What is the patient-level ROM for Black, White, and Asian patients at
   each TI-RADS category?"
   → cross-check vs m048_rom_by_race_x_tr.csv (patient grain rows)

2. "What is the per-nodule ROM at TR4 and TR5 for Black patients in the
   strict-eligible cohort?"
   → cross-check vs m048_rom_by_race_x_tr.csv (nodule grain, race=Black)

3. "How many strict-eligible nodules do we have for each race?"
   → cross-check vs m048_qa_gates.csv

4. "What is the AUC for ACR TI-RADS in White patients vs Black patients at
   the patient grain?"
   → cross-check vs m048_auc_by_race.csv

5. "Among Asian patients with a TR4 max category, how many had pathology-
   proven malignancy?"
   → cross-check vs m048_rom_by_race_x_tr.csv
```

If a Cortex result disagrees with the Python/SQL pipeline by more than
rounding tolerance, **stop and investigate**. Common causes:
- Cortex semantic model points at a slightly different denominator
  (`is_malignant IS TRUE` vs `is_malignant = TRUE` boolean handling).
- NULL race rows handled inconsistently between the YAML and the SQL.
- Bootstrapped CI vs analytic CI mismatch (expected; record but don't flag).

If the M025-bound Cortex Analyst model doesn't expose race, scaffold a
race-aware companion semantic model under
`snowflake_trial/semantic_models/m048_racial_disparities_semantic_model.yaml`
and bind it (see `CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md`). Track the
bind work in mig_315 sign-off.

### Step 5 — Independent recompute

Write `verification/independent_recompute.py` that:
- Pulls the 5 headline numbers (per-race patient AUC, per-race nodule TR4
  ROM, per-race nodule TR5 ROM, overall pooled AUC, overall pooled TR4 ROM).
- Re-derives them from `m025_analytic_master_*` joined to
  `canonical_patient_master.race` directly — without using any of the
  `m048_*` derivation tables.
- Asserts each matches the CSV-based pipeline output to ≤0.01% absolute
  difference.

If any assertion fails, the run does not pass QA.

### Step 6 — Figures (matplotlib, 300 dpi PNG + vector PDF)

Mirror `M025_submission_package/figures/build_figures.py` style and palette.
Race color encoding (consistent across all figures):
- Black:   `#1f4e79`
- White:   `#7a7a7a`
- Asian:   `#c55a11`
- Other:   `#9c9c9c`
- Unknown: `#cfcfcf`

Each figure includes a tagged version number and run timestamp footer.
**No annotations beyond the data, color legend, axis labels, and Wilson CI
error bars** — the writing chat adds narrative captions later.

### Step 7 — QA gates (pass/fail; block sign-off if any fail)

Verify:
- Patient cohort race totals reconcile to M025 Table_7 exactly:
  Black 1,535 / White 1,382 / Asian 204.
- Nodule strict cohort totals reconcile to M025 (3,687 strict-eligible).
- Pooled-across-race AUC reproduces M025 (patient 0.6478, nodule 0.6399) to
  4 decimal places.
- Pooled-across-race per-TR ROM reproduces M025 Table 3 to 0.05 pp.
- Per-race AUC bootstrap CIs computed; flag any race × grain combination
  where the CI includes 0.5 (Asian-stratum risk).
- All Wilson CI bounds in [0, 100] and lo ≤ point ≤ hi.
- Cortex Analyst smoke tests all reconcile to ≤rounding tolerance.
- Independent-recompute assertions all pass.

Write all 8 gate results to `m048_qa_gates.csv`.

### Step 8 — Handoff README for the manuscript writer

Produce `m048_handoff_README.md` containing **only**:
- Run metadata (db tag, mig_id, timestamp, git sha).
- A reference table of every headline number the writer will need
  (per-race AUC + 95% CI; per-race per-TR ROM + 95% CI; per-race threshold
  metrics; per-race inflation pp; per-race feature-distribution
  chi-sq/Bonferroni results; per-race FNA-audit numerators).
- Pointers to each CSV / figure file path.
- Explicit caveats the writer must acknowledge (Asian stratum power, race
  is self-reported, multiple comparisons not adjusted across the per-TR
  comparisons).

**Do NOT write any narrative interpretation, abstract, or discussion text.**

---

## Migration sign-off pattern

After QA gates pass, register the analysis:

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_315',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig315',
  'mig_315: M048 racial-disparities analytic package built. n=3,375 patients
   (Black 1,535 / White 1,382 / Asian 204 / Other 87 / Unknown 167);
   strict-eligible nodules n=3,687. Per-race AUC, ROM, threshold metrics,
   feature-distribution, FNA-compliance, inflation tables produced and
   verified via Cortex Analyst NL Q&A + independent-recompute. QA gates
   PASS. Manuscript authoring DEFERRED to a separate Cowork/Grok/ChatGPT
   session.'
);
```

Update `MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx` row for ID 48
Status from "Proposed" → "Analysis Complete — Awaiting Writing".

---

## Definition of done

- [ ] All eight `m048_*_v1` SQL tables populated and CSV-dumped.
- [ ] Per-race AUC, threshold metrics, ROM, feature-distribution,
      FNA-compliance, inflation, and Bethesda CSVs committed to
      `studies/m048_racial_disparities_tirads/`.
- [ ] Cortex Analyst smoke tests captured in
      `verification/cortex_smoke_tests.md` with reconciliation against CSVs.
- [ ] `verification/independent_recompute.py` executed and all 5 headline
      assertions pass.
- [ ] Seven figures rendered as 300 dpi PNG + vector PDF in
      `M048_submission_package/figures/`.
- [ ] `m048_qa_gates.csv` written; all 8 gates pass.
- [ ] `m048_handoff_README.md` written (numbers + paths only; no prose).
- [ ] mig_315 migration signed off.
- [ ] MASTER list row 48 Status → "Analysis Complete — Awaiting Writing".

When finished, post a one-paragraph Cowork summary noting:
- Per-race AUC values + bootstrap CIs.
- Per-race TR4 and TR5 ROM at nodule grain.
- Per-race TR4 / TR5 inflation magnitudes.
- Any feature-distribution disparities that survived Bonferroni.
- Any Cortex/independent-recompute reconciliation issues encountered.

That summary will be pasted into the manuscript-writing chat as the seed
for the human-authored draft.
