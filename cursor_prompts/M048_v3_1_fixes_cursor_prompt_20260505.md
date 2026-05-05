# Cursor prompt — M048 v3.1: surgical fixes to commit `339cd52`

**Builds on:** commit `339cd52` (M048 v3 analysis runner, stats lib, figures
builder, SQL QA, verification, mig_317 scaffold).
**Migration:** mig_317b (incremental fixes; do NOT change mig_317 signoff).
**Scope:** code-only. No SQL schema changes. No manuscript prose. Same
analysis-only contract as v3 — manuscript writing remains deferred.
**Estimated effort:** 30–60 minutes.

---

## Why this exists (Cowork review of 339cd52)

The v3 pipeline is structurally sound and the SQL bug (`surg_year` → `surg_first_date`)
was already caught and fixed. Five smaller items remain before MotherDuck
execution. Item 1 is the only one that would otherwise leave a real gap in
the manuscript-writing chat's inputs; items 2–5 are quality-of-life.

---

## Items to fix

### 1. (BLOCKING) Mediation: add Asian-vs-White indirect effects

Currently `bootstrap_mediation_product` in `m048_v3_stats_lib.py` hardcodes
`[T.Black]` extraction and returns only Black indirect effects. The Asian
TR5 ROM disparity (54.0% in v1) is the most extreme finding in the cohort
and needs a parallel mediation arm; otherwise `m048_v3_mediation.csv` is
incomplete for the writing chat.

**Required changes:**

`studies/m048_racial_disparities_tirads/m048_v3_stats_lib.py`
- Add a `race_target: str` parameter to `bootstrap_mediation_product`,
  defaulting to `"Black"`. Replace the two `[T.Black]` literals with
  f-string `[T.{race_target}]`.
- Update the return dict to include `race_target` so the caller can stack
  rows.

`studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py`
- In the mediation block, loop over `race_targets = ("Black", "Asian")`
  for each mediator. Emit one row per `(mediator, race_target)` to
  `m048_v3_mediation.csv`. Schema:
  ```
  mediator,type,race_target,scope,indirect_mean,ci_lo,ci_hi
  ```
- Update the `scope` value from the current
  `"univariate_black_vs_white"` to either `"univariate_black_vs_white"`
  or `"univariate_asian_vs_white"` so old downstream code that filters on
  scope still works.

### 2. (BLOCKING) Independent recompute: parity Asian assertion

`v3/verification/independent_recompute_v3.py` currently only validates the
Black full OR. Add a parallel Asian assertion at the same ≤2% relative
tolerance.

**Required changes:**
- Pull the Asian row from the cascade (`m6_full`, `race_level == "Asian"`).
- Compare to `race_or_table(res_full).set_index("race_level").loc["Asian"]`.
- Add `ok_asian = rel_diff(...) <= 0.02` and include it in the
  `all([...])` exit-code conjunction.
- Append an `asian_full_or_*` line to the report markdown.

### 3. Figure 13: split FNA metrics into subplots

`m048_build_figures_v3.figure_13_fna_pattern()` plots three metrics on a
single y-axis with mismatched scales (percentages 0–100 vs counts 1–2).
The mean-FNA bar will visually disappear.

**Required changes:**
- Refactor `figure_13_fna_pattern()` to use 3 subplots in one row
  (`fig, axes = plt.subplots(1, 3, figsize=(12, 4))`).
- Subplot A: `pct_with_fna` (y-axis 0–100, % units).
- Subplot B: `mean_fnas_per_patient` (y-axis auto, count units).
- Subplot C: `pct_repeat_fna_among_biopsied` (y-axis 0–100, % units).
- Each subplot grouped by race with the standard `RACE_COLORS`. Footer
  text + title preserved. Save under the same basename
  `Figure_13_FNA_Pattern_by_Race.{png,pdf}`.

### 4. Add Bethesda × race × TR ROM table for the manuscript figure

The current `m048_v3_bethesda_stratified_TR_ROM.csv` contains race ORs
from the additive Model B — perfect for analytic interpretation but
not the cell-level ROM heatmap the writing chat will likely want.

**Required changes:**

`studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql`
- After Section 7, append Section 7b:
  ```sql
  -- ----------------------------------------------------------------------------
  -- 7b. Bethesda x race x TR cell-level ROM (for v3 supplementary heatmap)
  -- ----------------------------------------------------------------------------
  CREATE OR REPLACE TABLE m048_bethesda_x_race_x_tr_rom_v1 AS
  SELECT race_strat,
         bethesda_bucket,
         max_tirads_category_ever AS tr_category,
         COUNT(*)                 AS n,
         SUM(is_malignant::INT)   AS n_malignant,
         CASE WHEN COUNT(*) > 0
              THEN ROUND(100.0 * SUM(is_malignant::INT) / COUNT(*), 2)
              ELSE NULL END       AS rom_pct
  FROM   m048_patient_master_v1
  WHERE  max_tirads_category_ever IS NOT NULL
     AND bethesda_bucket IS NOT NULL
  GROUP  BY race_strat, bethesda_bucket, max_tirads_category_ever
  ORDER  BY race_strat, bethesda_bucket, max_tirads_category_ever;
  ```

`m048_run_analysis_v3.py`
- Add `m048_bethesda_x_race_x_tr_rom_v1` to the `v3_tables` list and dump
  it to `v3/m048_v3_bethesda_x_race_x_tr_rom.csv`.

`m048_build_figures_v3.py`
- Add `figure_12b_bethesda_rom_heatmap()` that reads the new CSV and
  renders one heatmap per race (faceted, 3 panels), x = TR, y = Bethesda,
  cell value = ROM%. Save as `Figure_12b_Bethesda_x_Race_x_TR_ROM.{png,pdf}`.
- Add the call to `main()` after `figure_12_bethesda()`.
- **Keep the existing Figure 12 (race OR heatmap)** — both figures are
  useful and serve different audiences.

### 5. Bethesda Model B: add race × TR interaction as a secondary model

The current `run_bethesda_stratified()` fits `is_malignant ~ race + max_tr_int`
within each Bethesda stratum. The v3 spec asked the question "does the
per-race TR-ROM gradient persist within cytologic strata?", which is more
directly answered by an interaction model.

**Required changes:**

`m048_run_analysis_v3.py`
- After the existing `run_bethesda_stratified(df_model)` call (which
  produces `m048_v3_bethesda_stratified_TR_ROM.csv`), add a second pass
  using formula
  `is_malignant ~ C(race_strat, Treatment('White')) * max_tr_int` and
  emit `m048_v3_bethesda_stratified_TR_interaction.csv` with one row per
  `(bethesda_bucket, interaction_term)` reporting coefficient, OR, 95% CI,
  and p (Bonferroni-adjusted across the # of bethesda strata × 2 race
  interaction terms).
- If a stratum has zero events at TR≥4 for any race, fall back to
  `fit_logit_regularized` rather than failing the row.

This is a SECONDARY analysis — the additive Model B output remains the
primary Bethesda-stratified result.

---

## What to NOT change

- `M048_motherduck_queries.sql` Sections 0–7 (v1) and 9–26 (v2/v3) —
  schema-stable; only the new 7b is additive.
- mig_317 signoff (already drafted). Add a separate **mig_317b** stanza
  for these incremental fixes.
- The race color encoding (`Black=#1f4e79`, `White=#7a7a7a`,
  `Asian=#c55a11`).
- The disparity-direction signature rule definitions in
  `build_disparity_direction()`.
- The cascade Models 0–6 formulas. Leave them alone.
- The seven sensitivity arms (S048v2-A through S048v3-G).

---

## QA gate updates

After running the v3 pipeline with these fixes, the existing QA gates
should still pass. Add two new gates to `m048_v3_qa_gates.csv`:

- `mediation_has_asian_rows`:
  PASS if `m048_v3_mediation.csv` contains ≥1 row where
  `race_target == "Asian"`; FAIL otherwise.
- `bethesda_rom_table_complete`:
  PASS if `m048_v3_bethesda_x_race_x_tr_rom.csv` has ≥6 rows where
  `n >= 10` (cells with reportable denominators).

---

## Sign-off

After the fixes pass and the MotherDuck run completes:

```sql
INSERT INTO main.signoff_migration (mig_id, signed_off_at, by_actor, summary)
VALUES (
  'mig_317b',
  CURRENT_TIMESTAMP,
  'cursor_composer_mig317b',
  'mig_317b: M048 v3.1 surgical fixes — Asian mediation arm, Asian
   assertion in independent_recompute, Figure 13 subplots, Bethesda x
   race x TR ROM table + Figure 12b heatmap, Bethesda x TR interaction
   secondary model. mig_317 primary signoff unchanged.'
);
```

Update MASTER spreadsheet row 48 status: keep
"v3 Adjusted Analysis Complete — Awaiting Writing" (no status change
needed; this is incremental).

---

## Definition of done

- [ ] `bootstrap_mediation_product` accepts `race_target` parameter.
- [ ] `m048_v3_mediation.csv` contains rows for both Black and Asian per
      mediator (14 rows total: 7 mediators × 2 races).
- [ ] `independent_recompute_v3.py` asserts both Black and Asian full OR
      within ≤2% relative; report markdown shows both lines.
- [ ] Figure 13 renders as 3 subplots; PDF + PNG.
- [ ] `m048_bethesda_x_race_x_tr_rom_v1` SQL table exists; CSV dumped.
- [ ] Figure 12b (ROM heatmap, faceted by race) renders.
- [ ] `m048_v3_bethesda_stratified_TR_interaction.csv` emitted with
      Bonferroni-adjusted p-values.
- [ ] Two new QA gates added and passing.
- [ ] mig_317b signoff prepared.
- [ ] One-paragraph Cowork summary noting:
      - Asian mediation IE ranking vs Black mediation IE ranking
      - Whether any Bethesda × race × TR interaction terms survived
        Bonferroni
      - Whether the ROM heatmap (Figure 12b) shows visible per-race
        calibration shifts that aren't explained by the OR heatmap
        (Figure 12)

When done, post the Cowork summary back to the chat for handoff to the
manuscript-writing session.
