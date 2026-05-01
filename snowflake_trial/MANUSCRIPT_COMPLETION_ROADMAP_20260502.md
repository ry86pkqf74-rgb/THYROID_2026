# Manuscript Completion Roadmap — M044 ETE + M038 Massive Goiter
**Generated:** 2026-05-02 by Cowork

How many more Snowflake runs and Cursor migs to ship each manuscript.

---

## M044 — Extrathyroidal Extension and Outcomes

**Status: ~80% complete on the data/figures side.** Manuscript text + figures-from-data steps remain.

### Done (this trial)
| Output | File | What it gives the manuscript |
|---|---|---|
| ✅ Table 1 baseline | `reports/m044_table1.md` | Full demographics × ETE strata (none/micro/gross), p-values |
| ✅ Cox PH multivariable | `reports/m044_cox_ph.md` | HRs + 95% CI, c-index 0.717, log-rank p=0.001 |
| ✅ Cox PH sensitivity (cleaner LN) | `reports/m044_cox_sensitivity_ln_clean.md` | Same model on ln_status_source ≠ 'staging'; c-index 0.721 |
| ✅ KM curves data | `reports/m044_km_curves_data.csv` | Per-strata survival at 8 timepoints (0.5/1/2/3/5/7/10/15 yr) + n_at_risk + cumulative_events |
| ✅ Forest plot data | `reports/m044_forest_plot_data.csv` | HR + 95% CI per predictor, log_hr + se for plotting |
| ✅ Methods footnotes | mig_266 | F1-F6 conventions in M044_ETE_manuscript_draft.md |

### Remaining Snowflake runs (3-4 more)
1. **M044 race-disparity sub-analysis** — extend M037's Black/AA finding to M044 ETE strata. Does ETE rate × race × era show any pattern? ~30 min run.
2. **M044 Time-to-RAI subgroup** — among RAI receivers, time-from-surgery-to-RAI by ETE strata; does delayed RAI predict recurrence?
3. **M044 Cox model with interaction terms** — `ETE_GROSS × T_HIGH`, `ETE_GROSS × N_POS`, `ETE × tumor_size`. Tests whether ETE effect varies by other risk factors. Could surface a hidden subgroup.
4. **M044 final post-mig_264b refresh** — re-run the 6 outputs above against post-NIFTP-reclass cohort. Numbers will shift slightly (~24 patients out of cohort). Mostly verification.

### Remaining Cursor migs (1)
- **mig_274** (proposed): M044 manuscript figure-build pipeline. Take the 4 CSVs above + render with matplotlib/ggplot/altair → PNG/SVG → drop into `M044_submission_package_v1_0/06_figures/`. Could reuse existing M044 figure-build patterns (Logan already has scripts for `m044_fig*` per session memory).

### Total runway to submission-ready M044
- **3-4 Snowflake runs** (race disparity + interactions + post-264b refresh)
- **1 Cursor mig** (figure render pipeline)
- **Manuscript text editing** (Logan-side; not Cowork)

**Estimated 1-2 sessions of Cowork work + 1 Cursor mig + Logan's writing.**

---

## M038 — Massive Goiter (Definition Paper)

**Status: ~30% complete.** Cohort scaffold landed today; complications + surgical outcomes + manuscript text all remain.

### Done (this trial)
| Output | File | What it gives the manuscript |
|---|---|---|
| ✅ Cohort scaffold | Snowflake `COHORT_M038_MASSIVE_GOITER` view | Weight bucket (massive/moderate/small/unknown) + IS_MASSIVE_GOITER flag |
| ✅ Table 1 by weight | `reports/m038_table1_massive_goiter.md` | Demographics × weight bucket; sized at ≥200g=475 / 50-199g=2,467 / <50g=6,188 / NULL=1,741 |
| ✅ Cohort definition | mig_273 prompt (queued) | MD-side mirror view for downstream tools |

### Remaining Snowflake runs (5-7)
1. **M038 complications by weight strata** — strict definition (`finding_status='present'` AND `evidence_strength IN ('definitive','probable')`). Per Logan's mig_252 work-in-progress: ≥200g any-comp=30.7% but strict=2.1%; <200g any=21.3% strict=3.4%. **Requires mig_252 to land first** for strict to be defensible.
2. **M038 surgical-complexity proxies** — operative time, blood loss, hospital LOS by weight bucket. Need to discover these columns (probably in `canonical_operative_events_v1_FLAT` or new tables).
3. **M038 logistic regression** — predictors of "any post-op complication (strict)" with weight as primary exposure, adjusted for age/sex/multifocal/malignant/surgery type. Manuscript Table 2 candidate.
4. **M038 era × weight trend** — has the rate of massive goiter changed over time? Probably yes (better imaging finding goiters earlier).
5. **M038 propensity-matched analysis (optional)** — match each ≥200g patient to ~3 <200g patients on age/sex/era/malignancy; compare complication rates. Strengthens causal interpretation.
6. **M038 Cohort flow figure** — patients excluded at each step (NULL weight, missing followup, etc). Standard manuscript Figure 1.
7. **M038 Manuscript draft** — methods + results section text (Cowork can draft from the data above).

### Remaining Cursor migs (3-5)
- **mig_252** (already in flight in your queue): comp_*_confirmed rollup fix — strict definition unblocks strict complication rates everywhere
- **mig_273** (queued today): M038 cohort view in MD
- **mig_275** (proposed): M038 surgical-complexity column scaffolding — populate operative_time_min, ebl_ml, los_days from operative_events table to CPM
- **mig_276** (proposed): M038 manuscript draft scaffold (methods + results text generated from the data above)

### Total runway to submission-ready M038
- **5-7 Snowflake runs** (complications-by-weight + surgical complexity + logreg + era trend + flow + propensity-match + draft)
- **3-5 Cursor migs** (252 already in flight; 273 today; 275/276 follow-ons)
- **Manuscript text** (Cowork can draft methods/results; Logan does intro/discussion)

**Estimated 3-4 sessions of Cowork work + 3-5 Cursor migs + Logan's writing.**

---

## Combined timeline

| Phase | Sessions | Target ETA |
|---|---|---|
| Round 9 (today) | 1 | Done |
| Round 10 — M044 figures + M038 surgical scaffolding | 1 | Next session |
| Round 11 — M044 race + interactions; M038 logreg + complications (post-mig_252) | 1 | After mig_252 |
| Round 12 — M044 submission package; M038 manuscript draft | 1 | After all migs land |
| Logan revision pass | — | 1-2 weeks (manuscript-level work) |

**Conservative estimate: M044 submission-ready in 2-3 more Cowork sessions; M038 in 4-5.**

---

## Trial calendar reminder

- **PAT expires 2026-05-08** — rotate before next session
- **Trial converts 2026-05-29** — set cancellation reminder
- **Credits used so far:** ~$6-7 of $40. Round 10-12 will likely consume another $10-15 (M038 propensity matching + AI_AGG over operative notes if added).
