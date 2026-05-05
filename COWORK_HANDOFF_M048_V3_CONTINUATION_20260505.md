# Cowork handoff — M048 v3 continuation (2026-05-05)

**Audience:** a fresh Cowork/Claude/Grok/ChatGPT chat with Desktop Commander
+ MotherDuck connector + Cortex CLI access. **Mission: finish the M048
data-cleaning + verification pass and produce the v3.2 handoff package
for the manuscript-writing chat.** Manuscript writing remains out of
scope.

This document is self-contained. Read top-to-bottom; do not assume any
prior session memory.

---

## 0. Mission summary

M048 = "Racial Disparities in ACR TI-RADS Performance" (25-year
operative thyroid cohort, n=3,375; 45.5% Black, 40.9% White, 6.0%
Asian). Builds on M025's patient + nodule grain framework.

Cursor produced commits `e7984c4` (v3.1 fixes) and `339cd52` (v3 base).
A live MotherDuck run was started in the prior chat, surfaced two bugs,
one of which was patched locally (not yet committed). The mission of
this chat is to:

1. Land the local stats-lib int-cast fix (already applied to disk;
   needs commit + push).
2. **Patch the `had_any_fna` × `bethesda_bucket` perfect-collinearity bug**
   that killed cascade Models 5 and 6 and made all mediation indirect
   effects = 0.0.
3. Re-run the affected pieces (M5/M6 cascade, mediation, sensitivity
   arms that share the same controls).
4. Run `independent_recompute_v3.py` to confirm headline numbers.
5. Run `m048_build_figures_v3.py` to produce Figures 6–13 + 12b.
6. Run Cortex Analyst NL verification queries.
7. Update QA gates and write the v3.2 handoff README.
8. Sign off mig_317b on MotherDuck.
9. Update MASTER spreadsheet row 48 status.

---

## 1. Environment + access

### Repo
```
/Users/loganglosser/THYROID_2026
```
- main branch is at `e7984c4` on origin
- there is a local uncommitted edit in `studies/m048_racial_disparities_tirads/m048_v3_stats_lib.py`
  (line ~140, `is_malignant` cast to int — see Section 4 below).

### Python venv
```
/Users/loganglosser/THYROID_2026/.venv/bin/python
```
Has `duckdb`, `pandas`, `statsmodels`, `matplotlib`, `numpy`, `scipy`,
`openpyxl`. Python 3.14.

### MotherDuck
- Database: `thyroid_canonical_publication_v1_0` @ release tag `pub_v1_1` (2026-05-04)
- Token: `motherduck.local.toml` in repo root (gitignored). Loaded by
  `motherduck_client.get_token()`.
- Verified working: token len 480.

### Cortex CLI (Snowflake)
```
/Users/loganglosser/.local/bin/cortex
```
- Invocation pattern: `cortex analyst query "<NL question>"`.
- The mig_311 semantic model is bound to
  `COHORT_M025_NODULE_LEVEL_V1_FLAT` in Snowflake's
  `THYROID_VALIDATION.PUBLIC.CORTEX_ANALYST_STAGE`. See
  `CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md`.
- A v3 covariate semantic model is scaffolded but NOT bound:
  `studies/m048_racial_disparities_tirads/m048_v3_covariates_semantic_model.yaml`
  (would need Snowsight UI bind per the walkthrough).

### Desktop Commander
- Use `mcp__Desktop_Commander__start_process({command: "bash"})`,
  then `interact_with_process` with absolute paths.
- The user's git index occasionally has a stale lock at
  `.git/index.lock`. If `git add` fails with "Unable to create
  '.../index.lock': File exists", run `rm -f .git/index.lock`.

---

## 2. Repo / commit / file state

### Recent commits
```
e7984c4  M048 v3.1 surgical fixes (mig_317b)
90adc5f  feat(M048 v3.1): surgical fixes prompt
01e59a3  feat(mig_321): M032 v2 pre-submission numerical refresh for Thyroid
```

### Files produced by Cursor (commit e7984c4)
- `studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql`
  (sections 0–7, 7b, 8–26; 47 KB)
- `studies/m048_racial_disparities_tirads/m048_v3_stats_lib.py`
  (Wilson CIs, Logit fits, race OR extraction, mediation bootstrap)
- `studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py`
  (the full analysis runner)
- `studies/m048_racial_disparities_tirads/m048_build_figures_v3.py`
  (Figures 6–13 + 12b)
- `studies/m048_racial_disparities_tirads/m048_v3_covariates_semantic_model.yaml`
  (Cortex scaffold; not yet bound)
- `studies/m048_racial_disparities_tirads/mig_317_signoff.sql`
- `studies/m048_racial_disparities_tirads/mig_317b_signoff.sql`
- `studies/m048_racial_disparities_tirads/v3/verification/independent_recompute_v3.py`
- `studies/m048_racial_disparities_tirads/v3/verification/cortex_smoke_tests_v3.md`

### CSVs written so far by the partial run (in
`studies/m048_racial_disparities_tirads/v3/`):
- `m048_v3_patient_master_full.csv` (3,375 rows, 621 KB)
- `m048_v3_nodule_master_full.csv` (37,438 rows, 19.7 MB)
- `m048_rom_by_race_patient_v1.csv`
- `m048_nodule_count_by_race_v1.csv`
- `m048_genetics_access_by_race_v1.csv`
- `m048_v3_sql_qa_counts.csv`
- `m048_v3_bethesda_x_race_x_tr_rom.csv` (146 rows — ROM heatmap input)
- `m048_v3_us_to_surgery_interval.csv`
- `m048_v3_frozen_section_by_race.csv`
- `m048_v3_aggressive_features_by_race.csv`
- `m048_v3_histology_subtype_by_race.csv`
- `m048_v3_tumor_biology_descriptors.csv`
- `m048_v3_fna_path_concordance.csv`
- `m048_v3_attenuation_cascade.csv` (M0–M6 race ORs — see bug below)
- `m048_v3_full_model_OR.csv` (Model F coefficients)
- `m048_v3_interaction_race_x_tr.csv` (likely all zero — same root cause)
- `m048_v3_interaction_race_x_nodulect.csv`
- `m048_v3_bethesda_stratified_TR_ROM.csv` (Model B — looks valid)
- `m048_v3_bethesda_stratified_TR_interaction.csv` (Bonferroni; valid)
- `m048_v3_mediation.csv` (14 rows; **all indirect effects = 0.0** — bug)
- `m048_v3_nodule_model_race_OR.csv` (23 bytes — error row only)

### CSVs NOT yet written (run was still in sensitivity-arms phase
when the prior chat ended):
- `m048_v3_sensitivity_arms.csv`
- `m048_v3_disparity_direction_table.csv`
- `m048_v3_covariate_balance.csv`
- `m048_v3_qa_gates.csv`
- `m048_v3_run_snapshot.json`
- `m048_handoff_README_v3.md`
- `v3/verification/m025_reconciliation_v3.csv`

### Live process state (uncertain — check first)
The prior chat had PID 5180 still running after ~10 min:
```bash
.venv/bin/python studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py --skip-sql --mediation-boot 200
```
Log: `/tmp/m048_v3_run3.log`.

**First action in the new chat: check whether 5180 (or the run) is
still alive. If yes, decide whether to let it complete (the unwritten
CSVs will land) or kill it and re-run with patches applied. The
recommendation is to let it complete its sensitivity-arms pass first,
because those don't share the broken controls; then patch + re-run
mediation + M5/M6 only.**

```bash
pgrep -af m048_run_analysis_v3
ls -lt /Users/loganglosser/THYROID_2026/studies/m048_racial_disparities_tirads/v3/ | head -10
tail -10 /tmp/m048_v3_run3.log
```

---

## 3. The two bugs

### Bug A — `is_malignant` bool → Patsy 2-column categorical (FIXED LOCALLY, NOT COMMITTED)

`prepare_v3_frame` in `m048_v3_stats_lib.py` was producing
`is_malignant` as a Python `bool`. Patsy's `logit("is_malignant ~ ...")`
formula sees a bool and constructs a 2-column categorical endog,
producing the error
`endog has evaluated to an array with multiple columns that has
shape (3121, 2)`.

**Local edit already applied (line ~140 of `m048_v3_stats_lib.py`):**
```python
# Before:
df["is_malignant"] = df["is_malignant"].apply(lambda v: True if v in (True, "true", "True", 1, "1") else False)
# After:
df["is_malignant"] = df["is_malignant"].apply(lambda v: 1 if v in (True, "true", "True", 1, "1") else 0).astype(int)
```

Verified working — the run completed all the cascade fits and is_malignant
is now treated as numeric 0/1.

**TODO in this chat: commit + push this fix.** Suggested commit message:
> `fix(M048 v3): cast is_malignant to int(0/1) so Patsy treats endog as
> numeric, not 2-column categorical`

### Bug B — `had_any_fna` × `bethesda_bucket == "missing"` perfect collinearity (NOT YET FIXED)

This is the root cause of the M5/M6 cascade collapse and the
mediation = 0.0 problem.

**Mechanism.** `prepare_v3_frame` fills NULL `bethesda_bucket` with
`"missing"`. Patients without FNA have `had_any_fna == 0` AND
`bethesda_bucket == "missing"` — perfectly collinear. Patsy can't
disambiguate; the design matrix becomes rank-deficient; the optimizer
fails or zeros out the race coefficient.

**Evidence in `m048_v3_attenuation_cascade.csv`:**
- M0 (race only):    Black OR 0.317, Asian OR 1.323
- M1 (+TR):          Black OR 0.352, Asian OR 1.340
- M2 (+burden):      Black OR 0.352, Asian OR 1.343
- M3 (+gen+NM):      Black OR 0.359, Asian OR 1.431
- M4 (+bg path):     Black OR 0.348, Asian OR 1.468
- **M5 (+FNA+Bethesda):  Black OR 1.000  ← FAILED**
- **M6 (full):           Black OR 1.000  ← FAILED**

**Evidence in `m048_v3_mediation.csv`:** all 14 rows
(7 mediators × Black/Asian) have `indirect_mean = 0.0`,
`ci_lo = 0.0`, `ci_hi = 0.0`. The mediation `controls_formula_tail`
in `m048_run_analysis_v3.py` (~line 564) includes both
`had_any_fna` and `C(bethesda_bucket)`, hitting the same bug
inside the bootstrap workers.

**Fix (apply in this chat):** drop `had_any_fna` from any controls
that also include `C(bethesda_bucket)`. Bethesda category fully
encodes whether FNA happened (`"missing"` ↔ no FNA).

Specific code edits needed in `m048_run_analysis_v3.py`:

1. The cascade `m5_fna_path` and `m6_full` formulas — remove
   `had_any_fna +`. Keep `had_repeat_fna` and `n_fnas_total` (those
   describe the workup intensity, not the bare FNA Y/N).
2. `controls_tail` (the shared string used by Models I, M, F-Nodule,
   sensitivity arms) — remove `had_any_fna +`.
3. `med_controls` (the mediation control string ~line 564) — remove
   `had_any_fna +`.
4. `S048v3_G_had_fna` sensitivity arm filters on `had_any_fna == 1`,
   which is fine (filter, not formula term).

**Alternative (more conservative):** keep `had_any_fna` as a control
but drop `C(bethesda_bucket)` and instead include only the non-missing
Bethesda categories as a separate categorical. More invasive; the
recommended fix is to drop `had_any_fna` since `bethesda_bucket`
contains strictly more information.

---

## 4. Continuation plan (do these in order)

### Step 1 — Verify run state and commit Bug A fix

```bash
cd /Users/loganglosser/THYROID_2026
rm -f .git/index.lock 2>/dev/null
pgrep -af m048_run_analysis_v3
ls -lt studies/m048_racial_disparities_tirads/v3/ | head -10

# If a run is still in progress and producing files, let it finish.
# If it's stuck (no new files in >5 min), kill: kill <pid>

# Commit the Bug A fix
git status --short studies/m048_racial_disparities_tirads/m048_v3_stats_lib.py
git add studies/m048_racial_disparities_tirads/m048_v3_stats_lib.py
git commit -m "fix(M048 v3): cast is_malignant to int(0/1) so Patsy treats endog as numeric

Resolves Patsy 2-column-categorical error: 'endog has evaluated to an array
with multiple columns that has shape (3121, 2). This occurs when the variable
converted to endog is non-numeric (e.g., bool or str).'

prepare_v3_frame previously returned is_malignant as bool, which Patsy
interprets as a 2-level categorical when used as outcome in logit('y ~ x').
Cast to int(0/1) so all logit() calls (cascade, full model, Bethesda
stratified, F-Nodule, sensitivity arms, mediation bootstrap inner fits)
correctly treat the outcome as numeric."
git push origin main
```

### Step 2 — Apply Bug B fix (collinearity)

In `studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py`:

Find each occurrence of `had_any_fna +` inside formula strings and
delete that token plus the following space. Affected strings (search
for them):
- `cascade_specs` list — `m5_fna_path` and `m6_full` formulas
- `controls_tail` definition
- `med_controls` definition
- The Model M (race × nodule_burden) formula
- The Model F-Nodule formula

Do NOT remove `had_repeat_fna` or `n_fnas_total` — those are valid.

Then commit:
```bash
git add studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py
git commit -m "fix(M048 v3): drop had_any_fna from regression controls (collinear with bethesda_bucket=='missing')

Patients without FNA were assigned bethesda_bucket='missing' by
prepare_v3_frame. This makes had_any_fna == 0 perfectly equivalent to
bethesda_bucket == 'missing' — perfect collinearity that caused the
optimizer to zero out race coefficients in cascade Models 5 and 6
(both Black and Asian ORs collapsed to 1.000) and made all 14 rows of
m048_v3_mediation.csv return indirect_mean = 0.0.

bethesda_bucket carries strictly more information than had_any_fna
(it distinguishes missing from each Bethesda category). Removing
had_any_fna from the controls preserves all FNA-pattern signal via
had_repeat_fna and n_fnas_total."
git push origin main
```

### Step 3 — Re-run the affected pieces only

If the prior run completed all the sensitivity-arm and disparity-direction
work (check timestamps in `v3/`), you only need to re-run the broken
pieces. The cleanest way is to delete the broken outputs and re-run
the full script with `--skip-sql`:

```bash
cd /Users/loganglosser/THYROID_2026
rm -f studies/m048_racial_disparities_tirads/v3/m048_v3_attenuation_cascade.csv \
      studies/m048_racial_disparities_tirads/v3/m048_v3_full_model_OR.csv \
      studies/m048_racial_disparities_tirads/v3/m048_v3_interaction_race_x_tr.csv \
      studies/m048_racial_disparities_tirads/v3/m048_v3_interaction_race_x_nodulect.csv \
      studies/m048_racial_disparities_tirads/v3/m048_v3_mediation.csv \
      studies/m048_racial_disparities_tirads/v3/m048_v3_sensitivity_arms.csv \
      studies/m048_racial_disparities_tirads/v3/m048_v3_nodule_model_race_OR.csv

nohup .venv/bin/python studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py \
    --skip-sql --mediation-boot 1000 \
    > /tmp/m048_v3_run_v32.log 2>&1 &
echo "PID=$!"
```

Bootstrap with 1,000 reps (per spec) takes ~15–25 min on the
mediation phase. Lower to `--mediation-boot 200` for faster iteration
during debugging; raise to 1000 for final.

Monitor:
```bash
tail -f /tmp/m048_v3_run_v32.log    # Ctrl+C to detach
ls -lt studies/m048_racial_disparities_tirads/v3/ | head -10
```

### Step 4 — Independent recompute

```bash
.venv/bin/python studies/m048_racial_disparities_tirads/v3/verification/independent_recompute_v3.py
```

Output goes to
`studies/m048_racial_disparities_tirads/v3/verification/independent_recompute_v3_report.md`.
Should report PASS for all 5 hard-asserted headlines (Black full OR,
Asian full OR, attenuation %, Asian TR5 mean tumor size, Bethesda IV
Black OR) at ≤2% relative tolerance (5% for tumor size).

If any FAIL, investigate; do NOT proceed to figures/handoff.

### Step 5 — Build figures

```bash
.venv/bin/python studies/m048_racial_disparities_tirads/m048_build_figures_v3.py
```

Outputs to `M048_submission_package/figures/v3/` as 300-dpi PNG +
vector PDF. Eight figures: 6, 7, 8, 9, 10, 11, 12, 12b, 13.

Spot-check Figure 11 (disparity-direction quadrant), Figure 12b
(per-race ROM heatmap), and Figure 13 (FNA-pattern subplots).

### Step 6 — Cortex Analyst NL verification

The mig_311 semantic model is bound to the M025 nodule-level cohort.
It does NOT expose `race_strat` (per Cursor's note in
`v3/verification/cortex_smoke_tests_v3.md`). Two options:

**Option A (recommended; faster):** run NL queries that don't require
race stratification, against the M025 cohort, just to confirm the
pipeline reproduces M025 numbers.

```bash
PATH=/Users/loganglosser/.local/bin:$PATH
cortex analyst query "what is the per-TR ROM in the strict-eligible cohort"
cortex analyst query "what is the patient-level AUC for ACR TI-RADS"
cortex analyst query "what is the median number of US-detected nodules per malignant patient"
```

Expected results: TR4 ROM 18.7%, TR5 ROM 26.1%, AUC 0.6478, median
nodules per malignant patient ~2.

**Option B (cleaner; slower):** bind the v3 covariate semantic model
in Snowsight per the walkthrough at
`CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md`. Then run race-stratified
NL queries:
```
"what is the per-race patient-level ROM at TR4 and TR5?"
"how many strict-eligible nodules are there for each race?"
"what proportion of patients of each race had any FNA performed?"
```

Capture all queries + returned SQL + reconciliation against the
v3 CSVs in
`studies/m048_racial_disparities_tirads/v3/verification/cortex_smoke_tests_v3.md`.

### Step 7 — QA gates check

The runner emits `m048_v3_qa_gates.csv` with all gates. Verify:
- All gates report PASS (or document any WARN/FAIL with explanation)
- `mediation_has_asian_rows`: must be PASS
- `bethesda_rom_table_complete`: must be PASS

```bash
cat studies/m048_racial_disparities_tirads/v3/m048_v3_qa_gates.csv
```

### Step 8 — Handoff README

The runner emits `m048_handoff_README_v3.md`. Spot-check it contains:
- Black + Asian race ORs at M0, M4, M5, M6
- Top 3 mediators by `|indirect_mean|` for both Black and Asian
- Disparity-direction signature per race × TR4/TR5 cell
- Bethesda-stratified Model B race ORs

If any section shows "(missing: ...)" placeholders, the upstream CSV
didn't generate; investigate.

### Step 9 — Sign off mig_317b on MotherDuck

```bash
.venv/bin/python -c "
from motherduck_client import get_token
import duckdb
con = duckdb.connect(f'md:thyroid_canonical_publication_v1_0?motherduck_token={get_token()}')
with open('studies/m048_racial_disparities_tirads/mig_317b_signoff.sql') as f:
    sql = f.read()
# Strip USE statement (already in connection); execute INSERT
for stmt in sql.split(';'):
    s = stmt.strip()
    if not s or s.upper().startswith('USE'):
        continue
    con.execute(s)
print('mig_317b signed off')
"
```

### Step 10 — Update MASTER spreadsheet

Row 48 status should already be `v3 Adjusted Analysis Complete —
Awaiting Writing` from Cursor's previous update. After v3.2 fixes,
update to:
`v3.2 Collinearity-Fixed Adjusted Analysis Complete — Awaiting Writing`

```python
import openpyxl
wb = openpyxl.load_workbook('MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx')
ws = wb['Master Manuscript List']
for r in range(2, ws.max_row+1):
    if ws.cell(row=r, column=1).value == 48:
        ws.cell(row=r, column=4).value = 'v3.2 Collinearity-Fixed Adjusted Analysis Complete — Awaiting Writing'
        break
wb.save('MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx')
```

Commit + push.

### Step 11 — Final commit

```bash
git add MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx \
        studies/m048_racial_disparities_tirads/v3/ \
        M048_submission_package/figures/v3/
git commit -m "feat(M048 v3.2): collinearity fix re-run; full v3 outputs verified

- Bug A (is_malignant bool->int) fixed in m048_v3_stats_lib.py
- Bug B (had_any_fna x bethesda_bucket collinearity) fixed by removing
  had_any_fna from regression controls (had_repeat_fna and n_fnas_total
  retained for FNA-pattern signal)
- Re-ran cascade Models 5/6, Model F, Model I, Model M, mediation
  (Black + Asian, 1000 boot reps), 7 sensitivity arms
- independent_recompute_v3.py: all 5 hard assertions PASS at <=2% rel
- All 8 figures (6-13 + 12b) rendered at 300 dpi
- Cortex NL verification logged
- mig_317b signed off on MotherDuck
- MASTER row 48 status updated"
git push origin main
```

---

## 5. Headline numbers from the partial run (M0–M4 only — these are the SIGNAL)

These were computed under Bug B (so the M5/M6 column is broken),
but the M0–M4 numbers are clean because those formulas don't include
both `had_any_fna` and `C(bethesda_bucket)` together.

### Cascade race ORs (vs White reference)
| Step | Adds | Black OR | Asian OR |
|---|---|---|---|
| M0 | race only | **0.32** | **1.32** |
| M1 | + max_tr_int | 0.35 | 1.34 |
| M2 | + nodule burden | 0.35 | 1.34 |
| M3 | + genetics + NM | 0.36 | 1.43 |
| M4 | + background path | 0.35 | 1.47 |
| M5 | + FNA pattern + Bethesda | BUG (1.0) | BUG (1.0) |
| M6 | + age/sex/era/procedure | BUG (1.0) | BUG (1.0) |

**Interpretation (preliminary):** Black ORs are stable across M0–M4
(~0.32–0.36); Asian ORs are stable but in the opposite direction
(~1.32–1.47). v2 covariates (multinodular burden, genetics, nuclear
medicine, background path) explain almost none of the racial
differences. M5/M6 will be re-computed in v3.2.

### Bethesda-stratified Model B (additive race + max_tr_int)
| Bethesda | Race | OR | 95% CI | p |
|---|---|---|---|---|
| II benign | Black | 0.49 | 0.33–0.72 | 0.0003 |
| II benign | Asian | 2.32 | 1.02–5.30 | 0.045 |
| III AUS | Black | 0.65 | 0.43–0.99 | 0.043 |
| III AUS | Asian | 0.38 | 0.16–0.93 | 0.034 |
| IV FN | Black | 1.11 | 0.67–1.84 | 0.69 |
| IV FN | Asian | 0.41 | 0.08–2.21 | 0.30 |
| VI malig | Black | 0.54 | 0.32–0.89 | 0.017 |

**Interpretation:** Within Bethesda II (cytologically benign), Black
patients ~half as likely as White to have malignancy on path despite
the same TI-RADS distribution; Asian ~2.3× as likely. Calibration
divergence at the cytologically-best-controlled stratum.

---

## 6. Decision points needing senior-author input

These should be flagged in the v3.2 handoff README for the writing
chat (do NOT decide them autonomously):

1. **Black-vs-White M0 OR of 0.32 is large and stable across M0–M4
   adjustment.** This means Black patients in the operative cohort
   have substantially LOWER ROM than White patients at the same
   max_TR. The framing options are:
   - "Over-referral of indolent disease in Black patients"
   - "Differential institutional pathway routing of Black patients
     to surgery for benign indications"
   - Both / unresolvable in this dataset
   Senior author needs to weigh in on framing before drafting.

2. **Asian-vs-White Bethesda II OR of 2.32 is striking** — Asian
   patients with cytologically benign FNA had ~2.3× the malignancy
   rate of White patients with same Bethesda. Could be:
   - True false-negative cytology rate higher in Asian patients
   - Differential follow-up/repeat-FNA pathways
   - Sample size warning (Bethesda II Asian n is small; check
     `bethesda_stratified_TR_ROM.csv` for n + n_events per cell)

3. **Asian stratum power.** Overall n=204 patients. Many race × TR ×
   Bethesda cells will be N<10. The disparity-direction table for
   Asian × TR4/TR5 will likely have wide CIs.

---

## 7. Cortex NL queries to run

Paste one at a time:

```
cortex analyst query "what is the per-TR ROM in the strict-eligible cohort"
```
Expected: TR1 (none), TR2 ~12.9%, TR3 ~9.1%, TR4 ~18.7%, TR5 ~26.1%

```
cortex analyst query "what is the patient-level AUC for ACR TI-RADS in this cohort"
```
Expected: 0.6478 (M025 number)

```
cortex analyst query "how many strict-eligible nodules are in the cohort"
```
Expected: 3,687

```
cortex analyst query "what is the median tumor size in centimeters for path-malignant patients"
```
Expected: ~1.8 cm (M025 Table 1)

```
cortex analyst query "what proportion of malignant patients have multifocal disease"
```
Expected: ~61% (M025 multifocality stat)

If `cortex` returns "no semantic model bound", check that the bound
model still exists in Snowsight Cortex Analyst.

Capture all queries, returned SQL, results, and reconciliation in
`studies/m048_racial_disparities_tirads/v3/verification/cortex_smoke_tests_v3.md`.

---

## 8. Definition of done (gate before opening manuscript-writing chat)

- [ ] Bug A int-cast committed and pushed
- [ ] Bug B collinearity fix applied, committed, pushed
- [ ] Re-run produced new `m048_v3_attenuation_cascade.csv` with
      M5/M6 race ORs ≠ 1.000
- [ ] `m048_v3_mediation.csv` has `indirect_mean ≠ 0` for at least
      some mediator × race rows
- [ ] All 7 sensitivity arms emit non-error race ORs
- [ ] `independent_recompute_v3.py` exits 0 (all 5 assertions PASS)
- [ ] All 8 figures rendered to PNG + PDF
- [ ] `m048_v3_qa_gates.csv` shows all gates PASS (or documented WARN)
- [ ] `m048_handoff_README_v3.md` populated with all sections
- [ ] Cortex NL queries logged in `cortex_smoke_tests_v3.md`
- [ ] mig_317b signed off on MotherDuck
- [ ] MASTER row 48 status updated to v3.2
- [ ] Final commit pushed
- [ ] One-paragraph Cowork summary noting:
      - Black/Asian race ORs at M0 vs M4 vs M6 (post-fix)
      - Top 3 mediators by `|indirect_mean|` for Black AND Asian
      - Bethesda-stratified Model B race ORs (key strata)
      - Disparity-direction signatures (Black-TR4/TR5, Asian-TR4/TR5)
      - Cortex reconciliation result
      - Any WARN/FAIL gates with explanation
      - Senior-author decision items (the 3 from Section 6)

---

## 9. Appendix — file paths quick reference

```
Repo root:           /Users/loganglosser/THYROID_2026
Venv python:         /Users/loganglosser/THYROID_2026/.venv/bin/python
Cortex CLI:          /Users/loganglosser/.local/bin/cortex
MotherDuck token:    /Users/loganglosser/THYROID_2026/motherduck.local.toml
SQL driver:          studies/m048_racial_disparities_tirads/M048_motherduck_queries.sql
Stats lib:           studies/m048_racial_disparities_tirads/m048_v3_stats_lib.py
Run script:          studies/m048_racial_disparities_tirads/m048_run_analysis_v3.py
Figures script:      studies/m048_racial_disparities_tirads/m048_build_figures_v3.py
v3 CSV outputs:      studies/m048_racial_disparities_tirads/v3/
Figures output:      M048_submission_package/figures/v3/
Verification:        studies/m048_racial_disparities_tirads/v3/verification/
mig_317b signoff:    studies/m048_racial_disparities_tirads/mig_317b_signoff.sql
MASTER xlsx:         MASTER_MANUSCRIPT_LIST_DATA_READINESS_20260504.xlsx
Cortex bind walk:    CORTEX_ANALYST_BIND_WALKTHROUGH_20260505.md
This handoff:        COWORK_HANDOFF_M048_V3_CONTINUATION_20260505.md
```

---

End of handoff. Good luck.
