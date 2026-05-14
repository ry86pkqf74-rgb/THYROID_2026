# Cursor handoff: EXT2-4 v3 → v4 cohort rebuild (any preop 2–4 cm nodule)

**Recommendation: do this in Cursor.** Most of the work is local-file regeneration (CSVs, figures, docx, zip). BQ access required for re-deriving aggregate cells against the new cohort definition. Will require docx-js (`npm install docx` already done in `manuscript_v2_package_20260509/`), python+matplotlib+scipy (already in .venv), and write access to the EXT2-4 study folder.

Builds on:
- Logan's 2026-05-14 cohort decision (in `cohort_reconciliation_v1_vs_v3.md` §"DECISION"): include any patient with at least one preop nodule 2.0–4.0 cm.
- mig_323 platform reclassification, mig_324 date completeness, mig_325 guard cleanup, mig_326 thyroid 3D + parathyroid weight.
- 8-row MFL audit chain from `MFL-20260509-EXT2-4-ELICIT-EXPANSION` → `MFL-20260514-EXT2-4-WEIGHT-SIZE-EXTENSION`.

## Goal

Rebuild the EXT2-4 manuscript on the **broader "any preop 2–4 cm nodule"** cohort (~n=765 vs current v3 n=400). All Tables 1–4, Figures 1–4, executive summary, README_PACKAGE, manuscript draft docx, and zip refreshed end-to-end. v3 deliverables preserved in `superseded_v3/` per never-delete rule.

## Hard rules (NON-NEGOTIABLE)

1. **No PHI in Airtable / Linear / committed code.** Research_id only; aggregate counts only.
2. **Append-only.** Move v3 deliverables to `superseded_v3/` with a `SUPERSEDED_NOTE_v3_to_v4.md` documenting the cohort-definition change. Don't delete.
3. **Pre-edit DFL row** (`appJYOnUb7KrHKwpV.tblsiYKJtKcktkzze`): `change_type=major_revision`, target = EXT2-4 manuscript package. `your_request_summary` = "v3 → v4 cohort definition change to 'any preop 2–4 cm nodule' (Logan decision 2026-05-14)".
4. **MFL row post-edit** (`MFL-<YYYYMMDD>-EXT2-4-V4-COHORT-REBUILD`) linked to EXT2-4 (`rec1GJyrmKdKxjlaY`) with full pre/post Tables 1–4 number comparison.
5. **Skill version bump** v2.3.1 → **v2.4.0** (minor — cohort definition is a substantive analytical change). Pre-bump verified-state check required per the skill's hard rule.
6. **Snapshot any canonical-layer reads** — this rebuild is read-only on BQ; no canonical writes expected. If any are needed (unlikely), snapshot first.

## Phase 1 — Build the v4 cohort definition

The v4 cohort = surgical_b34 ∩ patients with at least one preop nodule 2.0–4.0 cm at any exam on or before surgery.

### Primary cohort SQL (`sql/04b_table3_v4_actual_reported_call.sql`)

Replace the v3 `imaging_nodule_size_cm BETWEEN 2.0 AND 4.0` filter (patient-grain index) with an EXISTS clause on `canonical_us_nodule_v2`:

```sql
WITH surgical AS (
  SELECT
    CAST(research_id AS STRING) AS rid_s,
    fna_bethesda_final AS bethesda,
    imaging_nodule_size_cm AS preop_size_cm_index,  -- kept for descriptive only
    surg_first_date,
    surg_total_thyroidectomy,
    surg_hemithyroidectomy,
    surg_procedure_type,
    age_at_surgery,
    sex,
    histology_final,
    LOWER(TRIM(IFNULL(histology_final,''))) AS histo_lower
  FROM `thyroid-canonical-pub-2026.pub_canonical.manuscript_cohort_v1`
  WHERE surg_first_date IS NOT NULL
    AND EXTRACT(YEAR FROM surg_first_date) BETWEEN 1999 AND 2025
    AND surg_procedure_type IN ('total_thyroidectomy','hemithyroidectomy')
),
cohort_v4_pts AS (
  -- "Any preop nodule 2.0–4.0 cm at any exam on or before surgery" — the v4 inclusion
  SELECT DISTINCT s.rid_s
  FROM surgical s
  JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_us_nodule_v2` n
    ON CAST(n.research_id AS STRING) = s.rid_s
   AND n.exam_date <= DATE(s.surg_first_date)
  WHERE n.size_cm_max BETWEEN 2.0 AND 4.0
),
v4 AS (
  SELECT s.*
  FROM surgical s
  WHERE s.rid_s IN (SELECT rid_s FROM cohort_v4_pts)
)
-- continue with whatever Table 3 / Table 1 etc. logic
SELECT COUNT(*) AS n_v4_total FROM v4;
```

Acceptance for Phase 1:
- `n_v4_total` ≈ **765** (±20) — must reproduce the Cowork-derived count from 2026-05-13
- Spot-check: 5 random patients in v4 should have at least one `canonical_us_nodule_v2.size_cm_max` value in [2.0, 4.0]

### Strict-nodal-exclusion sensitivity cohort (`v4_strict`)

Apply the v1 strict-nodal-exclusion logic to v4:

```sql
ct_susp AS (
  SELECT DISTINCT CAST(research_id AS STRING) AS rid_s, MIN(exam_date) AS earliest_ct_susp_date
  FROM `pub_canonical.canonical_ct_lymph_node_v1`
  WHERE suspicious_flag = TRUE GROUP BY rid_s
),
mri_susp AS (
  SELECT DISTINCT CAST(research_id AS STRING) AS rid_s, MIN(exam_date) AS earliest_mri_susp_date
  FROM `pub_canonical.canonical_mri_lymph_node_v1`
  WHERE suspicious_flag = TRUE GROUP BY rid_s
),
bethesda6_ln_fna AS (
  SELECT DISTINCT CAST(research_id AS STRING) AS rid_s
  FROM `pub_canonical.canonical_fna_events_v1`
  WHERE bethesda_final_num = 6
    AND (LOWER(IFNULL(specimen_location,'')) LIKE '%node%'
         OR LOWER(IFNULL(specimen_location,'')) LIKE '%lymph%'
         OR LOWER(IFNULL(fna_site,'')) LIKE '%node%'
         OR LOWER(IFNULL(fna_site,'')) LIKE '%lymph%')
),
v4_strict AS (
  SELECT v.*
  FROM v4 v
  LEFT JOIN ct_susp c ON v.rid_s = c.rid_s AND DATE(c.earliest_ct_susp_date) <= DATE(v.surg_first_date)
  LEFT JOIN mri_susp m ON v.rid_s = m.rid_s AND DATE(m.earliest_mri_susp_date) <= DATE(v.surg_first_date)
  LEFT JOIN bethesda6_ln_fna b ON v.rid_s = b.rid_s
  WHERE c.rid_s IS NULL AND m.rid_s IS NULL AND b.rid_s IS NULL
)
```

Acceptance: `n_v4_strict` ≈ 600–680 (analogous to v1's N=558 → 635 ratio, expanded by mig_323/325/326 platform corrections).

## Phase 2 — Re-derive all Tables 1–4 on v4

### Table 1 (cohort characteristics)
Stratify by surgical extent (lobe vs total), pre-2015 / 2015+, and the new v4 size strata. Add a "v4_strict" column for the strict-nodal-exclusion sensitivity arm.

### Table 2 (malignancy by Bethesda × era)
Drop the "size_band" column since the cohort itself is now size-defined. Replace with Bethesda × era only.

### Table 2b (surgical extent by Bethesda × era)
Same change — drop size_band, keep Bethesda × era.

### Table 3 v4 (diagnostic performance)
Restrict to Bethesda III/IV + named platform + final histology. Same logic as Table 3 v3, but the size_band filter on the test-positive/negative cells changes — instead of filtering Bethesda III/IV × size_band, just filter Bethesda III/IV (the cohort is already size-defined). The 2–4 cm subgroup row becomes the cohort-wide row, so Table 3 v4 should have:
- Bethesda III all sizes
- Bethesda IV all sizes
- Bethesda III+IV all sizes (= the cohort)
- Optional: sub-stratify by `imaging_nodule_size_cm_index` (the v3 patient-grain index) for cross-comparison with v3 reported numbers

### Table 4 (recurrence)
Re-derive on v4 cohort. Same `recurrence_path_proven` definition.

Save all CSVs as `tables/table*_v4_*.csv`. Don't overwrite v3 CSVs.

## Phase 3 — Refresh build scripts with v4 cells

### `build_table3_v4_actual_call.py`
Copy from `build_table3_v2_actual_call.py`. Update the `cells = [...]` block with v4-cohort cell counts from Phase 2. Output goes to `tables/table3_v4_*.csv` (not v3 paths).

### `build_figures_v4.py`
Copy from `build_figures_v2.py`. Update `forest_rows` with new v4 Bethesda III+IV Wilson CI numbers (no size_band breakdown since the cohort is size-defined). Update `rom_rows` with v4 numeric ROM% distribution. Update `era_rows` if the era-trend figure scope changes; otherwise era data is unchanged from v3 (entire surgical cohort, not v4-restricted).

### `build_elicit_expansion_v4.py`
Copy from `build_elicit_expansion.py`. Update:
- `table1_strata` with the v4 cohort denominator strata
- `table2_input` and `table2b_input` with v4 cells (drop size_band dimension)
- `cohort_flow` with the v4 inclusion: surgical 8,368 → cohort_v4_pts 765 → v4_strict ~620
- Cohort flow figure regeneration

## Phase 4 — Regenerate manuscript docx

### `build_manuscript_docx_v4.js`
Copy from `manuscript_v2_package_20260509/build_manuscript_docx.js`. Update:

- **Title** — adjust "Initial thyroidectomy extent among adults with preoperative 2.0–4.0 cm thyroid nodules" → consider "...adults with any preoperative thyroid nodule measuring 2.0–4.0 cm" to make the inclusion explicit.
- **Methods § "Cohort definition"** — rewrite the inclusion criterion: "...patients with at least one preoperative ultrasound nodule measuring 2.0–4.0 cm in greatest dimension at any exam on or before the index surgery (`canonical_us_nodule_v2.size_cm_max` ∈ [2.0, 4.0])." Explicit acknowledgement that this is broader than the v1 "index nodule" framing — cross-link to `cohort_reconciliation_v1_vs_v3.md` for the prior framing.
- **Results §** — n=765 primary, ~620 strict-nodal-excluded sensitivity arm. All Table 1–4 numbers refreshed.
- **Discussion §** — note that v4 supersedes v3; reference the cohort_reconciliation memo. Add the v3-to-v4 number-shift table for transparency.
- **Limitations §** — keep all existing caveats; ADD that the unified cohort definition was a post-hoc design decision after the v1 / v3 reconciliation, supported by clinical-judgment co-author input.
- **Tables (inline)** — refresh Table 1 summary, Table 3 v4 row data block, Table 4.
- **References** — insert the 8 new citations from `references_working_20260514.md` §B + §C: refs 11–18. Update Methods § to cite Patel 2018 + Steward 2019 + Cibas-Ali 2023 + Nikiforov 2016 + Haugen 2016 + Begg-Greenes 1983 + Wilson 1927 + Benjamini-Hochberg 1995 at appropriate insertion points.

## Phase 5 — Build manuscript_v4 package

```bash
# from elicit_expansion_20260509/
STAGE=/tmp/manuscript_v4_package_<YYYYMMDD>
mkdir -p $STAGE/tables $STAGE/figures $STAGE/sql
cp README_PACKAGE.md manuscript_v4_draft.docx executive_summary_elicit_alignment.md \
   data_dictionary.md cohort_flow_bq.csv build_*.py build_manuscript_docx_v4.js $STAGE/
cp tables/table*_v4_*.csv $STAGE/tables/
cp -R tables/superseded $STAGE/tables/
cp figures/* $STAGE/figures/
cp sql/* $STAGE/sql/
cd /tmp && zip -rq manuscript_v4_package_<YYYYMMDD>.zip manuscript_v4_package_<YYYYMMDD>
cp manuscript_v4_package_<YYYYMMDD>.zip $WORKSPACE/elicit_expansion_20260509/
```

## Phase 6 — Supersede v3 cleanly

```bash
mkdir -p superseded_v3
mv manuscript_v3_draft.docx manuscript_v3_package_20260509.zip superseded_v3/
# (do NOT move build_table3_v2_actual_call.py / build_figures_v2.py — those become
#  "v3 historical" reference scripts; rename to *_v3.py for clarity)
mv build_table3_v2_actual_call.py build_table3_v2_actual_call_v3_archived.py
mv build_figures_v2.py build_figures_v2_v3_archived.py
mv build_elicit_expansion.py build_elicit_expansion_v3_archived.py
```

Add `superseded_v3/SUPERSEDED_NOTE_v3_to_v4.md` documenting:
- The cohort-definition change (v3 patient-grain "resolved index nodule" → v4 "any preop nodule 2–4 cm")
- Logan's 2026-05-14 decision date
- v3-to-v4 number-shift table (cohort n, Table 1 / 2 / 3 / 4 deltas)
- MFL row that authorized the change

## Phase 7 — Airtable + skill bump

### DFL pre-edit
Filed at the START of this work. `change_type=major_revision`. Reference cohort_reconciliation_v1_vs_v3.md.

### MFL post-edit (`MFL-<YYYYMMDD>-EXT2-4-V4-COHORT-REBUILD`)
- Linked to EXT2-4 (`rec1GJyrmKdKxjlaY`)
- Pre-state: v3 deliverables, cohort n=400
- Post-state: v4 deliverables, cohort n=~765
- Document v3 → v4 number shifts in Tables 1–4
- Note skill bump v2.3.1 → v2.4.0

### Skill version bump v2.3.1 → v2.4.0
Required pre-bump verified-state check (per skill's hard rule):
```sql
-- Verify v4 cohort definition reproduces in BQ
WITH cohort_v4_pts AS ( ... -- same SQL as Phase 1)
SELECT COUNT(DISTINCT rid_s) AS n_v4_total FROM cohort_v4_pts;
```
Expected: ≈765. Paste the verified count into the CHANGELOG entry before the bump applies.

CHANGELOG entry (`.cowork/skills/thyroid-integration/references/CHANGELOG.md`):

```
## v2.4.0 — <YYYY-MM-DD>

EXT2-4 manuscript v3 → v4 cohort-definition change.

Cohort redefined from "resolved index nodule 2.0–4.0 cm" (patient-grain,
n=400) to "any preop ultrasound nodule 2.0–4.0 cm at any exam on or
before surgery" (broader, n=~765). Decision driver: clinical-judgment
co-author input — lobectomy-vs-total-thyroidectomy decision is driven
by any 2–4 cm nodule, not exclusively by an "index" lesion.

Verified-state check (BQ 2026-05-14):
- cohort_v4_pts: n=765 (verified pre-bump)
- v4_strict (after strict nodal exclusion): n=~620
- Reproduces v1 N=635 framing within ±20%

Deliverables: manuscript_v4_draft.docx, manuscript_v4_package_<YYYYMMDD>.zip,
tables/table{1,2,2b,3,4}_v4_*.csv, figures/{fig1_cohort_flow,fig2,fig3,fig4}_v4.png,
build_*_v4.{py,js}, references_working_20260514.md (refs 11–18 inserted into
manuscript prose).

v3 deliverables preserved at superseded_v3/.

MFL: MFL-<YYYYMMDD>-EXT2-4-V4-COHORT-REBUILD
DFL: <new DFL row id>
```

## Phase 8 — Verification

### Number-consistency spot-check
Run the same docx-text-extract / grep workflow Cowork has used:
```bash
python3 -c "
import zipfile, re
with zipfile.ZipFile('manuscript_v4_draft.docx') as z:
    text = re.sub(r'<[^>]+>', ' ', z.read('word/document.xml').decode('utf-8'))
text = re.sub(r'\s+', ' ', text)
# v4 headline numbers should appear; v3-specific ones should NOT
for n in ['n=765','n=620','n=400','n=91','n=222','n=30']:
    print(f'  {n!r}: {text.count(n)} hits')
"
```
Expected: v4 numbers (765, ~620) hit; v3 cohort n=400 / Table 3 v3 numbers (n=90, n=222, n=30) **should not appear** except in the v3-to-v4 transition table.

### Wilson CI verification
After Phase 3 re-runs the build scripts, the new Table 3 v4 CSV should have Sens/Spec/PPV/NPV with Wilson 95% CIs. Compare to the pre-rebuild v3 numbers (in the supersession table):

| Cell | v3 | v4 expected direction |
|---|---|---|
| Afirma B3+B4 cohort n | 76→90 (post mig_325) | likely 95–120 (broader cohort captures more 2–4 cm Afirma patients) |
| ThyroSeq B3+B4 cohort n | 222 (post mig_325) | likely 280–350 |
| Sens / Spec / PPV / NPV | already at the "true" reported-call values | should shift only marginally (Wilson CIs will tighten) |

### Cohort flow figure
Verify the new figure shows: 10,871 master → 8,368 surgical → 765 (any preop 2–4 cm) → ~620 (strict nodal excluded sensitivity arm). Pre-2015 vs 2015+ split also documented.

## Acceptance criteria

- [ ] `n_v4_total` ≈ 765 (±20) verified against BQ
- [ ] All Table 1–4 CSVs regenerated in `tables/table*_v4_*.csv`
- [ ] All Figures 1–4 regenerated in `figures/fig*_v4.{png,pdf}` (or overwritten with v4 numbers; preserve v3 in `superseded_v3/figures/`)
- [ ] `manuscript_v4_draft.docx` builds successfully via docx-js validation (`python3 scripts/office/validate.py`)
- [ ] `manuscript_v4_package_<YYYYMMDD>.zip` exists in the elicit_expansion folder
- [ ] v3 deliverables moved to `superseded_v3/` with SUPERSEDED_NOTE_v3_to_v4.md
- [ ] DFL row, MFL row filed
- [ ] References 11–18 inserted into manuscript v4 prose
- [ ] Skill version bumped to v2.4.0 with CHANGELOG entry
- [ ] EXT2-4 lifecycle still Active

## What's NOT in scope of this rebuild

- **Manuscript multivariable logistic regression on v4** — that's a separate analysis (the v1 manuscript's logistic was on N=558; would need to be re-run on v4). Defer; mark in the next-session checklist.
- **Survival / recurrence analyses on v4** — out-of-scope per the original Cohort decision (no follow-up data). Defer.
- **Co-author review pass** — that's a manual review step after v4 lands.
- **Senior-author fill-ins** (IRB, institution, funding, COI) — separate; not Cursor's job.

## When done, hand back to Cowork for

- Final manuscript prose pass — Cowork should review the v4 prose for accuracy, smooth transitions, and ensure the references 11–18 are insertion-correct.
- Co-author review prep — Cowork can compose a 1-page "v3 → v4 change summary" for distribution to co-authors.
- Optional Notable Finding filing if any v3 → v4 number shift is publishable on its own (e.g., the cohort-definition impact analysis).
- Any final pre-submission housekeeping.
