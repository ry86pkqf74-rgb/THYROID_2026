# Cursor Composer Dispatch — mig_252: Repair `comp_*_confirmed` rollup logic on `canonical_patient_master`

**Generated:** 2026-05-01 by Cowork at HEAD `0ae2881` (post-mig_250)
**Lane:** mig_252 — fix the boolean rollup flags on `canonical_patient_master` for the complication family. Source: `main.canonical_complications_events_v1`. Specification: `comp_<type>_confirmed = TRUE` iff the patient has at least one event with `finding_status = 'present'` AND `evidence_strength IN ('definitive','probable')` for `complication_type = '<type>'`.
**Recommended agent:** Cursor Composer (per-flag reasoning across 10 complication families + cohort-wide rollup)
**Estimated runtime:** 60–90 min
**Triggered by:** Cowork audit 2026-05-01 during M038 planning. Discrepancy summary in §1 below.
**Severity:** HIGH. Affects every manuscript that uses `any_confirmed_complication_flag` or `comp_*_confirmed` (M032 25-yr descriptive paper already in draft, M038 in planning, plus any future complications-touching manuscript).
**Closes carry-forward:** CF-COMP-CONFIRMED-ROLLUP-BUG (newly opened in this dispatch).

---

## §0 — First message to paste into Cursor Composer

> mig_252 dispatch. Read `cursor_prompts/CURSOR_PROMPT_MIG_252_COMP_CONFIRMED_ROLLUP_FIX_20260501.md` end-to-end before any tool use. You have MotherDuck MCP authed to `logan.glosser.eras@gmail.com`; database is `thyroid_canonical_publication_v1_0`. GitHub repo at `/Users/loganglosser/THYROID_2026`. Use Desktop Commander for git ops (FileVault — `.git/index.lock` cleanup may be needed).
>
> **Critical:** this is a `main.*` mutation. Per `feedback_dryrun_signoff_before_build.md`, dry-run all gate checks against scratch TEMP tables and surface the diff to Logan for sign-off BEFORE running any `UPDATE main.canonical_patient_master`.

---

## §1 — Why this lane exists

During M038 (Massive Goiter / Definition Paper) planning on 2026-05-01, Cowork ran a sanity check on `comp_seroma_confirmed` (39 confirmed seroma in n=475 ≥200g focal cohort = 8.2%; literature: 1–3%). Tracing to `canonical_complications_events_v1` revealed:

- 587/618 (95%) of `comp_seroma_confirmed = TRUE` patients have **zero** events with `finding_status = 'present'` for seroma.
- The dominant pattern: their flag is being driven by events with `finding_status = 'absent'` and `evidence_strength = 'possible'` — i.e., negation evidence (e.g., a discharge summary stating "no seroma") is being interpreted as a confirmation event.

**Generalized audit, patient-level (any event of that complication type):**

| complication_type | n pts with any event | with ≥1 'present' event | with 'present' AND def/probable | % failing 'present' check |
|---|---:|---:|---:|---:|
| chyle_leak | 1,576 | 5 | 3 | **99.7%** |
| seroma | 871 | 45 | 39 | **94.8%** |
| rln_injury | 690 | 21 | 21 | **97.0%** |
| hematoma | 250 | 70 | 68 | **72.0%** |
| hypoparathyroidism | 406 | 298 | 296 | 26.6% |

**Rate impact on M038 primary outcome:**

| Subset | `any_confirmed_complication_flag` | Strict (present + def/probable) |
|---|---:|---:|
| ≥200g focal cohort (n=475) | 30.7% (146 events) | **2.1% (10 events)** |
| <200g (n=8,655) | 21.3% | 3.4% |
| Weight NULL (n=1,741) | 28.7% | 4.7% |

The original 30.7% complication rate is a canonicalization artifact. Real strict-definition rate is ~2%.

---

## §2 — Pre-task probes

```sql
-- Tip-state confirmation
SELECT * FROM semantic_publication.vw_publication_qc_status_VIEW_v1;
-- Expected: gate1=218, gates 2-5=0, cohort_parity TRUE.

-- Source-event distribution (verify the bug pattern exists exactly as described)
SELECT
  LOWER(complication_type) AS comp,
  finding_status,
  evidence_strength,
  COUNT(*) AS n_events,
  COUNT(DISTINCT research_id) AS n_pts
FROM main.canonical_complications_events_v1
GROUP BY 1,2,3
ORDER BY 1, 4 DESC;

-- Current canonical_patient_master rollup flag distribution
SELECT
  SUM(CASE WHEN comp_seroma_confirmed THEN 1 ELSE 0 END) AS seroma,
  SUM(CASE WHEN comp_hematoma_confirmed THEN 1 ELSE 0 END) AS hematoma,
  SUM(CASE WHEN comp_rln_injury_confirmed THEN 1 ELSE 0 END) AS rln,
  SUM(CASE WHEN comp_chyle_leak_confirmed THEN 1 ELSE 0 END) AS chyle,
  SUM(CASE WHEN comp_vc_paresis_confirmed THEN 1 ELSE 0 END) AS vc_paresis,
  SUM(CASE WHEN comp_vc_paralysis_confirmed THEN 1 ELSE 0 END) AS vc_paralysis,
  SUM(CASE WHEN comp_hypocalcemia_confirmed THEN 1 ELSE 0 END) AS hypocalc,
  SUM(CASE WHEN comp_hypoparathyroidism_confirmed THEN 1 ELSE 0 END) AS hypopara,
  SUM(CASE WHEN any_confirmed_complication_flag THEN 1 ELSE 0 END) AS any_comp_flag
FROM main.canonical_patient_master;

-- Audit the existing rollup-build script (find what defined the flags)
-- Expected: a script under scripts/ or a prior migration that built these columns
-- without the finding_status='present' filter. Identify it; do NOT silently re-run it.
```

---

## §3 — Task spec

### Step 3.1 — Locate the existing rollup-build script

Inspect the most recent CREATE/UPDATE source for the `comp_*_confirmed` and `any_confirmed_complication_flag` columns on `canonical_patient_master`. Likely candidates:

- `complication_patient_summary_v1` (existing rollup view)
- `canonical_complications_patient_rollup_v1`
- A Python script under `scripts/` ending in `complications_rollup` or similar
- A migration in `qc_framework_v1/migrations/` numbered 100–200

Identify the file and the exact rollup expression. Document it in the migration header so the diff is traceable.

### Step 3.2 — Define the corrected rollup expression

For each of the 10 family complications, the corrected `comp_<type>_confirmed` is:

```sql
EXISTS (
  SELECT 1 FROM main.canonical_complications_events_v1 e
  WHERE e.research_id = pm.research_id
    AND LOWER(e.complication_type) = '<type>'
    AND e.finding_status = 'present'
    AND e.evidence_strength IN ('definitive','probable')
)
```

Where `<type>` ∈ {`seroma`, `hematoma`, `rln_injury`, `chyle_leak`, `vc_paresis`, `vc_paralysis`, `hypocalcemia`, `hypoparathyroidism`, `airway_complication`, `pneumothorax`, `mortality`}.

The composite `any_confirmed_complication_flag` is the OR of the corrected family flags.

### Step 3.3 — Decide: also correct the `_definitive`, `_probable_or_better`, `_any_evidence` flags?

The complication family has additional rollup variants:

- `comp_<type>_definitive` → `evidence_strength = 'definitive'` AND `finding_status = 'present'`
- `comp_<type>_probable_or_better` → `evidence_strength IN ('definitive','probable')` AND `finding_status = 'present'`
- `comp_<type>_any_evidence` → `finding_status = 'present'` (regardless of evidence_strength)
- `comp_<type>_suspected` → `finding_status = 'present'` AND `evidence_strength = 'possible'`

Audit each variant family analogously. The `_definitive` and `_probable_or_better` flags may also be missing the `finding_status = 'present'` filter. Repair all variants in the same migration.

### Step 3.4 — Dry-run

Build a TEMP table reproducing the corrected rollup against `canonical_patient_master`. Compare side-by-side:

```sql
-- expected diff (illustrative)
-- comp_seroma_confirmed:        618 → 27   (drop ~95%)
-- comp_hematoma_confirmed:      ~250 → ~68 (drop ~73%)
-- comp_rln_injury_confirmed:    ~700 → ~21 (drop ~97%)
-- comp_chyle_leak_confirmed:    ~1500 → 3  (drop ~99%)
-- comp_hypoparathyroidism_confirmed: 406 → 296 (drop ~27%)
-- any_confirmed_complication_flag: 2490 → ~388 (drop ~85%)
```

Surface this diff to Logan via the dispatch chat for explicit sign-off **before** any `UPDATE main.canonical_patient_master` runs.

### Step 3.5 — Apply

Build the corrected flags as a migration that:

- Updates the `canonical_patient_master` columns in place via `UPDATE` (not `CREATE OR REPLACE TABLE` — preserves all other columns and signoff registry)
- OR rebuilds the rollup table that `canonical_patient_master` reads from, then re-derives the master columns

The choice depends on how the master table is built today (Step 3.1 finding).

Update `signoff_registry` per `feedback_dryrun_signoff_before_build.md` to mark the affected columns as re-verified post-mig_252.

### Step 3.6 — Downstream cohort views

The cohort views in `manuscript_workspace.cohort_*` that select `comp_*_confirmed` or `any_confirmed_complication_flag` from `canonical_patient_master` will automatically pick up the corrected values. Verify by running `SELECT COUNT(*), SUM(CASE WHEN any_confirmed_complication_flag ...) FROM cohort_m038_massive_goiter_v1 WHERE gland_weight_final_g >= 200` and confirming the new event count matches the strict definition (~10 events, not 146).

### Step 3.7 — Update gates and dashboard

- Re-run `vw_publication_qc_status_VIEW_v1` and confirm gate1/2/3/4/5 = 218/0/0/0/0 / cohort_parity TRUE.
- Re-run `manuscript_feasibility_v1` re-score (mig_249 pattern) — though most manuscripts don't include complications as primary key_variables, M038/M032/M046/M047 should be checked specifically.
- Re-run `manuscript_dashboard_VIEW_v1` (no schema change expected, just a freshness check).

---

## §4 — Carry-forwards opened by this dispatch

| ID | Description |
|---|---|
| **CF-COMP-CONFIRMED-ROLLUP-FIX-DOWNSTREAM** | After mig_252 lands, M032 (25-yr descriptive paper, draft at `manuscript_outputs/v1_0_20260501/M032_25yr_descriptive_analysis_DRAFT_v1.md`) needs a complications-section rebuild. The current draft references the buggy 23% complication rate. |
| **CF-M038-PAUSED-ON-MIG_252** | M038 drafting paused until mig_252 lands. Once landed, primary-outcome rate becomes ~2% in ≥200g cohort, regression plan needs re-power calculation. |

---

## §5 — Acceptance criteria

1. `comp_*_confirmed` and `any_confirmed_complication_flag` on `canonical_patient_master` reflect strict definition (present + def/prob).
2. `vw_publication_qc_status_VIEW_v1` shows gate1=218 unchanged, gates 2–5 = 0, cohort_parity TRUE.
3. M038 cohort (`cohort_m038_massive_goiter_v1` filtered to gland_weight_final_g ≥ 200) reports ~10 confirmed-complication events, not 146.
4. A migration file `qc_framework_v1/migrations/252_comp_confirmed_rollup_fix_20260501.sql` exists and is committed.
5. The `signoff_registry` row for the affected columns has a fresh `mig_252` provenance entry.
6. Cowork is notified to update M038 planning doc + M032 draft.

---

**End of mig_252 dispatch.**
