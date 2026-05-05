# Cursor prompt — mig_310 v2 Phases B→G (FNA NLP via Cortex)

**Agent:** cursor_composer (Snowflake-capable, Cortex compute)
**Estimated wall time:** 30–60 min (15 min pilot + 30 min full + verification)
**Supersedes:** Phase A0-only commit `9ceaa5e` (already on main)
**Closes:** `CF-FNA-SIZE-CM-NULL`

## Problem

mig_310 v2 commit `9ceaa5e` shipped Phase A0 only — two MotherDuck views (`fna_content_corpus_v1`, `fna_event_note_linkage_v1`) deploy correctly with 3,432 corpus rows and 2,756/8,050 (34.2%) FNA-event-to-note linkages. The Cortex `EXTRACT_ANSWER` extraction (Phase D, ~11k calls), the SF→MD mirror (Phase F), and the downstream `imaging_fna_linkage_v4` build (Phase G) are all still pending. CF-FNA-SIZE-CM-NULL remains open.

Cowork inserted `mig_310_phaseA0` as a partial progress marker. The full `mig_310` signoff should be inserted by `scripts/mig_310_fna_size_mirror.py --md --signoff` after Phase G lands.

## Recipe

### Step 1 — Pilot run (200 random notes, ~$0.20 worst case)

```bash
cd /Users/loganglosser/THYROID_2026
SNOWFLAKE_PAT=$SNOWFLAKE_PAT \
  .venv/bin/python snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py --md --pilot 2>&1 | tee logs/mig_310_pilot.log
```

**Inspect the pilot output for:**
- `size_fill_pct` ≥ 60% (size extraction works on majority of HP-corpus notes)
- `lat_fill_pct` ≥ 50%
- `bethesda_extract_score` average ≥ 0.5 (the bonus 4th field is meaningful)
- No traceback in the log
- Sample-200 validation probe shows plausible size_cm range (0.3–8.0 cm typical, 0.1–15.0 cm allowed)

If any of these fail, **stop, debug, do not proceed to full run.**

### Step 2 — Full run (~2,756 notes, ~10–30 min on COMPUTE_WH)

```bash
SNOWFLAKE_PAT=$SNOWFLAKE_PAT \
  .venv/bin/python snowflake_trial/scripts/36_pull_sf_nlp_fna_size.py --md 2>&1 | tee logs/mig_310_full.log
```

This populates:
- SF: `THYROID_VALIDATION.PUBLIC.FNA_NOTES_MIG310_V2`, `NLP_FNA_SIZE_FULL_RESULTS_v1`, `NLP_FNA_SIZE_PATIENT_ROLLUP_v1`
- MD: `manuscript_workspace.nlp_fna_size_rollup_v1`

### Step 3 — Build imaging_fna_linkage_v4 + signoff

```bash
.venv/bin/python scripts/mig_310_fna_size_mirror.py --md --signoff 2>&1 | tee logs/mig_310_v4_build.log
```

This:
- Verifies `nlp_fna_size_rollup_v1` is populated (gate 1)
- Creates `manuscript_workspace.imaging_fna_linkage_v4` (LEFT JOIN onto v3 with ±14 day window)
- Prints v3→v4 coverage delta
- Inserts `mig_310` signoff row to `main.signoff_migration`

## Validation gates (acceptance)

```sql
-- Cortex output coverage
SELECT
  COUNT(*) AS n_rollup,
  COUNT(extracted_size_cm) AS n_size,
  ROUND(100.0 * COUNT(extracted_size_cm) / COUNT(*), 1) AS pct_size,
  COUNT(extracted_laterality) AS n_lat,
  ROUND(100.0 * COUNT(extracted_laterality) / COUNT(*), 1) AS pct_lat,
  COUNT(extracted_bethesda) AS n_beth
FROM manuscript_workspace.nlp_fna_size_rollup_v1;
-- Acceptance: n_rollup ≥ 2,500 (cf. linkage 2,756); pct_size ≥ 60%; pct_lat ≥ 50%; n_beth ≥ 1,500
```

```sql
-- v4 view shape
SELECT COUNT(*) FROM manuscript_workspace.imaging_fna_linkage_v4;
-- Acceptance: row count within 5% of v3 (LEFT JOIN should not drop rows)

SELECT fna_size_source_v4, COUNT(*) AS n
FROM manuscript_workspace.imaging_fna_linkage_v4
GROUP BY 1 ORDER BY 2 DESC;
-- Acceptance: 'structured' rows preserved; 'nlp_high'+'nlp_medium' contributes ≥1,500 newly resolved sizes
```

```sql
-- M025 nodule semantic model shouldn't drift on per-TR ROM (size is informational, not in aggregate)
-- Skip this in cursor; Cowork will run the smoke test post-signoff.
```

```sql
-- Signoff insertion
SELECT mig_id, signed_off_at, by_actor, LEFT(summary, 200) AS s
FROM main.signoff_migration
WHERE mig_id IN ('mig_310','mig_310_phaseA0')
ORDER BY signed_off_at DESC;
-- Acceptance: 'mig_310' row present after Step 3, with by_actor='cursor_composer_mig310'
```

## Bethesda cross-validation (post-Phase G QA)

```sql
-- NLP-extracted Bethesda vs canonical bethesda_final_num (where both exist)
WITH cross_val AS (
  SELECT
    fna.bethesda_final_num AS canonical_beth,
    n.extracted_bethesda AS nlp_beth,
    CAST(fna.bethesda_final_num AS INTEGER) = n.extracted_bethesda AS exact_match
  FROM main.canonical_fna_events_v1 fna
  JOIN manuscript_workspace.nlp_fna_size_rollup_v1 n
    ON fna.fna_event_id = n.fna_event_id
  WHERE fna.bethesda_final_num IS NOT NULL AND n.extracted_bethesda IS NOT NULL
)
SELECT
  COUNT(*) AS n_compared,
  SUM(CASE WHEN exact_match THEN 1 ELSE 0 END) AS n_exact,
  ROUND(100.0 * SUM(CASE WHEN exact_match THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_exact
FROM cross_val;
-- Acceptance: pct_exact ≥ 70% (typical for EXTRACT_ANSWER on numeric labels)
```

If pct_exact <70%, the prompt for `_bethesda_raw` may need refinement. This is informational — the canonical Bethesda field remains primary; do not overwrite from NLP.

## Carry-forward

Closes `CF-FNA-SIZE-CM-NULL` after Step 3 completes successfully and the validation gates pass.

## Out of scope

- Do NOT modify `imaging_fna_linkage_v3` or any earlier-version artifacts.
- Do NOT touch the M025 nodule cohort or semantic model — that smoke test is Cowork's lane after this lands.
- Do NOT add new EXTRACT_ANSWER prompts — the four-field schema is locked.

## Signoff SQL (script handles, included for reference)

```sql
-- Inserted automatically by scripts/mig_310_fna_size_mirror.py --signoff
-- by_actor = 'cursor_composer_mig310'
-- summary = "mig_310 v2 MD-side: imaging_fna_linkage_v4 created. Corpus + linkage built (HP-note keyword corpus). Rollup rows: <N>. v4 size_fill=<X>% lat_fill=<Y>%. Closes CF-FNA-SIZE-CM-NULL."
```

## When done, ping Cowork

Send a one-line message: `mig_310 v2 Phase G complete; imaging_fna_linkage_v4 size_fill=XX% rollup_n=YY; Cortex cost ~$Z`. Cowork will run the M025 nodule semantic-model smoke test and decide whether the M025 nodule analytic master needs a rebuild to incorporate the new size covariate.
