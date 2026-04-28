# Cursor Agent Prompt — `canonical_path_gland_events_v1` Repair

**Created:** 2026-04-28 **For:** Cursor / VSC agent execution **Database:** `thyroid_canonical_publication_v1_0` (MotherDuck `.eras` account) **Scope:** Repair 4 data-quality issues found during verification probe; re-verify; flip registry.

## Context

`canonical_path_gland_events_v1` (20 cols / 28,724 rows / 8,927 distinct specimens / 8,904 distinct surgery_episode_id) was probed for verification under Protocol v2. Four data-quality issues block verification:

IssueColsImpact`synoptic_row_ix` is **100% NULL**synoptic_row_ixPrimary linkage key per `reference_synoptic_row_ix.md` (Script 108 pandas-load-order anchor) is missing — no traceability back to `path_synoptics`.`gland_width_cm` is **100% NULL**gland_width_cmNever populated; needs to be sourced from `path_synoptics` (or marked structurally `na`).`gland_depth_cm` is **100% NULL**gland_depth_cmSame as width.`surgery_episode_id` is **21.5% NULL** (6,172 rows)surgery_episode_idLinkage to `canonical_operative_events_v1` broken for \~21% of rows.`gland_position` for parathyroid uses meaningless 1-6 ordinalgland_positionSource `parag_1-6_location` is free-text; needs parsing to a 7-value canonical taxonomy.

The current `build_script` column contains the value `'396'` for all 28,724 rows — but `scripts/396_specimen_master_repair.py` is a *specimen-master* repair, not a path_gland builder. The original path_gland builder is unknown; investigate.

## Logan-ratified parathyroid position taxonomy (7 values)

For rows with `gland_type = 'parathyroid'`, normalize the source text in `path_synoptics.parag_<N>_location` to one of:

1. `right_superior` — "right superior", "right upper", "right supeior" (typo), "right upper biopsy/excision/remainder", "RIGHT UPPER", "Right superior paratracheal soft tissue"
2. `right_inferior` — "right inferior", "right lower", "right infeiror" (typo), "right inferior paratracheal", "RIGHT LOWER PARATHYROID"
3. `left_superior` — "left superior", "left upper", "left suprerior" (typo), "left supeiror" (typo), "LEFT UPPER PARATHYROID"
4. `left_inferior` — "left inferior", "left lower", "left infeiror" (typo), "left inferior paratracheal"
5. `intrathyroidal_right` — "right intrathyroidal", "intrathyroidal right", "right lobectomy intrathyroidal", "right lobe intrathyroidal", "right intrathyroidal nodule/parathyroid"
6. `intrathyroidal_left` — "left intrathyroidal", "intrathyroidal left", "left lobectomy intrathyroidal", "left lobe intrathyroidal"
7. `extrathyroidal_other` — paratracheal mass, mediastinal, central compartment, level 6 lymph node, thymus, retropharyngeal, ectopic carotid sheath, "extrathyroidal" without specific anatomic side, "unspecified", whole-thyroidectomy specimen labels (when actually a parathyroid was found in/with a thyroid resection specimen)

Build a normalization dict + regex helper that handles all observed variants. **Print full mapping audit** (every distinct source value → assigned target value) to a report file so Logan can review.

NULL out the assignment if source is empty or not parseable; do **not** force assignment.

For `gland_type = 'thyroid_lobe'`, the existing values (left, right, isthmus, pyramidal, substernal, total) are already clean. Don't touch.

## Step-by-step plan

### Step 1 — Investigation (read-only)

1. Find the original path_gland builder script: `grep -rn "canonical_path_gland_events_v1\|INSERT.*path_gland\|CREATE.*path_gland" scripts/ | head -20`. Most likely it's a Script 200-300 era builder that pre-dates Script 396.

2. Read `scripts/108_*.py` (any script number 108) for the canonical synoptic_row_ix definition. Per memory `reference_synoptic_row_ix.md`: synoptic_row_ix is a Script 108 pandas-load-order global index assigned at synoptic ingestion. It is **NOT SQL-reproducible**; it must be inherited from `path_synoptics.synoptic_row_ix` (or whichever upstream table holds it natively).

3. Confirm `path_synoptics.synoptic_row_ix` exists and is populated. If yes, the path_gland repair is a JOIN-and-backfill. If `path_synoptics` doesn't have it either, we'll need `synoptic_tumor_long_v1.synoptic_row_ix` or another upstream that does.

4. Probe `path_synoptics` for width/depth source fields:

   ```sql
   SELECT column_name FROM information_schema.columns
   WHERE table_schema='main' AND table_name='path_synoptics'
     AND (column_name ILIKE '%width%' OR column_name ILIKE '%depth%' OR column_name ILIKE '%dimension%' OR column_name ILIKE '%size%');
   ```

   Identify which source columns map to thyroid_lobe vs parathyroid width/depth. Some specimens may have only length recorded (e.g. parathyroid weighed not measured).

5. Probe the 21.5% missing `surgery_episode_id` — what's distinctive about those 6,172 rows? Are they unlinked specimens (`linkage_quality='unlinked'` or `'synoptic_only'`)? Cross-check with `canonical_operative_events_v1`.

### Step 2 — Write `scripts/N_path_gland_repair.py`

Use the next available script number (likely 397+). Pattern after `mig_98*` apply scripts in `qc_framework_v1/scripts/` (use `motherduck_client.MotherDuckClient.connect_rw()` + `.eras` SSO, snapshot before mutate, dry-run + apply modes, provenance row, etc.).

Phases:

1. **Snapshot** `canonical_path_gland_events_v1` to `archive_pub_v1_0.canonical_path_gland_events_v1_pre_repair_<ts>` (28,724 rows).

2. **Backfill** `synoptic_row_ix` via JOIN to `path_synoptics`. Use the most-likely shared key (`(research_id, path_date, specimen_id)` or whichever Script-108 pattern Script 108 uses). Verify post-update: target ≥99% non-NULL.

   ```sql
   UPDATE main.canonical_path_gland_events_v1 AS g
   SET synoptic_row_ix = ps.synoptic_row_ix,
       build_ts = CAST(CURRENT_TIMESTAMP AS TIMESTAMP)
   FROM main.path_synoptics ps
   WHERE g.research_id = ps.research_id
     AND g.path_date = ps.path_date  -- or whichever the actual key is
     -- additional disambiguation if needed
   ;
   ```

   Per memory `feedback_alter_view_dependents.md`: if any VIEW depends on path_gland, recompile after.

3. **Populate** `gland_width_cm` **and** `gland_depth_cm` from `path_synoptics` source fields. If source has them per parathyroid gland (e.g. `parag_<N>_width`, `parag_<N>_depth`), JOIN and update. If source doesn't have them at all, mark them `na` in the column registry instead of repair.

4. **Repair** `surgery_episode_id` **linkage** for the 6,172 NULL rows by re-running the operative-events linkage logic. Check whether these are `linkage_quality='unlinked'` (no operation found) — those are legitimately NULL and don't need repair (but should be documented).

5. **Parse** `parag_<N>_location` **to 7-value canonical taxonomy** for parathyroid rows:

   - Build a normalization dict from observed source values (\~700 distinct values across parag_1-6_location). Print full audit to `scripts/output/path_gland_position_audit.csv`.
   - For each parathyroid path_gland_events row, look up the corresponding `parag_<N>_location` in `path_synoptics` (where N is the existing 1-6 ordinal, since that's the only mapping back) and apply normalization.
   - Update `gland_position` in canonical with normalized 7-value enum.
   - Document untranslatable values (probably \~30% — specimen labels mixed in with anatomic positions): leave as `unspecified` or NULL with a note.
   - Verify post-update: gland_position distribution for parathyroid rows shows ≤7 distinct + NULL.

6. **Range outlier review** — `gland_length_cm = 85.6` and `gland_weight_g = 1207` are extreme outliers. Probe the source rows and either confirm (large goiters / specimen weight mismeasurement / cm vs mm error) or sentinel. Document in the repair report.

### Step 3 — Re-verify post-repair

Re-run the verification probe queries:

- All 14 `not_started` cols should now be populatable
- NULL rates should be &lt;5% on critical cols (or documented as `na` with rationale)
- Range outliers reviewed
- `synoptic_row_ix` JOIN-able to `path_synoptics`

### Step 4 — Flip column registry

Update `main.canonical_column_verification_registry_v1` for `canonical_path_gland_events_v1`:

- Cols verified by repair → `verification_status='verified'`
- Cols structurally `na` (e.g. width/depth if source doesn't carry them) → `verification_status='na'`, with `verification_method='structurally_na_no_source_field'`
- `verified_by='logan_glosser_via_path_gland_repair'`
- `batch_id='path_gland_repair_20260428'`

Update `main.canonical_table_signoff_registry_v1`:

- `table_status='verified'`
- `signoff_migration='path_gland_repair_20260428'`

### Step 5 — Commit + push

Explicit-path `git add` (memory: `feedback_surgical_git_add.md`):

```
git add scripts/<N>_path_gland_repair.py
git add scripts/output/path_gland_position_audit.csv
git add qc_framework_v1/migrations/path_gland_repair_<date>.md
```

Commit message format follows mig_98 precedent (see commits `a4f0cf0`, `cb5e200`, `22d3fd1`, `f376ca9`, `01247c6`, `cbccd4a`).

## Standing protocol reminders

- Lint Python with `python3 -m py_compile` before commit (memory: `feedback_commit_workflow.md`)
- Author: `Logan Glosser <logan.glosser@gmail.com>`
- MotherDuck account: `.eras` (per `reference_protocol_v2_md_accounts.md`)
- DuckDB CURRENT_TIMESTAMP returns TIMESTAMPTZ — always `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` for build_ts (memory: `reference_duckdb_timestamp_tz.md`)
- Dry-run before apply; verify post-state assertions
- PHI rule: never print clinical text outside review .xlsx files (memory: `feedback_phi_safety.md`)

## Reference

- Memory: `reference_synoptic_row_ix.md` — Script 108 pandas-load-order; never synthesize via ROW_NUMBER (inherit OK)
- Memory: `project_complications_events_verified_2026-04-28.md` — pattern reference for the 8-sub-mig close-out we just finished
- Recent commits: `cbccd4a` (mig_98g+h close-out), `01247c6` (mig_98f hypoparathyroidism), etc.
- Continuation prompt: top of session — current state was 17 tables verified / 432 cols verified / 0 failed CFs

## Output expected

- `scripts/<N>_path_gland_repair.py` — repair migration (lint-clean, dry-run + apply modes)
- `scripts/output/path_gland_position_audit.csv` — full mapping of source `parag_*_location` text → canonical taxonomy
- `scripts/output/path_gland_repair_report.md` — pre-state, repair phases, post-state, registry-flip status
- `qc_framework_v1/migrations/path_gland_repair_<date>.md` — manifest
- Commit + push to `origin/main`
- Final state report comparing pre/post NULL rates per column

## Acceptance criteria

- \[ \] `synoptic_row_ix` ≥99% non-NULL (via `path_synoptics` JOIN)
- \[ \] `gland_position` for parathyroid rows uses only 7 canonical values + NULL
- \[ \] `gland_width_cm` / `gland_depth_cm` either populated OR registry-flipped to `na` with rationale
- \[ \] `surgery_episode_id` NULL rate documented (real unlinked vs repairable)
- \[ \] `canonical_path_gland_events_v1` has `table_status='verified'` in signoff registry
- \[ \] All 14 not_started cols flipped to `verified` or `na`
- \[ \] Range outliers reviewed
- \[ \] Commit + push origin/main; provenance row written
