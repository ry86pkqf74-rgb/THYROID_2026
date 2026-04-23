# qc_framework_v1

A small, self-contained QC layer for the canonical `gold_master_patient_facts_v1`
table — built in response to the 2026-04-22 diagnostic that kept turning up
issues on every large query (LN numerator > denominator, recurrence-before-
surgery, divergent surgery-date columns, un-normalized histology, etc.).

The thesis: those were never random bugs. The database lacks a **systematic
QC layer**, so every large query is the first time that specific rule has been
enforced. This framework turns "surprises" into a triage queue and produces
a `manuscript_cohort_v2` view with known-bad rows excluded by construction.

## Files

| File | What it does |
|------|--------------|
| `01_backup_motherduck_to_local.py` | One-shot export of MotherDuck `Thyroid 2026` → local `.duckdb` file. Run this first (today, before the trial ends). |
| `02_qc_violations_schema.sql` | Creates `qc_rules_v1` (rule registry) and `qc_violations_v1` (flagged research_ids). |
| `03_qc_violations_populate.sql` | One `INSERT ... SELECT` per rule. Idempotent — truncates first. |
| `04_cohort_v2_views.sql` | Defines `manuscript_cohort_v2` = v1 minus all `critical` violations, plus a PTC-normalized variant for the ETE → recurrence paper. |

## Step 1 — back up MotherDuck (do this today)

```bash
cd ~/THYROID_2026
python3 qc_framework_v1/01_backup_motherduck_to_local.py
```

Produces `backups/thyroid_2026_full_backup_<timestamp>.duckdb`. Verifies a
handful of known tables match row counts on both sides; non-zero exit if any
differ. Reads your token from `motherduck.local.toml` (same resolution order
as `motherduck_client.py`).

After this runs you have a complete local copy — every table, every view,
every schema. You can query it with the `duckdb` CLI or Python. If MotherDuck
access lapses tomorrow, nothing is lost.

## Step 2 — build the QC layer

Against MotherDuck:

```bash
python3 -c "
import duckdb, pathlib
con = duckdb.connect('md:Thyroid 2026')
for f in ['02_qc_violations_schema.sql','03_qc_violations_populate.sql','04_cohort_v2_views.sql']:
    sql = pathlib.Path(f'qc_framework_v1/{f}').read_text()
    con.execute(sql)
"
```

Or against the local backup:

```bash
duckdb backups/thyroid_2026_full_backup_*.duckdb \
  -c ".read qc_framework_v1/02_qc_violations_schema.sql" \
  -c ".read qc_framework_v1/03_qc_violations_populate.sql" \
  -c ".read qc_framework_v1/04_cohort_v2_views.sql"
```

## Step 3 — triage

```sql
-- What's wrong, by category and severity?
SELECT * FROM main.qc_violations_summary_v1;

-- How many patients does v2 drop vs v1?
SELECT * FROM main.cohort_flow_v1_to_v2;

-- Which rule is responsible for each exclusion? (for Methods section)
SELECT * FROM main.cohort_exclusion_attribution_v1;

-- Drill into specific violators
SELECT research_id, details
FROM main.qc_violations_v1
WHERE rule_id = 'REC01_RECURRENCE_BEFORE_SURGERY';
```

Every critical violation should be chart-reviewed (or the upstream extraction
fixed). Once a rule is cleared, rerun `03_qc_violations_populate.sql` — it
truncates and rebuilds from current data, so the numbers always reflect the
latest state.

## Rule catalog

Critical (excluded from `manuscript_cohort_v2`):

- `LN01_POSITIVE_GT_EXAMINED` — `ln_positive_final > path_ln_examined_raw`
- `LN02_POSITIVE_WITHOUT_EXAMINED` — positive nodes > 0 but examined = 0
- `REC01_RECURRENCE_BEFORE_SURGERY` — `recurrence_date < first_surgery_date`
- `SURG01_DATE_DIVERGENCE` — `first_surgery_date != surg_first_date`

Warnings (kept in cohort, flagged for review):

- `LN03_ROLLUP_DISAGREES_WITH_RAW` — two LN sources of truth disagree
- `REC02_CONFIRMED_BUT_NO_DATE` — `recurrence_confirmed=TRUE` but date null
- `REC03_DATE_WITHOUT_CONFIRM` — date set without confirmation flag
- `REC04_DAYS_FROM_SURG_INCONSISTENT` — derived days drift from date diff
- `HIST01_WHITESPACE` — `histology_final` has untrimmed whitespace
- `HIST02_UNNORMALIZED_VARIANT` — PTC-like string not in canonical list
- `HIST03_METASTATIC_PREFIX` — `"metastatic ..."` strings that need splitting

Info (no action required, observational):

- `LN04_LN_DATA_MISSING` — all LN numerator/denominator columns null
- `SURG02_DATE_DUPLICATE_COLUMN` — two surgery-date columns identical (schema
  smell — safe candidate for `ALTER TABLE ... DROP COLUMN`)

## Known gaps (deliberately deferred)

These rules need schemas that aren't in `gold_master_patient_facts_v1` — once
you confirm column names on the source tables, they're one-liners each:

- **FNA-after-surgery** — needs `fna_date` column on the FNA episode/events
  table. Join `gold_master_patient_facts_v1` on `research_id`, flag where
  `fna_date > first_surgery_date`.
- **Imaging-after-surgery** — same pattern against `canonical_us_exam_master`
  (or whatever the current view is called).
- **LN event-level duplication** — count rows per `research_id` in
  `canonical_cervical_ln_clinical_events_v1`; flag outliers (e.g. > 10 or
  > p99).
- **Nodule-level linkage** — US-nodule ↔ FNA ↔ pathology. This is genuinely
  hard and a source-data problem, not a query problem. Address separately.

## Deprecation notes (2026-04-23 onward)

As of migration 09 (2026-04-23), every `main.*` object superseded by a
prompt-01-to-08 view is marked with `COMMENT ON TABLE` / `COMMENT ON COLUMN`
metadata in MotherDuck, and a single-table index lives at
`manuscript_workspace.canonical_deprecation_log_v1`. Query it to see what's
deprecated, what replaces it, and which prompt is gated to hard-drop it
(if any):

```sql
SELECT closing_prompt, deprecation_kind, deprecated_object, superseding_object
FROM manuscript_workspace.canonical_deprecation_log_v1
ORDER BY closing_prompt;
```

Current (prompts 01-08) entries:

| Prompt | Kind | Deprecated | Superseded by |
|--------|------|------------|---------------|
| 01 | pointer_only | `main.canonical_path_malignant_events_v1` | `manuscript_workspace.canonical_path_malignant_events_v1_keyed` |
| 05 | column_only | `main.canonical_molecular_genetics_v2.molecular_episode_id` | `manuscript_workspace.molecular_episode_uid_v1.molecular_episode_uid` |
| 06 | linkage_only | `main.specimen_genomic_assay_v1` | `manuscript_workspace.specimen_genomic_assay_v1_relinked` |
| 07 | full | `main.canonical_molecular_genetics_from_notes_v2` | `manuscript_workspace.molecular_mentions_from_notes_v2` |
| 08 | column_only | `main.manuscript_cohort_v1.histology_final` | `manuscript_workspace.manuscript_cohort_v1_histology_clean` |

`deprecation_kind` legend:

- `full` — whole object is deprecated; use the replacement.
- `linkage_only` — the table stays, but its FK-style columns are unreliable; bind via the replacement.
- `column_only` — the table stays, one column is broken; read from the replacement's clean column.
- `pointer_only` — not actually deprecated; a cleaner derivative is available for the common join pattern.

**Non-breaking by design.** No `main.*` objects were dropped or renamed.
Hard drops are gated to prompt 46 (manuscript_cohort_v2 assembly). The
`hard_drop_gate` column on the deprecation log records which prompt, if any,
is allowed to drop the deprecated object.

**`views_readable.Genetics_from_Notes_LLM`** was re-pointed in migration 09
to `manuscript_workspace.molecular_mentions_from_notes_v2` (row count
unchanged at 1,738). Other `views_readable.*` views left as-is — they read
columns that are not deprecated.

**What "deprecated" means for each kind in practice:**

- `main.canonical_molecular_genetics_from_notes_v2` (full) — an NLP mentions
  layer. It holds "a note mentioned this variant/fusion/gene" signals, not
  verified structured assay results. Never join as a peer of
  `main.canonical_molecular_genetics_v2`. Use
  `manuscript_workspace.molecular_mentions_from_notes_v2` instead (same
  data, name disambiguates intent).
- `main.canonical_molecular_genetics_v2.molecular_episode_id` (column_only)
  — 3 distinct values across 1,384 rows; useless as a key. Use
  `manuscript_workspace.molecular_episode_uid_v1.molecular_episode_uid`.
- `main.specimen_genomic_assay_v1` (linkage_only) — its FK-style columns are
  ~98% broken. The table is fine for audit/provenance; downstream joins
  should go through
  `manuscript_workspace.specimen_genomic_assay_v1_relinked` and bind on
  `research_id` with the two boolean presence flags.
- `main.manuscript_cohort_v1.histology_final` (column_only) — raw, dirty.
  Analysis should use the clean columns on
  `manuscript_workspace.manuscript_cohort_v1_histology_clean`.

## Design notes

- **Schema:** Everything is written to `main`. If you prefer `qa` for
  governance artifacts, just find-and-replace `main.qc_` → `qa.qc_` across
  the three SQL files. No other changes needed.
- **Grain:** Patient (`research_id`). Episode-level rules would want an
  optional `event_id` column on `qc_violations_v1`; keep it NULL for patient
  rules.
- **Idempotency:** `03_populate` truncates `qc_violations_v1` before
  repopulating, so you can run it any time without accumulating stale rows.
- **Extensibility:** To add a rule, `INSERT` one row into `qc_rules_v1` and
  add one `INSERT ... SELECT` block to `03_populate`.
