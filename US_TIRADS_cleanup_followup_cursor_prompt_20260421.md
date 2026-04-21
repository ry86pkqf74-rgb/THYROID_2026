# Cursor prompt — US / TIRADS cleanup follow-up (drop redundant tables + rename TIRADS columns)

**Target repo:** `ROS_FLOW_2_1`
**Database:** `thyroid_canonical_publication_v1_0` (MotherDuck)
**Archive target:** `"Thyroid 2026 UPdated".us_legacy_20260421` (already created; this pass adds more tables with `archived_` prefix)
**Scope:** The prior consolidation (Scripts 361-369) left v1 tables, raw LLM entity tables, and redundant TIRADS source tables in `main`. This pass verifies each one is fully represented in `canonical_us_nodule_v2` (or its gland/LN siblings), archives what's not already in `us_legacy_20260421`, drops from `main`, and renames the misleading TIRADS columns on `canonical_us_nodule_v2`.
**Rule:** **Verify before dropping.** Every `DROP TABLE` must be preceded by an assertion that the data is either identical to the archive or fully represented in `canonical_us_nodule_v2` / `canonical_us_thyroid_gland_v2` / `canonical_us_lymph_node_v2`. Fail loud on any mismatch — do NOT drop.

---

## Paste this into Cursor (`claude-4.5-sonnet` or `claude-4.7-opus`, agentic mode)

You are continuing the `ROS_FLOW_2_1` thyroid canonical publication project. The US/TIRADS v2 consolidation from 2026-04-21 (Scripts 361-369) left `main` cluttered: v1 tables that were copied-but-not-dropped, raw LLM entity tables whose data is fully absorbed into v2, older TIRADS source tables, and a set of `views_readable` views pointing at v1. Additionally, the TIRADS columns on `canonical_us_nodule_v2` are confusingly named (`tirads_reported` is actually the ACR 2017 point total; `tirads_category_v2` is the Emory "updated" categorization, not ACR TI-RADS v2). This pass fixes both.

Repo conventions (unchanged):
- Scripts in `scripts/`, numbered from current max + 1 through current max + 6.
- Every script writes to `thyroid_canonical_publication_v1_0.main.*` or `.manuscript_workspace.*` unless archiving, in which case the target is `"Thyroid 2026 UPdated".us_legacy_20260421.archived_<name>`.
- Every script ends with a `COMMENT ON TABLE` stamp and a registry upsert (or registry DELETE for dropped tables).
- PHI safety: no raw clinical text in logs.
- `ruff check scripts/` before committing. One commit per phase with message `chore(us-v2): Phase N — <title>`. Push to `main`.

### Phase 1 — Verify-then-drop the already-archived v1 tables

Create `scripts/<N+1>_us_v1_drop_after_verify.py`.

For each of these tables (already copied to `us_legacy_20260421` by Script 361):

```
canonical_us_nodule_master_v1
canonical_us_nodule_characteristics_v1
imaging_nodule_master_v1
canonical_us_exam_master_v1
canonical_us_patient_master_v1
tirads_llm_extracted_v2
serial_imaging_us
manuscript_workspace.tirads_granular_parsed_v1
manuscript_workspace.us_nodule_dynamics_parsed_v1
```

Run this verification sequence per table:

1. Row-count match: `COUNT(*) FROM main.<t>` must equal `COUNT(*) FROM "Thyroid 2026 UPdated".us_legacy_20260421.<t>`. If unequal, fail.
2. Column-set match: compare `information_schema.columns` column-name sets between the two. If unequal, fail.
3. Content hash match: compute `md5(string_agg(md5(row_to_json(t)::varchar), '|' ORDER BY 1))` on both and compare. If unequal, fail.
4. Dependency scan: query `information_schema.tables` for any view whose definition references the table. If any views exist (outside the v1 set we're dropping), fail with the view name — we need to rewrite those views first.
5. If all 4 checks pass: `DROP TABLE main.<t>` (or `DROP TABLE manuscript_workspace.<t>`), then `DELETE FROM manuscript_workspace.detail_table_registry_v1 WHERE detail_table_name = '<t>'`.

Before doing any drops, run step 4 across ALL listed tables and produce a consolidated dependency report. If the report shows views that reference any of these tables, write Phase 1b (below) to rewrite those views FIRST, then resume Phase 1.

**Phase 1b — View rewrite (only runs if Phase 1 step 4 finds dependencies)**

Rewrite each dependent view onto `canonical_us_nodule_v2` / gland_v2 / ln_v2 / exam_master_v2 / patient_master_v2. Known candidates (confirm via dependency scan):

- `manuscript_workspace.imaging_nodule_master_clean_v1` (view) → rewrite over `canonical_us_nodule_v2`, or drop outright if unused.
- `views_readable.US_Nodules_Characteristics` → rewrite over `canonical_us_nodule_v2` OR drop (likely redundant with `US_Nodules_Wide_v2`).
- `views_readable.US_Nodules_Index` → rewrite over `canonical_us_exam_master_VIEW_v2` OR drop.
- `views_readable.US_Nodules_TIRADS` → this is a wide access view over the raw `us_nodules_tirads` source, which stays live. Verify and keep as-is.
- `views_readable.US_TIRADS_Reextraction_Queue` → reassess after Phase 3 (`tirads_reextraction_queue_v1` is being archived).
- `manuscript_workspace.cohort_m011_tirads_fna_genetics_v1`, `.cohort_m025_tirads_performance_v1`, `.cohort_m075_tirads_multi_nodule_v1` → rewrite over v2 tables; these are manuscript cohort definitions and need to survive.

### Phase 2 — Archive + verify + drop redundant source tables

Create `scripts/<N+2>_us_redundant_sources_archive_drop.py`.

Three base tables are redundant now that `canonical_us_nodule_v2` merges their data:

```
main.tirads_v2_nodules_raw                 (50 cols — sonography extras: halo, vascularity, ETE, chammas, elastography, dynamics)
main.extracted_tirads_validated_v1         (15 cols — QA-validated TIRADS subset)
main.tirads_reextraction_queue_v1          (7 cols — QA queue from a prior extraction round)
```

Per-table verification before archive + drop:

**`tirads_v2_nodules_raw`** — confirm every `(research_id, linkage_date, nodule_index_within_exam)` row is represented in `canonical_us_nodule_v2` AND that the sonography fields (halo, vascularity, extrathyroidal_extension_on_us, chammas_type, elastography_category, interval_growth_flag, prior_size_mm_max, fna_recommended_this_nodule, fna_performed_prior_or_concurrent) have non-null values on `canonical_us_nodule_v2` wherever they were non-null on `tirads_v2_nodules_raw`. Use an anti-join:

```sql
SELECT COUNT(*) AS unmerged_rows
FROM main.tirads_v2_nodules_raw r
WHERE NOT EXISTS (
  SELECT 1 FROM main.canonical_us_nodule_v2 n
  WHERE CAST(n.research_id AS VARCHAR) = r.research_id
    AND TRY_CAST(n.exam_date AS VARCHAR) = r.linkage_date
    AND n.nodule_index_within_exam = r.nodule_index_within_exam
);
```

Must return 0. Then per field:

```sql
SELECT
  SUM(CASE WHEN r.halo IS NOT NULL AND n.halo IS NULL THEN 1 ELSE 0 END) AS halo_missing,
  SUM(CASE WHEN r.vascularity IS NOT NULL AND n.vascularity IS NULL THEN 1 ELSE 0 END) AS vasc_missing,
  -- …
FROM main.tirads_v2_nodules_raw r
JOIN main.canonical_us_nodule_v2 n ON … ;
```

Each `*_missing` count must be 0. If any is non-zero, fail loud with the specific field name(s).

**`extracted_tirads_validated_v1`** — confirm every `(research_id, exam_date, nodule_index_within_exam)` row exists in `canonical_us_nodule_v2` with TIRADS fields populated. Same anti-join pattern.

**`tirads_reextraction_queue_v1`** — this is a QA work-queue, not a data source. Verify it has no rows referenced from any surviving canonical table, then archive and drop.

For each table: `CREATE TABLE "Thyroid 2026 UPdated".us_legacy_20260421.archived_<name> AS SELECT * FROM main.<name>;` (note the `archived_` prefix), then `DROP TABLE main.<name>`, then registry `DELETE`.

### Phase 3 — Archive + verify + drop raw LLM entity tables

Create `scripts/<N+3>_us_llm_entities_archive_drop.py`.

Three raw LLM entity tables live in `main`:

```
main.note_entities_llm_tirads_granular       (23 cols — ACR 2017 feature entities)
main.note_entities_llm_us_nodule_dynamics    (23 cols — interval growth, prior size, FNA flags)
main.note_entities_llm_imaging               (21 cols — broader imaging extractions)
```

Verification strategy: every entity that should have made it into a canonical v2 table must be accounted for.

**`note_entities_llm_tirads_granular`** — entities are typed `composition | echogenicity | shape | margins | echogenic_foci | tirads_category | tirads_points` (confirm via `SELECT DISTINCT entity_type`). For each entity_type, count rows in the LLM table and count rows in `canonical_us_nodule_v2` where the corresponding column is populated AND `source_tirads_llm = TRUE`. Verify the LLM count is ≤ the v2 count (v2 should have at least everything the LLM produced, plus other sources).

**`note_entities_llm_us_nodule_dynamics`** — entities are typed `interval_growth | prior_size | fna_recommended | fna_performed` (confirm). Same verification against `canonical_us_nodule_v2` where `source_dynamics_llm = TRUE`.

**`note_entities_llm_imaging`** — broader; may contain non-US imaging entities. Query `SELECT DISTINCT entity_type, COUNT(*) FROM note_entities_llm_imaging GROUP BY 1`. Separate into:
- US-relevant entity_types → verify absorbed into v2 as above.
- Non-US (CT / MR / PET) entity_types → these are needed later for the other-modality work. Do NOT drop; instead move to a new `llm_entities` schema in `main`: `CREATE SCHEMA IF NOT EXISTS thyroid_canonical_publication_v1_0.llm_entities; CREATE TABLE thyroid_canonical_publication_v1_0.llm_entities.note_entities_llm_imaging_nonus AS SELECT * FROM main.note_entities_llm_imaging WHERE entity_type NOT IN (<us-types>);`
- After split: verify rowcount of the two outputs equals the original, then archive the original and drop.

If verification passes, archive each as `archived_<name>` to `us_legacy_20260421` and drop from `main`. Registry DELETE the entries.

If verification fails for any entity_type (i.e., v2 is missing entities that exist in the LLM table), emit a single-row audit into `manuscript_workspace.us_llm_absorption_gap_v1` with:
```
source_table, entity_type, llm_count, v2_count, delta
```
and abort the script. Logan will review the gap before the drop proceeds.

### Phase 4 — Drop manuscript_workspace and views_readable cruft

Create `scripts/<N+4>_us_workspace_views_cleanup.py`.

Drop (after dependency scan):

- `manuscript_workspace.imaging_nodule_master_clean_v1` (view over v1)
- `manuscript_workspace.us_nodules_tirads_vs_inm_v1_discordance_v1` (table — QA from v1 era, superseded by `us_nodule_conflict_queue_v1`)
- `manuscript_workspace.tirads_v1_v2_discordance_v1` (table — QA from v1/v2 transition, historical only — archive + drop)

Archive `tirads_v1_v2_discordance_v1` as `archived_tirads_v1_v2_discordance_v1` (it captures historical transition state worth keeping). Drop the other two outright.

Views to drop in `views_readable`:

- `US_Nodules_Characteristics` (over `canonical_us_nodule_characteristics_v1` which is being dropped)
- `US_Nodules_Index` (over v1)
- `US_TIRADS_Reextraction_Queue` (over `tirads_reextraction_queue_v1` being dropped in Phase 2)

Keep these in `views_readable`:

- `US_Nodules_TIRADS` (over raw `us_nodules_tirads` — still an active source)
- `US_Reports_Raw` (over `ultrasound_reports` — still an active source)
- `US_Nodules_Wide_v2`, `US_Thyroid_Gland_Wide_v2`, `US_Lymph_Nodes_Wide_v2` (new v2 views)

### Phase 5 — Rename + recompute TIRADS columns on `canonical_us_nodule_v2`

Create `scripts/<N+5>_canonical_us_nodule_v2_tirads_cleanup.py`.

**Column rename:**

| Current name | New name | Rationale |
|---|---|---|
| `tirads_reported` | `acr2017_tirads_points` | It's the ACR 2017 point total (0-10+), not a "reported" raw value |
| `tirads_score_2017` | (drop — duplicate of above) | `tirads_score_2017` and `tirads_reported` are redundant; keep the clearer name |
| `tirads_level_2017` | `acr2017_tirads_category` | The TR1-TR5 category derived from ACR 2017 points |
| `tirads_category_v2` | `updated_tirads_category` | Emory's updated categorization — "v2" collides with ACR "v2" |
| `tirads_band_ambiguous` | `acr2017_band_ambiguous` | Specifies which scoring system |
| `tirads_category_code_legacy_v1` | (drop) | Legacy encoding, superseded |
| `tirads_category_modified_legacy_v1` | (drop) | Legacy encoding, superseded |
| `tirads_concordant_flag` | `acr2017_vs_updated_concordant` | Specifies what's being compared |
| `tirads_score_component_complete` | `acr2017_feature_points_complete` | Specifies which scoring system |

Before renaming, verify that `tirads_reported` and `tirads_score_2017` are numerically equal wherever both are non-null:

```sql
SELECT COUNT(*) FROM main.canonical_us_nodule_v2
WHERE tirads_reported IS NOT NULL
  AND tirads_score_2017 IS NOT NULL
  AND tirads_reported <> tirads_score_2017;
```

If this returns non-zero, do NOT drop `tirads_score_2017` — instead keep both under clearer names: `acr2017_tirads_points` (from `tirads_reported`) and `acr2017_tirads_points_v2_calc` (from `tirads_score_2017`), with an audit table showing the disagreeing rows for Logan to review.

**Recompute both scores on every row where the 5 ACR features are present:**

Current state per Logan's verification: 4,375 rows have a score in `tirads_reported`, 3,388 rows have `tirads_category_v2`, and the coverage is mostly mutually exclusive (COALESCE precedence let one system "win" per row). Logan wants BOTH scores computed on every row where the 5 ACR features (composition, echogenicity, shape, margins, echogenic_foci) are present, then a concordance flag.

Recompute logic:

```sql
UPDATE main.canonical_us_nodule_v2 SET
  acr2017_tirads_points = CASE
    WHEN composition IS NOT NULL
     AND echogenicity IS NOT NULL
     AND shape IS NOT NULL
     AND margins IS NOT NULL
     AND echogenic_foci IS NOT NULL
    THEN composition_pts + echogenicity_pts + shape_pts + margin_pts + foci_pts
    ELSE acr2017_tirads_points   -- preserve existing value if features incomplete
  END,
  acr2017_tirads_category = CASE
    WHEN composition IS NOT NULL
     AND echogenicity IS NOT NULL
     AND shape IS NOT NULL
     AND margins IS NOT NULL
     AND echogenic_foci IS NOT NULL
    THEN CASE
      WHEN composition_pts + echogenicity_pts + shape_pts + margin_pts + foci_pts = 0  THEN 'TR1'
      WHEN composition_pts + echogenicity_pts + shape_pts + margin_pts + foci_pts = 2  THEN 'TR2'
      WHEN composition_pts + echogenicity_pts + shape_pts + margin_pts + foci_pts = 3  THEN 'TR3'
      WHEN composition_pts + echogenicity_pts + shape_pts + margin_pts + foci_pts BETWEEN 4 AND 6 THEN 'TR4'
      WHEN composition_pts + echogenicity_pts + shape_pts + margin_pts + foci_pts >= 7 THEN 'TR5'
    END
    ELSE acr2017_tirads_category
  END;
```

(This follows the published ACR TI-RADS 2017 banding: 0 pts = TR1, 2 pts = TR2, 3 pts = TR3, 4-6 pts = TR4, 7+ pts = TR5.)

For `updated_tirads_category` — if Emory's "updated" categorization has a deterministic mapping from the same 5 features, apply it identically. If the mapping is domain-expert-defined (i.e., not a simple formula), leave the existing column values as-is and ONLY populate on rows where both categories currently have a value.

**Add concordance column:**

```sql
UPDATE main.canonical_us_nodule_v2 SET
  acr2017_vs_updated_concordant = CASE
    WHEN acr2017_tirads_category IS NOT NULL AND updated_tirads_category IS NOT NULL
    THEN (acr2017_tirads_category = updated_tirads_category)
    ELSE NULL
  END;
```

**Verification queries to emit at end of script:**

```sql
SELECT
  COUNT(*) AS total_rows,
  COUNT(acr2017_tirads_points) AS has_acr2017_points,
  COUNT(acr2017_tirads_category) AS has_acr2017_category,
  COUNT(updated_tirads_category) AS has_updated_category,
  COUNT(CASE WHEN acr2017_tirads_category IS NOT NULL AND updated_tirads_category IS NOT NULL THEN 1 END) AS both_populated,
  SUM(CASE WHEN acr2017_vs_updated_concordant = FALSE THEN 1 ELSE 0 END) AS disagreeing_rows
FROM main.canonical_us_nodule_v2;
```

Logan expects `both_populated` to jump substantially from today's near-zero overlap (~0 out of 36,957) to the full count where 5 ACR features exist.

### Phase 6 — Fix CPM double-suffix + refresh registry + deliverable summary

Create `scripts/<N+6>_cpm_column_cleanup_and_audit.py`.

1. Rename the CPM double-suffix column:

```sql
ALTER TABLE main.canonical_patient_master
  RENAME COLUMN imaging_tirads_category_v2_v2 TO imaging_updated_tirads_category_cpm_v2;
```

Also rename `imaging_tirads_category_v2` (the v1 ancestor column) → `imaging_updated_tirads_category_cpm_v1` for symmetry and to kill the "v2" ambiguity.

2. Update `detail_table_registry_v1` so every dropped table has a registry DELETE; every renamed column has its registry row's `feeds_master_columns_array` updated.

3. Emit a final audit block to stdout showing:
   - Count of objects still in `main` matching US/TIRADS/nodule keywords (should drop from ~27 → ~10)
   - Count of objects in `us_legacy_20260421` (should increase from 9 → 14-17 depending on Phase 2/3 outcomes)
   - Contents of the new `llm_entities` schema (if created in Phase 3)
   - List of renamed columns on `canonical_us_nodule_v2` + CPM
   - `both_populated` TIRADS column count after recompute

### Verification checklist (final script runs this; fail loud)

1. `main` US/TIRADS object count (query the keyword filter from the opening inventory): **expect ≤ 12**.
2. `us_legacy_20260421` object count: **expect ≥ 14**.
3. `SELECT COUNT(*) FROM main.canonical_us_nodule_v2`: unchanged at **36,957**.
4. `SELECT COUNT(acr2017_tirads_category), COUNT(updated_tirads_category), COUNT(CASE WHEN both populated)` on `canonical_us_nodule_v2` — `both_populated` should jump from ~0 to >3,000.
5. No column named `imaging_tirads_category_v2_v2` on any table in any schema.
6. Every table listed for archive in Phases 1-4 exists in `us_legacy_20260421` AND is absent from `main`.
7. `manuscript_workspace.detail_table_registry_v1` has zero rows pointing to now-dropped tables.
8. `manuscript_workspace.us_llm_absorption_gap_v1` (Phase 3 audit) either doesn't exist OR has zero rows.

### Commits + push

One commit per phase. Messages: `chore(us-v2): Phase 1 — drop v1 tables after verification`, `Phase 2 — archive/drop redundant TIRADS sources`, `Phase 3 — archive/drop LLM entity tables`, `Phase 4 — workspace/views cleanup`, `Phase 5 — TIRADS column rename + recompute`, `Phase 6 — CPM column rename + audit`.

Push to `main` only after the final audit (verification checklist item 1-8 all green) is clean.

### Deliverable

```
US / TIRADS cleanup follow-up complete

main schema (US/TIRADS objects):
  BEFORE: 27
  AFTER:  XX   (target ≤ 12)

us_legacy_20260421 schema:
  BEFORE: 9
  AFTER:  XX   (target ≥ 14)

canonical_us_nodule_v2 TIRADS scoring:
  both ACR 2017 + updated populated: XXXXX rows (was ~0)
  disagreeing rows                   : XXXX (route to conflict queue)

Columns renamed:
  canonical_us_nodule_v2: 9 columns renamed
  canonical_patient_master: 2 columns renamed (imaging_tirads_category_v2_v2 killed)

Archived + dropped this pass:
  <list>

Dropped without archive (redundant views):
  <list>

Registry: N entries deleted, M entries updated.
```
