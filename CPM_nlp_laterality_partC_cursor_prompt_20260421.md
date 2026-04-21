# CPM NLP TIRADS + laterality cleanup — Part C

**Target DB:** `thyroid_canonical_publication_v1_0`
**Archive DB:** `"Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421`
**Date:** 2026-04-21
**Follow-on to:** CPM TIRADS Part B (complete, commit `4358484`)
**Architecture decision:** Option C-soft continues — canonical TIRADS/US truth on `canonical_us_*_v2` surface; CPM carries NO TIRADS-shaped or TIRADS-adjacent-NLP columns.

## Goal

Finish the job Part B started. Part B left behind 9 CPM columns that were deliberately scoped out:

- **5 `nlp_tirads_*` columns** (Tier-1 NLP flags, part of the 118 nlp_* domain-flag pattern on CPM).
- **4 "unrelated non-TIRADS laterality columns"** (names TBD at Phase 0; likely path/FNA/tumor laterality signals that happened to share the laterality keyword and got skipped in Part B's TIRADS-only regex).

Remove them all. Port whatever has live consumers to `cupm_v2` (patient-grain) or `canonical_us_nodule_v2` (nodule-grain) per Option C-soft. Drop the rest. Match Part B's pattern: views-first, scripts-second, CPM-last.

## Why these weren't in Part B

- `nlp_tirads_*` were part of the 118 Tier-1 NLP flags on CPM — a general pattern across domains, not TIRADS-specific. Part B's mapping explicitly excluded them on the grounds that they serve a different architectural role (domain-level NLP presence flags vs analytical TIRADS rollups).
- The 4 laterality cols were non-TIRADS: they describe pathology/FNA/tumor laterality, not imaging-TIRADS laterality. Part B's coverage regex didn't catch them.

Logan's directive 2026-04-21: both groups should be cleaned up under the same architectural principle — no domain-specific CPM cols when canonical equivalents (or no consumers) exist.

## Constraints (unchanged from Part B)

- Do NOT touch `canonical_us_patient_master_VIEW_v2` view body without a pre-C micro-script with its own review.
- Drop order: views → scripts → writer freeze → CPM columns.
- PHI: research_id + structured fields only. No `clinical_note_text`.
- Archive destination: `"Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421` (new schema, matches the Part B namespace convention).
- Git workflow: commit per phase, push at the end. Lint with `ruff check` before each commit.

---

## Phase 0 — Discovery (read-only, no writes)

Goal: enumerate the exact 9 columns (5 nlp_tirads + 4 laterality) and their full consumer graph.

### Phase 0a — Column inventory

```sql
-- The 5 nlp_tirads_* columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND column_name ILIKE 'nlp_tirads%'
ORDER BY column_name;

-- Candidate non-TIRADS laterality columns
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = 'main' AND table_name = 'canonical_patient_master'
  AND (column_name ILIKE '%laterality%'
       OR column_name ILIKE '%_left_%'
       OR column_name ILIKE '%_right_%'
       OR column_name ILIKE '%_bilateral%')
  AND column_name NOT IN (
    -- Exclude any already dropped by Part B (cross-check against archive)
    SELECT column_name FROM information_schema.columns
    WHERE table_name = 'canonical_patient_master_pre_partB'
      AND column_name ILIKE '%laterality%'
  )
ORDER BY column_name;
```

Write inventory to `scripts/output/_cpm_nlp_laterality_partC_inventory.{json,md}`. Include for each column:
- `column_name`, `data_type`, `populated_count`, `distinct_value_count`, `sample_values` (top 10 + NULL count)
- Column comment if present (pull from `information_schema.columns.column_comment` or equivalent).

**Expected shape:** exactly 5 `nlp_tirads_*` + a to-be-discovered number of laterality cols. If the laterality discovery returns ≠ 4, show Logan the list — he may have miscounted or there may be overlap with cols already dropped in Part B.

### Phase 0b — Writer/reader/view grep

Use the Part B pattern:

```bash
git grep -nE 'nlp_tirads_[a-z_]+|<each discovered laterality column name>' \
  scripts/ sql/ manuscripts/ studies/ lakehouse/ utils/ app/
```

Plus view-definition scan:

```sql
SELECT view_schema, view_name, view_definition
FROM information_schema.views
WHERE view_definition ILIKE '%nlp_tirads%'
   OR view_definition ILIKE '%<each laterality col>%';
```

Cross-check against `scripts/frozen/` — any frozen script that already references these columns needs no further action.

Write to `scripts/output/_cpm_nlp_laterality_partC_writers_readers.{json,md}` and `_view_readers.json`.

**STOP gate:** show Logan the full inventory + consumer graph before any adjudication. He will decide per-column whether to port-to-canonical, drop, or keep-as-is.

Commit message: `CPM NLP+laterality Part C / Phase 0: inventory (read-only)`

---

## Phase 1 — Coverage audit + adjudication

For each discovered column, build `manuscript_workspace.cpm_nlp_laterality_coverage_v1`:

```
column_name            | cpm_dtype | canonical_target               | canonical_dtype | coverage_status     | n_writers | n_readers | n_views | recommendation
-----------------------|-----------|--------------------------------|-----------------|---------------------|-----------|-----------|---------|---------------
nlp_tirads_mention     | BOOLEAN   | cupm_v2.has_any_us_with_...    | BOOLEAN         | mapped_or_redundant | 2         | 3         | 1       | port_or_drop
nlp_tirads_score_extr  | BOOLEAN   | cunc_v2.tirads_score           | DOUBLE          | mapped_drill_down   | 1         | 0         | 0       | drop
tumor_laterality_v271  | VARCHAR   | <path_events.laterality?>      | VARCHAR         | gap_path_domain     | 2         | 4         | 2       | port_to_path_events
...
```

`coverage_status` ∈:
- `mapped_cupm_v2` — patient-grain equivalent exists on `canonical_us_patient_master_VIEW_v2`
- `mapped_cunc_v2` — nodule-grain equivalent exists on `canonical_us_nodule_v2`
- `mapped_or_redundant` — the NLP flag is strictly weaker than existing v2 capability (e.g., `nlp_tirads_mention` ⊆ `has_any_us = TRUE AND tirads_v2_worst_category IS NOT NULL`)
- `gap_path_domain` — column belongs to the path domain (tumor_laterality, fna_laterality) — port to the path canonical (Script 361 output) if that's landed, else defer to Part D
- `gap_other_domain` — column belongs to a domain without a canonical yet (e.g., molecular) — defer with documented rationale
- `retire_no_readers` — zero readers, zero views; drop without replacement
- `gap_ABORT` — has consumers, no canonical equivalent on any v2 table, not retirable

**Recommendation column** is one of: `drop`, `port_to_cupm_v2`, `port_to_cunc_v2`, `port_to_path_events`, `port_to_<other_canonical>`, `defer_pending_domain_canonical`, `keep_until_readers_migrated`.

**STOP gate:** show Logan the coverage table. He adjudicates each row, possibly overriding the `recommendation`. The adjudicated table is the source of truth for Phases 2-5.

If any column lands on `gap_ABORT`, STOP and report — same rule as Part B.

If any column lands on `defer_pending_domain_canonical`, keep it on CPM and register it in `manuscript_workspace.cpm_deferred_cleanup_v1` with the blocking canonical named.

Commit message: `CPM NLP+laterality Part C / Phase 1: coverage audit + adjudication`

---

## Phase 2 — Pre-C micro-script (port-to-canonical, only if needed)

Only needed if Phase 1 produces any `port_to_*` rows.

For each canonical target:

- **`port_to_cupm_v2`** — same pattern as the pre-B micro-script. Extend `cupm_v2_canonical_backfill_v1` with new columns, update the `cupm_v2` view body to surface them. If an NLP flag is strictly redundant with an existing cupm_v2 column (e.g., `nlp_tirads_mention` ⊆ an existing `has_any_us` + `tirads_v2_worst_category IS NOT NULL` combination), DO NOT port — recommend `drop` with a consumer-side coercion note instead.
- **`port_to_cunc_v2`** — nodule-grain ports need Logan explicit approval because they change event-grain semantics. Default to NOT porting; ask before adding columns to `canonical_us_nodule_v2`.
- **`port_to_path_events`** — only if Script 361 (operative pathology consolidation) has shipped. If 361 isn't done yet, defer those columns via `defer_pending_domain_canonical` and keep them on CPM for now.

Match pre-B's verification pattern: before/after column count, row count invariant, cell-by-cell spot check on 50 RIDs per ported column.

**STOP gate:** show Logan the pre-C view/table diffs before committing.

Commit message: `CPM NLP+laterality Part C / Phase 2: pre-C canonical backfill (if needed)`

---

## Phase 3 — Cohort view migration (if any views read the 9 cols)

Same pattern as Part B Phase 2:

1. Snapshot each referencing view's definition to `"Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421.view_def_<view_name>`.
2. `CREATE OR REPLACE VIEW` with references rewritten to either JOIN the canonical target or drop the reference entirely (if the column's recommendation is `drop`).
3. Shape checks: row count, column count, 20-RID spot check on retained columns.

Report per view: `row_count_before`, `row_count_after`, `cols_dropped`, `cols_replaced`, `cols_added`.

If a view's underlying analysis becomes non-sensical without the column (e.g., a "laterality concordance" analysis built on the dropped-without-replacement col), STOP and ask Logan before silently removing the metric.

Commit message: `CPM NLP+laterality Part C / Phase 3: migrated <N> cohort views`

---

## Phase 4 — Script reader migration

Same mechanics as Part B Phase 3 (learned the hard way about Cat B aliasing).

For each reader script:

- **Cat A (metadata/regex/docstring/frozenset):** text edit. Low risk.
- **Cat B (local CTAS alias collision):** freeze. Aliasing is rare in this column set but re-check every file before editing.
- **Cat C (genuine SQL reader):** surgical per-file migration with JOIN to the canonical target. If the recommendation is `drop`, remove the reference and the downstream logic that depended on it.
- **Cat D (Python probe list in active manuscript script):** surgical edit of the list.

Per-file procedure:
1. Read the file with `Read` tool.
2. Classify the usage (Cat A/B/C/D).
3. Make the edit (or mark for freeze).
4. `ruff check <file>` after each Python edit.
5. Log to `scripts/output/_cpm_nlp_laterality_partC_reader_migrations.md`.

Avoid mechanical global substitution. Part B's Cat B discovery cost a revert — don't repeat it.

Commit message: `CPM NLP+laterality Part C / Phase 4: migrated <N> readers, <M> deferred to freeze`

---

## Phase 5 — Writer freeze

Enumerate writer scripts via the Phase 0b grep. For each:

- If it writes any column with a `drop` or `port_to_*` recommendation → freeze.
- If it writes only `keep_until_readers_migrated` columns → leave for now, schedule for next cleanup pass.

Freeze pattern matches Part B exactly:
- `git mv scripts/<file> scripts/frozen/<file>`
- Prepend FROZEN header block with reason, replacement path, and date.
- Append one-line entry to `scripts/frozen/README.md`.
- If a writer has a `port_to_*` recommendation on one of its outputs, include a `# NEW TARGET ON REFRESH: <canonical_table>` line in the header.

Commit message: `CPM NLP+laterality Part C / Phase 5: froze <N> writer scripts`

---

## Phase 6 — DROP columns from CPM (STOP gate before this)

**Before executing Phase 6, show Logan the full Phase 1 → Phase 5 summary:**

- Adjudicated coverage table (rows = 9 + any laterality discovery delta)
- View migration report (Phase 3)
- Reader migration log (Phase 4)
- Frozen script list delta (Phase 5)
- The exact list of columns to DROP, cross-checked against live CPM

**Explicit "go" from Logan required before any `ALTER TABLE DROP COLUMN`.**

On go:

```sql
-- 1. Pre-drop archive
CREATE TABLE "Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421.canonical_patient_master_pre_partC AS
SELECT * FROM main.canonical_patient_master;

-- 2. Row count + col count snapshot
SELECT COUNT(*) AS row_count,
       (SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema='main' AND table_name='canonical_patient_master') AS col_count
FROM main.canonical_patient_master;

-- 3. DROP each adjudicated column (one ALTER per column for error isolation)
ALTER TABLE main.canonical_patient_master DROP COLUMN nlp_tirads_...;
-- ... repeat for every column with recommendation IN ('drop', 'port_to_*')
-- SKIP columns with recommendation='defer_pending_domain_canonical' or 'keep_until_readers_migrated'

-- 4. Post-drop verify
SELECT COUNT(*) AS row_count_after FROM main.canonical_patient_master;
-- must equal pre-drop row_count.

SELECT column_name FROM information_schema.columns
WHERE table_schema='main' AND table_name='canonical_patient_master'
  AND (column_name ILIKE 'nlp_tirads%'
       OR column_name IN ('<the adjudicated laterality cols>'));
-- must return only the deferred columns (if any), else 0 rows.
```

Commit message: `CPM NLP+laterality Part C / Phase 6: archived CPM + dropped <N> columns`

---

## Phase 7 — QA

Write results to `qa/qa_script_cpm_nlp_laterality_partC.json`. All checks must pass:

1. CPM row count unchanged (10,871).
2. CPM col count delta matches adjudicated drop count exactly.
3. 0 `nlp_tirads_*` cols on live CPM (unless any were deferred — list deferrals explicitly).
4. 0 adjudicated laterality cols on live CPM (same deferral caveat).
5. All Phase 3 migrated views resolve and return same row count as pre-migration.
6. `git grep` for adjudicated column names returns empty across active scripts (frozen/ and archive paths excluded).
7. `manuscript_workspace.cpm_nlp_laterality_coverage_v1` has 0 `gap_ABORT` rows.
8. Archive integrity: `"Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421.canonical_patient_master_pre_partC` row count + col count match live pre-drop CPM.
9. Frozen scripts present in `scripts/frozen/` with FROZEN headers; README updated.
10. Canonical spot check: for each `port_to_*` column, 50 random RIDs — canonical value matches CPM pre-drop archive value exactly (after type coercion where relevant).

Commit message: `CPM NLP+laterality Part C / Phase 7: QA bundle (10/10 pass)`

---

## Phase 8 — Commit summary + push

Single delivery commit on `origin/main` with the full Part C summary:

```
CPM NLP+laterality Part C — drop <N> additional CPM columns (2026-04-21)

Cleans up the 5 nlp_tirads_* flags + <M> non-TIRADS laterality columns
left behind by Part B.

Architecture: Option C-soft continues.
  - Ports: <list>
  - Drops: <list>
  - Deferred (pending other-domain canonical): <list>

canonical_patient_master column count: <before> -> <after>
Frozen scripts delta: <N> added to scripts/frozen/ (cumulative: <total>)
Archive: "Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421 / canonical_patient_master_pre_partC
Retained workspace tables (2-week retention):
  - manuscript_workspace.cpm_nlp_laterality_coverage_v1
```

Push to `origin/main`.

---

## Report back at each gate

1. **After Phase 0:** show inventory + consumer graph. Confirm column count matches expected (5 NLP + 4 laterality, adjust if discovery differs).
2. **After Phase 1:** show adjudicated coverage table. Logan overrides per-column recommendations as needed.
3. **After Phase 2 (if run):** show cupm_v2 / backfill diffs before commit.
4. **Before Phase 6:** show the full drop list cross-checked against live CPM + Phase 3/4/5 summaries. Drop is irreversible outside archive restore.
5. **After Phase 7:** full QA bundle + final commit SHA.

---

## Gotchas (learned from Parts A/B)

1. **Cat B aliasing.** Before editing any reader, check if the CPM column name is also used as a local CTAS alias. Mechanical substitution breaks those files. Part B cost a full revert on this.
2. **Redundant NLP flags.** An `nlp_tirads_mention` flag may be strictly weaker than existing v2 capability (e.g., having `tirads_v2_worst_category IS NOT NULL` implies a TIRADS was mentioned). Don't port redundancy — recommend `drop` with a consumer-side coercion note. Check value correspondence before deciding.
3. **Cross-domain laterality confusion.** Laterality cols may come from path, FNA, molecular, or imaging domains. Each needs to land on the appropriate canonical (path_events, fna_events, etc.), NOT all on cupm_v2. If the target canonical doesn't exist yet (e.g., `canonical_fna_events_v1` hasn't been built), defer via `defer_pending_domain_canonical` and document the blocking script.
4. **Script 361 dependency.** If any laterality col is path-domain (tumor_laterality, etc.), check whether Script 361 has landed. If not, defer the port — don't build a one-off canonical just for Part C.
5. **Archive schema naming.** Use `cpm_nlp_laterality_legacy_20260421` not `cpm_tirads_legacy_20260421` (Part B's archive). Separate schemas keep the audit trail clean.
6. **Deferred columns are allowed.** Unlike Part B (where everything dropped), Part C may legitimately leave some columns on CPM pending other-domain canonicals. Document deferrals in `manuscript_workspace.cpm_deferred_cleanup_v1`.
7. **Retention table from Part B.** Do NOT drop `manuscript_workspace.cpm_tirads_audit_classification_v1` or `cpm_tirads_canonical_coverage_v1` in this pass — they're still inside Part B's 2-week retention window.

---

## Success criteria

- 0 `nlp_tirads_*` columns on `main.canonical_patient_master` (or documented deferrals).
- 0 adjudicated laterality columns on CPM (or documented deferrals).
- All affected cohort views resolve with unchanged row count.
- All active readers migrated; `git grep` returns empty outside `scripts/frozen/` and archive paths.
- Writer scripts frozen with standard FROZEN headers.
- QA 10/10 pass.
- Commit chain pushed to `origin/main`.
- Archive DB `"Thyroid 2026 UPdated".cpm_nlp_laterality_legacy_20260421` contains pre-drop CPM snapshot + any migrated view definitions.
