# CURSOR PROMPT — Script 389: US zombie cleanup + view-body rewrites + complications aggregation audit

> **⚠ Phase 0B classifier reset 2026-04-22.** The Bug 1 baselines below
> (`18,310 / 17,090 / 2,152 / 27` for `clean_llm_parsed / clean_non_llm /
> zombie_parent / llm_parsed_but_blob`) were not reproducible against
> live `main.canonical_us_nodule_v2` state — direct MotherDuck probe
> (this session) confirmed they were phantom from compressed context,
> not a real prior classifier. The drift gate correctly halted. The
> classifier was replaced with a 4-bucket source-flag partition
> (`clean_dual_source / clean_base_only / needs_backfill /
> aggregate_rollup`) frozen at the 2026-04-22 baseline (26,402 / 8,919
> / 2,117 / 141). The Phase 2 DELETE step targeting "zombies" is
> RETIRED — none of the four new buckets are zombie in the structural
> sense; `needs_backfill` rows are legitimate entries awaiting NLP
> extraction. Phase 2 instead writes a single read-only provenance row
> to `main.__readme`. Phases 2D/2E (US view rewrites), 2F (complications
> rebuild), 2G (dependent re-bind), and 2H (registry/__readme bump) are
> unchanged. If a content-based blob audit is still wanted, draft as
> Script 389b after 389 closes. See the script's module docstring +
> `EXPECTED_BUCKETS` comment for the full rationale.

**Repo:** `/Users/ros/THyroid 2026`
**Branch:** `main`
**Script:** `scripts/389_us_zombie_view_rewrites_and_complications_audit.py`
**Output report:** `scripts/output/389_close_out_report.md`
**Pre-state probe report:** `scripts/output/389_prestate_probe_report.md`
**DB:** MotherDuck `md:Thyroid_2026` → `thyroid_canonical_publication_v1_0` (PUB, live)
**Archive DB:** `md:"Thyroid 2026 UPdated"`
**Run date (baseline):** 2026-04-22 (use `date +%Y%m%d` at runtime for the `_legacy_<stamp>` suffix; prompt uses `20260422` below as placeholder)
**Assumed prior state:** Scripts 386 and 388 have merged ahead of this. If they haven't, Phase 0 must detect and halt with a clear message — no silent skip.

---

## 0 · Why this script exists

Three independent data-quality bugs surfaced after Script 387 closed PUB v1_0 at 285 objects. They share no architectural root, but they all block clinical reviewers from trusting the v1_0 read surface, so they are packaged into one post-387 hotfix wave.

**Bug 1 — Zombie parent rows in `canonical_us_nodule_v2` (structural).**
Scripts 376/377/378 wrote LLM-parsed child rows by _adding_ them alongside the pre-LLM free-text parent row, not by superseding the parent. Result: 2,152 rows across 664 US exams still carry a multi-nodule free-text blob in `location_raw` while their LLM siblings carry per-nodule structured rows for the same exam. Probe already run (committed alongside the 387 close-out) classified this as:
- 592 exams (89%) where LLM siblings cover the parent → **parent rows are supersedable by DELETE**
- 72 exams (11%) where no LLM sibling exists → **parent rows need re-extraction, not deletion** — these become a carry-forward audit table

Separately, 27 rows are LLM-parsed-but-blob (cosmetic trim of `location_raw` only, no structural change).

**Bug 2 — View-stack phantoms in `canonical_us_patient_master_VIEW_v2`.**
Two related bugs in the US view stack:
- `canonical_us_exam_master_VIEW_v2` starts from a CPM scaffold (`FROM canonical_patient_master cpm LEFT JOIN exam …`), which emits one row per CPM patient regardless of whether US exam data exists. Result: 6,792 phantom rows (NULL `exam_date`, no nodule children, no findings).
- `canonical_us_patient_master_VIEW_v2` carries the literal line `CAST('t' AS BOOLEAN) AS has_any_us` in its CTE — the column is hardcoded TRUE for every patient (10,859 / 10,859 = 100%). The column is meaningless as shipped.

Downstream: 6,499 patients show `has_any_us=TRUE` with NULL `first_us_date` AND NULL `last_us_date`; 4,334 have no findings of any kind.

**Bug 3 — Complications rollup over-aggregation (semantic).**
`canonical_complications_patient_rollup_v1` OR-aggregates across events at the `any_evidence` tier without de-weighting. Case in point: research_id 9340 has `ever_rln_injury_any_evidence=TRUE` and `ever_hypoparathyroidism_any_evidence=TRUE` driven entirely by `note_entities_complications` legacy NLP entities (`source_kind='entity_legacy'`, `source_evidence_type='nlp_proxy'`, confidence 0.9) — while the paired `complication_phenotype_v1` structured discharge-summary rows for the same patient/date explicitly say `finding_status='absent'`. The structured negation is silently overridden by the legacy-NLP proxy.

This is a rollup-logic bug, not an events-layer bug. The events are correctly recorded with lineage (`source_table`, `source_row_id`, `source_evidence_type`, `evidence_strength`, `finding_status`, `evidence_span_hash`). What's broken is the aggregation rule.

---

## 1 · Guardrails (carry-forward from 360/361/362/387 and memory)

Read and obey before writing SQL:

1. **No cross-DB canonical sourcing.** Every rebuilt canonical is a live object in `main` of `thyroid_canonical_publication_v1_0`. Never `FROM "Thyroid 2026 UPdated".*` in a canonical's body. Rebuild from in-main sources; if a needed source is archived, restore-then-build or drop-the-dep are the only legal options.
2. **VIEW naming convention.** Any `main.*` VIEW must carry `_VIEW_v<N>` in its name. This script rewrites existing `_VIEW_v2` view bodies — it does NOT rename.
3. **PHI safety.** Never print `location_raw`, `evidence_text`, `value_raw`, `source_row_id` bodies, or any clinical note content in logs or reports. Use row counts, hashes, and `research_id` only.
4. **Surgical git add.** Never `git add -A` or `git add scripts/output/`. Stage explicit paths only: `scripts/389_*.py`, `scripts/output/389_*.md`, memory/registry updates.
5. **DuckDB TIMESTAMPTZ trap.** All `build_ts` columns must be `CAST(CURRENT_TIMESTAMP AS TIMESTAMP)` — never raw `CURRENT_TIMESTAMP` (returns TIMESTAMPTZ and creates a silent pytz dep).
6. **Idempotency.** Re-running the script after partial execution must detect archive-DB presence + log rows and resume mid-pipeline. Mirror the 387 pattern.
7. **NULL-aware probes.** `collapse = (total_rows − null_key_rows) − distinct_keys` — not `total − distinct`.
8. **Consumer-first archive order.** If any archive moves are needed, order so that the most-downstream consumer is archived first.
9. **ALTER VIEW dependents.** `ALTER VIEW RENAME` is catalog-only — dependents keep the old name in their body. This script does not rename, but any CREATE OR REPLACE must land dependents in the same commit so nothing points at a stale symbol mid-window.
10. **No new `tier2.*` / `verify.*` schemas.** Both dropped in Script 387.
11. **Two-phase execution with `--apply` gate.** Discovery + plan-review run without flags and print what _would_ happen. Apply phase runs only with `--apply` flag. Mirrors the 387/388 pattern.

---

## 2 · Phases

### Phase 0 — Discovery & re-probe (no writes)

Prints to `scripts/output/389_prestate_probe_report.md`. Halts the script with exit 0 if anything unexpected.

**0A. Preflight — confirm 386 and 388 have landed.**
- Query `archive_move_log_v1` in `"Thyroid 2026 UPdated"` for any `script_id IN ('386','388')` rows; if absent, halt with `"Script 389 requires 386 and 388 to have completed — run those first or override with --ignore-preflight"`.
- Confirm PUB object count is still **285** (or the post-388 target — print whichever matches). Halt on mismatch.

**0B. US nodule zombie re-probe.**
Re-run the classification query from the 387-era probe against the live table. Bucket counts must equal: 18,310 clean LLM-parsed / 17,090 clean non-LLM / 2,152 zombie parent / 27 LLM-parsed-but-blob. Halt on any bucket drift >2% — the table has moved under us and the delete plan is stale. Print exact SQL and counts to the probe report.

Sub-classify the 2,152 zombies by LLM-sibling coverage at the exam grain (`research_id` + `exam_date`):
- **supersedable (has ≥1 LLM sibling)** — expect ≈592 exams
- **needs_reextraction (no LLM sibling)** — expect ≈72 exams

**0C. US view-stack probe.**
- Read the bodies of `main.canonical_us_exam_master_VIEW_v2` and `main.canonical_us_patient_master_VIEW_v2` from `information_schema.views` and emit them to the probe report verbatim (these are non-PHI DDL). Confirm presence of the literal `CAST('t' AS BOOLEAN) AS has_any_us` and the CPM-LEFT-JOIN scaffold. Halt if either pattern is absent — bodies have changed and the rewrite plan is stale.
- Count phantoms: rows where `exam_date IS NULL AND n_nodules IS NULL AND <all findings columns> IS NULL`. Expect ≈6,792.
- Count patients where `has_any_us=TRUE AND first_us_date IS NULL AND last_us_date IS NULL`. Expect ≈6,499.

**0D. Enumerate `canonical_us_exam_master_VIEW_v2` and `canonical_us_patient_master_VIEW_v2` dependents.**
Query `information_schema.view_dependencies` (or the equivalent `duckdb_views`/`sqlite_master` path) to list every object whose body references either view. All dependents will need to resolve again after CREATE OR REPLACE — the rebuild must be bottom-up.

**0E. Complications events audit probe.**
Print to the probe report, grouped by `source_table` × `source_kind` × `source_evidence_type` × `finding_status` × `evidence_strength`:
- row count
- distinct `research_id` count
- distinct `research_id, complication_type` pair count

This is the evidence base for Phase 1's plan-review gate. It answers: across the whole table, how many rows fall into each `(source_kind × finding_status × strength)` cell? Crucially — how many patients would _lose_ an `any_evidence=TRUE` flag under each proposed reconciliation rule (see Phase 1 options).

Pre-compute the three candidate aggregation rules' deltas vs current rollup, per `complication_type`:
- **Rule A (status-aware):** `any_evidence` becomes TRUE only when at least one event has `finding_status='present'`. `finding_status='absent'` does not contribute.
- **Rule B (kind-weighted):** `any_evidence` excludes `source_kind='entity_legacy'` with `source_evidence_type='nlp_proxy'` — i.e. drops the weakest pre-LLM proxy entities. Everything else contributes.
- **Rule C (combined):** A AND B — both conditions must hold.

For each rule × complication_type, report: number of patients flipping TRUE→FALSE, number flipping FALSE→TRUE (should be 0), and the absolute count in each tier post-change.

**0F. Write 389_prestate_probe_report.md** with every bucket count, both view bodies, all three rule deltas, and the 72-exam carry-forward list. Halt at end of Phase 0 unless `--apply` is passed.

---

### Phase 1 — Plan-review gate (operator decision)

If `--apply` is passed but the file `scripts/output/389_plan_approval.txt` does not exist, the script:
1. Prints a summary of the planned writes (counts + table targets + commit message preview).
2. Prints the three candidate aggregation rules (A/B/C) and asks the operator to write `RULE=A` or `RULE=B` or `RULE=C` into the approval file, along with optional `INCLUDE_FINDING_STATUS_ABSENT_IN_any_evidence=FALSE` (the intended default for A and C) and `INCLUDE_ENTITY_LEGACY_NLP_PROXY_IN_any_evidence=FALSE` (default for B and C).
3. Halts.

On re-run with `--apply` and the approval file present, parse it and proceed to Phase 2. Abort with a clear error if the file format is malformed.

---

### Phase 2 — Apply (only with `--apply` + approval file)

All writes within one script session. Each sub-phase writes one `archive_move_log_v1` row in `"Thyroid 2026 UPdated"` regardless of sub-phase type, using `script_id='389'`.

**2A. US nodule zombie DELETE (supersedable subset).**
Within the 2,152 zombie rows, DELETE exactly those whose `(research_id, exam_date)` pair has ≥1 LLM sibling. Archive the deleted rows first to `"Thyroid 2026 UPdated".us_nodule_zombie_deleted_20260422` (table, not view) so they're recoverable. Expected: ~1,776 rows deleted (592 exams × ~3 blob rows avg; actual from probe). Log archive + delete counts.

**2B. US nodule needs_reextraction carry-forward table.**
CREATE TABLE `main.canonical_us_nodule_zombie_pending_reextraction_v1` with the 72-exam subset snapshot (full row bodies), plus a `reextraction_status` column defaulting to `'pending'`. This is a carry-forward audit object, not a consumer-facing canonical — register it as `canonical_*_v1` so the registry pass covers it, but the `__readme_*` entry must mark it as `audit_carry_forward`.

**2C. US nodule cosmetic location_raw trim.**
For the 27 LLM-parsed-but-blob rows: overwrite `location_raw` with the per-nodule substring matching that row's `laterality` + `nodule_index_within_exam`. Keep the pre-trim value in a new column `location_raw_pre_trim` (add via ALTER TABLE if absent) so the change is reversible. Log row count trimmed.

**2D. Rewrite `canonical_us_exam_master_VIEW_v2`.**
CREATE OR REPLACE VIEW removing the CPM scaffold. New body's FROM clause starts from the actual US exam source (the builder's input table — 378_build_us_exam_master.py references the canonical exam source), NOT from `canonical_patient_master`. Emit one row per actually-existing US exam. Expected row count after rewrite: 18,551 − 6,792 ≈ **11,759 rows**; actual value confirmed by probe.

Any CPM-derived column the view currently exposes (e.g. `first_surgery_date_v2`) must either be (a) dropped if downstream consumers don't need it, or (b) re-obtained via an INNER JOIN after the US-grounded FROM clause. Phase 0D's dependent list drives this decision. If ambiguous, prefer (a) + flag for downstream patch in the close-out.

**2E. Rewrite `canonical_us_patient_master_VIEW_v2`.**
CREATE OR REPLACE VIEW replacing the hardcoded `CAST('t' AS BOOLEAN) AS has_any_us` with a real derivation:

```sql
COUNT(*) FILTER (WHERE exam_id IS NOT NULL) > 0 AS has_any_us
```

…computed against the newly-corrected `canonical_us_exam_master_VIEW_v2`. Add `GROUP BY research_id` semantics; expected patient count after rewrite ≤ 10,859 (actually will drop meaningfully since 4,334 CPM-only rows go away). Preserve column order and name compatibility — downstream consumers should see the same column list.

Any consumer that depends on `has_any_us=TRUE` populating every CPM row (if such a consumer exists — Phase 0D will surface it) must be flagged as a carry-forward, not silently broken. If found, halt and ask.

**2F. Rebuild `canonical_complications_patient_rollup_v1` per selected rule.**
Read `RULE=X` from the approval file. CREATE OR REPLACE TABLE via CTAS from `canonical_complications_events_v1` using the filter logic corresponding to the chosen rule. All 36 `ever_*` `any_evidence` tier columns apply the rule; `probable_or_better` and `definitive` tiers are untouched (they already exclude `possible`-strength events by construction). Derived `hypoparathyroidism_*` and `hypocalcemia_clinical_*` columns: apply the same reconciliation if their upstream definitions share the `any_evidence` semantic (Phase 0 must confirm per-column — if they derive from a different tier they stay as-is).

Archive the prior rollup table to `"Thyroid 2026 UPdated".canonical_complications_patient_rollup_v1_legacy_20260422` before the CTAS overwrite.

**2G. Dependent-view re-bind pass.**
For every view listed in Phase 0D, CREATE OR REPLACE in dependency order to force catalog re-resolution against the new bodies. Even if the DDL is unchanged, this step catches silent broken dependents early. Any view that fails to recompile is logged with its error and the script halts — do not continue past a broken catalog.

**2H. Archive orphaned CPM-scaffold backfill source (`cupm_v2_canonical_backfill_v1`).**
Once 2E has rewritten `canonical_us_patient_master_VIEW_v2` to source from the corrected exam_master instead of the CPM scaffold, `main.cupm_v2_canonical_backfill_v1` (BASE TABLE, 10,871 rows) becomes orphaned. Verify no remaining dependents via `information_schema.view_dependencies` — if any exist, halt and flag as carry-forward. If zero dependents (expected post-2E), archive to `"Thyroid 2026 UPdated".cupm_v2_canonical_backfill_v1_legacy_20260422` via CTAS, then DROP from main. Log the move in `archive_move_log_v1`.

Note: This table was KEEP_AS_IS in Script 388's disposition pass with the explicit carry-forward that 389 would orphan it. 388's close-out names this step as the follow-up.

**2I. Post-state object count & registry bump.**
- Re-count PUB objects. Expect 285 + 1 (new `canonical_us_nodule_zombie_pending_reextraction_v1`) − 1 (archived `cupm_v2_canonical_backfill_v1`) = **285**.
- Update `detail_table_registry_v1` (via information_schema-aware INSERT, per memory) to add the new carry-forward table with `detail_table_name='canonical_us_nodule_zombie_pending_reextraction_v1'`, category `audit_carry_forward`. Remove any registry row for `cupm_v2_canonical_backfill_v1` if present.
- Update `__readme_*` entries for the three rewritten objects (complications rollup, us_exam_master_VIEW_v2, us_patient_master_VIEW_v2) — regenerate the `description` field to reflect the rule applied + phantom-row fix + has_any_us fix. Include the Rule (A/B/C) in the complications rollup description.

---

### Phase 3 — Post-state verification

Writes `scripts/output/389_close_out_report.md`.

3A. Re-run the Phase 0 probes against post-apply state. Every bucket must match expected post-values.
3B. Audit query: for research_id 9340, print the post-rule flag values for RLN injury and hypoparathyroidism across all three tiers + the derived postop columns. Confirm the specific bug case resolves (or doesn't, if Rule B was selected — depending on whether the legacy NLP row was the sole driver).
3C. Confirm `canonical_us_patient_master_VIEW_v2.has_any_us` is no longer 100% TRUE; print the new TRUE/FALSE distribution.
3D. Confirm `canonical_us_exam_master_VIEW_v2` row count matches expected post-value (no phantoms).
3E. Re-run the 387 dedup probe against the rebuilt complications rollup. Expect: rollup-invariant layer still clean. No new collapses.

---

### Phase 4 — Commit

Single commit, message template:

```
Script 389: US zombie cleanup + view-body rewrites + complications aggregation audit

- us_nodule_v2: DELETE <N> zombie parent rows across 592 exams (LLM siblings cover);
  preserve 72 re-extraction-pending exams in canonical_us_nodule_zombie_pending_reextraction_v1
- us_nodule_v2: cosmetic location_raw trim on 27 LLM-parsed-but-blob rows (reversible)
- canonical_us_exam_master_VIEW_v2: rewrite to drop CPM-LEFT-JOIN scaffold;
  <M> phantom rows eliminated (post-row-count: <N>)
- canonical_us_patient_master_VIEW_v2: replace hardcoded has_any_us=TRUE with derived
  value from corrected exam_master; new TRUE count: <N>
- canonical_complications_patient_rollup_v1: rebuild under Rule <A|B|C> to reconcile
  contradictory structured-vs-legacy-NLP events; <X> patients flipped TRUE→FALSE at
  any_evidence tier across <Y> complication types
- cupm_v2_canonical_backfill_v1: archived post-VIEW-rewrite (388 carry-forward closed)
- Dependent-view re-bind pass: <K> views recompiled successfully

Archive zone: "Thyroid 2026 UPdated".us_nodule_zombie_deleted_20260422 +
canonical_complications_patient_rollup_v1_legacy_20260422 +
cupm_v2_canonical_backfill_v1_legacy_20260422
Registry: +1 audit_carry_forward entry, −1 orphaned-backfill entry
Object count: 285 → 286 → 285 (net no change; +1 audit −1 orphan)
Probe: scripts/output/389_prestate_probe_report.md
Close-out: scripts/output/389_close_out_report.md
```

Staged paths (explicit, no `-A`):
- `scripts/389_us_zombie_view_rewrites_and_complications_audit.py`
- `scripts/output/389_prestate_probe_report.md`
- `scripts/output/389_close_out_report.md`
- `scripts/output/389_plan_approval.txt` (if tracked — prefer .gitignore and keep local)
- any touched registry/__readme files

Push main after local CI green.

---

## 3 · Expected carry-forwards from 389

Known carry-forwards the close-out must declare (do not auto-fix):

1. **72 US exams pending re-extraction** — now materialized in `canonical_us_nodule_zombie_pending_reextraction_v1`. Script 390+ should run an LLM re-extraction pass on their notes.
2. **Complications rollup rule choice** — whichever rule wasn't selected remains as a reference delta in the close-out for reviewer discussion. If Rule A was selected, the close-out should flag that 15 legacy-NLP-proxy-only patients still contribute to `any_evidence` for their respective types.
3. **Upstream complication event de-duplication** — the fact that `complication_phenotype_v1` (structured) and `note_entities_complications` (legacy_entity) are emitting contradictory rows for the same `(research_id, complication_type, finding_date)` is a builder-layer issue this script does not address. Flag as Script 390+ carry-forward.
4. **US view-stack column compatibility** — if any CPM-derived column was dropped from `canonical_us_exam_master_VIEW_v2` in Phase 2D, the downstream consumers flagged in Phase 0D need to be patched in a follow-up.
5. **Pre-387 flag_event key collapses (7 tables)** — still carry-forward from Script 387. 389 does not touch these.

---

## 4 · Roll-back

If post-commit verification detects a regression:

- Each rebuilt canonical's prior body is archived in `"Thyroid 2026 UPdated".*_legacy_20260422`. CTAS back into `main` restores.
- Deleted zombie rows are in `"Thyroid 2026 UPdated".us_nodule_zombie_deleted_20260422` — re-INSERT restores.
- Cosmetic `location_raw` trims are reversible via `location_raw_pre_trim` column.
- View body rewrites: git revert the commit — no schema drift; just re-run CREATE OR REPLACE via the reverted SQL.

No operation in this script deletes anything irretrievable. All destructive ops mirror to the archive DB first.

---

## 5 · Do-NOT list

- Do NOT silently patch `canonical_complications_events_v1` — builder-layer fix, out of scope.
- Do NOT rename any view.
- Do NOT drop or create schemas.
- Do NOT add cross-DB FROM clauses to any canonical body.
- Do NOT print `location_raw` contents anywhere (zombie rows are the ones with raw multi-nodule blobs — exactly the content that must stay out of logs).
- Do NOT `git add scripts/output/` wholesale.
- Do NOT let `--apply` run without the plan-approval file.
