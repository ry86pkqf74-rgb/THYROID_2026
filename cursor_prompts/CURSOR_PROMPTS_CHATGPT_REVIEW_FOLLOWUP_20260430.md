# ChatGPT Review Followup — 5 Lane Prompts (Logan-locked 2026-04-30)

After Cowork-direct apply of mig_209 (P1 registry reconciliation) + mig_210 (P7 rid 610 single-row fix), 5 issue areas remain. Each prompt below is **fully self-contained** with Logan's decisions baked in — no further approval needed. Copy-paste straight into the recommended agent.

**Status snapshot:** 5-gate 175/0/0/0/0; cohort 10,871; PM events 6,469; HEAD `27c3c74`. None of these lanes blocks v1.0 manuscript readiness.

**Logan-locked decisions (2026-04-30):**
- **A:** All 10 deferred composites are v1.0 scope. Verify all, no deferral.
- **B:** PM dedup VIEW name = `canonical_path_malignant_events_dedup_VIEW_v1`.
- **C:** Recurrence flag = `is_implausible_date_quarantine`; 132 rows; criteria as documented (year < 1990 on either date col + path_proven=TRUE with negative days).
- **D:** Investigate first; if confirmed patient-level-only → add `is_patient_level_only_evidence` flag (525 rows).
- **E:** Investigate size outliers + ACR rule violations (likely typos); document both `acr2017_tirads_category` (primary) and `updated_tirads_category` (sensitivity) as manuscript-facing.

---

## Lane A: Verify all 10 deferred analytic composites
**Agent:** **Cursor composer**
**Why composer:** ~600 col-rows total; needs to find build scripts in `scripts/` + cross-check upstream sources; multi-file IDE work.
**Mig batch:** `mig_211_verify_10_deferred_composites_20260430` (one mig, multiple §s)

### Tables (all v1.0 scope per Logan):
| Table | Cols | Schema |
|---|---:|---|
| `manuscript_cohort_v1` | 150 | main |
| `patient_analysis_resolved_v1` | 146 | manuscript_workspace |
| `ln_master_rollup_v1` | 78 | manuscript_workspace |
| `episode_analysis_resolved_v1_dedup` | 46 | manuscript_workspace |
| `lesion_analysis_resolved_v1` | 28 | manuscript_workspace |
| `imaging_fna_linkage_v3` | 20 | main |
| `tumor_stage_heterogeneity_v1` | 17 | main |
| `imaging_patient_summary_v1` | 13 | main |
| `recurrence_event_clean_v1` | 11 | main |
| `patient_cross_domain_timeline_v2` | 6 | main |

### Prompt:

> Verify all 10 deferred analytic composite tables in `thyroid_canonical_publication_v1_0` listed above. All are v1.0 manuscript scope per Logan. Currently all are `table_status='not_started'` with NULL `signoff_migration` in `canonical_table_signoff_registry_v1`.
>
> **For each table:**
> 1. Find the build script (search `scripts/`, `qc_framework_v1/migrations/`, `qc_framework_v1/scripts/` for the populating lane — e.g., `mig_204` populated `manuscript_cohort_v1`).
> 2. Document upstream sources, grain (patient/episode/lesion/event), key uniqueness, row count.
> 3. For each col, classify as `verified` (analytic, derivation traceable to upstream), `na` (helper/provenance — `build_ts`, `build_script`, etc.), or open as a CF if you can't trace derivation.
> 4. Apply via `mig_211_verify_10_deferred_composites_20260430` following `qc_framework_v1/migrations/205_us_gland_v2_signoff_registry_inserts_20260430.sql` template:
>    - Pre-snapshot signoff_registry + col_registry to `archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig211_20260430` and `..._column_verification_registry_v1_pre_mig211_20260430`
>    - INSERT 10 signoff_registry rows (with proper n_verified + n_na breakdown matching col-row classifications)
>    - INSERT col_registry rows with batch_id `mig_211_verify_10_deferred_composites_20260430`
>    - INSERT provenance row in `manuscript_workspace.cpm_reconciliation_provenance_v1`
>
> **Acceptance criteria:**
> - All 10 tables show `table_status='verified'` post-apply
> - Cowork verification suite §12 (governance gap) stays at 0
> - 5-gate stays clean (gate1 175 → 185; gate3 stays 0)
> - All `n_verified + n_na = n_columns_total` per table
> - Open carry-forwards documented for any cols you couldn't trace cleanly
>
> **Process:** Apply Cowork-direct via MotherDuck `query_rw` (use the same SSO account as the publication DB). Pre-snapshots required. Commit + push when done. Update memory with closeout note.

---

## Lane B: PM dedup VIEW
**Agent:** **Cline Sonnet 4.6**
**Why:** Single CREATE VIEW + 1 retro-signoff row + col-registry inserts. Pure pattern-following.
**Mig:** `mig_212_canonical_path_malignant_dedup_view_20260430`

### Prompt:

> Create `main.canonical_path_malignant_events_dedup_VIEW_v1` per `reference_view_naming_convention.md` to provide the manuscript-safe deduplicated PM surface. Filter rule per `mig_185b` (already documented in memory): `WHERE is_source_distinct_duplicate_grain=FALSE OR IS NULL` → 5,944 rows / 4,022 patients / 0 remaining duplicates by `(research_id, path_surgery_id, tumor_ordinal)`.
>
> **Steps:**
> 1. Pre-snapshot `canonical_table_signoff_registry_v1` to `archive_pub_v1_0.canonical_table_signoff_registry_v1_pre_mig212_20260430`
> 2. `CREATE OR REPLACE VIEW main.canonical_path_malignant_events_dedup_VIEW_v1 AS SELECT * FROM main.canonical_path_malignant_events_v1 WHERE is_source_distinct_duplicate_grain=FALSE OR is_source_distinct_duplicate_grain IS NULL;`
> 3. Verify post-apply: row count = 5,944; patient count = 4,022; `(research_id, path_surgery_id, tumor_ordinal)` is unique.
> 4. INSERT 1 signoff_registry row: `('main','canonical_path_malignant_events_dedup_VIEW_v1', <n_cols>, <n_cols>, 0, 0, 0, 'verified', ...)` with `priority_tier='tier2_canonical'` and notes documenting the filter rule.
> 5. INSERT col_registry rows for all cols with `verification_status='verified'` and `verification_method='view_filter_inheritance_from_canonical_path_malignant_events_v1_dedup_rule_mig185b'`.
> 6. INSERT provenance row.
>
> **Acceptance:**
> - View exists, row count = 5,944, distinct patients = 4,022
> - Cowork verification suite §12 stays at 0 ungoverned
> - 5-gate stays clean (gate1 +1)
>
> Commit + push. Update memory.

---

## Lane C: Recurrence implausible-date quarantine flag
**Agent:** **Cline Sonnet 4.6**
**Why:** ALTER TABLE ADD COLUMN + scoped UPDATE + 1 col registration. Mechanical.
**Mig:** `mig_213_recurrence_implausible_date_quarantine_20260430`

### Prompt:

> Add a quarantine flag to `canonical_recurrence_resolved_v1` for 132 rows with implausible recurrence dates. Logan ratified: imaging from <1990 is implausible; same for path-proven dates; negative days_to_path_proven on path-proven rows is also implausible.
>
> **Schema change:**
> ```sql
> ALTER TABLE main.canonical_recurrence_resolved_v1 ADD COLUMN is_implausible_date_quarantine BOOLEAN DEFAULT FALSE;
> ```
>
> **Quarantine criteria** (132 rows total):
> ```sql
> UPDATE main.canonical_recurrence_resolved_v1
> SET is_implausible_date_quarantine = TRUE
> WHERE EXTRACT(YEAR FROM recurrence_imaging_suspicious_date) < 1990
>    OR EXTRACT(YEAR FROM recurrence_path_proven_date) < 1990
>    OR (recurrence_path_proven=TRUE AND days_to_path_proven < 0);
> ```
>
> **Steps:**
> 1. Pre-snapshot `canonical_recurrence_resolved_v1` (full table) + col_registry + signoff_registry
> 2. ALTER TABLE ADD COLUMN
> 3. UPDATE for 132 rows (verify count = 132 post-apply)
> 4. INSERT 1 col_registry row for `is_implausible_date_quarantine` (status='verified', method='derivation_logan_ratified_pre_1990_dates_plus_path_proven_negative_days')
> 5. UPDATE `canonical_table_signoff_registry_v1` for `canonical_recurrence_resolved_v1`: `n_verified + 1`, `n_columns_total + 1`
> 6. INSERT provenance row
>
> **Acceptance:**
> - Quarantine flag = TRUE for exactly 132 rows
> - Spot-check: rid 12057 (0202-12-30 path date), rid 10622 (1950-06-10 path date), rid 9182 (-87 days path_proven), rid 8203 (-18 days path_proven) all flagged
> - Cowork verification suite stays clean
> - Gate3 holds for `canonical_recurrence_resolved_v1`
>
> **Add memory note** documenting: time-dependent recurrence analyses must add `WHERE is_implausible_date_quarantine=FALSE`.
>
> Commit + push.

---

## Lane D: Molecular patient-level evidence flag (investigation-then-apply)
**Agent:** **Cursor composer**
**Why:** Logan wants investigation first. Need to read `script_269_backfill` source + sample rows + confirm there's no recoverable per-test anchor before adding the flag.
**Mig:** `mig_214_molecular_patient_level_evidence_flag_20260430` (after investigation)

### Prompt:

> **Step 1 — Investigation (READ-ONLY).** Confirm that the 525 NULL-episode rows in `canonical_molecular_genetics_v2` truly lack any recoverable per-test/per-date anchor before we add a permanent flag.
>
> Probes to run:
> 1. Sample 20 rows: `SELECT research_id, ingestion_source, report_source_table, gene, mutation_or_fusion, builder_version, built_at, * FROM main.canonical_molecular_genetics_v2 WHERE molecular_episode_id IS NULL ORDER BY random() LIMIT 20;` — note all non-null cols, look for any date or platform fields hiding under different names.
> 2. Check upstream tables: `SELECT * FROM "Thyroid 2026 UPdated".molecular_legacy_20260421.thyroseq_molecular_enrichment LIMIT 10;` (or whatever lives in upstream; 443 rows came from there). Same for `extracted_braf_recovery_v1` (46 rows) and `ret_patient_adjudicated_v226` (36 rows).
> 3. Check the build script — find `script_269` or whatever populates this table (search `scripts/` for `269_*`).
> 4. Verify: do any of the 525 rows have a date hiding in a JSON blob, raw text, or alternate col?
>
> **Decision tree post-investigation:**
> - **If 525 rows truly lack recoverable per-test anchors** → proceed to Step 2.
> - **If any subset CAN have episode_id recovered** (e.g., from raw report text) → write a separate `mig_214a` to recover those episode_ids first, then re-probe NULL-episode count, then proceed to Step 2 with the residual.
> - **If you find a meaningful subset that's ambiguous** → carry-forward and ask Logan.
>
> **Step 2 — Apply flag** (only after Step 1 confirms):
> ```sql
> ALTER TABLE main.canonical_molecular_genetics_v2 ADD COLUMN is_patient_level_only_evidence BOOLEAN DEFAULT FALSE;
> UPDATE main.canonical_molecular_genetics_v2 SET is_patient_level_only_evidence = TRUE WHERE molecular_episode_id IS NULL;
> ```
>
> Pre-snapshot the table + col_registry + signoff_registry. Register the new col (status='verified', method='derivation_from_molecular_episode_id_null_indicator_post_investigation_confirmed_no_recoverable_anchor'). Bump signoff counts. Provenance row.
>
> **Acceptance:**
> - Investigation report committed at `qc_framework_v1/reports/mig_214_investigation_molecular_null_episode_20260430.md` documenting findings
> - If flag added: TRUE for exactly 525 rows / 520 distinct patients
> - Spot-check: `WHERE is_patient_level_only_evidence=TRUE` returns same set as `WHERE molecular_episode_id IS NULL`
> - Cowork verification suite stays clean
>
> **Memory note:** patient-level molecular cohort flags = no filter; per-test/per-date analyses = `WHERE is_patient_level_only_evidence=FALSE`.
>
> Commit + push.

---

## Lane E: TIRADS investigation + fix + dual-column documentation
**Agent:** **Cursor composer**
**Why:** Investigation-heavy (raw US report review for rid 8931); needs IDE for cross-referencing source extractor logic; multiple sub-fixes.
**Mig:** `mig_215_tirads_outliers_investigation_and_fix_20260430` + `mig_216_tirads_dual_column_doc_20260430`

### Prompt:

> Three sub-tasks on `canonical_us_nodule_v2`. **Investigate before any data writes** — Logan suspects E1+E2 are typos / re-derivation bugs.
>
> **E1 — 21 size_cm_max outliers** (`size_cm_max <= 0 OR > 20`)
> 20 are research_id 8931 (`size_cm_max=48` across 20 nodule_master_ids on 2 us_exam_ids: 2017-03-21 and 2019-08-29). 1 is research_id 8613 (`size_cm_max=21`).
>
> Investigation steps:
> 1. Pull raw US report text for rid 8931 exams: query upstream `us_nodules_tirads` or `us_reports` table (search information_schema for the source). Look for the 48 → likely a unit error (mm logged as cm? whole-thyroid measurement leaked into nodule sizes?).
> 2. Same for rid 8613.
> 3. Check the source extractor (search `scripts/` for the US-nodule build) — find where `size_cm_max` is derived; identify the bug.
> 4. **Fix decision tree:**
>    - If raw report has correct value: `UPDATE` the 21 rows with corrected values; document corrections in mig.
>    - If raw report itself has the implausible value: leave as-is, add `is_size_outlier_quarantine` flag for the 21.
>    - If extractor bug: fix the extractor (separate scripts/ commit) + rebuild affected rows.
>
> **E2 — 23 ACR rule violations** (`acr2017_tirads_points=1` mapped to TR1/TR2/TR3 — points=1 doesn't map to any TR per ACR 2017: TR1=0, TR2=2, TR3=3, TR4=4-6, TR5≥7)
> Distribution: 1× TR1, 21× TR2, 1× TR3.
>
> Investigation steps:
> 1. Sample the 23 rows: features used to compute the 1 point + how the category was assigned.
> 2. Check the category-assignment logic (likely in a build script) — points=1 should fall through to TR1 or NULL, not TR2/TR3.
> 3. **Fix decision tree:**
>    - If extractor logic bug: fix in scripts/ + rebuild category for the 23 (likely → TR1 if points=1 is "borderline TR1"; or NULL if points=1 is "uncomputable").
>    - Document the corrected mapping; UPDATE the 23 rows.
>
> **E3 — Document dual-column manuscript-facing semantics** (Logan ratified: BOTH columns are manuscript-facing)
> 17,938 of 37,579 rows have `acr2017_tirads_category` differing from `updated_tirads_category`.
>
> Steps:
> 1. Identify what `updated_tirads_category` represents (search build scripts; likely ACR 2024 or local Emory rule).
> 2. Update col_registry notes for both columns:
>    - `acr2017_tirads_category`: "Manuscript primary surface — strict ACR 2017 rule (TR1=0pts, TR2=2pts, TR3=3pts, TR4=4-6pts, TR5≥7pts)."
>    - `updated_tirads_category`: "Manuscript sensitivity-analysis surface — <document the rule>."
> 3. Write a memory note `feedback_tirads_category_canonical.md` documenting: primary = `acr2017_tirads_category`; sensitivity = `updated_tirads_category` (what rule); both reportable in manuscript supplementary.
>
> **E4 (Round 2 add) — Build 4 manuscript-facing TIRADS cohort views** (ChatGPT TIRADS doc Phase 1; Logan ratified)
>
> Create in `manuscript_workspace`:
> - `vw_us_nodule_tirads_strict_acr2017_v1` — non-aggregate + non-shell + `acr2017_feature_points_complete=TRUE` + ACR points/category present
> - `vw_us_nodule_tirads_any_reported_v1` — non-aggregate + non-shell + (tirads_reported_in_text OR acr2017_tirads_category OR updated_tirads_category present)
> - `vw_us_nodule_tirads_reported_not_fully_parsed_v1` — same as any_reported BUT `acr2017_feature_points_complete=FALSE`
> - `vw_us_nodule_tirads_unresolved_or_excluded_v1` — `is_aggregate_row=TRUE OR us_row_type='shell' OR nlp_backfill_pending=TRUE`
>
> Use `manuscript_workspace.canonical_us_nodule_v2_filtered` as source (already provides us_row_type + us_resolution_strength). Per `reference_view_naming_convention.md`: VIEW names need `_VIEW` suffix → use `vw_us_nodule_tirads_strict_acr2017_VIEW_v1` etc. Verify post-create row counts match Doc 2 expected: strict cohort ≈ 5,149; any_reported ≈ 22,276+12,070+27,885 deduped; reported-not-parsed ≈ 8,243; excluded ≈ 141 aggregate + 3,067 shell + 2,061 nlp_pending - overlaps.
>
> Register all 4 views per VIEW pattern (status=verified, method=`view_filter_inheritance_per_chatgpt_tirads_doc_phase1_2026-04-30`).
>
> **E5 (Round 2 add) — Resolve 2,640 high-priority TIRADS conflicts** (ChatGPT TIRADS doc Phase 2)
>
> `manuscript_workspace.us_nodule_conflict_queue_v1` has 2,640 high-priority conflicts (2,494 `tirads_reported` + 123 `tirads_category_v2` + 23 `tirads_score_2017`).
>
> Sample 30 of each conflict family. For each:
> 1. Identify both conflicting source values + their source provenance.
> 2. Apply Logan-ratified resolution rule (likely: prefer `source_tirads_v2` over `source_tirads_llm` for category/score; prefer text-reported for tirads_reported).
> 3. Write resolution into `canonical_us_nodule_v2` with new `tirads_conflict_resolution_source` provenance col (or document why no auto-resolution possible → leaves in queue).
>
> If resolution rules are non-trivial, escalate batch to Logan with sample CSV before bulk apply.
>
> **E6 (Round 2 add) — Clarify acr2017_feature_points_complete semantics** (ChatGPT TIRADS doc Phase 4)
>
> 21,454 rows have all 5 ACR point fields non-null but only 5,149 have `acr2017_feature_points_complete=TRUE`. The 4× gap needs explanation. Find the build script that sets `acr2017_feature_points_complete`. Document the actual semantic (likely "5 fields non-null AND text-evidence-grounded" vs "5 fields non-null only") in:
> 1. col_registry note for `acr2017_feature_points_complete`
> 2. Memory note: `feedback_acr2017_feature_points_complete_semantic.md`
> 3. Methods section update for manuscript
>
> If the strict semantic is too restrictive (5,149 rows = small primary cohort), Logan may want to relax. Surface for ratification.
>
> **Combined acceptance** (Lane E + E4 + E5 + E6):
> - All 4 cohort views created, named per VIEW convention, registered as verified
> - Conflict queue resolution: at least the 2,640 high-priority rows triaged (resolved or documented why deferred)
> - Completeness flag semantic documented in memory + methods + col_registry notes
> - Spot-checks: strict cohort row count ≈ 5,149; size_outlier-flagged rows visible; 23 ACR rule violations corrected
> - Cowork verification suite stays clean
>
> Commit + push.

---

## Lane F: Multi-nodule under-explosion + deferred LLM absorption triage
**Agent:** **Cline GPT-5.5**
**Why GPT-5.5:** Investigation-heavy; needs fresh-eyes reasoning on TIRADS extractor logic; multi-day autonomous loops; GPT-5.5's different reasoning patterns provide valuable cross-check vs Sonnet.
**Mig:** `mig_217_multi_nodule_under_explosion_triage_20260430` (per Logan F2: triage now)

### Prompt:

> ChatGPT TIRADS doc Phase 3: 448 multi-nodule under-explosion candidate exams (`manuscript_workspace.qc_tir03_llm_candidates_v1`) + 825 deferred LLM absorption patients (`manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1`) have unresolved nodule attribution. Logan ratified F2 = triage now (manuscript-relevant).
>
> **Step 1 — Read the queue tables in full + raw upstream**
> ```sql
> SELECT * FROM manuscript_workspace.qc_tir03_llm_candidates_v1 ORDER BY n_current_nodules DESC, n_reported_tirads DESC;
> SELECT * FROM manuscript_workspace.us_llm_absorption_deferred_multi_nodule_v1 ORDER BY 1;
> ```
> Identify the schema of each queue table. Cross-reference to source US reports (probably `raw.us_reports` or similar — search information_schema.tables for upstream).
>
> **Step 2 — Triage rules**
> Rank candidates by:
> 1. Patient has malignancy (`is_malignant=TRUE` in canonical_patient_master)
> 2. Patient has FNA event (`canonical_fna_events_v1`)
> 3. Patient has surgical pathology linkage (`canonical_path_malignant_events_v1`)
> 4. Patient has molecular testing (`canonical_molecular_genetics_v2`)
> 5. Number of nodules in exam (more = higher TIRADS conflict risk)
>
> Top-tier patients (malignancy + FNA + surgery + molecular) get priority review.
>
> **Step 3 — Decision per exam**
> For each candidate exam:
> - **Absorb**: if nodule identity unambiguous → write LLM-derived features into canonical row + provenance col
> - **Document as limitation**: if nodule attribution ambiguous → flag with `multi_nodule_attribution_unresolved` flag + remove from QC queue
> - **Escalate**: if pattern suggests extractor bug → write to Logan with sample
>
> **Step 4 — Apply with safety**
> Pre-snapshot `canonical_us_nodule_v2` + relevant queue tables. ALTER TABLE ADD COLUMN `multi_nodule_attribution_unresolved BOOLEAN DEFAULT FALSE` if needed for "document as limitation" path. Bulk UPDATE per Step 3 decisions. Provenance row.
>
> **Step 5 — Post-apply**
> - Both queue tables shrink (or empty) per absorption decisions
> - Methods section update describing remaining limitations
> - Memory note: `project_multi_nodule_attribution_triage_20260430.md`
>
> **Acceptance:**
> - 448 candidate exams + 825 deferred patients all categorized (absorbed / documented / escalated)
> - Cowork verification suite stays clean
> - Memory + methods updated

---

## Lane G: Release manifest + semantic_publication views
**Agent:** **Cline GPT-5.5**
**Why GPT-5.5:** Architectural cross-check valuable (vs Sonnet which built most of the canonical layer); GPT-5.5 brings fresh eyes to the manuscript-safe semantic layer design; autonomous multi-table build.
**Mig:** `mig_218_semantic_publication_layer_20260430` (Logan G1: build it)

### Prompt:

> Build the `semantic_publication` schema for manuscript reproducibility + future-release stability per ChatGPT MD/Power BI doc Priorities 1+3. This is V1.0 reproducibility work — needed before manuscript publication so the analysis surface is stable across future data additions.
>
> **Step 1 — Create schema**
> ```sql
> CREATE SCHEMA IF NOT EXISTS semantic_publication;
> ```
>
> **Step 2 — Create release_manifest_v1**
> ```sql
> CREATE TABLE semantic_publication.release_manifest_v1 (
>   release_id VARCHAR PRIMARY KEY,  -- e.g., 'pub_v1_0_20260430'
>   release_name VARCHAR,
>   source_database VARCHAR,
>   source_schema VARCHAR,
>   frozen_schema VARCHAR,
>   created_at TIMESTAMP,
>   created_by VARCHAR,
>   repo_name VARCHAR,
>   git_commit_hash VARCHAR,
>   motherduck_database VARCHAR,
>   n_patients INTEGER,
>   n_surgeries INTEGER,
>   n_malignant_patients INTEGER,
>   n_pathology_events INTEGER,
>   n_fna_events INTEGER,
>   n_molecular_events INTEGER,
>   n_us_exams INTEGER,
>   n_recurrence_path_proven INTEGER,
>   n_recurrence_imaging_only INTEGER,
>   qc_open_issue_count INTEGER,
>   notes VARCHAR
> );
> ```
> Populate with row 1 for `pub_v1_0_20260430` (current state).
>
> **Step 3 — Create 8 manuscript-safe semantic views**
> Per ChatGPT doc Priority 3 + memory `reference_view_naming_convention.md` (use `_VIEW` suffix):
> 1. `vw_patient_master_safe_VIEW_v1` — wraps `canonical_patient_master`; excludes hundreds of CPM cols not in manuscript scope; clean stable column names
> 2. `vw_path_malignant_tumor_safe_VIEW_v1` — uses `canonical_path_malignant_events_dedup_VIEW_v1` (from Lane B); exposes `publication_dedup_rank` + linkage tier + completeness
> 3. `vw_recurrence_safe_VIEW_v1` — uses `canonical_recurrence_resolved_v1` with `is_implausible_date_quarantine=FALSE` filter (from Lane C); preserves dual-track recurrence
> 4. `vw_molecular_safe_VIEW_v1` — uses `canonical_molecular_genetics_v2` with `is_patient_level_only_evidence` flag exposed (from Lane D)
> 5. `vw_fna_safe_VIEW_v1` — wraps `canonical_fna_events_v1` clean Bethesda + date cols
> 6. `vw_us_nodule_safe_VIEW_v1` — uses `vw_us_nodule_tirads_any_reported_VIEW_v1` (from Lane E4)
> 7. `vw_labs_long_safe_VIEW_v1` — UNION of 5 per-analyte canonical labs into one long table (`research_id`, `lab_analyte`, `lab_date`, `value_numeric`, `unit`)
> 8. `vw_cohort_membership_safe_VIEW_v1` — uses `manuscript_cohort_v1` (from Lane A) with `release_id` join
>
> **Step 4 — Register all in canonical_table_signoff_registry_v1** + col_registry per mig_205 retro-signoff pattern
>
> **Step 5 — Pre-snapshot signoff_registry + provenance row**
>
> **Acceptance:**
> - `semantic_publication` schema exists with `release_manifest_v1` table populated for v1.0
> - 8 vw_*_safe_VIEW_v1 views exist + registered as verified
> - Each view's row count matches expected (e.g., vw_path_malignant_tumor_safe = 5,944 from Lane B; vw_recurrence_safe excludes the 132 quarantined; etc.)
> - Cowork verification suite stays clean
>
> **Memory note:** `project_semantic_publication_layer_20260430.md` documenting the manuscript-safe read path.
>
> Commit + push.

---

## Future Tasks (Cowork to execute when v1.0 cleanup is complete)

These are deferred per Logan H + I decisions. Add to a `TASKS.md`-style backlog and execute when current cleanup waves finish.

### Future Task H: Power BI star-schema marts (`bi_powerbi.*`)
- **Trigger:** When Logan starts on Phase 4 actual Power BI Desktop migration
- **Scope:** Build 13 dim/fact tables per ChatGPT MD/Power BI doc Priority 2 (`dim_patient_v1`, `fact_surgery_v1`, `fact_pathology_tumor_v1`, etc.)
- **Source:** Reads from `semantic_publication.vw_*_safe_VIEW_v1` (Lane G output)
- **Agent:** TBD when triggered (Cursor composer probably; multi-day star-schema design)
- **Why deferred:** Eats remaining 5 days of MD Pro trial; Phase 4 work not blocking for v1.0 manuscript

### Future Task I: Parquet export of frozen tables
- **Trigger:** AFTER all current cleanup lanes (A-G) finish + Cowork verification suite passes clean
- **Scope:** `EXPORT TO PARQUET` for canonical_*, manuscript_cohort_v1, signoff registries, archive_pub_v1_0 freeze snapshots, semantic_publication.* (when built)
- **Why deferred per Logan:** No point exporting mid-cleanup — would have to re-export after each lane; one comprehensive export when state stabilizes
- **Agent:** Cline Sonnet 4.6 (mechanical EXPORT TO statements)
- **Estimated time:** ~1-2 hours when triggered

---

## Suggested execution order (updated)

| Order | Lane | Agent | Why this order |
|---|---|---|---|
| **Wave 1 (in flight)** | **A** + **B** in parallel | Cursor composer + Cline Sonnet 4.6 | A is biggest; B is quick |
| **Wave 2 (after A)** | **C** + **D** in parallel | Cline Sonnet 4.6 + Cursor composer | Avoid signoff_registry write contention with A |
| **Wave 3** | **E** (E1+E2+E3+E4+E5+E6 combined) | Cursor composer | TIRADS investigation + cohort views; longest single lane |
| **Wave 4** | **F** + **G** in parallel | Cline GPT-5.5 (both) | F=multi-nodule triage; G=semantic layer; touch different tables, safe to parallelize |
| **Wave 5 (post-cleanup)** | **I** Parquet export | Cline Sonnet 4.6 | One comprehensive export after state stabilizes |
| **Future** | **H** Power BI marts | TBD when Phase 4 triggers | Out of v1.0 scope |

After all of A-G land + I exports: re-run Cowork verification suite. Expected end-state: gate1 175 → ~190+ (10 from A + 1 from B + 4 from E4 + 8 from G + maybe more from F absorption rows).

---

## Reference: Cowork-applied this round (mig_209 + mig_210)

**mig_209 — P1 registry reconciliation:** Registered 9 missing `canonical_path_malignant_events_v1` cols (AJCC resolved + dup flag) + 1 `canonical_us_exam_master_VIEW_v2.exam_id_source`; deprecated 8 stale `canonical_invasion_patient_rollup_v1` cols.

**mig_210 — P7 rid 610 fix:** UPDATE `canonical_patient_master.first_surgery_date` 1945-07-13 → 2004-07-13 (matches operative event; only such pre-1990 row in entire 10,871-row cohort).

Both at HEAD `27c3c74` pushed to origin.

---

## Round 2 additions (post-Doc 2 + Doc 3 review, 2026-04-30)

Documents reviewed:
- `motherduck_powerbi_future_data_final_recommendations.md` — Architecture for Power BI / future-data integration
- `us_nodules_tirads_comprehensive_assessment_plan.md` — 5-phase US/TIRADS publication-readiness plan

Logan-locked decisions (Round 2):
- **F2:** Triage multi-nodule under-explosion now (Cline GPT-5.5)
- **G1:** Build semantic_publication schema (Cline GPT-5.5)
- **H:** Defer Power BI marts to post-cleanup (Future Task H)
- **I:** Defer Parquet export until all cleanup done (Future Task I)
- **E-extension:** Add E4 (4 cohort views) + E5 (2,640 conflicts) + E6 (completeness flag) to existing Lane E

ChatGPT verified counts (all real):
- Doc 2 TIRADS: 5/5 QA tables exist; 21,454 vs 5,149 completeness gap real (4×); 2,640 high-pri conflicts (2,494+123+23); 448 candidate exams / 319 patients; 825 deferred patients
- Doc 1 architecture: `semantic_publication` + `bi_powerbi` schemas DO NOT YET EXIST (need to be created); existing schemas are `main`, `manuscript_workspace`, `archive_pub_v1_0` (in attached "Thyroid 2026 UPdated"), `raw`, `views_readable`
