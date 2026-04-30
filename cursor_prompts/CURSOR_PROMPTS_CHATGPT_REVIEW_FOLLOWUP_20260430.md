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
> **Acceptance:**
> - Investigation reports for E1 + E2 committed at `qc_framework_v1/reports/mig_215_tirads_investigation_20260430.md`
> - Any data corrections applied with pre-snapshot
> - Col_registry notes updated for both TIRADS category columns
> - Memory note `feedback_tirads_category_canonical.md` written
> - Cowork verification suite stays clean
>
> Commit + push.

---

## Suggested execution order (parallelizable)

| Order | Lane | Why this order |
|---|---|---|
| 1 (start now) | **A** + **B** in parallel | A is biggest; B is quick — kick both off |
| 2 (after A done) | **C** + **D** in parallel | Can't overlap A signoff_registry writes |
| 3 (last) | **E** | Most investigation; do after the simpler lanes confirm tooling works |

After all 5 land: re-run Cowork verification suite. Expected: gate1 175 → 186 (10 from A + 1 from B), all gates stay clean, §12=0, §14=0.

---

## Reference: Cowork-applied this round (mig_209 + mig_210)

**mig_209 — P1 registry reconciliation:** Registered 9 missing `canonical_path_malignant_events_v1` cols (AJCC resolved + dup flag) + 1 `canonical_us_exam_master_VIEW_v2.exam_id_source`; deprecated 8 stale `canonical_invasion_patient_rollup_v1` cols.

**mig_210 — P7 rid 610 fix:** UPDATE `canonical_patient_master.first_surgery_date` 1945-07-13 → 2004-07-13 (matches operative event; only such pre-1990 row in entire 10,871-row cohort).

Both at HEAD `27c3c74` pushed to origin.
