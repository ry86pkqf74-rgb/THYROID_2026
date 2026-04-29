# Cursor Agent Task — `canonical_patient_master` FRAMEWORK + PROVENANCE CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_142b)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting
**Estimated effort:** 4-5 hours (~71 cols — biggest of next batch)
**Run order:** Lane 45 of next 4-prompt batch (mig_156)

---

## 0. Cleanliness & safety preamble (MUST READ)

Read §0 of the pathology_invasion prompt — same governance rules. Lane 45-specific risk: this lane bundles framework / aggregate / cross-domain cols. **Many will be tempting to mark `extraction_faithfulness` against a non-existent SSOT** (mig_151 mistake). For framework cols, the verification method should usually be:
- `derivation_vs_<source_canonical>` if it's a true rollup
- `cross_domain_aggregation_<rule>` for `any_*` overlap cols
- `helper_script_<N>_provenance_passthrough` for `gm_*` / `provenance_*` / `cpm_*` audit-trail cols
- `auto_provenance_skip` (na) for build-timestamp cols

**Never** name a non-existent live table.

---

## 1. Goal

Verify the **framework + provenance cluster** — 71 unverified cols on `canonical_patient_master` covering counts (`n_*`), patient-rule-master rollups (`prm_*`), generic-metadata provenance (`gm_*`), cross-domain ANY-overlap markers (`any_*`), `has_*` placeholder flags, and audit-trail metadata (`rollup_*`, `provenance_*`, `cpm_*`, `source_*`, `is_*`, `cross_*`, `analysis_*`, `longitudinal_*`).

### 1a. Pre-flight probe (must return exactly 71)

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    -- N counts
    'n_confirmed_complications','n_fna_cytology_records','n_fna_episodes',
    'n_notes_documenting_tsh_suppressed','n_stimulated_tg_measurements',
    'n_surgeries','n_surgeries_source','n_surgeries_v2',
    'n_tg_measurements_structured','n_tgab_measurements',
    'n_us_exams','n_us_exams_v2','n_us_nodules_total','n_us_nodules_total_v2','n_us_with_ln_assessment',
    -- PRM rules
    'prm_first_fna_date','prm_first_fna_days_from_surg','prm_fna_n_sources','prm_fna_source_tables',
    'prm_high_risk_marker_any','prm_hypocalcemia_lab_flag','prm_hypoparathyroidism_lab_flag',
    'prm_last_fna_date','prm_last_fna_days_from_surg','prm_margin_confidence','prm_margin_source',
    'prm_molecular_risk_category','prm_n_recurrence_sources','prm_recurrence_detection_category',
    'prm_rln_worst_grade','prm_size_concordance','prm_structural_disease_flag',
    -- GM provenance
    'gm_lab_completeness_score','gm_macis_calculable_flag','gm_path_lvi_raw','gm_path_pni_raw',
    'gm_path_vascular_inv_raw','gm_provenance_confidence','gm_recurrence_date_source',
    'gm_recurrence_site_primary','gm_recurrence_source','gm_recurrence_type_primary',
    'gm_tg_below_threshold_ever',
    -- ANY overlap
    'any_airway_anywhere','any_analysis_eligible_complication','any_capsular_anywhere',
    'any_confirmed_complication','any_confirmed_complication_flag','any_disease_concern_flag',
    'any_fusion_positive','any_fusion_positive_inferred_negative','any_lymphatic_microscopic_anywhere',
    'any_perineural_anywhere','any_recurrence_flag','any_soft_tissue_anywhere','any_vascular_microscopic_anywhere',
    -- HAS / IS / CROSS
    'has_low_calcium_flag','has_low_pth_flag','has_suspicious_candidate','has_voice_data',
    'is_malignant','cross_fna_concordance',
    -- Provenance metadata
    'analysis_eligible_flag','cpm_built_at','longitudinal_assessment_available',
    'provenance_confidence','provenance_note',
    'rollup_built_at','rollup_script_version','rollup_source_table','source_script'
  )
ORDER BY column_name;
```

Confirm count is **exactly 71**.

### 1b. Sub-clusters (8 sub-blocks)

- **156a — N counts (15 cols):** all `n_*` cols above
- **156b — PRM rules (17 cols):** all `prm_*`
- **156c — GM provenance (11 cols):** all `gm_*`
- **156d — ANY overlap (13 cols):** all `any_*`
- **156e — HAS placeholder flags (4 cols):** has_low_calcium_flag, has_low_pth_flag, has_suspicious_candidate, has_voice_data
- **156f — Singletons (3 cols):** is_malignant, cross_fna_concordance, analysis_eligible_flag
- **156g — Build provenance (8 cols):** cpm_built_at, longitudinal_assessment_available, provenance_confidence, provenance_note, rollup_built_at, rollup_script_version, rollup_source_table, source_script

---

## 2. Methodology

### 2a. SSOT pointers (verify each lives in `main` first!)

Pre-check:
```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND table_name LIKE 'canonical_%'
ORDER BY 1;
```

Cross-domain SSOTs you'll need: `canonical_recurrence_v1`, `canonical_complications_events_v1`, `canonical_fna_events_v1`, `canonical_invasion_events_v1`, `canonical_path_malignant_*`, `canonical_us_thyroid_*`, `canonical_molecular_genetics_v2`, `canonical_labs_*`, `canonical_operative_*`.

### 2b. Per-sub-cluster derivation rules

**N counts (`n_*`):** All are simple `COUNT(*) OVER (PARTITION BY research_id)` aggregations from a specific source. Verify the source for each col matches the suffix (e.g., `n_us_exams` from US imaging master; `n_fna_episodes` from canonical_fna_events_v1).
- Special: `n_surgeries_source` is VARCHAR (provenance label, not a count) — verify it's a single-value or low-cardinality source-string.
- `*_v2` cols are versioned — should match the non-v2 col when both populated; document `CF-mig156-N-COUNT-V1-V2-DRIFT` for any rid-level mismatch.

**PRM rules (`prm_*`):** PRM = "Patient Rule Master" — derived rule outputs. These are computed from cross-domain logic.
- `prm_*_flag` BOOLEANs: simple OR-rules across upstream signals
- `prm_*_category` VARCHARs: mapped category per rule
- `prm_first/last_fna_date` and `prm_first/last_fna_days_from_surg`: from canonical_fna_events_v1; **clinical event dates MUST be DATE** (calendar policy — see `feedback_clinical_dates_calendar_only.md`).
- `prm_size_concordance`: cross-source size agreement metric (US vs path vs imaging) — verify methodology.

**GM provenance (`gm_*`):** GM = "Generic Metadata" / passthrough provenance.
- `gm_*_raw` cols: raw passthrough — verify they're passthrough not derived
- `gm_recurrence_*`: provenance for the recurrence chain — should align with canonical_recurrence_v1 fields
- `gm_lab_completeness_score`: numeric 0-1 score; verify formula
- `gm_provenance_confidence`: confidence string

**ANY overlap (`any_*`):** Cross-domain "presence anywhere" booleans (e.g., `any_capsular_anywhere` = capsular invasion in path OR synoptic OR LLM extraction). Verify the OR logic:
```sql
-- Template for any_capsular_anywhere
SELECT pm.research_id, pm.any_capsular_anywhere,
       CASE WHEN inv.has_capsular OR pm_path.capsular_path_present THEN TRUE ELSE FALSE END AS expected
FROM main.canonical_patient_master pm
LEFT JOIN (SELECT research_id, BOOL_OR(invasion_type='capsular' AND finding_status='present') AS has_capsular FROM main.canonical_invasion_events_v1 GROUP BY 1) inv USING (research_id)
LEFT JOIN main.canonical_path_malignant_patient_rollup_v1 pm_path USING (research_id);
```
**Critical:** if any `any_*` col disagrees with the union for >50 patients, that's a derivation bug.

**HAS placeholder flags (`has_*`):** These are placeholder cols that may be Type-A (presence flag, NULL=no signal) or Type-B (degenerate). Run cohort-uniformity sweep first.
- `has_low_calcium_flag` / `has_low_pth_flag`: cross-validate against `postop_low_calcium_flag` / `postop_low_pth_flag` (mig_150 cluster). If 100% identical, document `CF-mig156-HAS-FLAG-DUP-OF-POSTOP`.
- `has_voice_data`: presence flag for voice/RLN data — likely Type-A.
- `has_suspicious_candidate`: surveillance / FNA candidate flag.

**Singletons:** `is_malignant` should be ~100% TRUE (cohort is thyroid cancer). Verify against canonical_path_malignant. `cross_fna_concordance` is cross-source FNA agreement metric.

**Build provenance (`cpm_built_at`, `rollup_*`, `provenance_*`, `source_script`):** All audit/provenance timestamps + metadata. `*_built_at` are TIMESTAMP — allowlist OK. `*_version` / `*_script` likely single-value VARCHARs — apply VALUE-DEGENERATE-UPSTREAM CF if 1 distinct.
- These can mostly be `auto_provenance_skip` (na) or `helper_script_<N>_provenance_passthrough` (verified informational).

### 2c. ⚠️ Cohort-uniformity sweep (REQUIRED — both directions)

This lane has many BOOLEANs (~30). Run sweep on every one. Watch:
- `prm_*_flag` should mostly be MIXED (real rules with both TRUE and FALSE)
- `any_*` should be HIGHER-TRUE than the individual sources (since OR aggregates)
- `gm_*` flags should be MIXED
- `is_malignant` ≈ 100% TRUE (cohort definition) — Type-A invariance, keep verified informational

### 2d. ⚠️ Date-type policy

- `prm_first_fna_date` / `prm_last_fna_date` MUST be DATE (clinical event dates)
- `cpm_built_at` / `rollup_built_at` TIMESTAMP allowlist OK (audit)
- Open `CF-mig156-PRM-FNA-DATE-RETYPE` if VARCHAR/TIMESTAMP

### 2e. ⚠️ Single-value placeholder audit

For every VARCHAR col, count distinct values. If 1 distinct, apply `CF-mig156-VALUE-DEGENERATE-UPSTREAM-<col>` informational note (don't reclassify; placeholder provenance has its uses). Likely candidates: `n_surgeries_source`, `rollup_script_version`, `rollup_source_table`, `source_script`, `provenance_note`.

### 2f. Cross-source spot-check (REQUIRED)

- Pick 5 random rids with `any_recurrence_flag=TRUE`. Verify `canonical_recurrence_v1.recurrence_confirmed=TRUE` for those rids.
- Pick 5 rids with `prm_high_risk_marker_any=TRUE`. Verify the underlying components.
- Pick 5 rids with `n_fna_episodes > 0`. Verify count against `canonical_fna_events_v1` GROUP BY research_id.

---

## 3. Sign-off SQL

File: `qc_framework_v1/migrations/156_patient_master_framework_provenance_cluster_signoff_20260429.sql`

```
batch_id = 'mig_156_patient_master_framework_provenance_cluster_20260429'
verification_method options:
  derivation_vs_canonical_<X> (use the appropriate live canonical for each col)
  cross_domain_aggregation_<rule> (for any_* OR-rules)
  helper_script_<N>_provenance_passthrough (for gm_/cpm_/rollup_/provenance_)
  auto_provenance_skip (na — for build_ts / extracted_at / *_built_at)
  internal_consistency_<rule> (for prm_* cross-rule cols)
  helper_<placeholder>_pending_real_extraction (for Type-B/C reclassifications)
```

Sub-blocks (8):
- 156a — N counts (15 cols)
- 156b — PRM rules (17 cols)
- 156c — GM provenance (11 cols)
- 156d — ANY overlap (13 cols)
- 156e — HAS flags (4 cols)
- 156f — Singletons (3 cols)
- 156g — Build provenance (8 cols)
- 156h — Resync `canonical_table_signoff_registry_v1`

### 3a. Pre-snapshot block at top

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig156_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig156_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE table_name='canonical_patient_master' AND column_name IN (<71 cols>);
```

---

## 4. Required CFs

- `CF-mig156-COHORT-UNIFORM-FALSE-<col>` — list each near-uniform-FALSE
- `CF-mig156-COHORT-NEAR-UNIFORM-TRUE-<col>` — list each Type-A presence flag
- `CF-mig156-VALUE-DEGENERATE-UPSTREAM-<col>` — list each 1-distinct-value VARCHAR
- `CF-mig156-N-COUNT-V1-V2-DRIFT` — drift between `n_*` and `n_*_v2` siblings
- `CF-mig156-HAS-FLAG-DUP-OF-POSTOP` — open if has_low_calcium_flag = postop_low_calcium_flag 100%
- `CF-mig156-ANY-VS-COMPONENT-DRIFT` — open per any_* col with >50 rid drift vs OR-rule
- `CF-mig156-PRM-FNA-DATE-RETYPE` — clinical event dates must be DATE
- `CF-mig156-IS-MALIGNANT-COHORT-INVARIANT` — informational: is_malignant ≈ 100% (cohort definition)
- `CF-mig156-CPM-VS-ROLLUP-BUILT-AT-DRIFT` — open if `cpm_built_at` and `rollup_built_at` differ by >1 day (build-pipeline incoherence)

---

## 5. Apply + verify (Logan-only after Cowork independent verification)

Same as Lane 43 §5. NO MD writes from agent.

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/156_patient_master_framework_provenance_cluster_signoff_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_156 CPM framework + provenance cluster sign-off (71 cols)"
git push origin main
```

---

## 7. Done definition

- [ ] Pre-flight probe returns exactly 71
- [ ] All 71 cols flipped (verified or na)
- [ ] Methodology distribution clean — no `_misc` placeholders
- [ ] Cohort-uniformity sweep documented for ALL ~30 BOOLEANs
- [ ] ANY-vs-component drift report in migration header (each `any_*` col validated)
- [ ] Single-value VARCHAR audit complete (each 1-distinct col CF-tagged)
- [ ] Pre-snapshot created in archive_pub_v1_0
- [ ] No verification_method strings name dead/archived tables
- [ ] SQL file committed + pushed; NO MD writes from agent
