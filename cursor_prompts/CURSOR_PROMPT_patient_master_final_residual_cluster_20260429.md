# Cursor Agent Task — `canonical_patient_master` FINAL RESIDUAL CLUSTER (Protocol v2)

**Generated:** 2026-04-29 (Cowork session, post-mig_149/150/151b/158)
**Working dir:** `/Users/ros/THyroid 2026`
**Tip of `origin/main`:** `git fetch && git pull` before starting (current tip `0b4f485`)
**Estimated effort:** 2-3 hours (~27 cols)
**Run order:** Lane 47 of next 4-prompt batch (mig_159)

---

## 0. Cleanliness & safety preamble (MUST READ)

Same governance rules as the prior batch (mig_154-157):
1. Verification methods name LIVE `main.*` tables only (pre-check `information_schema.tables`).
2. **AGENTS governance** — agent commits SQL only; do NOT write to MotherDuck. Logan/Cowork applies after independent verification. (mig_155 violated this in the prior batch — explicit reminder.)
3. Cohort-uniformity sweep BOTH directions on every BOOLEAN.
4. Pre-snapshot registry rows to `archive_pub_v1_0`.
5. Surgical git add — explicit paths only.
6. Clinical event dates MUST be DATE (not TIMESTAMP/VARCHAR).
7. Numeric measurements MUST be DOUBLE (not VARCHAR-with-units).

Lane-specific risk: this is the **final** residual cluster. Logan-reviewed scope; do not expand beyond the listed 27 cols. After this lane lands, PM should be at ~99-100% verified+na (with mig_152/154/156/157 also landed), enabling the PM finalization migration (Lane 50 mig_162).

---

## 1. Goal

Verify the **final residual cluster** — 27 unverified cols on `canonical_patient_master` that don't fit any prior thematic lane. Covers single-gene molecular markers, completion thyroidectomy, bilateral disease flags, stimulated Tg + anti-Tg, laryngoscopy timing, laterality, and a small misc.

### 1a. Pre-flight probe (must return exactly 27)

```sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_catalog='thyroid_canonical_publication_v1_0' AND table_schema='main'
  AND table_name='canonical_patient_master'
  AND column_name IN (
    -- Molecular single-gene markers (10)
    'alk_positive_v7','completion_braf_positive','completion_tert_positive','eif1ax_positive',
    'hras_positive_v11','kras_positive_v11','nras_positive_v11','ntrk_positive_v7',
    'pax8_pparg_positive','tp53_positive_v7',
    -- Completion thyroidectomy clinical (2)
    'completion_reason','completion_reason_confidence',
    -- Bilateral disease (2)
    'bilateral_disease_flag','bilateral_path_flag',
    -- Stim Tg + anti-Tg + Tg span (7)
    'anti_tg_nadir','anti_tg_rising_flag','max_stimulated_tg','max_stimulated_tg_date',
    'max_stimulated_tg_source','max_stimulated_tg_source_note_ref','days_first_to_last_tg',
    -- Laryngoscopy timing (2)
    'days_to_first_laryngoscopy','days_to_last_laryngoscopy',
    -- Misc (4)
    'date_traceability_status','laterality','r_class_true','total_ln_positive_v10'
  )
ORDER BY column_name;
```

Confirm count is **exactly 27**. All must be `verification_status='not_started'`.

### 1b. Sub-clusters

- **159a — Molecular single-gene markers (10 cols):** All BOOLEAN. SSOT: `canonical_molecular_genetics_v2` (verified). Check the gene field for matching mutations.
- **159b — Completion thyroidectomy (2 cols):** `completion_reason` VARCHAR (categorical), `completion_reason_confidence` DOUBLE. SSOT: `canonical_operative_*` and/or LLM extraction of completion-thyroidectomy notes.
- **159c — Bilateral disease (2 cols):** Both BOOLEAN. SSOT: `canonical_path_malignant_patient_rollup_v1` (laterality field) + `canonical_us_thyroid_*` for clinical bilateral.
- **159d — Stim Tg + anti-Tg (7 cols):** SSOT: `canonical_labs_thyroglobulin_v1` (Tg+TgAb shared per `project_lab_consolidation_script_347_closeout.md`). Note: `max_stimulated_tg_date` is DATE (already correct). `anti_tg_nadir` is DOUBLE.
- **159e — Laryngoscopy timing (2 cols):** Both BIGINT (days). SSOT: `canonical_complications_events_v1` (mig_98c voice/nerve cluster) — the laryngoscopy events have timing_days. Compute `MIN/MAX(timing_days)` per patient.
- **159f — Misc (4 cols):** `date_traceability_status` VARCHAR (likely a single-value provenance flag — value-degenerate audit), `laterality` VARCHAR (left/right/bilateral/unknown — should match path SSOT), `r_class_true` VARCHAR (margin R-classification truth — should match `margin_r_class_v10` from mig_154 within tolerance), `total_ln_positive_v10` INTEGER (should match `tp_ln_positive` from mig_150 within tolerance — cross-check expected).

---

## 2. Methodology

### 2a. SSOT pre-check

```sql
SELECT table_name FROM information_schema.tables
WHERE table_schema='main' AND (
  table_name LIKE 'canonical_molecular%' OR table_name LIKE 'canonical_path%'
  OR table_name LIKE 'canonical_labs_thyroglobulin%' OR table_name LIKE 'canonical_complications%'
  OR table_name LIKE 'canonical_operative%' OR table_name LIKE 'canonical_us_thyroid%'
) ORDER BY 1;
```

### 2b. Per-sub-cluster derivation rules

**Molecular single-gene markers** — `canonical_molecular_genetics_v2` has a wide schema. For each gene:
- `alk_positive_v7` ← `BOOL_OR(gene='ALK' AND result IN ('positive','fusion_positive'))` from molecular events
- `kras_positive_v11` ← similar with gene='KRAS'
- v7 vs v11 versions reflect different LLM-extraction passes (per `project_round2_llm_integration_script_386_closeout.md`)
- `completion_braf_positive` is BRAF result on the COMPLETION thyroidectomy specimen specifically (not initial). Verify the join logic.

**Completion thyroidectomy** — `completion_reason` is categorical (e.g., 'malignancy_on_FS', 'high_risk_features', 'patient_request'). `completion_reason_confidence` is LLM extraction confidence DOUBLE 0.0-1.0.

**Bilateral disease** — `bilateral_disease_flag` is clinical (US/imaging) or pathology-derived; `bilateral_path_flag` is path-only. They should NOT be 100% identical (different evidence tiers). If they are, document `CF-mig159-BILATERAL-FLAG-DUP`.

**Stim Tg + anti-Tg**:
- `max_stimulated_tg` ← `MAX(value_numeric) WHERE is_stimulated=TRUE` from `canonical_labs_thyroglobulin_v1`
- `anti_tg_nadir` ← `MIN(value_numeric) WHERE analyte='anti_tg'` (Tg+TgAb shared canonical)
- `anti_tg_rising_flag` ← derived from longitudinal slope
- `days_first_to_last_tg` ← `MAX(lab_datetime) - MIN(lab_datetime)` in days

**Laryngoscopy timing** — from `canonical_complications_events_v1` filtered to laryngoscopy procedure type (or via `note_entities_procedures` with proc_type='laryngoscopy'). `MIN/MAX(timing_days)` per patient.

**Misc**:
- `date_traceability_status` — provenance VARCHAR; check distinct cardinality. If 1-distinct, apply `CF-mig159-VALUE-DEGENERATE-UPSTREAM-date_traceability_status`.
- `laterality` — VARCHAR; cross-check against `canonical_path_malignant_patient_rollup_v1` laterality.
- `r_class_true` — VARCHAR (R0/R1/R2); cross-validate against `margin_r_class_v10` (mig_154 cluster). If `r_class_true` is the post-cleanup truth, document the lineage.
- `total_ln_positive_v10` — INTEGER; cross-validate against `tp_ln_positive` (mig_150 cluster) and `ln_total_positive` (mig_133 cluster). If 3-source discrepancy, open `CF-mig159-LN-POSITIVE-V10-VS-TP-VS-TOTAL`.

### 2c. ⚠️ Cohort-uniformity sweep on 13 BOOLEANs

Required template + decision rules same as prior batch. Watch:
- Single-gene molecular: BRAF ~50-60% TRUE (highest), TERT ~15-25%, RAS family ~5-10%, ALK/NTRK fusions ~1-3%, EIF1AX/PAX8-PPARG ~1-5%, TP53 rare. **0 TRUE on these = degenerate placeholder; reclassify to na.**
- `bilateral_disease_flag` ~10-25% TRUE expected
- `bilateral_path_flag` similar but slightly lower (path-only stricter)
- `anti_tg_rising_flag` ~5-15% TRUE expected
- `completion_*_positive`: can be NULL (no completion specimen) but TRUE/FALSE distribution on the cohort that had completion should mirror initial-specimen rates.

### 2d. ⚠️ Date-type policy (1 col in scope)

`max_stimulated_tg_date` is DATE per data dictionary. Verify it's actually DATE, not TIMESTAMP/VARCHAR. If wrong type, open `CF-mig159-MAX-STIM-TG-DATE-RETYPE`.

### 2e. ⚠️ Cross-source spot-check (REQUIRED)

- Pick 5 rids with `alk_positive_v7=TRUE`. Verify ALK fusion rows in `canonical_molecular_genetics_v2`.
- Pick 5 rids with `total_ln_positive_v10 > 0`. Cross-check `ln_total_positive` (mig_133) and `tp_ln_positive` (mig_150). All three should agree (or document divergence).
- Pick 5 rids with `bilateral_path_flag=TRUE`. Verify pathology shows tumor in both lobes.

---

## 3. Sign-off SQL

File: `qc_framework_v1/migrations/159_patient_master_final_residual_cluster_signoff_20260429.sql`

```
batch_id = 'mig_159_patient_master_final_residual_cluster_20260429'
verified_by = 'logan'
verification_method options:
  derivation_vs_canonical_molecular_genetics_v2 (single-gene markers)
  derivation_vs_canonical_path_malignant_patient_rollup_v1 (bilateral_path_flag, laterality)
  derivation_vs_canonical_labs_thyroglobulin_v1 (Tg/TgAb)
  derivation_vs_canonical_complications_events_v1 (laryngoscopy timing)
  cross_source_resolution_<rule> (r_class_true, total_ln_positive_v10)
  helper_<placeholder>_pending_real_extraction (Type-B/C reclassifications)
```

Sub-blocks (7):
- 159a — Molecular single-gene (10)
- 159b — Completion thyroidectomy (2)
- 159c — Bilateral disease (2)
- 159d — Stim Tg + anti-Tg (7)
- 159e — Laryngoscopy timing (2)
- 159f — Misc (4)
- 159g — Resync `canonical_table_signoff_registry_v1`

### 3a. Pre-snapshot block at top

```sql
CREATE TABLE "Thyroid 2026 UPdated".archive_pub_v1_0.canonical_column_verification_registry_pre_mig159_20260429 AS
SELECT *, CAST(CURRENT_TIMESTAMP AS TIMESTAMP) AS pre_mig159_snapshot_ts
FROM thyroid_canonical_publication_v1_0.main.canonical_column_verification_registry_v1
WHERE table_name='canonical_patient_master' AND column_name IN (<27 cols>);
```

---

## 4. Required CFs

- `CF-mig159-COHORT-UNIFORM-FALSE-<col>` — list each near-uniform-FALSE
- `CF-mig159-COHORT-NEAR-UNIFORM-TRUE-<col>` — Type-A presence flags
- `CF-mig159-VALUE-DEGENERATE-UPSTREAM-<col>` — single-value VARCHARs
- `CF-mig159-MOLECULAR-V7-V11-DRIFT` — drift between v7 and v11 versioned single-gene cols
- `CF-mig159-LN-POSITIVE-V10-VS-TP-VS-TOTAL` — 3-source LN positive count drift
- `CF-mig159-R-CLASS-TRUE-VS-V10-DIVERGENCE` — drift between r_class_true and margin_r_class_v10
- `CF-mig159-BILATERAL-FLAG-DUP` — open if bilateral_disease_flag = bilateral_path_flag 100%
- `CF-mig159-MAX-STIM-TG-DATE-RETYPE-CLEAR` or `-OPEN`

---

## 5. Apply + verify (Logan-only after Cowork independent verification)

DO NOT run UPDATEs against `thyroid_canonical_publication_v1_0`. Stop at "SQL file committed + pushed". This is the same governance the mig_154/156/157 prompts followed; the agent that did mig_155 violated this.

---

## 6. Git workflow

```bash
git add qc_framework_v1/migrations/159_patient_master_final_residual_cluster_signoff_20260429.sql
git -c user.name="Logan Glosser" -c user.email="logan.glosser@gmail.com" commit -m "qc: mig_159 CPM final residual cluster sign-off (27 cols)"
git push origin main
```

---

## 7. Done definition

- [ ] Pre-flight probe returns exactly 27
- [ ] All 27 cols flipped (verified or na)
- [ ] Cohort-uniformity sweep documented for ALL 13 BOOLEANs
- [ ] Cross-source LN positive count reconciliation in migration header (3 sources: ln_total / tp_ln / total_v10)
- [ ] Molecular-versioning drift report (v7 vs v11)
- [ ] Pre-snapshot in archive_pub_v1_0
- [ ] No verification_method strings name dead/archived tables
- [ ] SQL file committed + pushed; NO MD writes from agent
