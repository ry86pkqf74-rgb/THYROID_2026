# CPM BigQuery-native rebuild — Phase 1 (analysis & planning only)

**Date:** 2026-05-14  
**Scope:** `canonical_patient_master` (CPM) — full lineage map, input inventory, coarse column provenance, staged port plan.  
**Hard rule this phase:** no table builds, no BigQuery writes, no MotherDuck mutations.

**Cohort invariant (publication):** `scripts/_md_connect.connect_locked()` requires exactly **10,871** rows and **10,871** distinct `research_id` on `thyroid_canonical_publication_v1_0.main.canonical_patient_master`.

---

## Task 1 — Full CPM lineage DAG (dependency order)

### 1.1 Upstream spine: raw / structured inputs → `gold_master_patient_facts_v1`

| Node | Inputs (representative) | Output | Row set / column block |
|------|-------------------------|--------|------------------------|
| Institutional Excel + DB extracts | `raw/*.xlsx`, `processed/*.parquet`, DVC-tracked parquets, `exports/*` rebuild artifacts | Base MD tables: `path_synoptics`, `tumor_pathology`, `operative_episode_detail_v2`, `fna_*`, `imaging_*`, `clinical_notes_long`, `molecular_*`, `longitudinal_lab_canonical_v1`, `thyroglobulin_lab_canonical_v1`, etc. | Multi-table, multi-grain; keyed by `research_id` |
| Resolved analysis layer | Scripts **48–55** family (`patient_analysis_resolved_v1`, `episode_*`, `lesion_*`, linkage v3, scoring, complications, labs) | **`patient_analysis_resolved_v1`** (~12,886-row spine in dev docs; publication may subset) | Wide per-patient resolved fields |
| **Gold alias / gold table** | On at least one MotherDuck catalog, **`gold_master_patient_facts_v1`** is deployed as **`CREATE VIEW … AS SELECT * FROM patient_analysis_resolved_v1`** (see `exports/md_migration_20260415.duckdb.views.sql` and query-history export under `studies/20260407_live_publication_signoff_reaudit/`). On **eras** account, 221 docstring describes `gold_master_patient_facts_v1` as **146-column** table merged from glosser parquet + eras-only columns — treat as **environment-specific** until a single BQ-native definition is chosen. | **`gold_master_patient_facts_v1`** | **Spine demographics, surgery, pathology rollups, labs, RAI flags, eligibility** — whatever columns exist in the underlying object |

### 1.2 Canonical sub-models feeding assembly (same era as 200-series)

Built **before / alongside** `canonical_patient_master_v1` assembly; all join on `research_id`:

| Script / artifact | Inputs | Output | CPM columns (initial blocks) |
|-------------------|--------|--------|------------------------------|
| **200** `_canonical_diagnosis_standardization` | `gold_master_patient_facts_v1`, `tumor_pathology`, `path_synoptics` | `canonical_diagnosis_unified_v1` | `is_malignant`, `diagnosis_*`, `n_tumors` |
| **201** `_canonical_survival_followup` | Survival feeders | `canonical_survival_followup_v1` | Follow-up / vital / last contact |
| **202** `_canonical_molecular_tested` | Molecular episodes + gold | `canonical_molecular_tested_v1` | Molecular tested flags, platforms |
| **203** / **203b** recurrence | Recurrence SSOT tables | `canonical_recurrence_v1` | Recurrence flags, type, dates, TTR |

### 1.3 `gold_master_patient_facts_v1` → `canonical_patient_master_v1` (scripts **204** / **205**)

| Node | Inputs | Output | Column blocks |
|------|--------|--------|---------------|
| **204** `204_canonical_master_assembly.py` | **Spine:** `gold_master_patient_facts_v1`; **joins:** `canonical_diagnosis_unified_v1`, `canonical_recurrence_v1`, `canonical_survival_followup_v1`, `canonical_molecular_tested_v1` | **`canonical_patient_master_v1`** (first wide table; DB historically `thyroid_ete_fix_20260413`) | **~96-column** base: demo, surgery, diagnosis, path features, AJCC/ATA/MACIS, LN, Bethesda, imaging, molecular, RAI, Tg trajectory, recurrence, complications, operative flags |
| **205** `205_canonical_consolidation.py` | 204 output + `tirads_llm_extracted_v2` (copy from share), `fna_cytology`, `tumor_pathology`, `ultrasound_reports`, `patient_refined_master_clinical_v12` | **Rebuild `canonical_patient_master_v1`** | **FNA-path** columns, **multi-era Bethesda**, **combined TIRADS**, **imaging LN**, enhanced **tp_ln_** |

### 1.4 Expansion / integration on glosser path (200‑series continued)

Documented in `scripts/213_data_dictionary.py` **Script Lineage** table:

| Script | Action | Column direction |
|--------|--------|------------------|
| **207** | Expansion 125 → ~362 cols | gold / PRM / CT / NM / imaging summary / scoring / TIRADS |
| **208** | LN master rollup | `ln_rollup_*`, `ln_level_*` |
| **211** | Gap-fill from extracted/episode tables | ~129 cols: complications, RLN, ETE, postop labs, RAI, recurrence, survival, molecular variants |
| **212** | NLP rollup | `nlp_*` from `note_entities_*` |
| **214** | Final structured integration | `gm_*`, `prm_*`, `syn_*`, `lab_*`, `us_*` prefixed blocks |
| **215** | Deep NLP | `op_nlp_*`, `med_nlp_*`, `pmhx_*`, `proc_nlp_*`, etc. |
| **217** | Lab recovery + LN | Rebuilds **`canonical_patient_master_v1`** with lab/LN integration |

### 1.5 Bridge to publication DB: **`canonical_patient_master_v221`** → `thyroid_canonical_publication_v1_0.main.canonical_patient_master`

| Node | Inputs | Output | Notes |
|------|--------|--------|-------|
| **221** family (e.g. `221_eras_canonical_sync.py`) | Parquet export `scripts/output/parquet_backup/canonical_patient_master_v1.parquet` + merges from eras `gold_master` / `canonical_patient_master_v218` | **`canonical_patient_master_v221`** on **`Thyroid 2026 UPdated`** (eras) | Adds **days_from_surgery** temporal columns, **multi-surgery** linkage columns, merges eras-only columns |
| **223** `223_publish_canonical.py` | Source: `"Thyroid 2026 UPdated".main.canonical_patient_master_v221` | **CTAS** into `thyroid_canonical_publication_v1_0.main.canonical_patient_master` (+ companion tables) | **Primary promotion** of wide rowset into **publication** DB; Phase 1 also ingests `raw/` + `exports/` tables per `INGEST_SPEC` |
| **224** `_compare_canonical_versions` / **225** `_promote_canonical_version` | RC vs release governance | Versioned DB names | Not a column writer; gates promotion |

### 1.6 Post-promotion mutators on **MotherDuck** `thyroid_canonical_publication_v1_0.main.canonical_patient_master`

**Table swap / major blocks**

| Step | Script / SQL | Inputs | Mutates |
|------|--------------|--------|---------|
| **230** | `230_path_synoptic_rollup.py` + SQL | `synoptic_tumor_long_v1` | Fixes margin/LVI/multifocal in **`canonical_patient_master_v221`** path (see handoff MD); rolls up to `patient_tumor_rollup_v1` |
| **231** | `231_update_canonical_master.py` + `231_update_canonical_master.sql` | `patient_tumor_rollup_v1`, current CPM | **Swap** to `canonical_patient_master_v222` then rename; adds **`r_class_true`**, **`lvi_ordinal_worst`**, **`multifocal_flag_path`**, **`n_tumors_path`**, **`tumor_size_cm_max`**, invasion ordinals, **histology aggregates**, etc. |
| **233** family | ETE adjudication, phase0 inventory (`233_apply_ete_adjudication.py`, `233_phase0*.py`) | ETE resolved tables | **ETE / staging**-related CPM columns (per phase docs) |
| **236** | `236_canonical_finalization.py` | `complication_phenotype_v1`, entity map | **`comp_*_days_postop_v2`**, VC paralysis/paresis recalibration, **`nlp_path_multifocal_concordance_v2`**, archives `*_pre235_backup` |
| **234** | `234_rai_tg_cleanup_db_hygiene.py` | RAI / Tg tables | Hygiene touching **CPM-linked** surfaces (see script inventory list including `canonical_patient_master_v221`) |

**Python migration scripts** (non-exhaustive; grep-backed inventory):

| Script | Column / topic area |
|--------|---------------------|
| `mig_255_recurrence_flag_timing.py` | `time_to_recurrence_days`, `any_recurrence_flag` |
| `mig_257_followup_post_death.py` | `followup_years` vs survival clamp |
| `mig_263_ajcc_overlay_collapse.py` | AJCC stage overlay |
| `mig_264b_bethesda2_obvious_fix.py` | Bethesda fields |
| `mig_271_niftp_ajcc_sweep.py` | AJCC NULL sweep NIFTP / FA |
| `mig_275_m038_surgical_complexity.py` | ADD columns + complexity rollup |
| `mig_281_nlp_promotion.py` | PMH / vascular NLP promotion columns |
| `mig_313_m_stage_corruption_fix.py` | **M-stage**, `distant_mets_proxy` repair, ATA risk |
| `271b_laterality_normalization.py` | Laterality normalization columns + updates |
| `271_tirads_imaging_finalization.py` | Imaging / TIRADS finalization |
| `87_op_nlp_numeric_rollup.py` | Operative NLP numeric rollups |
| `runpod_402_tirads_granular_qwen25_rerun.py` | TIRADS granular columns |

**`qc_framework_v1/migrations/*.sql`** — many create **`canonical_patient_master_pre_<mig>*`** archives on `"Thyroid 2026 UPdated".archive_pub_v1_0` then **`UPDATE main.canonical_patient_master`**. Examples (not complete): **95**, **139**, **145b**, **158**, **163b**, **172b**, **173**, **174b**, **176b**, **177b/c**, **184**, **188/188b**, **210**, **252**, **253**, **254**, **259**, **269**, **277**, **287**, **288**, **294b**, **320** family (cited in mirror docs), **331c** (Python feeder from `operative_episode_detail_v2`).

### 1.7 BigQuery-only hops (post `bq load` mirror)

| Artifact | Role | Mutates / adds | Notes |
|----------|------|----------------|-------|
| **`bq_replicate_canonical_patient_master.py`** | Export MD → Parquet → **`bq load --replace`** | Full table replace | **Not** column-level derivation |
| **`sql/mig_079_emr_demographics_import.sql`** | Staging + **UPDATE** race/sex + `demo_*` | Demo fields only | Template; pre-image `pub_signoff.canonical_patient_master_pre_mig079` |
| **`sql/mig_079_operator_emr_cohort_demo_apply.sql`** | **Tracked twin** of mig_079 (avoids `.gitignore` `*_demographics_*` / `*demographics_import*`) | Same | Committed 2026-05-14 Phase 1 |
| **`bq_migrations/mig_080_h2_preop_rln_vc_columns.sql`** | ALTER + UPDATE | **Preop RLN / VC** columns |
| **`bq_migrations/mig_082_mig004_vc_finding_source_20260506.sql`** | ALTER + UPDATE | **VC finding / source** fields |
| **`sql/mig_081_mig003_vc_paresis_bq_update_20260506.sql`** | UPDATE | VC paresis-related fields |
| **`bq_migrations/mig_088_sistrunk_procedure_cpm_bq_20260506.sql`** + **`scripts/mig_322_sistrunk_procedure_*.py`** | DDL + data apply | **Sistrunk / procedure** NLP columns |
| **`qc_framework_v1/migrations/334_op_nlp_followup_rollups_20260506.sql`** | ALTER + UPDATE on **BQ** `pub_canonical.canonical_patient_master` | **Operative NLP follow-up** rollups |
| **`qc_framework_v1/migrations/45c_canonical_patient_master_v1_1_refresh.sql`** | **`CREATE OR REPLACE VIEW`** | **`canonical_patient_master_v1_1`** | **Not** CPM base table — **VIEW** over CPM **LEFT JOIN** `canonical_fna_patient_rollup_v1_1`; adds **strict-preop Bethesda** columns (see file header) |
| **`Mo36_v4/migrations/mig_cw_005_*`** | UPDATE | `aggressive_variant_flag` corrections | Mo36 lane |

**Parity warning:** After a full BQ replace-load, any column added only by historical BQ `ALTER`/`UPDATE` may be **absent** from MD until replayed (`bq_replicate_canonical_patient_master.py` docstring).

---

## Task 2 — Source-input inventory

### 2.1 In-repo (tracked) — CPM ultimately depends on

- **Raw / semi-raw:** `raw/*.xlsx`, `raw/*.csv` (subset tracked per `.gitignore` policy), `exports/**/*parquet`, `processed/*` (many DVC-tracked).
- **Ingest / ETL:** `scripts/113_*`, `scripts/41_*`, `scripts/223` `INGEST_SPEC` (MRI, NSQIP, thyroid weights/sizes, completion linkage).
- **Canonical builders:** scripts **200–217**, **221–236**, **230–234**, `llm_extraction/*`, `qc_framework_v1/migrations/*`.
- **Documentation:** `qc_framework_v1/manuscript/canonical_methods_footnotes/canonical_patient_master.md`, `scripts/213_data_dictionary.py` tier table.

### 2.2 Gitignored / operator-held

- **Token / connection:** `motherduck.local.toml` (and similar) — credentials only.
- **`.gitignore` hazard:** patterns **`*_demographics_*`** (line ~204) and **`*demographics_import*`** (line ~205). Any *new* path containing those substrings is invisible to `git status` unless forced. **`mig_079_emr_demographics_import.sql` matches `*demographics_import*`** (`git check-ignore` confirms) but remains tracked historically.
- **EMR CSV / GCS:** mig_079 expects **`gs://...`** restricted export — **never** commit row-level PHI/PII.

### 2.3 External (analyst-provided) — must land in a **controlled, documented** store

| Asset | Use | Action for BQ-native rebuild |
|-------|-----|------------------------------|
| **Thyroid Patient Demographic Refresh** (e.g. **20250806**) + **Epic** lists | Race/sex/DOB reconciliation, cohort lists | Check into **IRB-approved secure bucket** + **manifest** (counts, file hash, receipt date); reference by URI in migration metadata only |
| **Thyroid Dataset Demographic Data** workbook | Demo coverage | Same |
| **1999–2015 / 2016–2022 / Epic-Alive** patient lists | Cohort validation | Same |
| **Patient with Wrong DOB.csv** | QA | Same |
| **THY-1 EMR export** (`research_id,race,ethnicity,sex,dob`) | `mig_079` | Same |

### 2.4 Recovered operator SQL (version control)

- **Scan result (2026-05-14):** no standalone file matching `*demographics_import*` existed under repo root or Desktop.
- **Remediation:** committed **`sql/mig_079_operator_emr_cohort_demo_apply.sql`** — body aligned with **`sql/mig_079_emr_demographics_import.sql`**, with an audit header; filename avoids `.gitignore` patterns **`*_demographics_*`** and **`*demographics_import*`** (verified via `git check-ignore`).

---

## Task 3 — Column provenance (coarse, ~1,500 columns)

Map by **stage / prefix / thematic cluster** (not column-by-column). Use **`scripts/207_canonical_master_expansion.py` blocks A–X**, **`scripts/214`/`215` prefixes**, and **`scripts/221_eras_canonical_sync.py` `AUTO_COL_DESC`** for NLP / imaging / lab prefixes.

| Stage | Approx. column families | How to identify in CPM |
|-------|-------------------------|------------------------|
| **204 / 205 assembly** | Core demo, surgery, diagnosis, path, AJCC, ATA, MACIS, LN, Bethesda, imaging TIRADS, molecular, RAI, Tg, recurrence, comp, operative | Unprefixed core names; `diagnosis_*`, `tumor_size_cm`, `ete_grade`, … |
| **207 expansion** | `demo_*` provenance; path raw; TIRADS v12; FNA expanded; imaging summaries; CT/NM; ENE; molecular provenance; RAI expanded; labs; recurrence flags; complication detail; scoring flags; `date_traceability_status` | Many **`gm_`**, **`prm_`**, **`ct_`**, **`nucmed_`**, **`comp.`**-derived |
| **208** | `ln_rollup_*`, central/lateral neck | `ln_` prefix clusters |
| **211** | Refined complications, RAI episodes, molecular variants | Mixed; often `rec_`, `mol_`, episode-linked |
| **212** | Note NLP rollups | `nlp_*` |
| **214** | Structured integration | **`gm_*`, `prm_*`, `syn_*`, `lab_*`, `us_*`** |
| **215** | Deep NLP | **`op_nlp_*`, `med_nlp_*`, `pmhx_*`, `pshx_*`, `proc_nlp_*`, `sx_*`, `radtx_*`** |
| **217** | Lab / LN recovery | Updates/rebuild columns in lab/LN cluster |
| **221 eras bridge** | **Temporal** `*_days_from_surg*`, **multi-surgery** (`n_surgeries`, `second_surgery_date`, …), merged **eras-only** columns | Suffix **`_days_from_surg`**; surgery count block |
| **230 / `patient_tumor_rollup_v1`** | Per-tumor true margin/LVI/multifocal | Feeds **231** |
| **231 SQL swap** | **`r_class_true`, `margin_status_true`, `lvi_ordinal_worst`, `multifocal_flag_path`, `n_tumors_path`, `tumor_size_cm_*`** | True rollup columns |
| **233 / ETE adjudication** | ETE / AJCC-related patches | `ete_*`, `ajcc8_*` adjustments |
| **236 finalization** | **`comp_*`**, VC timing, multifocal concordance | `comp_*`, `nlp_path_multifocal_*` |
| **`mig_255`–`313` scripts** | Recurrence timing, follow-up clamp, AJCC collapse, Bethesda fix, NLP promotion, **M-stage** repair | Cross-cutting safety columns |
| **`271b` / imaging finals** | Laterality, TIRADS resolution | `tirads_*`, side-specific flags |
| **`87` / `334` / `331c`** | Operative NLP rollups | **`op_nlp_*`**, numeric rollup columns |
| **QC SQL migrations** | Targeted repairs (LN, imaging, recurrence spine, smoking, etc.) | File-specific; consult each migration header |
| **BQ `mig_080/082/088`, `mig_081`** | Preop VC/RLN, Sistrunk | **`comp_*_preop`**, `ops_preop_*`, **`prm_sistrunk_*`** (exact names per live `INFORMATION_SCHEMA`) |
| **45c** | **Strict-preop Bethesda** | **`canonical_patient_master_v1_1`** view only — `bethesda_*_strict_preop*` |

For **registry-level** verification categories, see `qc_framework_v1/manuscript/canonical_methods_footnotes/canonical_patient_master.md` (generated footnote doc).

---

## Task 4 — Staged BQ-native port plan

### Principles

1. **Single BQ rebuild driver** eventually replaces `MD → Parquet → bq load` for CPM, but **only after** stage-wise parity is demonstrated.
2. Each stage produces a **named scratch table** (`cpm_stage_NN_*`) or dataset partition **PLUS** a **diff report** vs current production CPM (MD snapshot or `pub_canonical.canonical_patient_master`).

### Proposed stages (ordered)

| Stage | MD analogue (script group) | BQ deliverable | Parity check |
|-------|---------------------------|----------------|--------------|
| **S0** | Cohort spine only | `SELECT DISTINCT research_id` from authoritative spine query | **COUNT = 10871**, **DISTINCT = 10871** |
| **S1** | **200–203** | BQ models: diagnosis, survival, molecular-tested, recurrence | Join keys present; row count 10871 per grain; hash sample 200 rids |
| **S2** | **`gold_master` / `patient_analysis_resolved_v1` parity** | Materialize gold-equivalent from BQ silver tables | Column-block hash or typed compare for **gold surrogate** (~150 cols); document **VIEW vs TABLE** split (eras vs glosser) |
| **S3** | **204–205** | Base CPM wide select (pre-207) | Compare against archived MD **`canonical_patient_master_v1`** if snapshot exists; else reconstruct from MD export manifest |
| **S4** | **207–208** | Add expansion + LN rollup | Block-wise compare: **207 blocks A–X** |
| **S5** | **211–215** | NLP + structured integration | Prefix families: **`nlp_*`, `prm_*`, `syn_*`, `op_nlp_*`** |
| **S6** | **217** | Lab/LN recovery | Lab/LN column diffs |
| **S7** | **221 temporal + multi-surgery** | Recompute `*_days_from_surg` + surgery counts | Compare to MD **`canonical_patient_master_v221`** export |
| **S8** | **223 promotion** (if needed only for row identity) | Final spine join = publication shape | **Exact 10871 / 10871** |
| **S9** | **230–231** | Rollups + true margin/LVI/multi | **`patient_tumor_rollup_v1` parity** then **`231` join parity** |
| **S10** | **233–236** | ETE + finalization | `comp_*` timing + VC columns |
| **S11** | **`mig_255` … `mig_313`, `271*`, `87`** | Re-run as **ordered** BQ scripts | One migration-id per diff artifact (`signoff` table) |
| **S12** | **QC framework SQL** | Replay in **documented order** (from `signoff_migration` / git history) | Per-migration row diff ≤ 0 rows threshold unless expected |
| **S13** | **BQ-only deltas** | `mig_080`, `mig_082`, `mig_088`, `334`, `Mo36` | Schema parity vs current BQ `INFORMATION_SCHEMA`; replay if load wipes |

### Final acceptance bar

1. **Row gate:** `COUNT(*) = 10871` AND `COUNT(DISTINCT research_id) = 10871`.
2. **Schema gate:** `INFORMATION_SCHEMA.COLUMNS` vs legacy — **every** column either (a) **value-identical** for all rows, (b) **intentionally deprecated** with signoff, or (c) **documented engine difference** (e.g. FLOAT64 vs DOUBLE rounding) with tolerance rules.
3. **Value gate:** For each column family, **`FULL OUTER JOIN` ON `research_id` with `IS DISTINCT FROM`** — **zero unexpected diffs**; allowed diffs listed in **`cpm_bq_rebuild_diff_registry`** (future artifact).
4. **Invariant replay:** Publish `connect_locked`-equivalent assertion as a **BQ scheduled query** or **CI check** blocking downstream publishers.
5. **View parity:** Rebuild **`canonical_patient_master_v1_1`** per **`45c`** SQL after base CPM passes.

---

## Immediate follow-ups (Phase 2+)

- Freeze a **MD `canonical_patient_master` Parquet manifest** (Git SHA + row hash) as baseline for **S13** diff.
- Extract ordered list of **`qc_framework_v1/migrations/*`** that touched CPM from **`signoff_migration`** or DB ledger.
- Move analyst demographic assets into **controlled storage** with **Manifest + THY / DFL** linkage only (no row data in git).

---

## References (in-repo)

- `scripts/204_canonical_master_assembly.py`, `scripts/205_canonical_consolidation.py`, `scripts/207_canonical_master_expansion.py`, `scripts/214_final_canonical_integration.py`, `scripts/221_eras_canonical_sync.py`, `scripts/223_publish_canonical.py`, `scripts/231_update_canonical_master.sql`, `scripts/236_canonical_finalization.py`
- `scripts/_md_connect.py`, `scripts/bq_replicate_canonical_patient_master.py`
- `sql/mig_079_emr_demographics_import.sql`, `sql/mig_079_operator_emr_cohort_demo_apply.sql`, `bq_migrations/mig_080_*.sql`, `mig_082_*.sql`, `mig_088_*.sql`, `qc_framework_v1/migrations/45c_canonical_patient_master_v1_1_refresh.sql`, `qc_framework_v1/migrations/334_*.sql`
- `qc_framework_v1/manuscript/canonical_methods_footnotes/canonical_patient_master.md`
- `_scripts/thy1_demographics_import_plan.md`
