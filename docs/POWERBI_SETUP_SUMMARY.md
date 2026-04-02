# THYROID_2026 Power BI & Microsoft Fabric Architecture — Comprehensive Summary

**Date:** 2026-03-31 (updated)
**Author:** Logan Glosser (Emory University Surgery Research)
**Purpose:** Share with AI assistants (Grok, ChatGPT, Claude) for architecture review before further implementation

---

## 1. Project Context

**What this is:** An 11,673-patient thyroid cancer research data lakehouse supporting clinical extraction, statistical analysis, and manuscript publication. The NLP extraction pipeline (running on 9 GPU servers) feeds structured data into this analytics layer.

**Current state:** The project has completed the MotherDuck cloud database export and is migrating to a Microsoft Fabric Lakehouse + Power BI stack. A star-schema Excel workbook (THYROID_CORE_POWERBI) already exists with dimension and fact tables populated from the exported data. The Fabric Lakehouse has been provisioned but not yet loaded with data.

**Why migrating:** IRB defensibility (no PHI in cloud except via Emory-managed Fabric tenant), publication-grade provenance, cost savings ($100-500/yr MotherDuck elimination), no vendor lock-in, and Emory provides full M365 Enterprise for free.

---

## 2. Previous State (MotherDuck — ELIMINATED)

**Status: EXPORT COMPLETE as of 2026-03-27**

- **Database:** MotherDuck cloud DuckDB — `thyroid_research_2026`
- **Export results:** 592 tables, 67 views, 4,678,536 total rows, 165 MB compressed Parquet
- **Export location:** `~/Desktop/Thyroid_Export_20260327/` (tables/, views/, schema.sql, export_manifest.json)
- **Export script:** `scripts/archive/export_motherduck_to_parquet_MIGRATION_COMPLETE_20260327.py` (archived)
- **Export report:** `exports/MOTHERDUCK_EXPORT_REPORT_20260327.md`
- **21 stale views** with schema drift were preserved as DDL only
- **Remaining cleanup:** 307 files with 1,786 MotherDuck references still in codebase (connection strings, queries, imports) — need updating/removal

### Key Exported Tables

| Table | Rows | Distinct research_ids |
|-------|------|-----------------------|
| master_cohort (view) | 11,673 | 11,673 |
| molecular_testing | 10,126 | 10,026 |
| clinical_notes | 10,863 | 10,863 |
| synoptic_pathology | 11,688 | 10,871 |
| thyroglobulin_labs | 30,245 | 2,569 |
| fna_episode_master_v2 | 59,620 | 5,263 |
| tumor_episode_master_v2 | 11,691 | 10,871 |
| operative_episode_detail_v2 | 9,371 | 9,368 |
| survival_cohort | 6,359 | 3,048 |

---

## 3. Current State — What Exists Today

### 3.1 Microsoft Fabric Workspace

**Account:** LGLOSSE@emory.edu (Emory Enterprise M365, tenant e004fb9c)
**Power BI Trial:** 56 days remaining (as of 2026-03-31)
**Workspace URL:** https://app.powerbi.com/groups/53b9b2b7-e013-4df0-b3b2-92207085aef1/

**Workspace contains 5 items:**

| Item | Type | Status |
|------|------|--------|
| Getting Started Report | Report + Semantic model | Default/template |
| THYROID_2026 | Lakehouse | Created, **Files and Tables empty** — no data loaded yet |
| THYROID_2026 | SQL analytics endpoint | Provisioned (linked to Lakehouse) |
| THYROID_CORE_POWERBI | Excel Workbook | **Populated with star schema data**, refreshed 2026-03-30 |

### 3.2 THYROID_CORE_POWERBI Workbook (Star Schema — EXISTS)

This Excel workbook in the Fabric workspace contains the following sheets with populated data:

**Dimension Tables:**
- **Dim_Patient** — research_id, age_at_surgery, sex, surgery_date, plus boolean data-availability flags (thyroglobulin, benign_path, ct_image, fna_cyto, frozen_section, mri_image, nuclear_scan, parathyroid, ultrasound_reports, etc.)
- **Dim_Date** — Standard date dimension

**Fact Tables:**
- **Fact_Treatment** — Surgery types, RAI dosing, treatment details
- **Fact_Outcome** — Clinical outcomes, recurrence status
- **Fact_LabTg** — Thyroglobulin lab results over time
- **Fact_LabAntiTg** — Anti-thyroglobulin antibody results
- **Fact_FnaEpisode** — Fine-needle aspiration episodes
- **Fact_Molecular** — Molecular testing results (ThyroSeq, Afirma, etc.)
- **Fact_Complications** — Surgical complications data

### 3.3 Fabric Lakehouse (THYROID_2026 — EMPTY)

The Lakehouse has been provisioned with a SQL analytics endpoint but **no data has been loaded** into Files or Tables yet. The upload script exists:

- **Script:** `scripts/09b_fabric_upload_notes_entities.py`
- **Method:** Azure Data Lake Storage Gen2 REST API via `azure-storage-file-datalake`
- **Auth:** Service principal (AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET) or DefaultAzureCredential fallback
- **Target path:** `abfss://<workspace_id>@onelake.dfs.core.windows.net/<lakehouse_name>.Lakehouse/Files/note_entities_{domain}/part-000.parquet`
- **Env vars needed:** FABRIC_LAKEHOUSE_WORKSPACE_ID, LAKEHOUSE_NAME, ONELAKE_ACCOUNT_URL
- **Status:** Script written but **not yet executed** — waiting for extraction pipeline to complete

---

## 4. Target Architecture

### 4.1 Data Flow (Medallion Architecture)

```
00_RAW_PHI/                    ← Raw Excel/CSV from clinical systems (read-only, FileVault-encrypted)
    │
    ▼  [00_deid_gateway.py — Python de-identification]
    │   • MRN → research_id mapping
    │   • Date shifting (±random 1-365 days)
    │   • PHI column stripping
    │   • SHA256 audit hash per run
    │
01_SILVER_DEID_PARQUET/        ← De-identified Parquet files (research_id only)
    │
    ▼  [09b_fabric_upload_notes_entities.py — ADLS Gen2 upload to OneLake]
    │
THYROID_2026 Lakehouse         ← Fabric Lakehouse (OneLake storage, SQL analytics endpoint)
    │
    ├── Power BI Semantic Model ← Star schema (Import or DirectLake mode)
    │     └── 6 report pages
    ├── 03_DEID_EXPORTS/       ← Date-stamped de-identified bundles for manuscripts
    ├── 04_EXTRACTION_OUTPUTS/  ← NLP extraction results (from GPU fleet)
    ├── SCRIPTS/               ← Python ETL scripts
    ├── DOCUMENTATION/         ← Data dictionary, SOPs
    └── VALIDATION_AUDITS/     ← QC reports, compliance sign-offs
```

All PHI stays in 00_RAW_PHI on the local encrypted Mac. Everything downstream uses research_id only.

### 4.2 Power BI Semantic Model Design (Planned Expansion)

**Mode:** Import or DirectLake (to be decided — DirectLake avoids data duplication in .pbix)

**Current star schema (9 tables in THYROID_CORE_POWERBI):**
- 2 Dimension tables: Dim_Patient, Dim_Date
- 7 Fact tables: Fact_Treatment, Fact_Outcome, Fact_LabTg, Fact_LabAntiTg, Fact_FnaEpisode, Fact_Molecular, Fact_Complications

**Planned additions (from NLP extraction pipeline):**
- Additional Fact tables: Staging, Recurrence, Medications, Procedures, Problem_List, Imaging, Pathology, Physical_Exam, etc. (35 extraction domains total)
- Additional Dimension tables: DimStaging (AJCC edition, T/N/M, risk strat), DimTreatmentIntent, DimOutcomeStatus, DimProvider, DimFacility, DimInsurance

**All tables join on research_id** (single linkage key everywhere)

**DAX Measures (20+ planned):**
- Total Patients, Recurrence Rate, Mortality Rate, Mean Age at Dx
- Median Follow-Up, RAI Dose Distribution, Complication Rate
- Time-to-Recurrence, Dynamic Risk Reclassification Rate
- Data Completeness Score, Missing Rate per Domain

**Report Pages (6 planned):**
1. Executive Dashboard — KPIs, cohort funnel, key outcomes
2. Labs Analytics — TSH/Tg/TgAb trends, suppression adequacy
3. Imaging — TI-RADS distributions, nodule size tracking
4. Pathology — Histology mix, margin status, molecular results
5. Treatment — Surgery types, RAI dosing, completion rates
6. Data Quality — Completeness heatmap, extraction coverage, validation flags

### 4.3 M365 App Roles

| App | Role | Status |
|-----|------|--------|
| **Power BI (Fabric)** | Semantic model engine, all analysis | Star schema built, reports pending |
| **Excel** | Power Query transforms, QC spot-checks | THYROID_CORE_POWERBI workbook active |
| **Word** | Manuscript authoring | Available |
| **OneDrive** | Metadata-only backups (no PHI) | Available |
| **Power Automate** | Weekly refresh orchestration | Not yet configured |
| **SharePoint** | Team document library (de-identified only) | Not yet configured |
| **Teams** | Alerts, collaboration | Available |
| **Lists** | Tracking extraction progress, QC tasks | Not yet configured |
| **Planner** | Project management | Not yet configured |

---

## 5. NLP Extraction Pipeline (Currently Running)

This feeds data INTO the Power BI model. Currently running on 9 GPU servers:

**V2 (Vast.ai H200 NVL, 140GB VRAM):**
- Model: qwen3:32b via Ollama (OLLAMA_NUM_PARALLEL=4)
- 4 parallel extraction workers running simultaneously
- 10/35 domains COMPLETE (~11,037 notes each)
- 4 domains in progress, 21 queued
- Budget: $7.37 remaining at ~$0.027/hr (~11 hours runtime)

**S1-S8 (Hetzner/Contabo, qwen3:14b):**
- 8 servers, 1 domain each + queued Phase 2 domains per server
- Currently 2-12% through their first domains

**35 Extraction Domains:**
recurrence, complications, staging, genetics, medications, procedures, problem_list, imaging, pathology, labs, physical_exam, rad_treatment, past_medical_hx, past_surgical_hx, operative_details, presenting_symptoms, dynamic_risk_response, survival_followup, vascular_invasion, rai_detailed, recurrence_detailed, medication_management, functional_outcomes, tg_kinetics, parathyroid_detail, airway_invasion, frozen_section_detail, us_nodule_dynamics, cervical_ln_detail, patient_decision_adherence, operative_v2_enrichment, complications_rln_laryngoscopy, molecular_thyroseq_afirma, synoptic_pathology_enrichment, tirads_granular, parathyroid_per_gland

Each domain produces a JSONL checkpoint (one JSON row per clinical note) following a 9-field schema: entity_type, entity_value, entity_date, date_confidence, date_source_keyword, present_or_negated, confidence, evidence_text, source_line.

---

## 6. Security & Compliance (Non-Negotiable)

1. **PHI isolation** — Raw PHI read-only on local encrypted Mac; research_id-only downstream
2. **research_id linkage** — Single minimal pseudonymous identifier everywhere
3. **De-identification audit trail** — Every run logged (timestamp, row counts, columns dropped, SHA256)
4. **Fabric tenant** — Emory-managed M365 Enterprise tenant (e004fb9c); no personal cloud storage for PHI
5. **Backup & DR** — Weekly encrypted local snapshots; 7-year HIPAA retention
6. **Quarterly compliance sign-offs** — Access logs, breach protocol documented

---

## 7. Remaining Implementation Work

| Task | Focus | Status |
|------|-------|--------|
| Load Lakehouse | Upload exported Parquet + extraction outputs to THYROID_2026 Lakehouse via 09b script | NOT STARTED |
| Build semantic model | Create Power BI semantic model on top of Lakehouse tables (DirectLake or Import) | NOT STARTED |
| Create report pages | 6 report pages with DAX measures | NOT STARTED |
| Configure Power Automate | Weekly refresh orchestration (Cloud→Desktop→Python→Power BI) | NOT STARTED |
| MotherDuck code cleanup | Remove 1,786 references across 307 files | NOT STARTED |
| Add NLP extraction tables | Integrate 35 extraction domain outputs into star schema | BLOCKED (extraction in progress) |
| Validation & compliance | Production-ready sign-offs, data quality monitoring | NOT STARTED |

---

## 8. Cost Analysis

| Item | Previous (MotherDuck) | Current (Fabric + M365) |
|------|----------------------|------------------------|
| Database | $0.20/GB/month | $0 (Fabric included in Emory M365 E5) |
| Query compute | $0.003/GB scanned | $0 (Fabric capacity via Emory) |
| Dashboard hosting | Streamlit Cloud or local | $0 (Power BI via Emory) |
| M365 apps | N/A | $0 (Emory Enterprise) |
| Power BI Trial | N/A | 56 days remaining (free) |
| LangSmith | $39/mo (Plus plan) | $39/mo (retained for NLP monitoring) |
| Vast.ai GPU | ~$2.20/hr (H200 NVL) | Temporary (extraction only, $7.37 remaining) |
| **Annual savings** | | **~$100-500/yr** |

---

## 9. Key Files in Repository

**Completed exports:**
- `exports/MOTHERDUCK_EXPORT_REPORT_20260327.md` — Full export audit (592 tables, 67 views, 4.7M rows)
- `scripts/archive/export_motherduck_to_parquet_MIGRATION_COMPLETE_20260327.py` — Archived export script

**Fabric integration:**
- `scripts/09b_fabric_upload_notes_entities.py` — ADLS Gen2 upload script for OneLake (not yet run)

**Planning docs (in `docs/microsoft_deployment/`):**
- `MICROSOFT_DEPLOYMENT_PLAN.md` — Master plan (572 lines, 12 sections)
- `M365_APP_MATRIX.md` — App-by-app capability matrix (433 lines)
- `MOTHERDUCK_MIGRATION_MAP.md` — File-by-file migration guide (855 lines)
- `FOLDER_SETUP.sh` — Bash script to create folder structure (164 lines)
- `COWORK_IMPLEMENTATION_PROMPT.md` — Ready-to-paste briefing for AI sessions

**Extraction pipeline:**
- `processed/output/v2_checkpoints/` — V2 GPU server checkpoint files
- `processed/output/v2_parquets/` — V2 Parquet exports
- `processed/output/server_checkpoints/S1-S8/` — Per-server checkpoint files

---

## 10. Questions for Review

1. **DirectLake vs Import mode** — The Fabric Lakehouse supports DirectLake mode (queries Parquet/Delta directly without import). Given our 11,673-patient dataset, is DirectLake worth the setup complexity, or is Import mode simpler and sufficient?

2. **NLP → Lakehouse integration** — The extraction produces JSONL with a flat 9-field schema per entity. Best approach to load into Fabric: upload as Parquet via ADLS Gen2 (script exists) then register as Delta tables in a Fabric notebook? Or use Dataflows Gen2?

3. **Star schema expansion** — Current schema has 2 dim + 7 fact tables. With 35 extraction domains incoming, should each domain become its own fact table, or should we consolidate into fewer, wider tables?

4. **DAX measures** — Any critical clinical research measures we're missing? Particularly around time-to-event analysis, Kaplan-Meier survival curves, or longitudinal lab trending?

5. **Power Automate architecture** — Is the Cloud→Desktop→Python→Power BI chain the right pattern for weekly refresh, or can Fabric Pipelines handle the orchestration natively?

6. **MotherDuck cleanup sequencing** — 307 files with 1,786 references remain. Should we clean these up now, or wait until the Fabric Lakehouse is fully loaded and validated?

7. **Data quality monitoring** — What's the best approach for ongoing extraction quality validation in Power BI? Completeness heatmaps, inter-rater reliability dashboards, or something else?
