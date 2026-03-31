# THYROID_2026 Power BI & Microsoft 365 Architecture — Comprehensive Summary

**Date:** 2026-03-31
**Author:** Logan Glosser (Emory University Surgery Research)
**Purpose:** Share with AI assistants (Grok, ChatGPT, Claude) for architecture review before further implementation

---

## 1. Project Context

**What this is:** A 11,673-patient thyroid cancer research data lakehouse supporting clinical extraction, statistical analysis, and manuscript publication. The NLP extraction pipeline (running on 9 GPU servers) feeds structured data into this analytics layer.

**Migration in progress:** Moving from MotherDuck (cloud DuckDB) + Streamlit dashboards → 100% local Power BI Desktop + Microsoft 365 stack. The migration is planned but implementation has not yet started.

**Why migrating:** IRB defensibility (no PHI in cloud), publication-grade provenance, cost savings ($100-500/yr MotherDuck elimination), no vendor lock-in, and Emory provides full M365 Enterprise for free.

---

## 2. Current State (Being Eliminated)

- **Database:** MotherDuck cloud DuckDB (307 files, 1,786 references in codebase)
- **Dashboards:** Streamlit (dashboard.py + app/ modules)
- **Data files:** DVC-tracked Parquet files in processed/
- **Auth:** Token-based (MOTHERDUCK_TOKEN, MD_SA_TOKEN)
- **Problems:** Cloud PHI exposure risk, recurring costs, vendor dependency, no M365 integration

---

## 3. Target Architecture

### 3.1 Data Flow (Medallion Architecture)

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
    ▼  [Power Query / Python ETL]
    │
02_GOLD_POWERBI/               ← Power BI .pbix semantic model (star schema, Import mode)
    │
    ├── 03_DEID_EXPORTS/       ← Date-stamped de-identified bundles for manuscripts
    ├── 04_EXTRACTION_OUTPUTS/  ← NLP extraction results (from GPU fleet)
    ├── 05_ARCHIVE_BACKUPS/    ← Weekly encrypted snapshots
    ├── SCRIPTS/               ← Python ETL scripts (updated for local paths)
    ├── DOCUMENTATION/         ← Data dictionary, SOPs
    └── VALIDATION_AUDITS/     ← QC reports, compliance sign-offs
```

All PHI stays in 00_RAW_PHI on the local encrypted Mac. Everything downstream uses research_id only.

### 3.2 Power BI Semantic Model Design

**Mode:** Import (no cloud DirectQuery — all data local in .pbix file)

**Star Schema:**

13 Fact Tables:
- Demographics, Episodes, Labs, Imaging, Pathology, Treatment, NSQIP, Outcomes
- Plus NLP extraction tables: Complications, Staging, Recurrence, Medications, Procedures

8 Dimension Tables:
- DimPatient (research_id, age_at_dx, sex, race, insurance)
- DimDate (standard date dimension with fiscal periods)
- DimStaging (AJCC edition, T/N/M, risk strat)
- DimTreatmentIntent (curative, palliative, diagnostic)
- DimOutcomeStatus (NED, recurrence, persistent, deceased)
- DimProvider, DimFacility, DimInsurance

12 Relationships: All join on research_id (single linkage key everywhere)

**DAX Measures (20+ planned):**
- Total Patients, Recurrence Rate, Mortality Rate, Mean Age at Dx
- Median Follow-Up, RAI Dose Distribution, Complication Rate
- Time-to-Recurrence, Dynamic Risk Reclassification Rate
- Data Completeness Score, Missing Rate per Domain

**Report Pages (6):**
1. Executive Dashboard — KPIs, cohort funnel, key outcomes
2. Labs Analytics — TSH/Tg/TgAb trends, suppression adequacy
3. Imaging — TI-RADS distributions, nodule size tracking
4. Pathology — Histology mix, margin status, molecular results
5. Treatment — Surgery types, RAI dosing, completion rates
6. Data Quality — Completeness heatmap, extraction coverage, validation flags

### 3.3 M365 App Roles

**Account:** LGLOSSE@emory.edu (Emory Enterprise M365, tenant e004fb9c)

| App | Role | Phase |
|-----|------|-------|
| **Power BI Desktop** | Semantic model engine, all analysis | 4C |
| **Excel** | Power Query transforms, QC spot-checks | 4C |
| **Word** | Manuscript authoring | 4C |
| **OneDrive** | Metadata-only backups (no PHI) | 4D |
| **Power Automate** | Weekly refresh orchestration | 4D |
| **SharePoint** | Team document library (de-identified only) | 4D |
| **Teams** | Alerts, collaboration | 4D |
| **Lists** | Tracking extraction progress, QC tasks | 4D |
| **Planner** | Project management | 4D |
| **Forms** | Data collection (REDCap supplement) | 4F |
| **Visio** | Data flow diagrams | 4F |

### 3.4 Automation Pipeline

```
Weekly Refresh (Monday 9 AM UTC):
  Power Automate Cloud trigger
    → Power Automate Desktop (RPA)
      → Run Python ETL scripts (local)
        → 00_deid_gateway.py (de-identify any new raw data)
        → Silver layer Parquet refresh
      → Power BI Desktop refresh (Import mode)
      → Teams notification (success/failure + row counts)
      → OneDrive backup (metadata manifest only, no PHI)
```

---

## 4. NLP Extraction Pipeline (Currently Running)

This feeds data INTO the Power BI model. Currently running on 9 GPU servers:

**V2 (Vast.ai H200 NVL, 140GB VRAM):**
- Model: qwen3:32b via Ollama
- 4 parallel extraction workers
- 10/35 domains COMPLETE (11,037 notes each)
- 4 domains in progress, 21 queued
- ~18-20 hours to finish all 35

**S1-S8 (Hetzner/Contabo, qwen3:14b):**
- 8 servers, 1 domain each + 6 Phase 2 domains queued per server
- Currently 2-12% through their first domains

**35 Extraction Domains:**
recurrence, complications, staging, genetics, medications, procedures, problem_list, imaging, pathology, labs, physical_exam, rad_treatment, past_medical_hx, past_surgical_hx, operative_details, presenting_symptoms, dynamic_risk_response, survival_followup, vascular_invasion, rai_detailed, recurrence_detailed, medication_management, functional_outcomes, tg_kinetics, parathyroid_detail, airway_invasion, frozen_section_detail, us_nodule_dynamics, cervical_ln_detail, patient_decision_adherence, operative_v2_enrichment, complications_rln_laryngoscopy, molecular_thyroseq_afirma, synoptic_pathology_enrichment, tirads_granular, parathyroid_per_gland

Each domain produces a JSONL checkpoint (one JSON row per clinical note) following a 9-field schema: entity_type, entity_value, entity_date, date_confidence, date_source_keyword, present_or_negated, confidence, evidence_text, source_line.

---

## 5. MotherDuck Elimination Plan

**Scope:** 307 files, 1,786 references across:
- Python scripts (connection strings, queries, views)
- Streamlit dashboard (dashboard.py, app/ modules)
- Config files (.env, settings)
- Tests and notebooks
- DVC pipeline definitions

**Strategy:** Replace MotherDuck operations with local DuckDB + Parquet:
- `md:thyroid_2026` → `local_thyroid.duckdb` (or direct Parquet reads)
- MotherDuck views → DuckDB views or Power Query transforms
- Streamlit dashboards → Power BI report pages
- Cloud access control → FileVault + folder permissions

---

## 6. Security & Compliance (Non-Negotiable)

1. **PHI isolation** — Raw PHI read-only on local encrypted Mac; research_id-only downstream
2. **research_id linkage** — Single minimal pseudonymous identifier everywhere
3. **De-identification audit trail** — Every run logged (timestamp, row counts, columns dropped, SHA256)
4. **No cloud materialization** — Metadata only to OneDrive/SharePoint/Teams
5. **Backup & DR** — Weekly encrypted local snapshots; 7-year HIPAA retention
6. **Quarterly compliance sign-offs** — Access logs, breach protocol documented

---

## 7. Implementation Timeline (Not Yet Started)

| Phase | Days | Focus | Status |
|-------|------|-------|--------|
| 4A | 1 | Security: folder structure, credentials purge | NOT STARTED |
| 4B | 2-3 | De-identification: Silver layer Parquet + audit trail | NOT STARTED |
| 4C | 4-5 | Power BI: .pbix file, star schema, 6 report pages | NOT STARTED |
| 4D | 6-7 | Automation: Power Automate weekly refresh | NOT STARTED |
| 4E | 8-10 | MotherDuck elimination: 307 files cleaned | NOT STARTED |
| 4F | 11-14 | Validation: production-ready, compliance sign-offs | NOT STARTED |

**Optional extended phases (supplementary toolchain):**

| Phase | Days | Focus |
|-------|------|-------|
| 4G | 15-18 | Replit dashboard + ElevenLabs voice assistant |
| 4H | 19-22 | LangGraph agents (extraction QA, NLQ) |
| 4I | 23-25 | End-to-end integration testing |

---

## 8. Cost Analysis

| Item | Current (MotherDuck) | Target (M365 Local) |
|------|---------------------|---------------------|
| Database | $0.20/GB/month | $0 (local DuckDB) |
| Query compute | $0.003/GB scanned | $0 (local) |
| Dashboard hosting | Streamlit Cloud or local | $0 (Power BI Desktop) |
| M365 apps | N/A | $0 (Emory Enterprise) |
| LangSmith | $39/mo (Plus plan) | $39/mo (retained for NLP monitoring) |
| Vast.ai GPU | ~$2.20/hr (H200 NVL) | Temporary (extraction only) |
| **Annual savings** | | **~$100-500/yr** |

---

## 9. Key Files in Repository

All planning docs are in `docs/microsoft_deployment/`:
- `MICROSOFT_DEPLOYMENT_PLAN.md` — Master plan (572 lines, 12 sections)
- `M365_APP_MATRIX.md` — App-by-app capability matrix (433 lines)
- `MOTHERDUCK_MIGRATION_MAP.md` — File-by-file migration guide (855 lines)
- `FOLDER_SETUP.sh` — Bash script to create folder structure (164 lines)
- `COWORK_IMPLEMENTATION_PROMPT.md` — Ready-to-paste briefing for AI sessions
- `SUPPLEMENTARY_TOOLCHAIN.md` — ElevenLabs, Replit, LangGraph integration plan

---

## 10. Questions for Review

1. **Star schema design** — Is 13 fact + 8 dimension tables the right granularity, or should we consolidate/split differently for Power BI Import mode performance?

2. **NLP → Power BI integration** — The extraction produces JSONL with a flat 9-field schema per entity. Best approach to ingest into the star schema? Direct Parquet import vs. Python consolidation step vs. Power Query transforms?

3. **DAX measures** — Any critical clinical research measures we're missing? Particularly around time-to-event analysis, Kaplan-Meier survival curves, or longitudinal lab trending?

4. **Power Automate architecture** — Is the Cloud→Desktop→Python→PowerBI chain the right pattern, or is there a simpler approach for weekly local refresh?

5. **MotherDuck elimination sequencing** — Should we build the Power BI model first (Phase 4C) and THEN eliminate MotherDuck (4E), or eliminate MotherDuck first to avoid maintaining two systems?

6. **Supplementary toolchain priority** — Given budget constraints ($7.37 Vast.ai credits remaining, $39/mo LangSmith), should Phases 4G-4I (Replit, ElevenLabs, LangGraph) be deferred or are any components worth fast-tracking?

7. **Data quality monitoring** — What's the best approach for ongoing extraction quality validation in Power BI? Completeness heatmaps, inter-rater reliability dashboards, or something else?
