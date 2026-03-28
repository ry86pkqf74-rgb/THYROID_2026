# THYROID_2026 Implementation Prompt for Cowork
## Ready-to-Paste Complete Briefing

Copy and paste this prompt into a new Cowork session to begin Phase 4 implementation.

---

## IMPLEMENTATION BRIEFING

I am implementing a comprehensive Microsoft 365 deployment for THYROID_2026, a thyroid cancer research project with 11,673 de-identified patients. The project is currently using MotherDuck (cloud database) and Streamlit (web dashboards) — both of which are being eliminated. I need to replace them with a 100% local, air-gapped architecture using Power BI Desktop, DuckDB, and local Parquet files.

### Project Overview

**Current State:**
- 11,673-patient thyroid cancer research lakehouse
- Using: DuckDB/MotherDuck (cloud) + Streamlit (web dashboards)
- 307 files contain MotherDuck references (1,786 total code instances)
- 100+ Python ETL scripts in scripts/
- Primary key: research_id across 13 base tables + 8+ analytic views
- Environment: macOS (FileVault enabled), 2TB disk, 45.8% free

**Target State:**
- 100% local, air-gapped architecture (zero cloud PHI)
- Power BI Desktop (.pbix) as primary semantic model + reporting engine
- De-identified Parquet files (research_id only) in local 01_SILVER_DEID_PARQUET/
- Python ETL scripts refactored to use local DuckDB + Parquet
- Automated weekly refresh via Power Automate + osascript RPA
- Full elimination of MotherDuck (307 files cleaned)

**User:** Logan Glosser (LGLOSSE@emory.edu), Emory University
**Timeline:** 14 days (Phase 4A through 4F)
**Environment:** /Users/lhglosser/THYROID_SECURE_2026/ (FileVault-encrypted)

### Non-Negotiable Security Rules

1. **PHI Isolation:** Raw PHI (00_RAW_PHI/) stays read-only on local disk; only de-identified research_id-keyed Parquet (01_SILVER_DEID_PARQUET/) feeds Power BI
2. **research_id Linkage:** Only identifier used in analysis; MRN↔research_id lookup locked in 00_RAW_PHI/
3. **No Cloud PHI:** OneDrive/SharePoint/Teams receive metadata only (aggregate counts, QC reports), never raw data or Parquet
4. **Audit Trail:** Every de-ID run logged (timestamp, row counts, columns dropped, SHA256 hash) in VALIDATION_AUDITS/
5. **Backup Strategy:** Weekly encrypted snapshots (05_ARCHIVE_BACKUPS/); metadata copies to OneDrive; 7-year retention for HIPAA

### M365 Apps in Scope

**P0 CRITICAL (Must Work):**
1. Power BI Desktop (local .pbix file hosting star schema)
2. Excel (Power Query transforms, QC pivot tables)
3. Word (manuscript drafting, Copilot-assisted narratives)
4. OneDrive (metadata-only backups)
5. Power Automate (cloud-based orchestration for weekly refresh)

**P1 IMPORTANT (Enable Collaboration):**
6. SharePoint (team collaboration space, read-only de-ID tables)
7. Teams (#thyroid-research channel, refresh alerts)
8. Outlook (calendar reminders, alert emails)
9. Lists (QC status tracking, MotherDuck elimination progress)
10. Planner (Phase 4 task management, 307 files tracked)
11. Forms (manual data corrections, audit trail capture)
12. Calendar (weekly QC check reminders)
13. Visio (ER diagrams, data flow diagrams)
14. Power Automate Desktop (local RPA robot for automation)

**P2 NICE-TO-HAVE:**
Power BI Web (Phase 5), To Do, Loop, OneNote, Whiteboard, Copilot (limited at Emory)

### Folder Structure (Secure, Encrypted)

All folders at `/Users/lhglosser/THYROID_SECURE_2026/` (FileVault-encrypted):

```
├── 00_RAW_PHI/                    ← Original Excel + identifiers (read-only after ingestion)
├── 01_SILVER_DEID_PARQUET/        ← DVC-tracked de-identified Parquet (research_id only) → Power BI imports
│   ├── patient_demographics.parquet/
│   ├── episode_facts.parquet/ (partitioned by year)
│   ├── lab_facts.parquet/
│   ├── imaging_facts.parquet/
│   ├── pathology_facts.parquet/
│   ├── treatment_facts.parquet/
│   ├── nsqip_facts.parquet/
│   ├── outcome_facts.parquet/
│   └── validation_tables/ (Dim_Date, Dim_Staging, Dim_Treatment, Dim_Outcome)
├── 02_GOLD_POWERBI/               ← Power BI .pbix + templates
│   ├── THYROID_2026_SEMANTIC_MODEL.pbix
│   ├── templates/ (manuscript, QC report templates)
│   └── queries/ (Power Query M scripts for imports)
├── 03_DEID_EXPORTS/               ← Date-stamped de-identified bundles for sharing
├── 04_EXTRACTION_OUTPUTS/         ← NLP cell extractions, outlier flags
├── 05_ARCHIVE_BACKUPS/            ← Weekly encrypted snapshots
├── SCRIPTS/                       ← Python ETL (updated for local paths)
│   ├── 00_deid_gateway.py         ← Main de-identification script
│   ├── 01_ingest_demographics.py
│   ├── ... (8 more ingest scripts)
│   ├── 09_validate_relationships.py
│   ├── 10_generate_deid_audit.py
│   └── 11_refresh_powerbi.py
├── DOCUMENTATION/                 ← Data dictionary, SOPs, architecture diagrams
├── VALIDATION_AUDITS/             ← QC reports, compliance sign-offs, access logs
└── .dvc, .gitignore, README_THYROID_SECURE.md
```

### Implementation Phases (14 Days)

**Phase 4A (Day 1): Foundation & Security**
- Run FOLDER_SETUP.sh: create /Users/lhglosser/THYROID_SECURE_2026/ with correct permissions
- Remove MotherDuck credentials from .env, GitHub Secrets, CI/CD
- Initialize git-lfs (for .pbix files) and DVC (for Parquet partitions)
- Verify FileVault encryption
- **Deliverable:** Secure folder structure; zero credentials in repo

**Phase 4B (Days 2-3): Silver Layer & De-Identification**
- Write 00_deid_gateway.py: Load 00_RAW_PHI/ → Apply de-ID rules (MRN→research_id, date shifts, PHI masking) → Output 01_SILVER_DEID_PARQUET/
- Ingest all 13 base tables (demographics, episodes, labs, imaging, pathology, treatment, NSQIP, outcomes)
- Generate 4 validation tables (Dim_Date, Dim_Staging, Dim_Treatment, Dim_Outcome)
- Test: Verify 11,673 patients de-identified; no PHI leakage; audit trail created
- DVC commit de-identified Parquet
- **Deliverable:** 01_SILVER_DEID_PARQUET/ with all fact + dimension tables; audit trail in VALIDATION_AUDITS/

**Phase 4C (Days 4-5): Power BI Star Schema & Reports**
- Open Power BI Desktop; create data model
- Import Parquet tables via Power Query M scripts (one script per table)
- Define 12 relationships (all on research_id only):
  - Fact tables → Dim_Patient, Dim_Date, Dim_Staging, Dim_Treatment, Dim_Outcome
- Write 20+ DAX measures: Total Patients, Mean Age, Recurrence Rate, Mortality 30d, Avg TSH by Year, etc.
- Create 6 report pages:
  1. Dashboard (KPI cards, overview charts by Stage/Year)
  2. Labs (TSH trends, distributions by cohort)
  3. Imaging (modality × body part matrix, findings summary)
  4. Pathology (TNM × outcome matrix, grade × survival)
  5. Treatment (treatment type × outcome, complications, LOS)
  6. Data Quality (row counts, missing %, date shift verification, audit log)
- Add slicers: Date range, Stage, Treatment Intent
- Test: All 6 pages render; slicers filter correctly; measures calculate without errors
- Save as 02_GOLD_POWERBI/THYROID_2026_SEMANTIC_MODEL.pbix (git-LFS)
- **Deliverable:** Power BI .pbix file with 6 production-ready report pages

**Phase 4D (Days 6-7): Automation Wiring**
- Create Power Automate Cloud flow: "Weekly THYROID_2026 Silver Refresh" (Monday 9 AM UTC)
- Create Power Automate Desktop RPA robot: De-ID script → Parquet validation → Power BI refresh → Teams alert
  - Open Terminal via osascript
  - Run: `python3 SCRIPTS/00_deid_gateway.py --table=all`
  - Run: `python3 SCRIPTS/09_validate_relationships.py`
  - Open Power BI Desktop via osascript
  - Trigger: Cmd+Shift+R (refresh)
  - Close Power BI & Terminal
  - Copy VALIDATION_AUDITS/ to OneDrive metadata folder
  - Send Teams message with status
- Write osascript helpers: open/close apps, copy files, create calendar events
- Test: Manual robot trigger completes full refresh cycle in < 30 min; all logs created
- **Deliverable:** Automated weekly refresh pipeline working end-to-end

**Phase 4E (Days 8-10): MotherDuck Elimination**
- Refactor 35 Python ETL scripts: Replace `motherduck.query()` with `duckdb.sql()` or `pd.read_parquet()`
- Archive/remove 50 Streamlit app files (move to ARCHIVE/streamlit_apps_deprecated/ or delete)
- Update 50 documentation files: Remove MotherDuck architecture references; add local DuckDB + Power BI
- Update 20 test files: Replace MotherDuck mocks with local DuckDB fixtures
- Archive 30 notebooks (if unused)
- Global find-and-replace: "motherduck" → "duckdb" in 27 comment files
- Final audit: `grep -r "motherduck" . | wc -l` should return 0
- Update CI/CD: Remove MotherDuck token from GitHub Secrets; update pipelines to run scripts locally
- **Deliverable:** 307 files cleaned; final git commit "Phase 4E: Eliminate MotherDuck (1,786 refs removed)"

**Phase 4F (Days 11-14): Validation & Compliance Sign-Off**
- Run comprehensive validation tests:
  - Row count reconciliation: MotherDuck cloud ↔ local Parquet (all tables match)
  - No PHI leakage: Scan Parquet for MRN, SSN, phone patterns (0 found)
  - research_id uniqueness: All 11,673 values unique
  - Referential integrity: All foreign keys present
  - Date shifts: Verify all dates shifted by documented amount
  - Power BI model: All relationships active; all measures calculate correctly
  - Automation: Full refresh cycle works; audit logs created
- Generate sample manuscript: Export Power BI tables to CSV → paste into Word template → use Copilot to generate narrative → create PowerPoint deck
- Team review + sign-off in SharePoint (read-only de-identified link)
- Test backup & restore from 05_ARCHIVE_BACKUPS/
- Create quarterly compliance sign-off document (Logan + Emory IRB/Privacy Officer signature)
- Document all SOPs: sop_add_new_data.md, sop_edit_records.md, sop_manuscript_generation.md, sop_power_automate_refresh.md, etc.
- **Deliverable:** Production-ready system; all validation tests pass; sample manuscript approved; compliance sign-offs complete

---

### Key Technical Decisions

1. **Local vs. Cloud:** All data stays local (FileVault-encrypted Mac); only metadata syncs to OneDrive
2. **Power BI Desktop vs. Web:** Phase 4 uses local Desktop (.pbix); Phase 5 (future) considers Web publishing with RLS
3. **DuckDB vs. Postgres:** DuckDB for local ETL (zero server setup); no migration to Postgres needed (Parquet is portable)
4. **Power Automate Desktop vs. Cron:** Power Automate Desktop for UI automation (triggers Power BI refresh button); cron as backup
5. **Git-LFS vs. Zip:** Git-LFS for .pbix versioning; DVC for Parquet partitions; weekly zip backups for disaster recovery
6. **research_id Only:** No embedded identifiers in analyses; MRN lookup remains encrypted in 00_RAW_PHI/ (read-only)

### Documentation to Review

Before starting, review these reference documents (all in `/sessions/optimistic-trusting-brahmagupta/mnt/THyroid 2026/THYROID_2026/docs/microsoft_deployment/`):

1. **MICROSOFT_DEPLOYMENT_PLAN.md** — Complete 40+ page master plan (Sections 1-12 cover everything)
2. **FOLDER_SETUP.sh** — Bash script to create folder structure with correct permissions
3. **M365_APP_MATRIX.md** — Detailed capabilities of all 30 M365 apps; focus on P0 & P1
4. **MOTHERDUCK_MIGRATION_MAP.md** — Line-by-line migration guide for 307 files (code examples, troubleshooting)

### Success Criteria (Phase 4F Validation)

✓ Secure folder structure created with FileVault encryption
✓ De-identification script runs; 11,673 patients de-identified; 01_SILVER_DEID_PARQUET/ populated
✓ Power BI Desktop .pbix loads; 6 report pages render; all measures calculate; slicers filter correctly
✓ Power Automate Cloud flow triggers on schedule; Power Automate Desktop robot completes refresh in < 30 min
✓ osascript automation (open/close apps, file operations) works reliably
✓ All 307 MotherDuck references removed (grep -r "motherduck" . = 0)
✓ Row count reconciliation: MotherDuck cloud ↔ local Parquet (100% match)
✓ No PHI leakage in Parquet files
✓ Weekly audit logs created; compliance sign-offs in place
✓ Sample manuscript generated (Power BI → CSV → Word → PowerPoint)
✓ Backup & restore tested; weekly snapshots created

### Contingencies

- **If Power BI Desktop fails:** Fall back to local Jupyter Notebook + matplotlib/plotly for reports; convert to static HTML for sharing
- **If Power Automate Desktop unavailable:** Use macOS cron job + shell script wrapper for scheduling
- **If Power Automate Cloud flow fails:** Use osascript to create local Calendar reminders + manual robot trigger
- **If OneDrive sync fails:** Use SFTP backup to Emory network drive or Backblaze for off-site encrypted backup
- **If FileVault encryption fails:** Entire /Users/lhglosser/THYROID_SECURE_2026/ automatically encrypted at OS level; failure triggers emergency backup to Emory

---

### Supplementary Toolchain (ElevenLabs + Replit + LangChain/LangGraph)

In addition to the Microsoft 365 stack, the following tools extend the pipeline beyond what M365 can do alone:

**ElevenLabs** (Free tier: 10K TTS credits, 15 min voice agent calls):
- Voice-powered research assistant with Knowledge Base (de-identified data dictionary + SOPs)
- Audio narration for conference presentations and dashboard walkthroughs
- Speech-to-text pipeline for clinical note dictation (Phase 5+, requires HIPAA BAA upgrade)

**Replit** ($200/mo team credits, ROS Workspace, 12 existing projects):
- Full-stack thyroid research dashboard (FastAPI + React) replacing Streamlit — shareable via URL
- LangGraph agent backend API (always-on reserved VM deployment)
- Manuscript figure generation service (matplotlib/seaborn → publication-grade SVG/PNG)
- Batch data processing workers (survival analysis, PSM, cure models offloaded from local Mac)

**LangChain/LangGraph** (open source, $0 + ~$3-5/mo Claude API):
- Clinical note extraction agent: operative notes → structured Parquet via stateful graph pipeline
- Data quality monitoring agent: weekly scans for missing data, outliers, referential integrity
- Natural language query agent: "What's the recurrence rate for PTC tall cell?" → SQL → answer + chart
- Manuscript drafting agent: Power BI tables → statistical narrative → draft Methods/Results sections

**Architecture principle:** PHI stays local (FileVault Mac). Only de-identified research_id-keyed exports reach cloud services (Replit, ElevenLabs). LangGraph runs locally for any PHI-adjacent processing.

See `SUPPLEMENTARY_TOOLCHAIN.md` for the full integration plan, code examples, PHI safety matrix, and implementation timeline (Phases 4G-4I, Days 15-25).

---

## READY TO START

I am ready to begin Phase 4A (Day 1) implementation.

**Next Immediate Action:** Run FOLDER_SETUP.sh to create the secure folder structure.

Please confirm:
1. Shall I proceed with Phase 4A folder setup?
2. Any constraints on timeline (can we complete by 2026-04-10)?
3. Should I prioritize any specific phase (e.g., get Power BI running ASAP)?
4. Do you have Emory SSO credentials verified for Power Automate / SharePoint access?

Once confirmed, I will:
- Step through FOLDER_SETUP.sh creation
- Verify FileVault encryption
- Initialize git-lfs + DVC
- Remove all MotherDuck credentials
- Document all Phase 4A completions in VALIDATION_AUDITS/

Ready to execute.

---

## For Cowork Session Use

When pasting into Cowork, the Claude Code agent will:
1. Create the THYROID_SECURE_2026 folder structure
2. Write the de-identification Python script
3. Build the Power BI .pbix file
4. Wire up Power Automate flows + osascript automation
5. Refactor 307 files to eliminate MotherDuck
6. Generate audit trails and compliance documentation
7. Validate the system end-to-end
8. Deploy Replit dashboard + LangGraph agent backend
9. Configure ElevenLabs voice research assistant
10. Wire up the full voice → agent → answer pipeline

All work will be tracked in git commits and VALIDATION_AUDITS/ logs. The entire implementation is modular (each phase can be paused/resumed) and fully reversible (all changes backed up daily).

**Estimated Total Time:** 25 days elapsed; ~110 hours active work
**Team Required:** Logan (owner/reviewer), optionally: Emory IRB/Privacy Officer (Phase 4F sign-off)

### Full Toolchain Summary
| Tool | Role | PHI Access | Monthly Cost |
|------|------|-----------|-------------|
| Power BI Desktop | Semantic model + reports | Local only | $0 (M365 Enterprise) |
| Excel / Power Query | Light transforms, QC | Local only | $0 (M365 Enterprise) |
| Power Automate | Weekly refresh orchestration | Metadata only | $0 (M365 Enterprise) |
| OneDrive / SharePoint | De-id backup + collaboration | De-id only | $0 (M365 Enterprise) |
| Desktop Commander | File ops, Python execution | Local shell | $0 |
| osascript | App control, UI automation | Local shell | $0 |
| Claude in Chrome | M365 web app interaction | Browser only | $0 |
| Replit | Dashboard, API, workers | De-id exports only | $200/mo (existing) |
| ElevenLabs | Voice assistant, TTS | De-id KB only | $0 (Free tier) |
| LangGraph | Agent orchestration | Local execution | $0 (open source) |
| Claude API | LLM for agents | De-id prompts only | ~$3-5/mo |
| **Total** | | | **~$203-205/mo** |

---

**Prompt Version:** 2.0
**Created:** 2026-03-27
**Updated:** 2026-03-27 (added ElevenLabs, Replit, LangChain/LangGraph)
**For:** Claude Code / Cowork Session
