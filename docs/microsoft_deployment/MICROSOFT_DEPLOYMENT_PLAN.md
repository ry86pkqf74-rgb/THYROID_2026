# Microsoft Deployment Plan: THYROID_2026
## Phase 4 Implementation Blueprint — 100% Local Power BI Desktop Architecture

**Project:** 11,673-patient thyroid cancer research lakehouse
**Current State:** DuckDB/MotherDuck + Streamlit (307 files reference MotherDuck, 1,786 total references)
**Target State:** Local Microsoft 365 power stack with Air-Gapped PHI isolation
**Primary Key:** research_id across 13 base tables + 8+ analytic views
**User:** Logan Glosser (LGLOSSE@emory.edu), Emory University
**Start Date:** 2026-03-27
**Deployment Environment:** macOS (FileVault enabled, 2TB, 45.8% free), local MotherDuck elimination

---

## Executive Summary

This plan replaces MotherDuck's cloud database + Streamlit's browser-based dashboards with a **100% local, air-gapped architecture** built on Microsoft 365 tools. The migration preserves research_id-only linkages, enforces strict PHI isolation, and leverages Power BI Desktop as the primary semantic layer and reporting engine.

### Why Microsoft Over Others?
- **Emory Enterprise SSO:** Outlook, Teams, SharePoint, Power Platform, OneDrive, Word, Excel, PowerPoint all integrated
- **Power BI Desktop:** Superior star-schema capabilities, DAX, M language for ETL transforms
- **Office Desktop Clients:** FileVault-aware, local-first by default
- **Automation Ready:** Power Automate + Desktop Commander + osascript + Claude in Chrome form a complete RPA stack
- **No Cloud Dependency for PHI:** All sensitive data stays on FileVault-encrypted local disk

### Elimination Goal
Remove all 307 MotherDuck references (1,786 code instances) by Phase 4E. Replace with local DuckDB + Parquet + Power BI import mode.

---

## 1. Non-Negotiable Security & Privacy Rules

These rules are **immutable** and override convenience. Violations expose patients and research to HIPAA breach liability.

### 1.1 PHI Isolation (Principle: Never Cloud-Native)
- **Raw PHI Source (00_RAW_PHI):** Read-only after ingestion; original Excel files with identifiers (MRN, DOB, patient names)
- **De-identification Gate:** Python script running locally on Mac; outputs research_id-keyed Parquet files only
- **Silver Layer (01_SILVER_DEID_PARQUET):** DVC-tracked de-identified Parquet; **only this layer** feeds Power BI Desktop
- **No Exception:** If a dataset enters OneDrive, SharePoint, or Teams (even in encrypted folders), it triggers re-evaluation

### 1.2 research_id Linkage Rule (Principle: Minimal Unique Identifier)
- **Only Identifier in Shared/Cloud Contexts:** research_id (integer, randomized, no embedded patterns)
- **Lookup Table (Locked in 00_RAW_PHI):** MRN ↔ research_id mapping; **never** synced to cloud
- **DAX Measures Reference Only research_id:** No Lookup() functions that could reconstruct identity

### 1.3 De-Identification Audit Trail (Principle: Transparency & Reversibility)
- **Every Export:**
  - Timestamp, script version, input row count, output row count
  - Columns excluded (reason: PHI, PII, diagnostic free-text)
  - Hash of script + input data for reproducibility
  - Signed (SHA256) by de-identification script
- **Storage:** VALIDATION_AUDITS/ with weekly digest in Word
- **Retention:** 7 years (HIPAA minimum for research)

### 1.4 No Cloud Materialization (Principle: Local-First by Default)
- **Power BI Import Mode Only:** No DirectQuery to cloud databases
- **OneDrive/SharePoint:** Metadata & non-sensitive outputs only (manuscript drafts, QC reports, aggregate stats)
- **Teams/Outlook:** De-identified summary tables only; raw data never shared via Teams channels
- **MotherDuck Elimination:** All remaining cloud data sources migrated to local Parquet by Phase 4E

### 1.5 Backup & Disaster Recovery
- **Weekly Snapshots:** 05_ARCHIVE_BACKUPS/ with full SilverLayer zip, Power BI .pbix, script versions
- **Versioning:** git-LFS for .pbix files (too large for regular Git); DVC for Parquet partitions
- **Off-Site:** Weekly encrypted copy to Emory OneDrive (metadata only; PHI never leaves local disk)
- **Immutable Archive:** Monthly immutable snapshot to Emory's institutional repository

### 1.6 Audit & Compliance
- **Quarterly Review:** Logan + Emory IRB/Privacy Officer; signature in VALIDATION_AUDITS/quarterly_sign_off.md
- **Access Logs:** FileVault login, Power BI refresh logs, script execution logs in VALIDATION_AUDITS/
- **Breach Protocol:** If any researcher accidentally uploads PHI to Teams/SharePoint, immediate notification to Emory Privacy; backup restore to prior known-good state

---

## 2. Secure Local Folder Structure

All folders reside on FileVault-encrypted disk at: **/Users/lhglosser/THYROID_SECURE_2026/**

Complete folder structure with permissions documented in FOLDER_SETUP.sh (see companion file).

Key directories:
- **00_RAW_PHI/** - Original identifiable extracts (read-only after processing)
- **01_SILVER_DEID_PARQUET/** - DVC-tracked clean Parquet (research_id only) — Power BI imports here
- **02_GOLD_POWERBI/** - .pbix files + templates
- **03_DEID_EXPORTS/** - Date-stamped manuscript bundles (no PHI)
- **04_EXTRACTION_OUTPUTS/** - Structured columns from NLP cell extraction
- **05_ARCHIVE_BACKUPS/** - Weekly encrypted snapshots
- **SCRIPTS/** - Python ETL (updated for local paths)
- **DOCUMENTATION/** - data_dictionary.md + SOPs
- **VALIDATION_AUDITS/** - QC reports, audit trails, compliance sign-offs

---

## 3. M365 App Inventory & Pipeline Roles

Complete matrix in companion file **M365_APP_MATRIX.md**.

**Summary:**
- **P0 CRITICAL (5 apps):** Power BI Desktop, Excel, Word, OneDrive, Power Automate
- **P1 IMPORTANT (8 apps):** SharePoint, Teams, Outlook, Forms, Lists, Planner, Calendar, Visio
- **P2 NICE-TO-HAVE (7 apps):** Power BI Web, To Do, Loop, OneNote, Whiteboard, Copilot (limited)
- **P3 NOT APPLICABLE (10 apps):** Bookings, Culture Cloud, Find a Room, Clipchamp, Reflect, Sway, Power Pages, Engage, Connections, Org Explorer

**Emory SSO Ready:** All P0, P1, P2 apps; most available both desktop + web

**Copilot Note:** HasCopilotAI=false at Emory; standard M365 Copilot in Word available (limited). Monitor for license upgrades.

---

## 4. Build the Silver Layer (De-Identification + research_id-only)

### 4.1 De-Identification Pipeline (`00_deid_gateway.py`)

**Input:** Original Excel files from 00_RAW_PHI/ (with MRN, DOB, names)
**Output:** De-identified Parquet files in 01_SILVER_DEID_PARQUET/ (research_id only)

**De-Identification Rules:**
1. **MRN → research_id:** Load MRN lookup table. Generate pseudo-random integer 1-99999; hash(MRN + salt) mod 100000 for deterministic reproducibility
2. **DOB → Age at Diagnosis/Admission:** Calculate years; round down. Flag pediatric cases separately
3. **Patient Name:** Drop entirely
4. **Address:** Keep only state (geographic analysis)
5. **Encounter Dates:** Shift by ±random(1-365) days; preserve day-of-week and seasonality. Document shifts in audit trail
6. **Free-Text Fields:** Mask with NLP (spaCy); replace PHI patterns (names, phone, SSN) with [MASKED]; send to 04_EXTRACTION_OUTPUTS/ for future LLM re-extraction
7. **Diagnosis Codes (ICD-O-3):** Keep as-is (research identifier, not PHI)
8. **Staging TNM:** Keep as-is
9. **Lab Values:** Keep quantitative; drop reference unit text if it leaks date context

### 4.2 Source Tables (13 Base) → Silver Layer Mapping

| Base Table | Silver Layer File | Key Rule |
|---|---|---|
| Patient Demographic | patient_demographics.parquet | Drop MRN, DOB, name, address; keep state |
| Episode | episode_facts.parquet | Shift encounter dates; keep ICD-O-3 codes |
| Lab Results | lab_facts.parquet | Shift dates; keep numeric values |
| Imaging | imaging_facts.parquet | Shift dates; mask free-text findings |
| Pathology | pathology_facts.parquet | Shift dates; mask narrative; extract entities |
| Treatment | treatment_facts.parquet | Shift dates; keep procedure codes |
| NSQIP (surgery outcomes) | nsqip_facts.parquet | Shift dates; keep numeric outcomes |
| Outcome | outcome_facts.parquet | Shift follow-up dates; keep status codes |
| Lab Ref Ranges | dim_lab_ref_ranges.parquet | No de-ID; reference table |
| Date Dimension | dim_date.parquet | No de-ID; support analytic joins |
| Staging | dim_staging.parquet | No de-ID; reference table |
| Treatment Intent | dim_treatment_intent.parquet | No de-ID; reference table |
| Outcome Status | dim_outcome_status.parquet | No de-ID; reference table |

### 4.3 Validation Post-De-ID

1. **Row Count Check:** Input vs. output match
2. **No PHI Leakage:** Scan for MRN/SSN/phone patterns in Parquet files
3. **research_id Uniqueness:** All 11,673 patients have unique research_id
4. **Referential Integrity:** All foreign keys present in child tables
5. **Date Shifts:** Verify all dates shifted by documented amount
6. **Signature:** SHA256 hash stored in audit log

### 4.4 DVC Tracking (Version Control for Parquet)

```bash
dvc init
dvc add 01_SILVER_DEID_PARQUET/
dvc push  # (pushes to local cache; never to cloud)
dvc commit
```

---

## 5. Build the Power BI Desktop Semantic Model (Star Schema)

Power BI Desktop file hosted locally: **02_GOLD_POWERBI/THYROID_2026_SEMANTIC_MODEL.pbix**

### 5.1 Star Schema Design

**Fact Tables:**
- Fact_Episodes, Fact_Labs, Fact_Imaging, Fact_Pathology, Fact_Treatment, Fact_NSQIP, Fact_Outcomes

**Dimension Tables:**
- Dim_Patient, Dim_Date, Dim_Staging, Dim_Treatment, Dim_Outcome

**All relationships join on research_id only** (plus date keys for temporal queries).

### 5.2 Import Configuration (Power Query M)

Each fact table imported via Power Query from local Parquet:
```m
let
    Source = Parquet.Contents("file:///Users/lhglosser/THYROID_SECURE_2026/01_SILVER_DEID_PARQUET/[tablename].parquet/"),
    #"Converted to Table" = Table.FromRecords(Source),
    #"Changed Type" = Table.TransformColumnTypes(...),
    #"Removed Errors" = Table.RemoveRowsWithErrors(...),
    #"Final Output" = ...
in
    #"Final Output"
```

### 5.3 DAX Measures (Key Analytic Calculations)

All measures reference research_id; preserve PHI boundaries.

Examples:
- `Measure_Total_Patients = DISTINCTCOUNT(Dim_Patient[research_id])`
- `Measure_Mean_Age = AVERAGE(Dim_Patient[age_at_diagnosis])`
- `Measure_Recurrence_Rate = DIVIDE(CALCULATE(DISTINCTCOUNT(...), outcome_status=recurrence), Measure_Total_Patients)`
- `Measure_Mortality_30d = DIVIDE(CALCULATE(DISTINCTCOUNT(...), nsqip_mortality=1), ...)`
- `Measure_Avg_TSH_by_Year = AVERAGEX(FILTER(Fact_Labs, ...), result_value)`

### 5.4 Report Pages (Power BI Desktop)

1. **Dashboard - Overview:** Cards (Total Patients, Age, % Female); charts (by Stage, by Year); KPIs (Recurrence, Mortality)
2. **Labs Analytics:** TSH/T3/T4 trends by cohort; scatter (TSH vs. Age); distributions by intent
3. **Imaging Report:** Matrix (Modality × Body Part); findings summary
4. **Pathology & Staging:** Matrix (TNM combinations vs. Outcome); histology × grade × survival
5. **Treatment & Outcomes:** Stacked bar (Treatment × Outcome); complication rates; LOS; survival curves
6. **Data Quality & Validation:** Row counts; missing data %; date shift verification; audit trail log

### 5.5 Sensitivity Labels & Row-Level Security (RLS)

- Mark all columns as "Confidential" (for future cloud publishing)
- RLS Rule: `[research_id] = USERNAME()` (prepare for multi-user scenario)
- Test: Browse as guest user; should see no rows

---

## 6. Future Data Incorporation & Editing Workflow (SOP)

### 6.1 Receiving New Data

1. New Excel extract arrives (from EHR, NSQIP, registry)
2. Validation gate: Confirm schema; scan for PHI
3. Store in 00_RAW_PHI/incoming/
4. Run de-ID script: `python3 SCRIPTS/00_deid_gateway.py --input=incoming --table=labs`
5. Audit trail: Script logs new rows, date shifts, columns dropped
6. Merge into Silver: New rows appended to existing Parquet
7. Refresh Power BI: Trigger via Power Automate Desktop or osascript
8. QC Report: Reconciliation script validates new data

### 6.2 Correcting Existing Records

**Example:** Lab value for research_id=5012 was entered incorrectly (TSH 15.2 should be 1.52).

1. Locate record in 00_RAW_PHI/lab_results.xlsx (via research_id lookup)
2. Correct in RAW_PHI; document change in EXTRACTION_LOG.md
3. Re-run de-ID: `python3 SCRIPTS/00_deid_gateway.py --table=labs --force-overwrite`
4. Version control: `git commit -m "Correct TSH value for research_id=5012"`
5. Refresh Power BI
6. Audit: Log in VALIDATION_AUDITS/correction_log.md

### 6.3 Edge Cases

- **Patient withdrawn:** Delete from 00_RAW_PHI/; re-run de-ID; research_id disappears from Power BI reports
- **New diagnosis code:** Add to 00_RAW_PHI/; de-ID script processes; no manual Power BI changes needed
- **Script version update:** Bump script version; re-run full de-ID; all downstream Power BI sees updated results

---

## 7. LLM/NLM Extraction from Unparsed Excel Cells

Unstructured free-text (imaging findings, pathology reports) extracted to structured fields locally.

### 7.1 Workflow (Microsoft-Native, Local-First)

1. Identify masked text in 01_SILVER_DEID_PARQUET/
2. Export to CSV via Power Query
3. Local NLP pipeline (Python + spaCy): Entity recognition runs on Mac (research_id context only)
4. Structured output: Save to 04_EXTRACTION_OUTPUTS/pathology_entities_structured.parquet
5. Manual review (if needed): Copy masked text + entities to Excel; Logan reviews; corrections uploaded back
6. Merge into Silver: Join structured entities back to Fact_Pathology on research_id + date
7. Power BI refresh: Include extracted entities in reports

### 7.2 Why Not Cloud LLM (e.g., OpenAI)?

- **PHI Risk:** Sending masked text to cloud API could leak context
- **Cost:** Per-token billing for 50K+ text fields
- **Latency:** Cloud RPC overhead vs. local spaCy (< 100ms per record)
- **Reproducibility:** Local spaCy version-controlled; cloud API updates break reproducibility

---

## 8. Manuscript Generation & Dashboard Replacement

### 8.1 From Power BI to Word/PowerPoint

**Workflow:**

1. Create 6-page report in Power BI Desktop
2. Export tables & charts: Power BI Desktop → .csv; charts → .png or copy to clipboard
3. Word document assembly: Open template (02_GOLD_POWERBI/templates/manuscript_table_template.docx); paste Power BI tables (research_id completely absent; summary stats only); Copilot in Word generates narrative
4. PowerPoint presentation: Open template; paste Power BI charts; add Copilot-generated speaker notes
5. Version control & sign-offs: Save as 03_DEID_EXPORTS/2026-04-10_Manuscript_DRAFT_v3.docx; share to SharePoint (read-only); Logan + co-authors sign off in VALIDATION_AUDITS/manuscript_sign_off.md

### 8.2 Dashboard Replacement: Power BI Replaces Streamlit

**Before (MotherDuck + Streamlit):**
- Streamlit app on cloud; queries MotherDuck cloud DB
- Slow, cloud dependency, token management

**After (Power BI Desktop):**
- Power BI Desktop on Mac; imports local Parquet
- Fast, offline-capable, no credentials needed
- Share read-only .pbix with team (future: Power BI Web in Phase 5)

**Feature Parity:**
- Streamlit filters → Power BI Slicers
- Streamlit tables → Power BI Tables
- Streamlit charts → Power BI Visualizations
- Streamlit drill-through → Power BI Drill-through

---

## 9. Automation Architecture

Complete orchestration using four tools working in concert.

### 9.1 Automation Stack

| Tool | Role | Runs On | Trigger |
|---|---|---|---|
| **Desktop Commander** | Local file ops, Python script execution, process monitoring | macOS terminal | Manual or scheduled |
| **osascript** | Open/close apps, keystroke simulation, Finder ops | macOS native | osascript CLI or Power Automate Desktop |
| **Claude in Chrome** | M365 web app interaction (SharePoint, Power Automate, Power BI web) | Browser | Manual or web automation |
| **Power Automate Desktop** | Local RPA for Excel↔Parquet cycles, PowerBI.exe refresh | macOS Robot Desktop client | Cloud Power Automate trigger or scheduled |

### 9.2 Weekly Silver Layer Refresh Cycle

```
Monday 9:00 AM
├─ Power Automate Cloud: "Weekly Silver Refresh" flow triggers
│  └─ Calls Power Automate Desktop robot on Mac
│
Power Automate Desktop:
├─ Launch Terminal (osascript)
├─ Run: python3 SCRIPTS/00_deid_gateway.py --table=all
├─ Run: python3 SCRIPTS/09_validate_relationships.py
├─ Launch Power BI Desktop (osascript)
├─ Trigger Power BI Refresh (Cmd+Shift+R)
├─ Close Power BI Desktop
├─ Copy VALIDATION_AUDITS/weekly_reconciliation.md → OneDrive/Metadata/
└─ If failures: send Teams message to #thyroid-research-admins
```

### 9.3 Power Automate Desktop Flow (RPA Robot)

See COWORK_IMPLEMENTATION_PROMPT.md for detailed step-by-step robot build guide.

### 9.4 osascript (AppleScript) Examples

- `osascript -e 'open application "Microsoft Power BI"'` — Open Power BI Desktop
- `osascript -e 'quit application "Microsoft Power BI"'` — Close Power BI
- Copy file to OneDrive via Finder automation
- Create calendar reminders

### 9.5 Claude in Chrome (M365 Web Interactions)

- Upload QC reports to SharePoint
- Build Power Automate Cloud flows
- Publish Power BI reports to web (Phase 5)
- Manage Teams channels & notifications

---

## 10. MotherDuck Elimination Tracker

### 10.1 Current State: 307 Files, 1,786 MotherDuck References

**Breakdown by Type:**
- Python Scripts (100+ files): `.sql` queries + connection strings
- Documentation (50+ files): Architecture diagrams, setup guides
- Configuration (30+ files): `.yaml`, `.json` with credentials
- Tests (20+ files): Unit test mocks
- Streamlit Apps (50+ files): App pages
- Notebooks (30+ files): Jupyter notebooks
- Other (27 files): Comments, README, CI/CD

### 10.2 Categorized Cleanup Plan

| Category | File Count | Effort | Approach | Deadline |
|---|---|---|---|---|
| **High-Priority ETL Scripts** | 35 | 1 day | Replace `motherduck.query()` with `duckdb.sql()` + tests | Phase 4A |
| **Streamlit App Refactoring** | 50 | 2 days | Remove entirely; convert to Power BI Desktop reports | Phase 4C |
| **Configuration Files** | 30 | 2 hours | Remove .motherduck_token, MotherDuck URLs | Phase 4A |
| **Documentation** | 50 | 1 day | Update guides to reference local Parquet + Power BI | Phase 4B |
| **Tests & Mocks** | 20 | 4 hours | Update fixtures to use local DuckDB | Phase 4D |
| **Notebooks** | 30 | 1 day | Archive or convert to Python scripts | Phase 4E |
| **Comments & Minor Refs** | 27 | 1 hour | Find-and-replace MotherDuck → DuckDB | Phase 4F |
| **CI/CD & Deployment** | 15 | 4 hours | Remove token from GitHub Secrets; update pipelines | Phase 4A |
| **TOTAL** | **307** | **~7 days** | — | **Phase 4E (Day 10)** |

### 10.3 Migration Mapping: MotherDuck → Microsoft

| MotherDuck Operation | Current Code | Microsoft Replacement | New Code |
|---|---|---|---|
| Connect to cloud database | `motherduck.connect()` | Connect to local DuckDB | `duckdb.connect('THYROID_2026.db')` |
| Query cloud table | `pd.read_sql("SELECT ...", conn)` | Read local Parquet | `pd.read_parquet('01_SILVER_DEID_PARQUET/table.parquet')` |
| Share database via token | `conn.get_share_token()` | Share de-identified CSV via SharePoint | `upload_to_sharepoint(df)` |
| Materialized views (cloud) | `CREATE VIEW cloud_view AS ...` | Power Query transforms + DAX measures | Power BI data model relationships + measures |
| Dashboard (Streamlit + cloud) | Streamlit app querying MotherDuck | Power BI Desktop semantic model + reports | Local .pbix file |
| Scheduled refresh | MotherDuck background jobs | Power Automate Desktop RPA robot on Mac | Weekly trigger via Power Automate Cloud |

### 10.4 Implementation Checklist (307 Files)

**Phase 4A (Day 1):**
- Remove MotherDuck token from GitHub Secrets
- Remove `motherduck` from requirements.txt
- Create local DuckDB database
- Test: Run top 10 ETL scripts with local DuckDB

**Phase 4B (Days 2-3):**
- Refactor 35 Python ETL scripts
- Update 50 documentation files
- Test: Run full ETL pipeline locally

**Phase 4C (Days 4-5):**
- Remove 50 Streamlit app files (or archive)
- Create 6 Power BI Desktop report pages
- Test: Validate Power BI star schema

**Phase 4D (Days 6-7):**
- Update 20 test files
- Wire automation: Power Automate Desktop robot
- Test: Scheduled refresh cycle works

**Phase 4E (Days 8-10):**
- Archive/remove 30 notebooks
- Global find-and-replace (27 comment files)
- Final audit: `grep -r "motherduck" . | wc -l` = 0

**Phase 4F (Days 11-14):**
- Validation: Reconcile MotherDuck cloud ↔ local Parquet
- Manuscript generation: Word/PowerPoint from Power BI
- Weekly backup verification
- Final sign-off

---

## 11. Testing, Validation & Maintenance Checklist

### 11.1 Pre-Production Validation (Phase 4F)

**Data Quality Tests:**
- Row Count Reconciliation: Cloud DB total rows == local Parquet total rows
- Column Completeness: All expected columns present
- No PHI Leakage: Scan for MRN, SSN, phone patterns (should be 0)
- research_id Uniqueness: All 11,673 values unique
- Foreign Key Integrity: All lookups exist
- Date Shift Verification: All dates shifted by documented amount
- Missing Value Audit: % missing by column

**Power BI Model Tests:**
- Relationship Tests: All 12 relationships active; no circular dependencies
- DAX Measure Tests: All measures calculate without errors
- Slicer Tests: All slicers respond correctly
- Drill-through Tests: Click-through navigation works
- Performance Tests: Model refresh < 60 seconds; visualizations render < 2 seconds

**Automation Tests:**
- Power Automate Desktop Robot: Manual trigger works end-to-end
- osascript: Open/close Power BI, Terminal; copy files
- Claude in Chrome: Upload QC report to SharePoint
- Full Cycle: RAW → de-ID → Parquet → Power BI completes < 30 min

**Security & Compliance Tests:**
- FileVault Status: Folder encrypted
- Backup Integrity: Weekly snapshot created; can restore
- Audit Trail: VALIDATION_AUDITS/ logs all runs
- Access Logs: FileVault login, script execution logged

### 11.2 Weekly Maintenance (Ongoing)

**Every Monday 9:00 AM (Post-Refresh):**
- Check audit: Input rows = Output rows?
- Check reconciliation: Referential integrity OK?
- Power BI: Any refresh errors?
- Backup: Latest snapshot exists?

**Every Friday (Pre-Backup):**
- Row count check
- Data quality scan
- File system: Disk space OK?
- Script execution: No errors in logs?

**Monthly (Quarterly Sign-Off):**
- Logan + IRB/Privacy Officer review VALIDATION_AUDITS/
- Sign-off in VALIDATION_AUDITS/monthly_compliance_sign_off.md

### 11.3 Troubleshooting Guide

| Issue | Symptom | Root Cause | Resolution |
|---|---|---|---|
| Power BI Refresh Fails | "Refresh Failed" error | Parquet moved/corrupted | Restore from 05_ARCHIVE_BACKUPS/; re-run de-ID |
| research_id Mismatch | Report shows N; RAW shows M | New records not de-identified | Re-run de-ID with `--table=all` |
| Date Shifts Not Applied | Dates unchanged | De-ID script failed silently | Check VALIDATION_AUDITS/ log; re-run with `--verbose` |
| RPA Robot Hangs | Robot stops mid-execution | Terminal command timeout | `kill -9 [PID]`; restart robot |
| OneDrive Sync Fails | Metadata not syncing | FileVault permission or network | Verify OneDrive symlink exists; re-mount |
| Audit Trail Missing | VALIDATION_AUDITS/ empty | De-ID script not creating logs | Verify 10_generate_deid_audit.py called |

---

## 12. Implementation Phases & Timeline

### Phase 4A: Folder Structure & Security Setup (Day 1)
- Create /Users/lhglosser/THYROID_SECURE_2026/ hierarchy
- Set permissions (chmod 700, 500, 755 per folder)
- Initialize git-lfs, DVC
- Remove MotherDuck credentials
- Verify FileVault encryption
- **Deliverable:** Secure folder structure; zero MotherDuck credentials

### Phase 4B: Silver Layer Build & De-Identification (Days 2-3)
- Write 00_deid_gateway.py
- Ingest 13 base tables from 00_RAW_PHI/
- Generate validation tables
- Test de-ID; verify no PHI leakage
- DVC commit Parquet
- **Deliverable:** 01_SILVER_DEID_PARQUET/ with 13 fact tables + 4 dims; audit trail

### Phase 4C: Power BI Semantic Model & Report Pages (Days 4-5)
- Open Power BI Desktop
- Import Parquet via Power Query
- Define 12 relationships
- Write 20+ DAX measures
- Create 6 report pages
- Test slicers, drill-through, RLS, performance
- Save as .pbix (git-LFS)
- **Deliverable:** Power BI Desktop .pbix with 6 report pages; validated star schema

### Phase 4D: Automation Wiring (Days 6-7)
- Create Power Automate Cloud flow
- Create Power Automate Desktop robot
- Write osascript helpers
- Test end-to-end cycle
- Document in SOP
- **Deliverable:** Automated weekly refresh cycle; audit trails logged

### Phase 4E: MotherDuck Elimination (Days 8-10)
- Refactor 35 Python ETL scripts
- Remove 50 Streamlit app files
- Update 50 documentation files
- Update 20 test files
- Archive 30 notebooks
- Global find-and-replace (27 files)
- Final audit: grep -r "motherduck" . = 0
- **Deliverable:** 307 files cleaned; git commit

### Phase 4F: Validation & Manuscript Pipeline (Days 11-14)
- Run all validation tests
- Generate sample manuscript
- Team review + sign-off
- Test backup & restore
- Create quarterly compliance sign-off
- Final sign-off: Logan + Emory IRB
- **Deliverable:** Production-ready system; all tests pass; sample manuscript approved

---

## Summary & Go-Live

By end of Phase 4F (Day 14), the system will be:

✓ **Secure:** Air-gapped PHI in local FileVault; research_id-only in cloud
✓ **Automated:** Weekly refresh cycle via Power Automate + osascript
✓ **Validated:** 11,673 de-identified patients; 13 fact tables + 8 views
✓ **Documented:** Data dictionary, SOPs, architecture diagrams, audit trails
✓ **Compliant:** HIPAA-aligned; quarterly sign-offs; breach protocol
✓ **Production-Ready:** Power BI Desktop replaces Streamlit; Power Automate replaces MotherDuck

**Next Phase (Phase 5, Future):**
- Publish Power BI Desktop .pbix to Power BI Web (cloud read-only embed)
- Enable Emory team members to access read-only reports via Power BI Web
- Implement Power BI RLS (Row-Level Security) for multi-user scenarios
- Explore advanced analytics (Python/R scripts in Power BI)

---

**Plan Document Prepared:** 2026-03-27
**Prepared By:** Implementation Team
**Review Date:** 2026-04-10 (Phase 4F)
