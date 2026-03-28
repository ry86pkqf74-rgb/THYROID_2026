# Microsoft Deployment Plan: THYROID_2026
## Complete Documentation Index

**Project:** 11,673-patient thyroid cancer research lakehouse
**Current:** DuckDB/MotherDuck + Streamlit (being eliminated)
**Target:** Local Microsoft 365 power stack + ElevenLabs + Replit + LangGraph (100% air-gapped PHI)
**Timeline:** Phase 4 (25 days: 2026-03-27 to 2026-04-21)
**Owner:** Logan Glosser (LGLOSSE@emory.edu), Emory University

---

## Files in This Directory

### 1. MICROSOFT_DEPLOYMENT_PLAN.md (572 lines)
**The master plan document — START HERE**

Complete specification of Phase 4 implementation across 12 major sections:

- **Section 1:** Non-negotiable security & privacy rules (PHI isolation, research_id linkage, audit trails)
- **Section 2:** Secure folder structure (/Users/lhglosser/THYROID_SECURE_2026/)
- **Section 3:** M365 app inventory (30 apps mapped to thyroid project roles; P0/P1/P2/P3 prioritization)
- **Section 4:** Silver layer de-identification pipeline (00_RAW_PHI → research_id-only Parquet via Python script)
- **Section 5:** Power BI star schema (13 fact tables, 8 dimensions, 12 relationships, 20+ DAX measures, 6 report pages)
- **Section 6:** Future data incorporation & editing SOP (adding new data, correcting records, edge cases)
- **Section 7:** LLM/NLM extraction from unstructured text (local spaCy for entity recognition)
- **Section 8:** Manuscript generation & dashboard replacement (Power BI replaces Streamlit)
- **Section 9:** Automation architecture (Power Automate Cloud + Desktop + osascript + Claude in Chrome)
- **Section 10:** MotherDuck elimination tracker (307 files, 1,786 references, cleanup plan)
- **Section 11:** Testing, validation & maintenance checklist
- **Section 12:** Implementation phases & timeline (Phase 4A-F, 14 days total)

**Use this file for:** Overview, reference architecture, deep-dive into each component

---

### 2. FOLDER_SETUP.sh (164 lines)
**Executable bash script — RUN THIS FIRST**

Creates the entire secure folder hierarchy at `/Users/lhglosser/THYROID_SECURE_2026/` with correct permissions.

**What it does:**
- Creates 15+ directories (RAW_PHI, SILVER, GOLD_POWERBI, EXTRACTION_OUTPUTS, ARCHIVE_BACKUPS, SCRIPTS, DOCUMENTATION, VALIDATION_AUDITS, etc.)
- Sets permissions: chmod 700 (root), 500 (RAW_PHI read-only), 755 (everything else read-write)
- Verifies FileVault encryption is enabled
- Outputs folder structure and next steps

**How to run:**
```bash
chmod +x FOLDER_SETUP.sh
./FOLDER_SETUP.sh
```

**Expected output:**
- All directories created with correct permissions
- Summary of folder structure
- Security reminders (FileVault, read-only 00_RAW_PHI, no PHI in git)

**Use this file for:** Phase 4A (Day 1) folder structure setup

---

### 3. M365_APP_MATRIX.md (433 lines)
**Complete feature matrix for all 30 M365 apps**

App-by-app breakdown: availability, thyroid project role, automation method, priority level, testing criteria, contingencies.

**Organized by:**
- **P0 CRITICAL (5 apps):** Power BI Desktop, Excel, Word, OneDrive, Power Automate — all must function
- **P1 IMPORTANT (8 apps):** SharePoint, Teams, Outlook, Forms, Lists, Planner, Calendar, Visio — enable collaboration
- **P2 NICE-TO-HAVE (7 apps):** Power BI Web, To Do, Loop, OneNote, Whiteboard, Copilot (limited), Power Automate Desktop
- **P3 NOT APPLICABLE (10 apps):** Bookings, Culture Cloud, Find a Room, Clipchamp, Reflect, Sway, Power Pages, Power Apps, Engage, Org Explorer

**Key sections:**
- Emory SSO & licensing notes (unlimited OneDrive, Power BI Desktop free, Copilot limited at Emory)
- Integration patterns (3 examples: scheduled refresh, data quality gate, manuscript authorship loop)
- Next steps for Phase 4A-5 configuration

**Use this file for:** Understanding which M365 apps do what, how they integrate, licensing/SSO details

---

### 4. MOTHERDUCK_MIGRATION_MAP.md (855 lines)
**Line-by-line migration guide for 307 files**

Complete mapping of MotherDuck operations to Microsoft 365 / local DuckDB equivalents.

**Coverage:**
1. Database connection & authentication (motherduck.connect → duckdb.connect)
2. Cloud table queries (SQL → DuckDB SQL / Pandas Parquet read)
3. Shares & access control (tokens → SharePoint / OneDrive exports)
4. Materialized views (cloud views → Power Query / DuckDB views / DAX measures)
5. Dashboards (Streamlit + cloud DB → Power BI Desktop local .pbix)
6. Credentials & secrets (tokens → zero credentials needed)
7. Scheduled refresh (CI/CD cloud → Power Automate Desktop RPA robot on Mac)
8. Data warehouse tables (cloud tables → local Parquet partitions)
9. User access & permissions (tokens → FileVault + Power BI RLS Phase 5)
10. Backup & disaster recovery (cloud backups → local weekly encrypted snapshots)
11. Cost comparison (MotherDuck $100-500/year → Microsoft $0 + savings)
12. Migration checklist (307 files, 7-day cleanup plan)
13. Troubleshooting guide (10 common issues + solutions)
14. Sustainability & future-proofing (Phases 4-6 roadmap)

**Code examples:** Before/after Python scripts for every operation type

**Use this file for:** Refactoring Python scripts, understanding migration equivalents, troubleshooting failures

---

### 5. SUPPLEMENTARY_TOOLCHAIN.md (449 lines)
**ElevenLabs + Replit + LangChain/LangGraph integration plan**

Extends the Microsoft-only pipeline with three supplementary tools that fill gaps M365 can't:

**Coverage:**
- ElevenLabs: Voice-powered research assistant (Knowledge Base), TTS for presentations, STT for clinical notes (Phase 5+)
- Replit: Full-stack dashboard (FastAPI + React), LangGraph agent backend API, figure generation service, batch workers
- LangChain/LangGraph: Clinical note extraction agent, data quality monitoring, natural language query agent, manuscript drafting
- Combined architecture diagram (local Mac ↔ cloud services with PHI safety)
- PHI Safety Matrix (9-row assessment of every service)
- Implementation timeline: Phases 4G-4I (Days 15-25)
- Cost: $0 incremental (Replit credits pre-paid monthly, ElevenLabs Free tier, LangGraph agents routed through existing Claude subscription)
- Capability comparison: Microsoft-only vs. full stack (12 capabilities)

**Use this file for:** Understanding how ElevenLabs, Replit, and LangGraph extend the M365 pipeline

---

### 6. COWORK_IMPLEMENTATION_PROMPT.md (300+ lines)
**Ready-to-paste prompt for starting Cowork implementation session**

Complete briefing that can be copied directly into a Claude Code Cowork session to begin Phase 4 work.

**Includes:**
- Project overview (current state, target state, timeline)
- Non-negotiable security rules (6 core principles)
- M365 apps in scope (P0, P1, P2 prioritization)
- Folder structure overview
- 14-day implementation phase breakdown (4A-4F)
- Key technical decisions
- Documentation to review
- Success criteria (Phase 4F validation checklist)
- Contingencies
- Next immediate actions

**Use this file for:** Starting new Cowork sessions, briefing team members, quick reference during implementation

---

## Implementation Timeline (14 Days)

| Phase | Duration | Focus | Deliverables |
|---|---|---|---|
| **4A** | Day 1 | Folder structure, security, credentials removal | Secure folder hierarchy; zero credentials in repo |
| **4B** | Days 2-3 | De-identification pipeline, Silver layer | 01_SILVER_DEID_PARQUET/ with 13 fact tables; audit trail |
| **4C** | Days 4-5 | Power BI star schema, 6 report pages | .pbix file with validated schema; 6 production-ready pages |
| **4D** | Days 6-7 | Power Automate automation, osascript helpers | Automated weekly refresh pipeline working end-to-end |
| **4E** | Days 8-10 | MotherDuck elimination, refactor 307 files | 307 files cleaned; grep -r "motherduck" = 0 |
| **4F** | Days 11-14 | Validation, compliance sign-off, manuscript pipeline | Production-ready system; all tests pass; sign-offs complete |
| **4G** | Days 15-18 | ElevenLabs voice assistant + Replit dashboard deployment | Voice research agent live; FastAPI + React dashboard deployed |
| **4H** | Days 19-22 | LangGraph clinical extraction + data quality agents | Note extraction pipeline tested on 100+ notes; QA agent running |
| **4I** | Days 23-25 | Integration testing + voice→agent→answer pipeline | Full stack validated end-to-end; all services connected |

---

## Quick Start

### For New Team Members
1. Read: **COWORK_IMPLEMENTATION_PROMPT.md** (5 min overview)
2. Reference: **MICROSOFT_DEPLOYMENT_PLAN.md** Section 1-3 (architecture)
3. Review: **M365_APP_MATRIX.md** P0 & P1 apps (15 min)

### For Implementation
1. Execute: **FOLDER_SETUP.sh** (Phase 4A, Day 1)
2. Reference: **MOTHERDUCK_MIGRATION_MAP.md** (refactor scripts, Phase 4E)
3. Build: Power BI .pbix using MICROSOFT_DEPLOYMENT_PLAN.md Section 5 (Phase 4C)
4. Validate: MICROSOFT_DEPLOYMENT_PLAN.md Section 11 checklist (Phase 4F)

### For Troubleshooting
1. Check: **MOTHERDUCK_MIGRATION_MAP.md** Section 13 (10 common issues)
2. Verify: **M365_APP_MATRIX.md** contingency columns
3. Review: **MICROSOFT_DEPLOYMENT_PLAN.md** Section 11 (testing & maintenance)

---

## Key Statistics

| Metric | Value |
|---|---|
| **Project Size** | 11,673 patients, 13 base tables, 8+ analytic views |
| **Files to Clean** | 307 files, 1,786 MotherDuck references |
| **Documentation Created** | 7 files, 2,950+ lines total, 142 KB |
| **M365 Apps in Scope** | 30 apps, 5 P0-critical, 8 P1-important, 7 P2-nice-to-have |
| **Power BI Star Schema** | 13 fact tables, 8 dimensions, 12 relationships, 20+ DAX measures, 6 report pages |
| **Implementation Timeline** | 25 days (Phase 4A-I), ~110 hours active work |
| **Supplementary Tools** | ElevenLabs (Free tier), Replit (pre-paid, $0 extra), LangGraph (OSS), Claude via Cowork |
| **Net Incremental Cost** | $0 — all tools are free, Emory-provided, or pre-paid |
| **Security Baseline** | FileVault encryption, research_id-only linking, 7-year audit retention, HIPAA compliance |

---

## Security Principles

All files in this directory implement 6 non-negotiable security rules:

1. **PHI Isolation:** Raw PHI read-only locally; only research_id-keyed Parquet in analysis
2. **research_id Linkage:** Single minimal identifier; MRN lookup locked in 00_RAW_PHI/
3. **De-Identification Audit Trail:** Every run logged (timestamp, row counts, columns, hash, signature)
4. **No Cloud Materialization:** OneDrive/SharePoint/Teams metadata only; never raw data
5. **Backup & Disaster Recovery:** Weekly encrypted snapshots; 7-year retention; off-site metadata copies
6. **Audit & Compliance:** Quarterly sign-offs; access logs; breach protocol

---

## Next Steps

1. **Immediate (Today):** Read COWORK_IMPLEMENTATION_PROMPT.md for overview
2. **Phase 4A (Tomorrow):** Run FOLDER_SETUP.sh to create secure folder structure
3. **Phase 4B (Days 2-3):** Write de-identification script (MICROSOFT_DEPLOYMENT_PLAN.md Section 4)
4. **Phase 4C (Days 4-5):** Build Power BI .pbix (MICROSOFT_DEPLOYMENT_PLAN.md Section 5)
5. **Phase 4D (Days 6-7):** Wire automation (MICROSOFT_DEPLOYMENT_PLAN.md Section 9)
6. **Phase 4E (Days 8-10):** Eliminate MotherDuck (MOTHERDUCK_MIGRATION_MAP.md)
7. **Phase 4F (Days 11-14):** Validate system (MICROSOFT_DEPLOYMENT_PLAN.md Section 11)
8. **Phase 4G (Days 15-18):** Deploy Replit dashboard + ElevenLabs voice assistant (SUPPLEMENTARY_TOOLCHAIN.md)
9. **Phase 4H (Days 19-22):** Build LangGraph extraction + QA agents (SUPPLEMENTARY_TOOLCHAIN.md)
10. **Phase 4I (Days 23-25):** Integration testing, full pipeline validation (SUPPLEMENTARY_TOOLCHAIN.md)

---

## Questions?

- **Architecture & Security:** MICROSOFT_DEPLOYMENT_PLAN.md Sections 1-3
- **Specific App Usage:** M365_APP_MATRIX.md (search by app name)
- **Code Migration:** MOTHERDUCK_MIGRATION_MAP.md (search by operation type)
- **Folder Structure:** FOLDER_SETUP.sh (execute it first)
- **Automation Details:** MICROSOFT_DEPLOYMENT_PLAN.md Section 9
- **Phase Status:** Check VALIDATION_AUDITS/ logs after each phase

---

**Documentation Version:** 2.0
**Created:** 2026-03-27
**Status:** Publication-Grade, Ready for Implementation
**Next Review:** Phase 4A completion (2026-03-28)
**Author:** Implementation Team
**Owner:** Logan Glosser (LGLOSSE@emory.edu)
