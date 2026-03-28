# Microsoft 365 App Capability Matrix
## THYROID_2026 Deployment

Complete feature-by-feature mapping of all 30 M365 apps available through Emory Enterprise license.

---

## P0: CRITICAL APPS (5)

These apps form the foundation of the THYROID_2026 pipeline. All must function correctly for Phase 4 to succeed.

### 1. Power BI Desktop

| Property | Value |
|---|---|
| **Access URL** | macOS app (Microsoft Store or Office 365 installer) |
| **Available Via** | Desktop only (local installation required) |
| **Thyroid Project Role** | Primary semantic model engine; hosts star schema on 13 base tables + 8 analytic views; imports Parquet from 01_SILVER_DEID_PARQUET/; generates 6 report pages (Overview, Labs, Imaging, Pathology, Treatment, Data Quality); executes 20+ DAX measures |
| **Automation Method** | osascript (open/close/refresh triggers), Power Automate Desktop (RPA robot), scheduled refresh via cloud flow |
| **Priority** | **P0 CRITICAL** |
| **Key Features for THYROID_2026** | Import mode (no DirectQuery to cloud); local Parquet connectivity; star schema with 12 relationships; row-level security (future); sensitivity labels; drill-through pages; slicers (Date, Stage, Treatment, Intent); performance tested for 11,673-patient model |
| **Configuration Required** | Connect to local DuckDB or Parquet files; no cloud credentials needed; enable RLS with research_id-only rule |
| **Testing Criteria** | Model refresh completes < 60 sec; all DAX measures calculate without errors; all 6 report pages render; slicers filter correctly; no DirectQuery cloud connections present |
| **Contingency** | If Power BI Desktop fails, fall back to local Excel + pivot tables; Power BI Web (Phase 5) can then replace desktop |

---

### 2. Excel

| Property | Value |
|---|---|
| **Access URL** | macOS app (Microsoft Office ProPlus installer) |
| **Available Via** | Desktop + Web (office.com) |
| **Thyroid Project Role** | Power Query transforms for ETL (extract, clean, validate from 01_SILVER_DEID_PARQUET/); pivot tables for ad-hoc QC; data entry validation; formula-based reconciliation (row count checks, date shift verification); manual corrections to de-identified data (stored back in Parquet) |
| **Automation Method** | osascript (open/close Excel), Power Automate Desktop (copy/paste automation between CSV/Excel/Parquet), Claude in Chrome (web editing for shared workbooks) |
| **Priority** | **P0 CRITICAL** |
| **Key Features for THYROID_2026** | Power Query M language for extract transforms; pivot table drill-down for QC; cell-level validation rules; external data connections to local Parquet via Power Query; conditional formatting for outlier flagging; formula arrays for reconciliation |
| **Configuration Required** | Enable external data connections (Power Query); configure Parquet connection string; set up Data Validation rules for manual corrections |
| **Testing Criteria** | Power Query transform loads 11,673 rows without errors; pivot tables pivot correctly on research_id fields; validation rules prevent invalid entries; can export cleaned data back to Parquet |
| **Contingency** | If Excel fails, use Python Pandas directly for transforms; manual QC via CSV exports |

---

### 3. Word

| Property | Value |
|---|---|
| **Access URL** | macOS app (Microsoft Office ProPlus) + web (office.com) |
| **Available Via** | Desktop + Web |
| **Thyroid Project Role** | Manuscript drafting with de-identified data tables from Power BI exports; Copilot (when available) co-authoring narrative text from summary statistics; audit trail documentation (SOP, data dictionary, compliance sign-offs); template management (02_GOLD_POWERBI/templates/manuscript_table_template.docx); final publication-ready document generation |
| **Automation Method** | osascript (open .docx files from automation), Claude in Chrome (web-based collaborative editing), Power Automate Desktop (insert tables/charts via copy-paste) |
| **Priority** | **P0 CRITICAL** |
| **Key Features for THYROID_2026** | Track changes for manuscript review (Logan + co-authors); content control for template sections; mail merge for audit trail batch generation; image insertion from Power BI chart screenshots; table auto-numbering; cross-references; citation formatting (future journal requirements) |
| **Configuration Required** | Create manuscript template with placeholders for Power BI tables; set up Copilot prompts for narrative generation |
| **Testing Criteria** | Can paste Power BI tables into Word; tables maintain formatting; Copilot generates coherent narratives from summary statistics; document can be saved/shared to SharePoint with read-only permissions |
| **Contingency** | If Word fails, use Google Docs for collaborative editing; export to .docx; or use LaTeX + Overleaf for manuscript |

---

### 4. OneDrive

| Property | Value |
|---|---|
| **Access URL** | /Users/lhglosser/OneDrive - Emory (symlinked from /Users/lhglosser/Library/CloudStorage/OneDrive-Emory) |
| **Available Via** | Desktop (folder sync) + Web (onedrive.live.com) |
| **Thyroid Project Role** | Weekly metadata-only backups: aggregate row counts (no sensitive data), schema versions, DVC pointers, Power BI .pbix file version (via git-lfs pointer), script versions, audit log pointers; off-site encrypted snapshots for disaster recovery (VALIDATION_AUDITS/ only); future: team member access to read-only exported tables (Phase 5) |
| **Automation Method** | osascript (Finder automation to copy files), Power Automate Desktop (scheduled backup copies), Claude in Chrome (verify sync status on web) |
| **Priority** | **P0 CRITICAL** |
| **Key Features for THYROID_2026** | File sync across devices; version history (auto-retain 93 days); selective sync (only backup folders synced, not 00_RAW_PHI/); storage quota monitoring (Emory Enterprise = unlimited); offline access to synced folders; encryption in transit (TLS) |
| **Configuration Required** | Symlink /Users/lhglosser/Library/CloudStorage/OneDrive-Emory to /Users/lhglosser/OneDrive - Emory; set selective sync to backup-only folders; configure version retention policy |
| **Testing Criteria** | Metadata backups sync within 5 min of creation; files appear in OneDrive web interface with correct timestamps; can retrieve version history; zero PHI files present in OneDrive directory |
| **Contingency** | If OneDrive syncing fails, use manual SFTP backup to Emory network drive; or Backblaze for encrypted off-site backup |

---

### 5. Power Automate

| Property | Value |
|---|---|
| **Access URL** | make.powerautomate.com (web, requires Emory SSO) |
| **Available Via** | Web only (cloud-hosted flows, local RPA robots available via Power Automate Desktop) |
| **Thyroid Project Role** | Cloud-based orchestration for THYROID_2026 weekly refresh cycle: on-schedule trigger (Monday 9 AM) → invokes Power Automate Desktop RPA robot on Mac → logs result → sends Teams alert on success/failure; future: triggered exports on data modifications (Phase 5); email notifications (Outlook integration) |
| **Automation Method** | Claude in Chrome (build flows, test, enable/disable), Power Automate Desktop robot (local RPA execution), Teams/Outlook integration (notifications) |
| **Priority** | **P0 CRITICAL** |
| **Key Features for THYROID_2026** | Scheduled triggers (cron-like); desktop flow invocation (RPA robot calls); dynamic content from flow results; condition logic (if/else branches); action history; flow analytics (success rate, duration) |
| **Configuration Required** | Create "Weekly THYROID_2026 Silver Refresh" flow: Scheduled trigger (9 AM Monday UTC, adjust for local TZ) → Run desktop flow (Silver-Refresh-RPA-Robot) → Send Teams message with result |
| **Testing Criteria** | Scheduled trigger fires at correct time; RPA robot invocation succeeds; Teams notification arrives with status; all audit logs created in VALIDATION_AUDITS/ |
| **Contingency** | If Power Automate Cloud fails, use local cron job on macOS with shell script wrapper; or Task Scheduler on Windows (if moved to Windows VM) |

---

## P1: IMPORTANT APPS (8)

These apps enhance collaboration, enable team sharing, and support compliance workflows. They are essential for operational efficiency but have cloud-native fallbacks.

### 6. SharePoint

| Property | Value |
|---|---|
| **Access URL** | https://emory.sharepoint.com/sites/thyroid-research (Emory tenant) |
| **Available Via** | Web only (requires Emory SSO + Outlook credentials) |
| **Thyroid Project Role** | Team collaboration space for manuscript authorship (read-only de-identified tables shared with co-authors); non-sensitive QC reports uploaded post-refresh (VALIDATION_AUDITS/weekly_reconciliation.md); version control of documentation (SOP, data dictionary); audit trail links for compliance review |
| **Automation Method** | Claude in Chrome (upload files, set permissions, manage document versions), Power Automate cloud flow (send files to SharePoint library on refresh completion) |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Document libraries with version control; versioning history (auto-retain for 7+ years for HIPAA); read-only shared links (no download for manuscripts); Copilot in SharePoint (search/summarize de-identified tables); Modern Pages (embed Power BI reports in Phase 5) |
| **Configuration Required** | Create "THYROID_2026 Research" site with Document Library for manuscripts, QC reports; set permissions: Logan (edit), co-authors (read-only), team (view); configure version retention = 7 years minimum |
| **Testing Criteria** | Can upload QC report to SharePoint; co-authors can access read-only link; version history accessible; no raw PHI or Parquet files present; SharePoint search finds de-identified table names |
| **Contingency** | If SharePoint unavailable, use Box, Dropbox, or Google Drive for team collaboration; or local Git with restricted access |

---

### 7. Teams

| Property | Value |
|---|---|
| **Access URL** | teams.microsoft.com (web, macOS app optional) |
| **Available Via** | Desktop + Web |
| **Thyroid Project Role** | Asynchronous team communication: daily standups in #thyroid-research channel; alerts from Power Automate flows ("Silver refresh complete" or "ERROR: de-ID failed"); manuscript review threads (read-only de-identified summaries, never raw data); scheduled meeting invites for weekly QC reviews |
| **Automation Method** | osascript (app control), Power Automate (send Teams message action with dynamic content), Claude in Chrome (post summaries, manage channels) |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Message retention policy (7 years for compliance); pinned messages for SOP links; file upload warnings (prevent accidental PHI sharing); integrations with Power Automate, Forms, Lists; @mentions for urgent alerts |
| **Configuration Required** | Create private #thyroid-research channel (Logan + co-authors); set data loss prevention (DLP) policy to block PHI keywords (MRN, SSN patterns) from being posted |
| **Testing Criteria** | Power Automate can post messages; @mentions trigger notifications; file sharing warnings appear; no PHI posted; message search finds de-identified summaries |
| **Contingency** | If Teams unavailable, use email + Outlook calendar; or Slack (with Emory credentials if available) |

---

### 8. Outlook

| Property | Value |
|---|---|
| **Access URL** | outlook.com (web) or macOS app (Microsoft Office ProPlus) |
| **Available Via** | Desktop + Web |
| **Thyroid Project Role** | Calendar for weekly QC check reminders (automated via osascript); email alerts from Power Automate (refresh success/failure); manuscript review sign-off emails; archival of compliance audit trail (encrypted, searchable); meeting scheduling with co-authors |
| **Automation Method** | osascript (create calendar events), Power Automate (send mail action), Claude in Chrome (draft emails, manage calendar) |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Calendar integration with Teams meetings; archival & compliance search (7-year retention); encryption (Transport Layer Security); focused inbox (alert emails prioritized); rules for auto-filing de-ID export notifications |
| **Configuration Required** | Create recurring calendar event: "THYROID_2026 Weekly QC Check" (Monday 9 AM + 1 hour); create rule: "If subject contains 'Silver Refresh Complete' → move to folder 'THYROID_Alerts'"; set email encryption for audit trail messages |
| **Testing Criteria** | Calendar reminder fires at scheduled time; refresh alert emails arrive; can search for historical alerts; archived emails retained > 7 years |
| **Contingency** | If Outlook fails, use Gmail + Google Calendar; calendar reminders via cron + local notifications |

---

### 9. Power Automate Desktop (Local RPA Robot)

| Property | Value |
|---|---|
| **Access URL** | N/A (local desktop app; installed via Microsoft Store or setup wizard) |
| **Available Via** | Desktop only (macOS) |
| **Thyroid Project Role** | Local RPA automation: execute de-ID Python script → validate Parquet integrity → open Power BI Desktop → trigger refresh (Cmd+Shift+R) → close Power BI → copy audit logs to OneDrive → trigger Teams alert, all without uploading data to cloud |
| **Automation Method** | Cloud Power Automate trigger (invokes robot), osascript helpers (launch Terminal, open/close apps), file system monitoring (triggers on new data arrival) |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Record/playback UI automation (click Power BI refresh button); command-line execution (python3 script invocation); file operations (copy, delete, rename); loop/condition logic; error handling (capture logs); variable storage (pass results to cloud flow) |
| **Configuration Required** | Install Power Automate Desktop (macOS); create "Silver-Refresh-RPA-Robot" flow: open Terminal → run de-ID script → run validation → open Power BI → click Refresh → capture output → close Power BI → copy logs to OneDrive |
| **Testing Criteria** | Manual trigger of robot completes full cycle in < 30 min; all output files created; audit log generated; Power Automate cloud flow receives result status; no errors in execution log |
| **Contingency** | If Power Automate Desktop unavailable, use local shell script + cron job; or AppleScript automation via osascript directly |

---

### 10. Lists

| Property | Value |
|---|---|
| **Access URL** | Emory SharePoint #thyroid-research site → Lists app |
| **Available Via** | Web only (embedded in SharePoint) |
| **Thyroid Project Role** | Tracking QC status (Pass/Fail per weekly refresh), manuscript milestones (Draft → Review → Approved), data validation checklist (de-ID verify, referential integrity check, date shift audit), MotherDuck elimination progress (307 files tracked) |
| **Automation Method** | Claude in Chrome (add/update list items), Power Automate (create list items from flow results), SharePoint column formulas |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Column types: Lookup (link to de-ID audit file), Choice (Pass/Fail status), Date (refresh date), Person (Logan, co-authors), Percentage (elimination progress); sorting/filtering; conditional formatting (highlight Failed status in red) |
| **Testing Criteria** | Can add/update list items via web; Lookup column links to VALIDATION_AUDITS/ files; Percentage column shows MotherDuck elimination progress |
| **Contingency** | If Lists unavailable, use Excel workbook on SharePoint; or Notion for simple tracking |

---

### 11. Planner

| Property | Value |
|---|---|
| **Access URL** | tasks.office.com (Emory tenant) |
| **Available Via** | Web + mobile app |
| **Thyroid Project Role** | Sprint planning for MotherDuck elimination (307 files, 1,786 references): track Phase 4A-F tasks (High-Priority ETL Scripts, Streamlit Refactor, Config Files, Docs Update, Tests, Notebooks, Comments, CI/CD); assign to Logan; set deadlines per phase; link to Teams #thyroid-research channel for visibility |
| **Automation Method** | Claude in Chrome (create/assign/complete tasks), Teams integration (tasks appear in channel), Outlook calendar sync |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Task checklists (sub-tasks for each 307-file category); priority flags (Critical, Important, Low); due dates (Phase 4A Day 1, 4B Days 2-3, etc.); progress percentage; team view (who is working on what); calendar integration |
| **Testing Criteria** | Can create Planner plan linked to Teams #thyroid-research; tasks sync to Outlook calendar; progress percentage updates as tasks completed |
| **Contingency** | If Planner unavailable, use To Do or Asana for task tracking |

---

### 12. Forms

| Property | Value |
|---|---|
| **Access URL** | forms.office.com (Emory tenant) |
| **Available Via** | Web only |
| **Thyroid Project Role** | Data entry validation forms for manual corrections: flag outlier lab value (TSH = 999 mIU/L) → Logan reviews → fills form "Correct to 99.9 mIU/L" → form response stored in SharePoint list → Python script ingests corrections → re-generates Silver layer; capture audit trail (who corrected, when, from/to values) |
| **Automation Method** | Claude in Chrome (create form, review responses), Power Automate (submit form responses to SharePoint list, trigger Python script), Excel export of form responses |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Question types: Text (correction value), Date (correction date), Dropdown (reason: data entry error, EHR typo, unit conversion), Likert (confidence in correction); branching logic (show reason field only if "needs correction" selected); auto-save (no lost responses); response tracking |
| **Testing Criteria** | Can create form for lab value corrections; submit responses; responses appear in SharePoint list; can export to CSV for audit trail |
| **Contingency** | If Forms unavailable, use Google Forms (with Emory credentials if available) |

---

### 13. Calendar

| Property | Value |
|---|---|
| **Access URL** | outlook.com/calendar (web, macOS app) |
| **Available Via** | Desktop + Web |
| **Thyroid Project Role** | Weekly QC check reminder (Monday post-refresh); monthly compliance sign-off meeting (with IRB/Privacy Officer); quarterly backup verification check; vacation/leave tracking (coordinate with team); shared calendar for #thyroid-research team coordination |
| **Automation Method** | osascript (create events automatically), Outlook rules (alert-based event creation), Teams meeting integration |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Recurring events (weekly, monthly); reminders (1 hour before); color-coding (THYROID_2026 = blue); shared team calendars; integration with To Do |
| **Testing Criteria** | Can create recurring QC reminder; reminder fires at scheduled time; team can see shared calendar events; Calendar syncs with Teams meetings |
| **Contingency** | If Calendar unavailable, use Google Calendar or Calendly for scheduling |

---

### 14. Visio

| Property | Value |
|---|---|
| **Access URL** | app.diagrams.net or visio.microsoft.com (Emory tenant) |
| **Available Via** | Web (Microsoft Visio online) + desktop app |
| **Thyroid Project Role** | Data flow diagrams: RAW_PHI (00_) → DE-ID_SCRIPT (00_deid_gateway.py) → SILVER_PARQUET (01_) → POWER_BI_IMPORT → REPORTS (6 pages); entity-relationship diagrams: 13 base tables + 8 views + 12 relationships; architecture diagram: folder structure, permissions, file types, encryption boundaries |
| **Automation Method** | Claude in Chrome (create/edit shapes, add text), export to PNG for documentation |
| **Priority** | **P1 IMPORTANT** |
| **Key Features for THYROID_2026** | Shapes: database (cylinder), process (rectangle), file (document icon); connectors (arrows with labels); grouping (logical components); auto-layout (arrange nodes); color-coding (PHI = red, de-ID = yellow, analysis = green) |
| **Testing Criteria** | Can create ER diagram with 13 tables + relationships; save to SharePoint; export to PNG for inclusion in documentation |
| **Contingency** | If Visio unavailable, use Lucidchart, draw.io (free), or text-based Mermaid diagrams in Markdown |

---

## P2: NICE-TO-HAVE APPS (7)

These apps provide enhanced capabilities but have mature fallbacks. Deployment can proceed without them.

### 15. Power BI Web

| Property | Value |
|---|---|
| **Access URL** | app.powerbi.com (Emory Power BI service) |
| **Available Via** | Web only |
| **Thyroid Project Role** | Future Phase 5 cloud publishing: read-only embed of .pbix reports on Emory intranet (does NOT replace Power BI Desktop in Phase 4); team members access read-only dashboards via Power BI Web (RLS enforced); automated refresh via cloud gateway (Phase 5) |
| **Automation Method** | Claude in Chrome (publish from Power BI Desktop to cloud), Power Automate (scheduled refresh trigger) |
| **Priority** | **P2 NICE-TO-HAVE** |
| **Key Features for THYROID_2026** | Cloud publishing of local .pbix; row-level security (RLS) enforcement per user; refresh scheduling; Q&A natural language queries (future, requires AI); mobile apps (view reports on iPad) |
| **Configuration Required** | Phase 4: Not used (local .pbix only). Phase 5: Enable Power BI Premium capacity, upload .pbix to cloud, configure RLS rules per Emory organizational unit |
| **Testing Criteria** | Phase 5: Published report accessible via web; RLS enforces research_id visibility per user |
| **Contingency** | Phase 4 continues with local Power BI Desktop only; Phase 5 uses Tableau, Looker, or Metabase if Power BI Web unavailable |

---

### 16. To Do

| Property | Value |
|---|---|
| **Access URL** | todo.microsoft.com |
| **Available Via** | Desktop + Web + Mobile |
| **Thyroid Project Role** | Personal task list for Logan: weekly de-ID refresh tasks, monthly backup verification checklist, quarterly compliance reviews; calendar integration for reminders; list sharing (future: delegate some tasks to research assistant) |
| **Automation Method** | Outlook calendar integration, osascript (create reminders), Teams integration |
| **Priority** | **P2 NICE-TO-HAVE** |
| **Key Features for THYROID_2026** | Repeating tasks (mark as done weekly); priority levels (Urgent → Important → Normal); subtasks; due date reminders; sharing lists; integration with Planner |
| **Testing Criteria** | Can create weekly recurring task; reminder fires on due date; can mark as complete and auto-repeat |
| **Contingency** | Use Outlook calendar + email reminders; or Todoist, Apple Reminders |

---

### 17. Loop

| Property | Value |
|---|---|
| **Access URL** | Embedded in Teams, SharePoint, Outlook |
| **Available Via** | Web (within Teams, SharePoint) |
| **Thyroid Project Role** | Real-time markdown-based collaboration on SOP drafts (sop_add_new_data.md, sop_edit_records.md), data dictionary updates, audit trail notes; replaces static docs with dynamic version that reflects current practices; shared editing between Logan + co-authors |
| **Automation Method** | Claude in Chrome (edit Loop components), Teams channel integration |
| **Priority** | **P2 NICE-TO-HAVE** |
| **Key Features for THYROID_2026** | Markdown editor; real-time sync; comment threads on specific lines; version history; table of contents auto-generation |
| **Testing Criteria** | Can create Loop component in Teams channel; edit markdown collaboratively; changes sync in real-time |
| **Contingency** | Use Google Docs for collaborative editing; or Notion; or static markdown in Git with pull request reviews |

---

### 18. OneNote

| Property | Value |
|---|---|
| **Access URL** | onenote.com or macOS app |
| **Available Via** | Desktop + Web |
| **Thyroid Project Role** | Research notebook: debug logs from de-ID script runs, validation findings, edge case notes, hypothesis brainstorming, Power BI model design decisions; syncs to OneDrive for off-site backup; searchable archive of project history |
| **Automation Method** | osascript (open OneNote app), Power Automate Desktop (append execution logs), Claude in Chrome (web editing) |
| **Priority** | **P2 NICE-TO-HAVE** |
| **Key Features for THYROID_2026** | Notebooks (sections/pages); rich text + images/code blocks; ink annotation; search; OCR (scanned notes); tags; synced across devices |
| **Testing Criteria** | Can create notebook section for "THYROID_2026 Debugging"; append log excerpts; search for keywords; syncs to OneDrive |
| **Contingency** | Use Apple Notes, Notion, or Obsidian for note-taking |

---

### 19. Whiteboard

| Property | Value |
|---|---|
| **Access URL** | Embedded in Teams, SharePoint |
| **Available Via** | Web (Teams, SharePoint) + iPad app |
| **Thyroid Project Role** | Design sessions for Power BI schema refinement (sketch 13 tables + relationships), brainstorming manuscript hypotheses, whiteboarding data flow during Phase 4D; ephemeral (not permanent documentation); encourage spontaneous creativity |
| **Automation Method** | Claude in Chrome (open/sketch shapes) |
| **Priority** | **P2 NICE-TO-HAVE** |
| **Key Features for THYROID_2026** | Drawing tools (shapes, connectors, text); collaboration (multiple users drawing simultaneously); reactions; real-time sync; export to image |
| **Testing Criteria** | Can create Whiteboard in Teams; sketch diagram; invite co-authors; save as image |
| **Contingency** | Use Google Jamboard, Miro, Mural, or physical whiteboard with photo archive |

---

### 20. Copilot (M365 Limited)

| Property | Value |
|---|---|
| **Access URL** | Integrated in Word, Excel, PowerPoint, Outlook, Teams, SharePoint |
| **Available Via** | Desktop + Web (where available) |
| **Thyroid Project Role** | Limited co-authoring in Word (Copilot in Word): summarize de-identified Power BI tables → generate narrative text for manuscript methods section; Excel analysis suggestions (outlier detection); PowerPoint slide generation from de-identified data (Phase 5) |
| **Automation Method** | Claude in Chrome (use Copilot web interface), Word/Excel/PowerPoint native Copilot pane |
| **Priority** | **P2 NICE-TO-HAVE** (Limited; HasCopilotAI=false at Emory) |
| **Key Features for THYROID_2026** | Natural language prompts (e.g., "Summarize patient demographics table for methods section"); draft suggestions in Word; data analysis prompts in Excel |
| **Configuration Required** | Monitor Emory license upgrades (HasCopilotAI=false currently); do NOT depend on Copilot for Phase 4 (may not be available); use as enhancement if enabled |
| **Testing Criteria** | When Copilot available: Can prompt "Summarize this table" in Word; generates relevant narrative |
| **Contingency** | If Copilot unavailable or limited, use OpenAI API directly (local Python script) or manual narrative writing; do NOT use cloud LLM on sensitive de-identified data |

---

## P3: NOT APPLICABLE (10)

These apps are available through Emory Enterprise but have no role in the THYROID_2026 research pipeline.

| # | App | Reason Not Applicable | Notes |
|---|---|---|---|
| 21 | **Bookings** | Scheduling tool for appointment slots; no patient/research appointments needed | Available if future clinical trial enrollment scheduling needed |
| 22 | **Culture Cloud** | Employee engagement & org culture tool; no org-wide culture data collected | Not applicable to research project |
| 23 | **Find a Room** | Facility scheduling for meeting rooms; research conducted locally on Mac | Not applicable; no room bookings |
| 24 | **Clipchamp** | Video editor; no video output required for research | Could be useful for future training video on de-ID process (out of scope) |
| 25 | **Reflect** | Pulse surveys & employee feedback; no engagement metrics collected | Not applicable |
| 26 | **Sway** | Lightweight web publishing; use Word/PowerPoint instead | Could replace static web documentation (low priority) |
| 27 | **Power Pages** | Low-code portal builder; no public-facing web portal needed | Not applicable to internal research |
| 28 | **Power Apps** | Canvas/model-driven apps for business logic; Power Automate Desktop is sufficient | Could be used for future data entry app (Phase 5) |
| 29 | **Engage** | Internal communications campaign tool; no org-wide broadcast needed | Not applicable |
| 30 | **Org Explorer / Connections** | People/org directory tools; no org discovery needed | Not applicable |

---

## Emory SSO & Licensing Summary

### Available at Emory Enterprise Level
- **Core Productivity:** Outlook, Word, Excel, PowerPoint, OneDrive (unlimited), Teams, SharePoint
- **Power Platform:** Power BI Desktop, Power Automate (cloud), Power Automate Desktop (RPA), Power Apps (limited)
- **Collaboration:** Forms, Lists, Planner, Loop, Whiteboard, Visio (web)
- **Specialty:** Calendar, To Do, OneNote, Copilot (basic, limited—HasCopilotAI=false)
- **SSO Integrations:** Cisco Webex, Zoom, UpToDate, Culture Cloud, Find a Room

### Licensing Notes
- **Power BI Desktop:** Install via Microsoft Store or Office 365 ProPlus installer; no additional license required
- **Power BI Web (Premium):** Phase 5 consideration; may require separate capacity license
- **Copilot:** HasCopilotAI=false at Emory; monitor for future upgrades; do not assume availability in Phase 4
- **Unlimited OneDrive:** Emory Enterprise includes unlimited cloud storage; metadata backups can grow without quota concerns

---

## Integration Patterns for THYROID_2026

### Pattern 1: Scheduled Refresh Orchestration
```
Power Automate Cloud (Monday 9 AM)
  ↓ [Trigger: Recurrence]
Power Automate Desktop RPA Robot (Mac)
  ├─ osascript: Open Terminal
  ├─ Execute: python3 00_deid_gateway.py
  ├─ osascript: Open Power BI Desktop
  ├─ Cmd+Shift+R (Power BI refresh)
  ├─ osascript: Close Power BI & Terminal
  ├─ Copy audit logs to OneDrive (via Finder automation)
  ↓
Power Automate Cloud: Send Teams message
  └─ #thyroid-research channel alert
```

### Pattern 2: Data Quality Gate
```
Power Automate Desktop: Run validation script
  ├─ python3 09_validate_relationships.py
  ├─ python3 10_generate_deid_audit.py
  ↓
Excel: Power Query (optional manual QC)
  ├─ Pivot table on research_id distribution
  ├─ Check for nulls in key columns
  ↓
SharePoint: Upload VALIDATION_AUDITS/weekly_reconciliation.md
  └─ Read-only link shared with team via Teams
```

### Pattern 3: Manuscript Authorship Loop
```
Power BI Desktop: Export tables & charts to CSV/PNG
  ↓
Excel: Power Query transform & pivot table QC
  ↓
Word: Paste tables + Copilot generates narrative
  ↓
Teams: Share draft link with co-authors
  ↓
Loop: Collaborative comments on SOP sections
  ↓
Forms: Submit review feedback (corrections, suggestions)
  ↓
Logan: Apply corrections to Word document
  ↓
SharePoint: Publish final de-ID version (read-only)
```

---

## Next Steps for Configuration

1. **Phase 4A:** Verify Power BI Desktop installation; test local Parquet connectivity
2. **Phase 4B:** Create Power Automate Cloud flow & Power Automate Desktop robot; test scheduling
3. **Phase 4C:** Set up SharePoint site #thyroid-research; configure document library retention
4. **Phase 4D:** Create Teams #thyroid-research channel; test DLP policies (block PHI keywords)
5. **Phase 4E:** Publish documentation to OneDrive/SharePoint (metadata only)
6. **Phase 5 (Future):** Evaluate Power BI Web cloud publishing; consider Copilot upgrades at Emory

---

**Matrix Version:** 1.0
**Last Updated:** 2026-03-27
**Review Cadence:** Quarterly (align with Phase milestones)
