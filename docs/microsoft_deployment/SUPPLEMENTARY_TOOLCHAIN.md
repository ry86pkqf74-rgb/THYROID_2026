
---

## Combined Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────┐
│                         THYROID_2026 SUPPLEMENTARY TOOLCHAIN ARCHITECTURE          │
└─────────────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────┐           ┌───────────────────────────────┐
│   LOCAL MAC (FileVault Encrypted)       │           │   CLOUD SERVICES (De-ID Only) │
│   /Users/lhglosser/THYROID_SECURE_2026/ │           │                               │
│                                          │           │  ╔═══════════════════════════╗ │
│  ┌────────────────────────────────────┐ │           │  ║    REPLIT (Reserved VM)   ║ │
│  │ 00_RAW_PHI/                        │ │           │  ╠═══════════════════════════╣ │
│  │  - operative_notes/                │ │           │  ║  FastAPI Backend          ║ │
│  │  - pathology_reports/              │ │           │  ║  ┌──────────────────────┐ ║ │
│  │  - imaging_studies/                │ │           │  ║  │ /api/cohort          ║ │
│  │  (MRN lookup locked here)          │ │           │  ║  │ /api/tables          ║ │
│  └────────────────────────────────────┘ │           │  ║  │ /api/query (NLQ)     ║ │
│                                          │           │  ║  │ /api/extract (LLM)   ║ │
│  ┌────────────────────────────────────┐ │           │  ║  │ /api/validate        ║ │
│  │ 01_SILVER_DEID_PARQUET/            │ │─nightly──▶│  ║  │ /api/figures         ║ │
│  │ (research_id only)                 │ │ sync via  │  ║  └──────────────────────┘ │
│  │  - master_cohort.parquet           │ │ scp      │  ║                            ║ │
│  │  - tumor_pathology.parquet         │ │           │  ║  LangGraph Agents         ║ │
│  │  - fna_cytology.parquet            │ │           │  ║  ┌──────────────────────┐ ║ │
│  │  - 10 more base tables             │ │           │  ║  │ Clinical Extraction   ║ │
│  └────────────────────────────────────┘ │           │  ║  │ Data Quality Monitor  ║ │
│                                          │           │  ║  │ NLQ Decomposition     ║ │
│  ┌────────────────────────────────────┐ │           │  ║  │ Manuscript Drafting   ║ │
│  │ 02_GOLD_POWERBI/                   │ │           │  ║  └──────────────────────┘ │
│  │ (Power BI .pbix local files)       │ │           │  ║                            ║ │
│  └────────────────────────────────────┘ │           │  ║  React Frontend           ║ │
│                                          │           │  ║  ┌──────────────────────┐ ║ │
│  ┌────────────────────────────────────┐ │           │  ║  │ Dashboard              ║ │
│  │ 04_EXTRACTION_OUTPUTS/             │ │           │  ║  │  - Cohort Builder      ║ │
│  │ (LLM-extracted structured data)    │ │           │  ║  │  - Labs Timeline       ║ │
│  │  - operative_extractions.parquet   │ │           │  ║  │  - Imaging Analysis    ║ │
│  └────────────────────────────────────┘ │           │  ║  │  - Data Quality        ║ │
│                                          │           │  ║  └──────────────────────┘ │
│  ┌────────────────────────────────────┐ │           │  ║                            ║ │
│  │ VALIDATION_AUDITS/                 │ │           │  ║  PostgreSQL (Session DB)  ║ │
│  │ (QC reports, extraction logs)      │ │           │  ║  - User filters          ║ │
│  └────────────────────────────────────┘ │           │  ║  - Query history         ║ │
│                                          │           │  ║  - Report queue          ║ │
│  ┌────────────────────────────────────┐ │           │  ╚═══════════════════════════╝ │
│  │ DuckDB                              │ │           │                               │
│  │ (thyroid_master.db, local)         │ │           │  ╔═══════════════════════════╗ │
│  └────────────────────────────────────┘ │           │  ║    ELEVENLABS AGENT       ║ │
│                                          │           │  ╠═══════════════════════════╣ │
│  ┌────────────────────────────────────┐ │           │  ║  Knowledge Base:          ║ │
│  │ Python/LangGraph (local execution) │ │           │  ║  - data_dictionary.md     ║ │
│  │  - Clinical extraction             │◀─────────┐ │  ║  - research_sops.md       ║ │
│  │  - Data quality monitoring         │           │  ║  - cohort_summaries.md    ║ │
│  │  - Manual review queues            │           │  ║                            ║ │
│  └────────────────────────────────────┘ │           │  ║  Tools:                   ║ │
│                                          │           │  ║  - /api/query (Replit)    ║ │
│  ┌────────────────────────────────────┐ │           │  ║  - /api/figures           ║ │
│  │ Power BI Desktop                   │ │           │  ║                            ║ │
│  │  - Star schema (13 base + 8 views) │ │           │  ║  Voice I/O:                ║ │
│  │  - 6 report pages                  │ │           │  ║  - Speech-to-Text (STT)   ║ │
│  │  - 20+ DAX measures                │ │           │  ║  - Text-to-Speech (TTS)   ║ │
│  │  - RLS by research_id (Phase 5)    │ │           │  ║                            ║ │
│  └────────────────────────────────────┘ │           │  ║  Web Widget + Phone API    ║ │
│                                          │           │  ╚═══════════════════════════╝ │
│  ┌────────────────────────────────────┐ │           │                               │
│  │ Excel + Power Query                │ │           │  ╔═══════════════════════════╗ │
│  │  - ETL transforms                  │ │           │  ║    CLAUDE API             ║ │
│  │  - Data entry validation           │ │           │  ║    (LLM Backbone)         ║ │
│  │  - QC pivot tables                 │ │           │  ║                            ║ │
│  └────────────────────────────────────┘ │           │  ║  Models:                  ║ │
│                                          │           │  ║  - claude-3-5-sonnet      ║ │
│  ┌────────────────────────────────────┐ │           │  ║  - claude-opus (future)   ║ │
│  │ Microsoft 365                      │ │           │  ║                            ║ │
│  │  - Word (manuscript authoring)     │ │           │  ║  Cost: ~$3-5/mo (typical) ║ │
│  │  - OneDrive (metadata backups)     │ │           │  ║                            ║ │
│  │  - Power Automate (workflows)      │ │           │  ╚═══════════════════════════╝ │
│  │  - Teams (notifications)           │ │           │                               │
│  └────────────────────────────────────┘ │           │  ╔═══════════════════════════╗ │
│                                          │           │  ║    EXTERNAL SERVICES      ║ │
│  Desktop Commander                      │           │  ║                            ║ │
│  osascript                               │           │  ║  - GitHub (code storage)  ║ │
│  Claude in Chrome                        │           │  ║  - Zenodo (data sharing)  ║ │
│                                          │           │  ║  - Journal APIs           ║ │
└────────────────────────────────────────┘           │  ╚═══════════════════════════╝ │
                                                      │                               │
                                                      └───────────────────────────────┘

DATA FLOW EXAMPLES:

1. Clinical Note Extraction (Phase 4H):
   00_RAW_PHI/operative_notes/ → LangGraph agent (local) → 04_EXTRACTION_OUTPUTS/
   
2. Dashboard Query (Phase 4G):
   Researcher browser request → Replit /api/query → DuckDB (synced) → Result + Chart
   
3. Voice Query (Phase 4G):
   ElevenLabs Agent (speech) → /api/query-voice (Replit) → LangGraph NLQ (local)
   → Results → TTS response + visualization
   
4. Data Quality Monitoring (Continuous):
   01_SILVER_DEID_PARQUET/ → LangGraph QA agent (weekly) → VALIDATION_AUDITS/ report
   
5. Manuscript Generation (Phase 4F+):
   Power BI export → LangGraph manuscript agent → Draft docx → Word editor
```

---

## PHI Safety Matrix

| Service | Data Received | PHI Risk | Mitigation | Audit |
|---|---|---|---|---|
| **ElevenLabs Agent** | De-identified data dictionary, SOP summaries, aggregated cohort stats | LOW | KB receives only de-id source docs; tool outputs are aggregated summaries; no row-level data | Conversation logs in ElevenLabs; enterprise BAA available for Phase 5 |
| **ElevenLabs STT** | Future: surgical audio (if Emory provides) | HIGH | Phase 5+; requires HIPAA BAA; audio never stored; transcript discarded post-extraction | BAA required; audio encryption; timestamp logs |
| **ElevenLabs TTS** | Research narratives, presentation scripts | LOW | Input is de-identified summaries only; output is MP3 audio file | No data retention; normal usage logs |
| **Replit FastAPI** | De-identified Parquet exports (research_id + computed fields) | LOW | Read-only DuckDB; nightly synced copy; no direct access to 00_RAW_PHI/ | PostgreSQL session logs; query audit trail |
| **Replit React Frontend** | De-identified cohort summary data via API | LOW | All displayed data is aggregated or de-identified; filtered per API schema | Browser logs (client-side); no PII stored |
| **Replit PostgreSQL** | User session state, query history, report queue metadata | MEDIUM | Session data is temporal; no PHI stored; encrypted at rest (Phase 5) | SQL audit logs; weekly retention policy |
| **LangGraph (Local)** | Raw operative notes (00_RAW_PHI) during extraction; clinical text with MRNs | HIGH | Runs locally on encrypted Mac; LLM calls send de-identified prompt + note excerpt only; no persistent storage of raw text | Extraction logs to VALIDATION_AUDITS/; Claude API call metadata via Anthropic account |
| **Claude API** | De-identified note excerpts; extraction prompts; NLQ questions | MEDIUM | Prompts do not include MRN or date; only research_id; API calls logged by Anthropic; org policy: no sensitive data in prompts | Anthropic API logs; org has data processing agreement; IP whitelisting (Phase 5) |
| **OneDrive (Metadata Only)** | Row count snapshots, schema versions, audit log pointers | LOW | No raw data synced; only metadata; encrypted in transit; zero-knowledge backup | OneDrive version history; manual audit quarterly |

**Key Risk Mitigation Principles:**
1. **No direct PHI export to cloud services.** All cloud services receive de-identified, aggregated, or statistical summaries only.
2. **LangGraph runs locally.** Raw clinical text never leaves the Mac; only de-identified prompts sent to Claude API.
3. **Read-only data on cloud.** Replit DuckDB is read-only; no mutations possible.
4. **Audit trail mandatory.** Every extraction, query, and API call logged with timestamp, user, operation, result hash.
5. **Enterprise BAAs available.** ElevenLabs, Replit, and Anthropic all support HIPAA BAAs for production deployment (Phase 5+).

---

## Implementation Priority & Budget

### Phase Timeline

| Phase | Duration | Focus | Deliverables | Dependencies |
|---|---|---|---|---|
| **4A** | Day 1 | Folder structure, security setup | THYROID_SECURE_2026/ hierarchy, FileVault verified | None (prerequisite) |
| **4B** | Days 2-3 | De-identification pipeline | Silver layer Parquet, audit trail, validation | Phase 4A complete |
| **4C** | Days 4-5 | Power BI star schema | .pbix file, 6 report pages, DAX measures | Phase 4B (Silver data) |
| **4D** | Days 6-7 | Automation wiring | Power Automate flows, osascript helpers, scheduled refresh | Phases 4B-C complete |
| **4E** | Days 8-10 | MotherDuck elimination | 307 files refactored, grep -r "motherduck" = 0 | Phases 4B-D complete |
| **4F** | Days 11-14 | Validation & compliance | All test suites pass, sign-offs, manuscript pipeline ready | Phases 4A-E complete |
| **4G** | Days 15-18 | **Replit dashboard + ElevenLabs agent** | Research dashboard live, web widget deployed, voice assistant trained | Phase 4F complete; Replit account ready |
| **4H** | Days 19-22 | **LangGraph clinical extraction** | Extraction agent tested on 100 notes, validation reports, human review queue | Phase 4G complete; Claude API enabled |
| **4I** | Days 23-25 | **Integration testing & go-live** | End-to-end voice pipeline, dashboard + agent communication, documentation | Phases 4G-H complete |

### Monthly Cost Estimate

| Service | Tier | Cost | Notes |
|---|---|---|---|
| **ElevenLabs** | Free | $0 | 10K TTS credits + 15 min Agents; 0 credits used to date |
| **Replit** | Core + Team | $0 additional | Pre-paid $200/mo credits reset monthly — already budgeted, not an incremental charge |
| **LangGraph agents (LLM)** | Claude via Cowork | $0 additional | Route agent calls through existing Claude subscription; no separate API key needed |
| **LangChain/LangGraph** | Open source | $0 | Apache 2.0; no licensing cost |
| **DuckDB** | Open source | $0 | No licensing cost; local execution |
| **M365** | Emory Enterprise | $0 | Institution-provided |
| **Total incremental cost** | | **$0** | Every tool is free, pre-paid, or institution-provided |

**Net cost:** $0 additional spending. Replit credits are pre-allocated and reset regardless of usage. ElevenLabs Free tier has unused capacity. LangGraph runs locally using Claude through your existing Cowork subscription. M365 is Emory-funded.

### Team Effort (Phases 4G-4I)

| Activity | Hours | Owner | Phase |
|---|---|---|---|
| Replit FastAPI backend development | 12 | Logan | 4G |
| React dashboard UI | 8 | Logan + Claude | 4G |
| ElevenLabs Knowledge Base setup & integration | 4 | Logan | 4G |
| LangGraph extraction agent build | 16 | Logan + Claude | 4H |
| Testing & debugging | 8 | Logan | 4I |
| Documentation | 6 | Logan | 4I |
| **Total** | ~54 hours | | |

---

## Comparison: Microsoft-Only vs. Full Stack

### Capability Comparison Table

| Capability | Microsoft-Only (Phase 4A-F) | + ElevenLabs + Replit + LangGraph (Phases 4G-I) |
|---|---|---|
| **Data Extraction** | Power Query regex (limited) + manual entry | Power Query + LangGraph + Claude structured extraction (high accuracy) |
| **Example:** Extract tumor size from "largest nodule measures 2.1 cm" | Manual → Excel cell | LLM agent → confidence score → Parquet (automated) |
| **Dashboard** | Power BI Desktop (local .pbix only) | Power BI Desktop + Replit web app (shareable, collaborative) |
| **Sharing:** Co-author reviews analysis | Email .pbix file (large, no sync) | Browser link to Replit dashboard (live, updated nightly) |
| **NLP** | Copilot (limited, regex-based) | Claude + LangGraph structured agents (entity extraction, validation) |
| **Clinical Note Processing** | Manual read + data entry (error-prone) | Auto-extraction to structured tables (audit trail, human review queue) |
| **Voice/Audio** | None (keyboard-only) | ElevenLabs voice agent + TTS (researcher asks verbally) |
| **Data Quality** | Manual Excel pivot tables (ad hoc) | LangGraph QA agent (weekly automated scans, anomaly detection) |
| **Natural Language Query** | DAX formulas (requires analyst) | NLQ agent: "How many tall cell PTC <30 had LN mets?" → SQL → results |
| **Manuscript Assistance** | Manual in Word; Copilot limited | LangGraph multi-agent pipeline (stats → narrative → draft sections) |
| **Figure Generation** | Power BI charts | Power BI + Replit matplotlib/seaborn (publication-grade SVG, KM curves, etc) |
| **Automation** | Power Automate Cloud + Power Automate Desktop | Power Automate + Replit workers + LangGraph background jobs |
| **Cost** | $0 (M365 enterprise) | +$2,400/yr Replit + $36-60/yr Claude API (~$200/mo team budget already allocated) |

### Use Case Examples

**Use Case 1: Co-Author Reviews Imaging Findings**

**Microsoft-Only:**
- Logan runs Power BI Desktop locally
- Co-author requests specific imaging subset
- Logan exports CSV, emails
- Co-author opens in Excel, manually filters
- Turnaround: 24+ hours

**Full Stack:**
- Co-author opens Replit dashboard (URL)
- Clicks "Filter by imaging modality: CT"
- Sees interactive charts live
- Asks question: "What's the recurrence rate for solid nodules >2cm?"
- ElevenLabs agent answers: 34.2% (voice response)
- Turnaround: seconds

---

**Use Case 2: Extract Operative Note Data**

**Microsoft-Only:**
- Logan reads 50 operative notes
- Manual entry into Excel (error-prone; 50 notes = 4-6 hours)
- Validation via pivot tables (ad hoc)
- Turnaround: 1-2 days per 50 notes

**Full Stack:**
- Batch 50 notes → LangGraph extraction agent (2 minutes)
- Agent: extracts tumor size, LN counts, ETE, frozen section, confidence
- Validation report: 4 high-confidence, 2 flags for review (6 minutes manual review)
- Turnaround: 10 minutes total (30x speedup)

---

**Use Case 3: Manuscript Draft Generation**

**Microsoft-Only:**
- Logan manually writes Results section
- 2-3 hours per section
- Copy-paste tables from Power BI
- Copilot can't reliably link findings to stats

**Full Stack:**
- Researcher selects analysis: "Tall Cell PTC Demographics"
- LangGraph agent: exports cohort data → statistical summary → narrative prose
- Output: polished paragraph
- Logan edits/reviews (30 min)
- Turnaround: 1 hour vs. 3 hours (2x faster)

---

**Use Case 4: Data Quality Monitoring**

**Microsoft-Only:**
- Manual monthly pivot table review
- Ad hoc outlier flagging
- No systematic checks

**Full Stack:**
- LangGraph QA agent runs weekly
- Automated checks: missing data %, outliers, referential integrity
- Reports to VALIDATION_AUDITS/
- Flags anomalies for investigation
- Proactive rather than reactive

---

## Integration Examples & Code Snippets

### Example 1: Voice Query (ElevenLabs + Replit + LangGraph)

**Researcher speaks:** "How many young papillary cancers had multifocal disease?"

**Execution Flow:**
```
1. Browser: ElevenLabs widget listens
2. Audio → STT → "How many young papillary cancers had multifocal disease?"
3. POST to Replit: /api/query/voice with question text
4. Replit FastAPI → calls LangGraph NLQ agent locally
5. LangGraph decompose: age<40 + histology='PTC' + multifocal=yes
6. Generate SQL:
   SELECT COUNT(DISTINCT tp.research_id)
   FROM tumor_pathology tp
   JOIN master_cohort mc ON tp.research_id = mc.research_id
   WHERE tp.histology_1_type LIKE '%PTC%'
     AND mc.age_at_surgery < 40
     AND tp.multifocal = 'yes'
7. DuckDB execute: 234 patients
8. LangGraph generate answer: "In the 40 and under age group,
   234 of 412 papillary cancer patients had multifocal disease,
   representing 57 percent of young PTC cases."
9. Claude API TTS: narrate answer
10. Browser plays audio + shows chart
```

---

### Example 2: Batch Clinical Extraction (LangGraph + Python)

**Input:** 100 operative notes in 00_RAW_PHI/OPERATIVE_NOTES/

**Script:**
```python
import os
import json
from pathlib import Path

# Read all notes
notes_dir = "/Users/lhglosser/THYROID_SECURE_2026/00_RAW_PHI/OPERATIVE_NOTES"
notes = [f for f in os.listdir(notes_dir) if f.endswith(".txt")]

# Process in batches
results = []
for note_file in notes[:100]:
    with open(os.path.join(notes_dir, note_file)) as f:
        raw_note = f.read()

    # Extract research_id from filename or note header
    research_id = note_file.split("_")[0]

    # Invoke extraction agent
    state = ExtractionState(
        research_id=research_id,
        raw_note=raw_note,
        audit_log={"filename": note_file}
    )

    result = extraction_agent.invoke(state)

    # Check if requires manual review
    if result.extracted_fields.human_review_required:
        print(f"{research_id}: FLAGGED for review (confidence: {result.extracted_fields.extraction_confidence})")

    results.append(result.extracted_fields.model_dump())

# Save extractions
import pandas as pd
df = pd.DataFrame(results)
df.to_parquet(
    "/Users/lhglosser/THYROID_SECURE_2026/04_EXTRACTION_OUTPUTS/operative_batch_20260327.parquet"
)

print(f"Processed {len(results)} notes")
print(f"High-confidence: {sum(1 for r in results if r['extraction_confidence'] > 0.85)}")
print(f"Requires review: {sum(1 for r in results if r['human_review_required'])}")
```

---

### Example 3: Dashboard Data Export (Replit FastAPI)

**Researcher clicks "Export for Analysis" on Replit dashboard**

**Request:**
```json
POST /api/export
{
  "cohort_name": "tall_cell_ptc",
  "filters": {"histology": "PTC Tall Cell"},
  "tables": ["master_cohort", "tumor_pathology", "fna_cytology", "thyroglobulin_labs"],
  "format": "csv"
}
```

**Replit response:**
```json
{
  "status": "completed",
  "files": [
    "tall_cell_ptc_master_cohort.csv",
    "tall_cell_ptc_tumor_pathology.csv",
    "tall_cell_ptc_fna_cytology.csv",
    "tall_cell_ptc_thyroglobulin_labs.csv"
  ],
  "download_url": "https://research-flow-studio.replit.app/api/download/tall_cell_ptc_export_20260327.zip",
  "n_patients": 47,
  "audit_log": {
    "requested_by": "user",
    "requested_at": "2026-03-27T14:32:00Z",
    "exported_tables": 4,
    "row_counts": {...}
  }
}
```

---

## Next Steps

### Immediate (Phase 4G, Days 15-16)

1. **Replit Setup:**
   - Create new Python project in ROS Workspace
   - Initialize FastAPI + PostgreSQL + React template
   - Configure nightly sync from Mac (scp 01_SILVER_DEID_PARQUET/)
   - Deploy to reserved VM

2. **ElevenLabs Setup:**
   - Create Agent in workspace
   - Upload 3 knowledge base docs (data_dictionary, SOPs, cohort summaries)
   - Test tool integrations (query + figures APIs)
   - Deploy web widget to Replit dashboard

3. **Testing:**
   - Query Replit API manually (curl)
   - Ask ElevenLabs agent test questions
   - Verify no PHI leakage in logs

### Medium-Term (Phase 4H-I, Days 19-25)

4. **LangGraph Development:**
   - Build extraction agent locally
   - Test on 10 sample operative notes
   - Integrate with Replit /api/extract endpoint
   - Deploy to Replit background worker

5. **End-to-End Testing:**
   - Voice query → NLQ → SQL → TTS response
   - Clinical note batch extraction → Parquet → validation reports
   - Dashboard filters → API → live updates

6. **Documentation & Handoff:**
   - Update implementation guides
   - Create quick-start for co-authors
   - Set up monitoring & alerting

---

## Conclusion

The supplementary toolchain (ElevenLabs + Replit + LangChain/LangGraph) **extends** the Microsoft-only Phase 4 pipeline without compromising PHI security. It adds three critical capabilities:

1. **Voice interface** (ElevenLabs) — researchers ask questions verbally
2. **Shareable dashboard** (Replit) — collaborators access live data via browser
3. **Intelligent extraction** (LangGraph) — unstructured clinical notes → structured data automatically

All three services handle **de-identified data only**. Raw PHI remains locked in 00_RAW_PHI/ on the encrypted Mac. Total incremental cost is ~$200/month (already budgeted in Replit team plan).

**Phases 4G-4I (Days 15-25) will integrate these tools and make THYROID_2026 the first locally-hosted, AI-augmented thyroid cancer research lakehouse with voice capabilities and multi-agent orchestration.**

---

**Document Version:** 1.0
**Created:** 2026-03-27
**Status:** Design Complete, Ready for Phase 4G Implementation
**Next Review:** 2026-03-28 (Phase 4G kickoff)
**Author:** Implementation Team
**Owner:** Logan Glosser (LGLOSSE@emory.edu)

