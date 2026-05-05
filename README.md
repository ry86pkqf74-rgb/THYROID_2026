# THYROID_2026 — Build Repo

> **This is the DATA BUILD repo.** For manuscript work, use the analysis repo:
> [`../thyroid-2026-analysis/`](../thyroid-2026-analysis/) — pinned canonical versions, backend abstraction, per-manuscript notebooks.

## Current canonical

| Resource | Location |
|----------|----------|
| Version registry | [`RELEASE.md`](RELEASE.md) |
| Build history | [`CHANGELOG.md`](CHANGELOG.md) |
| Active canonical DB | `thyroid_canonical_publication_v1_0` (MotherDuck, eras account) |
| Parquet backup | `../thyroid-2026-analysis/data/v1_0/` (109 tables, 61.4 MB) |

**To build the next version:**
```bash
# After new data lands in "Thyroid 2026 UPdated"
python scripts/223_publish_canonical.py --version v1_1 --candidate --skip-ingest
python scripts/224_compare_canonical_versions.py --from v1_0 --to v1_1_rc
python scripts/225_promote_canonical_version.py --candidate v1_1_rc --release v1_1
```

---

**Analyst quick-start:** [`MANUSCRIPT_DATA_START_HERE.md`](MANUSCRIPT_DATA_START_HERE.md) — exact tables, views, and rules for citing data in manuscripts.

---

## Project tracking & manuscript pipeline (Airtable + Linear + Claude)

> **Anyone editing this repo, drafting a manuscript, or modifying `thyroid_master.duckdb` must read [`CLAUDE.md`](CLAUDE.md) and [`INTEGRATION_PROPOSAL.md`](INTEGRATION_PROPOSAL.md) before making changes.** The integration described there is the audit trail of record for cohort decisions, override evidence, manuscript drafts, and verification checks.

### Architecture in one paragraph

`thyroid_master.duckdb` and the parquets in `processed/` remain the analytical source of truth. **Airtable** holds the structured inventory of every Source File, every Column with verification status, every Verification Check from the reconciliation matrix, every gold-standard Override Decision, every Cohort Patient (de-identified `research_id` only), and every Manuscript (90+ planned, 19+ active). **Linear** ([team `Thyroid Database`, key `THY`](https://linear.app/rostemp/team/THY/all)) holds work-in-flight: per-manuscript projects, QA findings, override-review tasks, drafting issues. **Claude** orchestrates a daily sync that creates Linear issues from new Airtable findings, mirrors closed issues back to advance Airtable lifecycle, snapshots evidence on submission, and appends to the immutable Issue Ledger.

### IDs at a glance

| Resource | Identifier |
|---|---|
| Airtable workspace | `wspDGHtW2HNuT20GQ` |
| Airtable base — Data Registry (9 tables) | `appTGeB1jIizZbjnw` |
| Airtable base — Manuscript (7 tables) | `appJYOnUb7KrHKwpV` |
| Linear team | Thyroid Database / `THY` / `c4afb51b-8bca-413a-a53e-15eb825cffbd` |
| Daily-sync anchor issue | [THY-6](https://linear.app/rostemp/issue/THY-6/) |
| Scheduled daily sync | `thyroid-daily-sync` (07:04 local) |
| Cowork skill | `.cowork/skills/thyroid-integration/` (v1.4.0) |

### Hard rules (full list in [`CLAUDE.md`](CLAUDE.md))

1. **No PHI in Airtable or Linear, ever.** `research_id` only. Pathology text, op notes, MRNs, and dates of service narrower than year stay in DuckDB / local files. Override Decision evidence is Claude-summarized 1–2 sentences, never raw text.
2. **Nothing is ever deleted.** Linear issues close, never delete. Airtable records archive (`lifecycle = Archived`). `Manuscript-Locked` records cannot be edited at all without explicit unlock.
3. **Every change at user request gets logged BEFORE the change.** Manuscript edits → row in `Manuscript Feedback Log`. Data/registry edits → row in `Data Feedback Log`. The log row is created first; if logging fails, the edit doesn't happen.
4. **Pending Auto-Close, not auto-close.** When a Verification Check or Section reaches Verified/Finalized, the linked Linear issue moves to state `In Review` + label `auto-close:pending`. After 48h with no `/keep-open` it transitions to Done. `/close-now` skips the wait.

### Where to find the full architecture

| Doc | Purpose |
|---|---|
| [`INTEGRATION_PROPOSAL.md`](INTEGRATION_PROPOSAL.md) | v3 architecture proposal — schema, lifecycle, daily-sync prompt, execution sequence |
| [`CLAUDE.md`](CLAUDE.md) | Project context loaded automatically by Claude — hard rules, Session Opening Protocol, IDs |
| [`.cowork/skills/thyroid-integration/SKILL.md`](.cowork/skills/thyroid-integration/SKILL.md) | Operational playbook (auto-loads in Claude/Cowork on thyroid keywords) |
| [`.cowork/skills/thyroid-integration/references/airtable_schema.md`](.cowork/skills/thyroid-integration/references/airtable_schema.md) | Full Airtable schema (16 tables, all field IDs) |
| [`.cowork/skills/thyroid-integration/references/linear_schema.md`](.cowork/skills/thyroid-integration/references/linear_schema.md) | Full Linear schema (projects, labels, states, templates) |
| [`.cowork/skills/thyroid-integration/references/airtable_ids.md`](.cowork/skills/thyroid-integration/references/airtable_ids.md) | Live Airtable IDs (base/table/field/record) for direct API use |
| [`.cowork/skills/thyroid-integration/references/linear_ids.md`](.cowork/skills/thyroid-integration/references/linear_ids.md) | Live Linear IDs (team/projects/labels/initial issues) |
| [`.cowork/skills/thyroid-integration/references/daily_sync_prompt.md`](.cowork/skills/thyroid-integration/references/daily_sync_prompt.md) | Verbatim 11-phase daily sync prompt |
| [`.cowork/skills/thyroid-integration/references/manuscript_inventory.md`](.cowork/skills/thyroid-integration/references/manuscript_inventory.md) | Snapshot of all manuscripts (active + dormant + planned) |
| [`.cowork/skills/thyroid-integration/references/CHANGELOG.md`](.cowork/skills/thyroid-integration/references/CHANGELOG.md) | Skill version history |
| [`.cowork/skills/thyroid-integration.skill`](.cowork/skills/thyroid-integration.skill) | Packaged `.skill` bundle for Cowork installation |

### What this means in practice

- **Modifying a column or running a reconciliation** → expect a Verification Check row in Airtable, and a Linear issue in `Database Reconciliation & QA` once the daily sync runs.
- **Drafting a manuscript section** → expect a Manuscript Feedback Log row capturing the diff before the file changes; section status transitions reflected in the matching Linear project.
- **Submitting a paper** → status flip to `Submitted` automatically creates a Manuscript Snapshot freezing all linked records as `Manuscript-Locked`.
- **Future Claude / Cowork sessions** are required to run the Session Opening Protocol (verify connectors, read target record state, check lifecycle gates, pull recent ledger, sanity-check status) before responding to any thyroid-related request — see SKILL.md.

---

## Source of truth — live MotherDuck publication gate (2026-04-14)

**Canonical contract (read first):** [`docs/final_source_of_truth_contract.md`](docs/final_source_of_truth_contract.md) — live MotherDuck **`main`** (analytics) and **`qa`** (governance); **`qa.release_manifest`** is the release ledger; analyst surfaces **`main.master_fact_long_verified_v1`**, **`main.master_patient_rollup_verified_v1`**, **`main.master_source_lineage_v1`**; machine-generated repo mirror [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](studies/CURRENT_MOTHERDUCK_REPO_STATE.md) via [`scripts/144_md_repo_current_state_summary.py`](scripts/144_md_repo_current_state_summary.py). **Same top-level story** as [`truth_sync_summary.md`](truth_sync_summary.md) and [`docs/REPO_STATUS.md`](docs/REPO_STATUS.md). **Historical only:** checked-in `exports/release_manifests/` JSON (see [`exports/release_manifests/README.md`](exports/release_manifests/README.md)), March-era evidence packs, local freeze snapshots — **not** interchangeable with live counts without regenerating `144` + `119`.

**Technical vs governance:** `119 --release-mode` can be green while manuscript policy still requires human-reviewed MRQ / promotion where applicable — automation does **not** prove per-fact clinician review. Reviewer fields on `master_*_verified_v1` are joined from `qa.manual_review_queue` at **(research_id, domain)** grain (see `scripts/125_master_verified_views.py`).

**Latest live parity — full `138` replay + `143` QA diagnostics + `119 --release-mode` (prod):** [`studies/specimen_fhir_live_parity_20260413_1626/report.md`](studies/specimen_fhir_live_parity_20260413_1626/report.md) — QA clone refresh (`130 clone --qa`), **prepromote-backup** `specimen_fhir_replay_20260413_1625`, connection UA `specimen_fhir_live_parity_v1`, **Check 13** green, `v_diag_specimen_fhir_broken_refs_v1` empty, before/after row counts (DUCKLAKE — no named snapshots).

**Authoritative release-truth + live prod capture (specimen / FHIR; query UA `specimen_fhir_release_truth_v2` on this run):** [`studies/specimen_fhir_release_truth_20260408T141710Z/report.md`](studies/specimen_fhir_release_truth_20260408T141710Z/report.md) — **live** `Thyroid 2026` probe, row counts, `qa.release_manifest`, **Check 13** summary (2026-04-08 UTC), DUCKLAKE snapshot caveat, manifest lag vs `exports/release_manifests/LATEST_MANIFEST.json`. **Superseded** prior capture: [`studies/specimen_fhir_release_truth_20260408T122117Z/report.md`](studies/specimen_fhir_release_truth_20260408T122117Z/report.md). Operator-only prep (no live token session): [`studies/specimen_fhir_release_truth_20260408T121042Z/report.md`](studies/specimen_fhir_release_truth_20260408T121042Z/report.md).

**Four “ready” lenses (do not conflate the first three):**

| Lens | What it means today | Evidence |
|------|---------------------|----------|
| **1. Structural — specimen + analytic FHIR** | Tables, QA views, and **`119 --release-mode` Check 13** can **PASS**; specimen merge/genomic linkage may still show **WARN** review burden | **2026-04-13 parity replay:** [`studies/specimen_fhir_live_parity_20260413_1626/report.md`](studies/specimen_fhir_live_parity_20260413_1626/report.md) · **April 2026 release-truth:** [`studies/specimen_fhir_release_truth_20260408T141710Z/report.md`](studies/specimen_fhir_release_truth_20260408T141710Z/report.md) · **Superseded** bundles: [`studies/specimen_fhir_release_truth_20260408T122117Z/`](studies/specimen_fhir_release_truth_20260408T122117Z/), [`studies/specimen_fhir_release_truth_20260408T065318Z/`](studies/specimen_fhir_release_truth_20260408T065318Z/), [`studies/specimen_fhir_release_truth_20260408T121042Z/`](studies/specimen_fhir_release_truth_20260408T121042Z/) |
| **2. Technical — MotherDuck release readiness** | **`119 --release-mode`** promotability: **0** pending MRQ, **0** synthetic-placeholder statuses (CHECK **5b**), manifests / presentation views / domain parity per validator — **not** the same as “every row was clinician-reviewed” | Rebaseline: [`studies/manuscript_blocker_rebaseline_20260408T144500Z/report.md`](studies/manuscript_blocker_rebaseline_20260408T144500Z/report.md) (older triage: [`studies/manuscript_blocker_rebaseline_20260408T073548Z/report.md`](studies/manuscript_blocker_rebaseline_20260408T073548Z/report.md)) · Triage export: [`docs/review_queue_triage_export.md`](docs/review_queue_triage_export.md) |
| **3. Manuscript-governance readiness** | Publication policy: **human reviewer identity** + substantive decisions vs **tier-policy `auto_accepted_*`**; `qa.promotion_review_decisions` batch provenance vs row-level adjudication | Same rebaseline report · Governance memo (**historical / conditional**; not row-count SSOT): [`studies/20260407_publication_signoff_live/final_verdict_memo.md`](studies/20260407_publication_signoff_live/final_verdict_memo.md) · Gate: [`docs/publication_governance_gate.md`](docs/publication_governance_gate.md) · **Live governance snapshot (MRQ + specimen queues, no fabricated review):** [`studies/manuscript_human_review_release_20260413T170316Z/report.md`](studies/manuscript_human_review_release_20260413T170316Z/report.md) |
| **4. Institutional non-Tg labs (coverage)** | **Wave present** on prod (`final_institutional_20260407` via **`127`**); residual gaps are **source-limited** enrichment (analytes not in extract, sparsity), not “no append path” | [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](studies/20260411_final_master_release/EVIDENCE_PACK.md) (**current** vs **superseded** [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](studies/20260409_final_master_release/EVIDENCE_PACK.md)) · Scaffold: [`docs/lab_layer_scaffold_plan_20260313.md`](docs/lab_layer_scaffold_plan_20260313.md) |

**Separate these three explicitly:**

- **Technical release readiness** — Validator + structural gates (`119 --release-mode`, empty pending MRQ, Check 13, manifests). Green here does **not** prove clinician row review.
- **Human-review / manuscript readiness** — Named reviewer + timestamp where policy requires; row-level decisions in `qa.promotion_review_decisions` vs tier-policy batch lines; specimen/genomic **review burden** (`qa.specimen_genomic_link_review_v1`, `qa.v_diag_specimen_review_burden_v1`) may still be large while `119` **WARN**s.
- **Lab coverage limitations** — Institutional non-Tg chemistry **ingest path** is **`127`** when a new analyst file exists; missing analytes or sparse rows are **coverage** limits, not “forgot to run the script” unless no source file was delivered.

**Live row-count mirror (regenerate; do not trust stale SHA):** [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](studies/CURRENT_MOTHERDUCK_REPO_STATE.md) — default output of [`scripts/144_md_repo_current_state_summary.py`](scripts/144_md_repo_current_state_summary.py) (`--md`, optional `--md-env qa|prod`, `--also-write` to refresh this file). Older lineage-audit tree: [`studies/20260407_live_truth_and_lineage_contract_audit/`](studies/20260407_live_truth_and_lineage_contract_audit/). Early 20-check era snapshot: [`studies/20260407_formalization_validation_release_mode/`](studies/20260407_formalization_validation_release_mode/) — **history only**.

**Single-sentence release posture (last committed live narrative, 2026-04-13):** Expect **`119 --release-mode` PASS WITH WARNINGS** when specimen/FHIR diagnostics are deployed; **Check 13** should show **no FAIL** (WARN possible on specimen-adjacent / genomic **review burden**). **Manuscript / governance** sign-off may still require **human-reviewed** MRQ where policy demands it. Re-run `119` after any `138`/`143` change — see [`studies/specimen_fhir_live_parity_20260413_1626/report.md`](studies/specimen_fhir_live_parity_20260413_1626/report.md) for the latest operator loop.

**Checked-in vs cloud manifests:** `exports/release_manifests/LATEST_MANIFEST.json` is a **checkpoint** (may be historical — see `role` in JSON and [`exports/release_manifests/README.md`](exports/release_manifests/README.md)). **Authoritative promotion history** is `qa.release_manifest` on the **live** catalog — refresh pointer with [`scripts/145_export_release_manifest_pointer.py`](scripts/145_export_release_manifest_pointer.py) `--md` or rely on `144 --md` warnings vs live.

---

## Dataset Maturation Layer (v2026.03.13)

**Layout (2026-04-02):** LLM extraction lives in [`llm_extraction/`](llm_extraction/) (merged legacy `notes_extraction` + `notes_extraction_new`). Staging checkpoints sit under [`processed/output/`](processed/output/); study/manuscript artifact trees under [`processed/outputs/`](processed/outputs/). Medallion tiers are documented in [`docs/REPO_ARCHITECTURE_V2.md`](docs/REPO_ARCHITECTURE_V2.md).

**Read this first — three layers of “ready”:**

1. **2026-03-13 local manuscript-ready freeze** — Point-in-time **local DuckDB** hardening, 7/7 readiness gates, publication bundle, and Zenodo snapshot ([`v2026.03.10-publication-ready`](../../releases/tag/v2026.03.10-publication-ready)). This is **not** the same artifact as the live MotherDuck formalization below; GitHub `main` and Zenodo can diverge until a new Zenodo version is cut.
2. **2026-04-06 — 07 MotherDuck formalization / release candidate** — Cloud **MotherDuck** `v2_stage` → `main` promotion, `qa.*` governance, immutable `release_YYYYMMDD` snapshots, analyst presentation views from [`scripts/125_master_verified_views.py`](scripts/125_master_verified_views.py), and strict release checks in [`scripts/119_md_formalization_validate.py`](scripts/119_md_formalization_validate.py) (`--release-mode`). **Domain SSOT:** [`config/extraction_domain_registry.yaml`](config/extraction_domain_registry.yaml); reconciled inventory snapshot: [`studies/20260406_domain_inventory_current/`](studies/20260406_domain_inventory_current/) (**23** promoted v2 domains, **31** parent domains = 8 v1 + 23 v2, **0** unclaimed on-disk parquets per regenerated inventory).
3. **What blocks a signed *MotherDuck* manuscript release today (April 2026)** — **Structural (specimen/FHIR):** operator-acceptable **Check 13** (see specimen truth reports). **Technical (`119` strict):** can be **GREEN** on MRQ *structure* (0 pending, 0 synthetic placeholders) while **~100%** of rows are still **`auto_accepted_*`**. **Governance:** manuscript sign-off may still require **human-reviewed** MRQ + substantive `decision_batch_id` **scope** where policy demands it — see [`studies/manuscript_blocker_rebaseline_20260408T144500Z/report.md`](studies/manuscript_blocker_rebaseline_20260408T144500Z/report.md) (live triage: **0** synthetic, **0** human-identity bucket, **2** `confirmed_correct` without reviewer). **Institutional labs:** wave **ingested** via **`127`**; remaining gaps are **coverage / analytes**, not “missing wave.” Historical formalization snapshot (20-check era): [`studies/20260407_formalization_validation_release_mode/`](studies/20260407_formalization_validation_release_mode/) — prefer **live truth audit** for current `119` evidence.

**Historical status table (2026-04-08 rebaseline era — point-in-time; not the current live posture row-for-row):** For **2026-04-13** live specimen/FHIR parity, governance snapshots, and current release posture, use the **Source of truth** section at the top of this README ([`studies/specimen_fhir_live_parity_20260413_1626/report.md`](studies/specimen_fhir_live_parity_20260413_1626/report.md), [`studies/manuscript_human_review_release_20260413T170316Z/report.md`](studies/manuscript_human_review_release_20260413T170316Z/report.md), [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](studies/CURRENT_MOTHERDUCK_REPO_STATE.md) when regenerated). The table below is retained as a **dated audit snapshot**.

| Phase | Status (2026-04-08) |
|-------|--------|
| V2 extraction | **Complete** — 31 parent domains (8 v1 + 23 v2), 7 sub-prompt domains, 6 concordance-audit stems (not staged; registry `legacy-concordance`) |
| MotherDuck structure | **Formalized** — v2_stage ↔ main parity for **23** `canonical_output` domains; multiple `release_*` schemas; **`qa.release_manifest`** ordering is **live-only** — see [`studies/specimen_fhir_release_truth_20260408T141710Z/report.md`](studies/specimen_fhir_release_truth_20260408T141710Z/report.md) |
| Repo / inventory consistency | **Aligned** — registry is SSOT; 0 unclaimed parquets in latest inventory run; [`scripts/119_md_formalization_validate.py --md --release-mode`](scripts/119_md_formalization_validate.py) enforces queue, manifest, canonical provenance, **presentation views**, and specimen/FHIR QA checks |
| Manual review queue | **Live gate (2026-04-08):** 5,622 rows, **0** pending / **0** NULL `verification_status`; Check **5b** PASS (no synthetic-placeholder statuses). **Manuscript** sign-off may still require **human-reviewed** CSV + hydrate where publication policy says so (`studies/20260407_publication_blocker_assessment/`). **Newer governance snapshot:** [`studies/manuscript_human_review_release_20260413T170316Z/report.md`](studies/manuscript_human_review_release_20260413T170316Z/report.md). |
| Non-Tg lab pull | **Closed (2026-04-07 UTC)** — `final_institutional_20260407` ingested via `127` (`exports/incoming/final_institutional_chemistry_20260407.csv`); closeout memos under `studies/20260407_institutional_lab_wave_closeout/`; latest master evidence `studies/20260411_final_master_release/EVIDENCE_PACK.md` |
| Final-master operator path | **Make targets** — `make md-final-master-dryrun` / `md-final-master-final` (see [Makefile](Makefile)); orchestrator [`scripts/126_final_master_release.py`](scripts/126_final_master_release.py) |

A final manuscript-readiness hardening pass on 2026-03-13 audited 578 local DuckDB
tables, 16 `val_*` validation tables, and all prior audit documents. Subsequent
validation/benchmark scripts (81–93) added 27 tables. Further episode-linkage repair
(scripts 94–97) and final verification (script 98) brought the total to 624. The
**analysis-resolved layer** is populated and all 7 readiness gates pass. The
extraction pipeline is complete (13 phases, 11 engine versions). A subsequent
**hardening pass** fixed 3 missing Streamlit tables, ran ANALYZE on 17 key tables,
and verified all dashboard data dependencies against live local DuckDB state.

**Honest assessment:** ~50% of patients have clinical notes; operative NLP enrichment
fields (berry ligament, frozen section, EBL) remain at 0% due to pipeline architecture;
88.8% of recurrence dates are unresolved; RAI dose coverage is 41%. These are
documented source limitations, not data quality failures.

### Key references

| Artifact | Location |
|----------|----------|
| Definitive verification report | [`docs/final_repo_verification_20260313.md`](docs/final_repo_verification_20260313.md) |
| Database hardening audit | [`docs/database_hardening_audit_20260313.md`](docs/database_hardening_audit_20260313.md) |
| Manuscript metric reconciliation | [`docs/manuscript_metric_reconciliation_20260313.md`](docs/manuscript_metric_reconciliation_20260313.md) |
| Freeze alignment report | [`docs/manuscript_freeze_alignment_20260313.md`](docs/manuscript_freeze_alignment_20260313.md) |
| Canonical backfill report | [`docs/canonical_backfill_report_20260313.md`](docs/canonical_backfill_report_20260313.md) |
| Publication bundle (62 files) | `exports/FINAL_PUBLICATION_BUNDLE_20260313/` |
| Readiness assessment (7/7 PASS) | `exports/FINAL_PUBLICATION_BUNDLE_20260313/readiness_assessment.json` |
| Zenodo DOI | [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510) (point-in-time archive; **GitHub `main` may be newer**) |
| Zenodo ↔ GitHub | [`docs/ZENODO_GITHUB_SYNC_NOTES_20260326.md`](docs/ZENODO_GITHUB_SYNC_NOTES_20260326.md) — how to publish a new Zenodo version after pushes |
| ETE manuscript revision packet | [`manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md`](manuscripts/ete_ajcc8_202603/MANUSCRIPT_REVISION_PACKET_20260326.md) |
| LLM extraction handoff | [`docs/llm_extraction_handoff_20260327.md`](docs/llm_extraction_handoff_20260327.md) |
| Repo architecture (medallion) | [`docs/REPO_ARCHITECTURE_V2.md`](docs/REPO_ARCHITECTURE_V2.md) |
| LLM validation workspace | [`studies/llm_extraction_validation/README.md`](studies/llm_extraction_validation/README.md) |
| Architecture sign-off memo | [`studies/20260407_signoff_memo/signoff_memo.md`](studies/20260407_signoff_memo/signoff_memo.md) |
| Specimen + FHIR design audit (memo) | [`studies/specimen_fhir_design_20260407_184730/design_memo.md`](studies/specimen_fhir_design_20260407_184730/design_memo.md) |
| MotherDuck DB contract | [`docs/motherduck_database_contract_v1.md`](docs/motherduck_database_contract_v1.md) (includes specimen + analytic FHIR v1) |
| Specimen/FHIR reviewer + release contract | [`docs/specimen_fhir_contract_review.md`](docs/specimen_fhir_contract_review.md) · QA views `142` · point-in-time repo/live summary [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](studies/CURRENT_MOTHERDUCK_REPO_STATE.md) (**regenerate**; not the `119` SSOT) · **Note:** final-master **`release_*`** / **`118 --final-master`** bundles are manuscript-analytic only (no `specimen_*`/`fhir_*` copies — see contract doc) |
| Specimen + FHIR materialization | [`scripts/138_md_specimen_fhir_layer.py`](scripts/138_md_specimen_fhir_layer.py), [`scripts/sql/138_specimen_fhir_tail_ddl.sql`](scripts/sql/138_specimen_fhir_tail_ddl.sql), [`scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql`](scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql) |
| FHIR specimen NDJSON export (de-identified bundles) | [`scripts/141_fhir_specimen_json_export.py`](scripts/141_fhir_specimen_json_export.py) → `exports/fhir_specimen_<UTC>/` (`specimen_bundles.ndjson`, `manifest.json`, `README.md`); prefers `main.fhir_bundle_specimen_export_v1`, else reconstructs from `fhir_specimen_v1` + procedure + encounter + episode tables (`--force-reconstruct` skips bundle table); `--local-duckdb` for offline/CI; `manifest.json` records `source_catalog`, `source_views`, `from_prebuilt_bundle_view`, git SHA, row counts, UA `specimen_fhir_export_restore_v1` |
| Molecular prod release (dev/qa/prod, 137) | [`docs/release_runbook.md`](docs/release_runbook.md) |
| Multimodal contract (128/129) — operator runbook | [`docs/multimodal_contract_runbook.md`](docs/multimodal_contract_runbook.md) |
| Multimodal strict release gate (fail conditions) | [`docs/multimodal_release_gate.md`](docs/multimodal_release_gate.md) |
| DICOM header ingest (150) — operator runbook | [`docs/dicom_header_ingest_runbook.md`](docs/dicom_header_ingest_runbook.md) · design memo [`studies/20260408_dicom_header_ingest/design_memo.md`](studies/20260408_dicom_header_ingest/design_memo.md) |
| Review queue triage export (script 120) | [`docs/review_queue_triage_export.md`](docs/review_queue_triage_export.md) |
| Domain mapping rules | [`docs/domain_mapping_rules.md`](docs/domain_mapping_rules.md) |
| Domain inventory (current) | [`studies/20260406_domain_inventory_current/`](studies/20260406_domain_inventory_current/) |
| Release-mode validation (specimen/FHIR; prod **2026-04-08**) | [`studies/specimen_fhir_release_truth_20260408T141710Z/119_release_validation_prod/`](studies/specimen_fhir_release_truth_20260408T141710Z/119_release_validation_prod/) · Report: [`studies/specimen_fhir_release_truth_20260408T141710Z/report.md`](studies/specimen_fhir_release_truth_20260408T141710Z/report.md) · **Superseded:** [`studies/specimen_fhir_release_truth_20260408T122117Z/`](studies/specimen_fhir_release_truth_20260408T122117Z/) · Older: [`studies/specimen_fhir_release_truth_20260408T065318Z/119_release_validation/`](studies/specimen_fhir_release_truth_20260408T065318Z/119_release_validation/) |
| Release-mode validation (April 2026 lineage audit snapshot) | [`studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/`](studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/) |
| Release-mode validation (historical 20-check PASS) | [`studies/20260407_formalization_validation_release_mode/`](studies/20260407_formalization_validation_release_mode/) |
| Git tag | [`v2026.03.10-publication-ready`](../../releases/tag/v2026.03.10-publication-ready) |

### DICOM header ingest layer (additive, v1)

- **Script:** [`scripts/150_ingest_dicom_headers.py`](scripts/150_ingest_dicom_headers.py) with DDL [`scripts/sql/150_dicom_header_layer_ddl.sql`](scripts/sql/150_dicom_header_layer_ddl.sql) and helpers [`utils/dicom_header_helpers.py`](utils/dicom_header_helpers.py).
- **Supported inputs:** flattened radiology header exports (CSV, XLSX, JSON, Parquet) **and** raw `.dcm` files with **metadata-only** reads (`stop_before_pixels=True`; pixel data are never decoded).
- **Default behavior:** export-only Parquet + `manifest.json` under `exports/dicom_header_ingest_<UTC>/` — **no database writes** unless the operator runs with `--write-db` (see runbook).
- **Relationship to imaging / multimodal:** **additive** alongside `imaging_nodule_master_v1`, [`scripts/128_multimodal_contract_mm_v1.py`](scripts/128_multimodal_contract_mm_v1.py), and [`scripts/129_imaging_fna_linkage_mm_v1.py`](scripts/129_imaging_fna_linkage_mm_v1.py); it does not replace or mutate those pipelines. Optional joins live in helpers only; linkage scripts stay unchanged unless you opt in later.
- **Canonical MotherDuck `main`:** tables are **repo-defined** and become **contract/canonical-live** only after operator materialization and promotion — do not assume they exist in prod without verification; see [`docs/motherduck_database_contract_v1.md`](docs/motherduck_database_contract_v1.md) (DICOM subsection) and [`studies/20260413_dicom_promotion_reconciliation/report.md`](studies/20260413_dicom_promotion_reconciliation/report.md).
- **Docs:** runbook [`docs/dicom_header_ingest_runbook.md`](docs/dicom_header_ingest_runbook.md), design memo [`studies/20260408_dicom_header_ingest/design_memo.md`](studies/20260408_dicom_header_ingest/design_memo.md).

### What "manuscript-ready" means

The manuscript cohort (`manuscript_cohort_v1`, 10,871 patients, 139 columns), the
analysis-eligible cancer subcohort (N=4,136), episode-level dedup table, scoring
systems (AJCC8/ATA/MACIS/AGES/AMES), Tables 1–3, and Figures 1–5 are generated
and verified. **All manuscript-facing metrics are governed by the canonical metrics registry**
(`canonical_metrics_registry_v1`; see `docs/canonical_metrics_governance_20260315.md`).
Drift detection and staleness enforcement are integrated into the release promotion gate (G7).

### Dataset Verification Status (March 13 2026)

The dataset maturation pass resolved the following:

1. **Operative CND/LND flags** — wired from structured `path_synoptics` fields;
   CND: 0 -> 2,497 TRUE (26.6%); LND: 0 -> 241 TRUE (2.6%)
2. **Operative note dates** — 9,366 of 9,371 episodes now have resolved dates
3. **Imaging layer** — `imaging_nodule_master_v1` (19,891 rows) is now canonical;
   `imaging_nodule_long_v2` deprecated (schema stub)
4. **Provenance columns** — unified `source_table`, `source_script`, `provenance_note`,
   `resolved_layer_version` added to all 4 analysis tables
5. **Chronology anomalies** — 626 classified (102 benign, 14 extraction errors, 510 true conflicts)
6. **local DuckDB optimization** — ANALYZE TABLE run on 10 canonical tables
7. **Health monitoring** — 3 dashboard tables deployed (`val_dataset_integrity_summary_v1`,
   `val_provenance_completeness_v2`, `val_episode_linkage_completeness_v1`)
8. **Canonical gap closure** (`scripts/76_canonical_gap_closure.py`) — RAI dose
   provenance (20% -> 41%), RAS subtype propagation (325 rows), linkage ID
   propagation (6 tables), recurrence date hardening (4 tiers)
9. **Lab canonical layer** (`scripts/77_lab_canonical_layer.py`) —
   `longitudinal_lab_canonical_v1` (45,954 rows, 5 analytes, 3,349 patients)
   with forward-compatible schema for future institutional lab extract
10. **Workflow dashboard refactor** — 39 flat tabs reorganized into 6
    workflow-first sections; new QA workbench and manual review workbench modules
11. **Final hardening** (`scripts/78_final_hardening.py`) — recurrence review
    queue, imaging-FNA linkage fix, RAI missingness classification, lab
    contract validation

### Remaining Source-Limited Gaps

- Non-Tg lab **temporal fidelity / coverage** — institutional wave `final_institutional_20260407` is **ingested**; residual sparsity and edge cases remain **source-limited** (see [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](studies/20260411_final_master_release/EVIDENCE_PACK.md)), not a “no extract received” blocker
- Zero nuclear medicine notes in corpus — RAI dose recovery capped at ~41%
- 87% vascular invasion remains `present_ungraded` — synoptic template limitation
- Pre-2019 operative notes absent — institutional data limitation
- 1,764 recurrence dates unresolved — requires manual chart review (prioritized queue deployed)
- 8 operative V2 NLP enrichment fields at 0% — extractor exists but outputs not materialized

### LLM-Assisted Entity Extraction (v2026.03.27)

The extraction pipeline uses **GitHub Models gpt-4o-mini** (free tier) for
structured JSON entity extraction from clinical notes.

| Component | Location |
|-----------|----------|
| LLM extractor | `llm_extraction/extract_llm.py` |
| Pipeline runner | `llm_extraction/run_extraction.py` |
| Expanded domain prompts (V2 fleet) | `llm_extraction/prompts/` |
| Validation workspace | [`studies/llm_extraction_validation/README.md`](studies/llm_extraction_validation/README.md) |
| System prompt | `prompts/lab_date_extraction_v1.txt` |
| Handoff doc | [`docs/llm_extraction_handoff_20260327.md`](docs/llm_extraction_handoff_20260327.md) |

**Run:**
```bash
export GITHUB_TOKEN='ghp_...'
.venv/bin/python llm_extraction/run_extraction.py \
  --workers 3 --input processed/clinical_notes_long.parquet
```

- 11,037 notes, 5,641 patients, 6 entity domains
- Output: `processed/note_entities_{domain}.parquet` plus `processed/note_entities_llm.parquet` when LLM extraction is enabled
- Validation and side-by-side comparison: `studies/llm_extraction_validation/`
- Post-extraction: publish entity outputs with the repo uploader for your target environment; the checked-in uploader is `scripts/09b_fabric_upload_notes_entities.py`

**API priority:** GitHub Models (`GITHUB_TOKEN`) → OpenAI fallback (`OPENAI_API_KEY`).
Thread-local clients with 5-retry exponential backoff and `--workers` concurrency.

### Current repo status

**Formalization (2026-04-06 — 07):** MotherDuck has a full promotion path: v2_stage
loader, eight-gate promotion, QA schema hydrate, canonical materialization (103),
contract views (117), **analyst presentation views** (125:
`main.master_fact_long_verified_v1`, `main.master_patient_rollup_verified_v1`,
`main.master_source_lineage_v1`), release snapshots (115), and parquet bundle (118).

**Release-mode validator** — `scripts/119_md_formalization_validate.py --md --release-mode`
fail-closes on: live MotherDuck attach, row parity for **23** promoted v2 domains,
`v2_stage.load_inventory` row_match, **empty pending manual review queue**,
presence of `release_*` schema + `qa.release_manifest`, non-blank
`extraction_run_id` on `main.canonical_extracted_fact_long_v2`, and **traceability
columns / non-null core fields** on the three `master_*_verified_v1` presentation
views. Evidence: `studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/validation_report.md` (27-check `119`); early snapshot: `studies/20260407_formalization_validation_release_mode/validation_report.md`.

Domain inventory (registry SSOT, on-disk parity): regenerate with
`studies/20260406_domain_inventory_current/generate_inventory.py` — latest summary
shows **0** unclaimed parquets and **23** v2 fleet domains with `canonical_output`.

See [`docs/FINAL_REPO_STATUS_20260313.md`](docs/FINAL_REPO_STATUS_20260313.md) for the
definitive single source of truth (readiness, maturity, safe/unsafe claims).
See [`docs/REPO_STATUS.md`](docs/REPO_STATUS.md) for the navigable index of all
audit documents, export bundles, and open backfill items.

#### Manual review queue triage (read-only export)

CSVs + `summary.md` under a timestamped `exports/review_queue_triage_<UTC_YYYYMMDD_HHMMSS>/` folder; **SELECT-only** on `qa.manual_review_queue`. **Specimen–genomic merge review** (separate table): `scripts/151_specimen_genomic_review_queue_export.py` → `exports/specimen_genomic_review_*` (see same doc). **Full usage, artifact list, and token notes:** [`docs/review_queue_triage_export.md`](docs/review_queue_triage_export.md).

**MotherDuck:**

```bash
export MOTHERDUCK_TOKEN='md_…'   # or MD_SA_TOKEN with --md-sa
.venv/bin/python scripts/120_review_queue_triage.py --md
```

**Local DuckDB file:**

```bash
.venv/bin/python scripts/120_review_queue_triage.py --db-path thyroid_master.duckdb
```

Optional: `--md-sa`, `--run-label …`, `--output-root exports/offline`. Offline smoke: `python -m pytest tests/test_120_review_queue_triage.py`.

---

Thyroid cancer research lakehouse — 11,673 patients across 13 base tables,
8+ analytic views, and a local Power BI Desktop analytics layer.
All data encrypted on local drive (PHI compliance).

## Repository layout

```
.
├── dashboard.py              # Streamlit dashboard (main entry point)
├── requirements.txt          # Python dependencies
├── runtime.txt               # Python 3.11 pin for Streamlit Cloud
├── .streamlit/
│   ├── config.toml           # Server, theme, and browser settings
│   └── secrets.toml          # (gitignored) local config
├── .github/workflows/
│   └── ci.yml                # CI: Ruff/Mypy, YAML validation, MotherDuck formalization path, legacy gates
├── llm_extraction/           # Regex + LLM entity extraction (package: llm_extraction)
├── lakehouse/                # Medallion docs: bronze/ silver/ gold/
├── prompts/                  # Shared prompt text (e.g. lab_date_extraction_v1)
├── scripts/                  # ETL and view-creation scripts
├── notebooks/                # Jupyter exploration notebooks
├── exports/                  # Publication-ready CSV exports (often gitignored)
├── processed/                # Silver: DVC parquets, remaining/, output/, outputs/
├── studies/                  # Per-proposal analysis folders
├── docs/                     # Documentation (QA report, architecture)
├── data_dictionary.md        # Full schema documentation
└── RELEASE_NOTES.md          # Publication release notes
```

## Quick start (local)

```bash
# 1. Clone
git clone https://github.com/ry86pkqf74-rgb/THYROID_2026.git
cd THYROID_2026

# 2. Create virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Launch dashboard
streamlit run dashboard.py
```

> **After any view change**, run `python scripts/03_research_views.py` locally
> before pushing so that local DuckDB has the updated views.

Open **http://localhost:8501** in your browser.

### Lint / types

Install tooling (pinned in [`requirements-dev.txt`](requirements-dev.txt)):

```bash
pip install -r requirements.txt -r requirements-dev.txt
```

**Pyflakes-style correctness (Ruff F-rules only)** — same scope as CI:

```bash
ruff check scripts app utils llm_extraction motherduck_client.py dashboard.py --select F
```

**Mypy** reads `[tool.mypy]` in [`pyproject.toml`](pyproject.toml) (`files = [...]`). Only those paths are checked in CI; expanding the list toward full-repo strict typing is intentional and incremental.

```bash
mypy
```

GitHub Actions runs the Ruff and Mypy commands above in the `ruff-and-mypy` job (no database secrets required).

### MotherDuck quickstart (staging / QC only)

MotherDuck holds **no raw PHI**; tokens authenticate the cloud DuckDB attach. Use env vars (not CLI flags) for secrets.

**Read/write vs read-scaling:** Staging loaders (`116_md_stage_loader.py`), promotion gate (`112_*`), generated promote SQL, materializers (`103_*`), QA hydration (`114_*`), release snapshots (`115_*`), parquet bundle (`118_*`), and **release-mode validation** (`119_* --release-mode`) **must** use a **read/write** MotherDuck API token (`MOTHERDUCK_TOKEN` or `MD_SA_TOKEN`). A **read-scaling** dashboard token (`MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN`) is **only** for `MotherDuckClient.connect_read_scaling()` / analyst read load; `connect_rw()` refuses environments where that is the only credential (see [`docs/motherduck_database_contract_v1.md`](docs/motherduck_database_contract_v1.md) §8). Dashboard connection strings, opt-in Streamlit flags, and `CREATE SNAPSHOT` / `REFRESH DATABASE` helpers: [`docs/motherduck_read_scaling_dashboard.md`](docs/motherduck_read_scaling_dashboard.md).

1. Copy [`.env.motherduck.example`](.env.motherduck.example) to `.env.motherduck` (gitignored) and set **`MOTHERDUCK_TOKEN`** (personal) or **`MD_SA_TOKEN`** (CI). Same keys may live in `.streamlit/secrets.toml`.
2. Confirm a **read/write** API token resolves (read-scaling tokens are ignored by this check; they alone must **not** pass):  
   `.venv/bin/python -c "from motherduck_client import token_mode; m=token_mode(); print(m); assert m != 'none'"`
3. Fail-closed MotherDuck smoke check: `scripts/smoke_test_md_connection.py --md` calls `connect_md_fail_closed` and reuses the shared `PRAGMA database_list` verification in `utils/md_connect.py` (not `SHOW DATABASES` / heuristics). The process exits 1 if `--md` was requested but the attach is not MotherDuck, if no read/write token is available, or if the cloud connection fails — there is no silent fallback to `thyroid_master.duckdb`. Without `--md`, the script opens the local file as before.  
   Run: `.venv/bin/python scripts/smoke_test_md_connection.py --md` or `make md-smoke` (the Make target requires `MOTHERDUCK_TOKEN` or `MD_SA_TOKEN` in the environment before invoking Python).
4. **Staging** for new v2 parquets is schema **`v2_stage`**; **`main`** is the promoted canonical surface after the gate and promotion steps (see [`docs/motherduck_v2_staging_runbook.md`](docs/motherduck_v2_staging_runbook.md), [`docs/motherduck_database_contract_v1.md`](docs/motherduck_database_contract_v1.md)).

#### MotherDuck API token examples (env vars)

Use real values from the MotherDuck dashboard (tokens typically start with `md_`). Resolution details: [`motherduck_client.py`](motherduck_client.py) module docstring and contract §8.

**1) Personal read/write** — interactive development, ad-hoc SQL, notebooks when you are the human operator:

```bash
export MOTHERDUCK_TOKEN='md_…'   # read/write MotherDuck API token
.venv/bin/python motherduck_client.py --env prod
```

**2) CI / service-account read/write** — GitHub Actions or automation; prefer this over personal tokens in CI:

```bash
export MD_SA_TOKEN='md_…'
.venv/bin/python motherduck_client.py --env prod --sa
```

**3) Business read-scaling (dashboard read-only)** — scaled read replicas / analyst dashboards **only**; never for promotion or validators. `MD_READ_SCALING_TOKEN` wins if both scaling env vars are set. Optional session affinity:

```bash
export MD_READ_SCALING_TOKEN='md_…'
export MD_READ_SCALING_SESSION_HINT='streamlit_prod_dashboard'
.venv/bin/python -c "
from motherduck_client import MotherDuckClient
con = MotherDuckClient.for_env('prod').connect_read_scaling()
print(con.execute('SELECT current_database()').fetchone())
con.close()
"
```

**5) Writer / reader freshness (read scaling)** — after data changes, operators can run `scripts/136_md_read_scaling_snapshot_refresh.py` (`writer` / `reader` subcommands; use `--dry-run` to print SQL). See the read-scaling doc above.

## Data architecture

| Property           | Value |
|--------------------|-------|
| Storage            | Local DuckDB + Parquet lakehouse |
| Encryption         | FileVault (full-disk encryption) |
| Patients           | 11,673 |
| Base tables        | 13 |
| Analytic views     | 8+ (ptc_cohort, recurrence_risk_cohort, advanced_features_view, etc.) |

All data resides on the encrypted local drive. Parquet files in `processed/`
are the source of truth, tracked via DVC.

## Dashboard (6 Workflow Sections)

The Streamlit dashboard is organized into 6 workflow-first sections:

1. **Overview** — cohort KPIs, data completeness by surgery year, date rescue
   rate, dataset health monitoring, linkage/provenance completeness, caveats
2. **Patient Explorer** — per-patient timeline with date-status legend and
   eligibility badges, patient audit, data explorer, visualizations
3. **Data Quality** — QA workbench (integrity, provenance, imaging-FNA linkage
   status, chained molecular metrics, RAI missingness, recurrence date
   resolution, lab coverage), manual review workbench (chronology conflicts,
   extraction errors, linkage ambiguities, unresolved recurrence prioritized
   queue), validation engine, diagnostics, cohort QC
4. **Linkage & Episodes** — extraction completeness, molecular/RAI/imaging/
   operative episode analytics, QA & adjudication, features explorer, timeline
5. **Outcomes & Analytics** — survival, advanced survival, statistical analysis,
   predictive analytics (model comparison, competing risks, ML nomograms,
   cure calculator), advanced analytics, cure probability
6. **Manuscript & Export** — genetics, specimen, complications, imaging,
   ThyroSeq integration, review queues (histology, molecular, RAI)

## Interactive Stats & Modeling

The `ThyroidStatisticalAnalyzer` class (`utils/statistical_analysis.py`) provides a publication-ready statistical engine:

```python
from utils.statistical_analysis import ThyroidStatisticalAnalyzer
analyzer = ThyroidStatisticalAnalyzer(con)

# Table 1 with SMD
t1_df, meta = analyzer.generate_table_one(data=df, groupby_col="braf_positive")

# FDR-corrected hypothesis tests
results = analyzer.run_hypothesis_tests(df, "event_occurred", features, correction="fdr_bh")

# Logistic regression with clinical snippet
result = analyzer.fit_logistic_regression("event_occurred", predictors, data=df)
snippet = ThyroidStatisticalAnalyzer.format_clinical_snippet(result, model_type="OR")

# Longitudinal Tg mixed-effects
long = analyzer.longitudinal_summary(marker="tg")

# Power analysis
n = ThyroidStatisticalAnalyzer.power_two_proportions(p1=0.15, p2=0.05)
```

**CLI demo** (outputs to `studies/statistical_analysis_examples/`):
```bash
.venv/bin/python scripts/36_statistical_analysis_examples.py --md
```

**Notebook**: `notebooks/36_statistical_analysis_examples.ipynb` (10 sections, 35 cells)

## CI / CD

| Component | Detail |
|-----------|--------|
| GitHub Actions | `.github/workflows/ci.yml` — Ruff (F-rules) + Mypy, workflow YAML parse, offline pytest suites (including multimodal, validation SQL contracts, **script 145** pytest + **145 CLI** parquet smoke), MotherDuck **Syntax / Lint** job (live token: RO-share smoke uses `manuscript_cohort_v1` on `thyroid_research_ro_v2`; RW checks use prod DB), **formalization job** after offline gates (`116_md_stage_loader.py --md --dry-run` → `112_v2_domain_promotion_gate.py --motherduck-check` → `119_md_formalization_validate.py --md`; **requires** `data-contract-gate-offline`), **live release audit dry-run** on `refs/tags/v*` or manual dispatch (`124_md_live_release_audit.py --md --dry-run`). Legacy jobs (v2-domain-validation shell checks, RO share publication, script `91` promotion gate) are labeled in the workflow. Secrets: `MD_SA_TOKEN` and/or `MOTHERDUCK_TOKEN` only for formalization/live-audit jobs (`LOCAL_DB_PATH` cleared there). Never log token values. |
| Streamlit Cloud | Auto-deploys from `main` branch on push |
| Runtime | Python 3.11 (pinned in `runtime.txt`) |

**Make — MotherDuck formalization (env tokens; fail-closed `--md`):** `make md-v2-gate-md-dryrun`, `make md-live-release-dryrun`, `make md-live-release-final`, `make md-molecular-promote-rehearsal`, `make md-molecular-promote` (see [`docs/release_runbook.md`](docs/release_runbook.md)). **Local / legacy:** `make md-v2-gate-local-dryrun` (alias `md-v2-gate-dryrun`), manifest targets `md-release-manifest-*`, `md-promote-dryrun-*` — see `Makefile` header comments.

## Streamlit Cloud deployment

The app auto-deploys from `main` at
**[thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app](https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/)**.

### Cure Modeling (PTCM + MCM)

The **Outcomes & Analytics** section includes Promotion Time Cure Model (PTCM)
and Mixture Cure Model (MCM) sub-sections with head-to-head comparison.

```bash
python scripts/39_promotion_time_cure_models.py
python scripts/38_mixture_cure_models.py
streamlit run dashboard.py
```
## Data dictionary

See [data_dictionary.md](data_dictionary.md) for full schema documentation
of all 13 tables and 8+ views.

## License

Private research data — do not redistribute without permission.


## Pipeline & Deployment

### Materialize to local DuckDB

```bash
.venv/bin/python scripts/75_dataset_maturation.py --all --md   # dataset maturation phases
.venv/bin/python scripts/29_validation_engine.py --md           # val_* tables
.venv/bin/python scripts/78_final_hardening.py --md             # hardening pass
.venv/bin/python scripts/30_readiness_check.py --md             # readiness audit
```

### Daily refresh

```bash
.venv/bin/python scripts/36_daily_refresh.py --md
```

## Release history

**Current:** v2026.03.13 (truth-sync) — see [RELEASE_NOTES.md](RELEASE_NOTES.md)
**Dashboard:** v3.3.0-2026.03.13
**Zenodo DOI:** [10.5281/zenodo.18945510](https://doi.org/10.5281/zenodo.18945510)

Requires: `pip install -r requirements.txt` (includes lifelines, scikit-survival, shap, etc.)
