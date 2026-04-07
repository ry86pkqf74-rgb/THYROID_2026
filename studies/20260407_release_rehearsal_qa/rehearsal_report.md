# MotherDuck QA release rehearsal — 2026-04-07

## Environment

| Item | Value |
|------|--------|
| **Plane** | `qa` (`MOTHERDUCK_ENV=qa`) |
| **Catalog** | `Thyroid 2026 Molecular QA 20260407` (from `config/motherduck_environments.yml`) |
| **Attribution** | `MOTHERDUCK_CUSTOM_USER_AGENT=THYROID_2026_release_rehearsal` |
| **Session hint** | `MOTHERDUCK_SESSION_HINT=qa_release_rehearsal_20260407` |
| **Token preflight** | `MD_SA_TOKEN` / `MOTHERDUCK_TOKEN` / `motherduck_token` (env): MISSING; `MOTHERDUCK_TOKEN` in `.streamlit/secrets.toml`: SET (length reported at runtime only in operator console — not stored here). |

**Prod:** `Thyroid 2026` was not used as the writer catalog for this rehearsal. Attached databases visible in preflight include prod and other sandboxes (read-only context); all orchestrated steps targeted the QA database resolved for `--md-env qa`.

## What ran (repo helpers only)

1. **`scripts/124_md_live_release_audit.py`** — `--md --dry-run --md-env qa --output-dir studies/20260407_release_rehearsal_qa/md_live_audit_dryrun --tag 20260407`  
   - Chains 116 (dry-run), 112, 103 (dry-run), 114, 117 (dry-run), 132 (`--validate-only` in dry-run mode), specimen/FHIR gate (non-enforced), 125 (dry-run), 115 (dry-run), 118 (dry-run), **119 without `--release-mode`** (structural formalization only when `--final-release` is unset — see `124` Step 7).

2. **Post-rehearsal spot checks** — read-only SQL on QA for null keys and MRQ status (values in `rehearsal_metrics.csv`).

3. **Code fix** — `scripts/132_molecular_fact_lineage_views.py`: `--validate-only` now **SKIP**s cleanly when lineage views are absent on the connected catalog, instead of emitting misleading `ERROR` text pointing at other attached databases.

## Schemas / objects (read + dry-run scope)

Observed on QA (`information_schema` / logs): **`main`**, **`v2_stage`**, **`qa`**, multiple **`release_YYYYMMDDr`** schemas, **`mm_contract_dev`**, and other non-Thyroid sandboxes.  

**Concrete row-touch surfaces (dry-run exports from 118):** `main.canonical_extracted_fact_long_v2`, `main.canonical_fact_quarantine_v2`, `main.note_extraction_runs`, core episode tables (`tumor_episode_master_v2`, `molecular_test_episode_v2`, `rai_treatment_episode_v2`, `operative_episode_detail_v2`), promoted `main.note_entities_llm_*` domains, and **`qa.promotion_scorecard`**, **`qa.domain_validation`**, **`qa.manual_review_queue`**.

**Not present on QA `main`:** unified molecular lineage views (`molecular_fact_long_v`, etc.) — `132 --validate-only` reported SKIP (deploy with `--execute --md` when QA should mirror prod molecular analytics).

**Specimen / analytic FHIR:** Check 13 **not applicable** on QA today — `main.synoptic_tumor_long_v1` absent; structural validator skipped specimen/FHIR section (see `validation_report.md`).

## Evidence captures

| Artifact | Path |
|----------|------|
| Full 124 dry-run logs + preflight JSON | `studies/20260407_release_rehearsal_qa/md_live_audit_dryrun/` |
| Structural validation report | `md_live_audit_dryrun/validation_report.md` |
| Metrics table | `rehearsal_metrics.csv` |

### Row counts / queues (high level)

- Canonical long facts: **123,577** rows; quarantine: **199**.  
- **MRQ:** **5,622** total; **0** rows with `verification_status IS NULL` on QA at rehearsal time.  
- **Presentation rollup:** **5,574** distinct `research_id` in `main.master_patient_rollup_verified_v1` (validator-aligned).  
- **`canonical_extracted_fact_long_v2`:** **0** null `research_id`.

### Duplicate / key hygiene

- **Heavy duplicate-key GROUP BY** over full canonical long was **not** re-run here (expensive on remote); **112** reported **G4 Duplicate rate — PASS** in this rehearsal.  
- Spot check: null primary patient key on canonical long = **0**.

### Snapshots / query history

- **Snapshots:** `md_information_schema.database_snapshots` returned **186** rows at preflight.  
- **Query history:** `md_information_schema.query_history` is populated (**~120k** rows visible via `COUNT(*)`); session-specific filtering is left to MotherDuck console / support workflows — this rehearsal did not paste queries (length/PII).

## Verdict

| Layer | Status | Notes |
|-------|--------|--------|
| **Structural rehearsal (119 w/o release-mode) + 124 dry-run chain** | **PASS** | Exit 0; 20/20 structural checks; all 124 steps succeeded. |
| **Release-mode / publication sign-off** | **HOLD** | **`119 --release-mode` was not run** (by design for this rehearsal). Molecular contract + strict governance gates were **not** exercised. |
| **Specimen / FHIR release gate** | **N/A on QA** | Anchor table absent; no 138/143 materialization required for Check 13 today. |

## Before prod or public release-mode validation

1. On **QA**, run **`119_md_formalization_validate.py --md --md-env qa --release-mode`** after any missing **presentation / molecular / specimen** prerequisites are deployed (see `docs/publication_governance_gate.md`, `docs/specimen_fhir_release_integration.md`).  
2. If `main.synoptic_tumor_long_v1` is created, run **`138` / `143`** as needed so Check 13 can execute.  
3. Deploy **`132 --execute --md --md-env qa`** (or refresh clone from prod) if QA must validate **`main.molecular_fact_long_v`** and duplicate-candidate QA in release mode.  
4. Keep using **`MOTHERDUCK_CUSTOM_USER_AGENT`** and **`MOTHERDUCK_SESSION_HINT`** on production-affecting runs for MotherDuck query attribution.
