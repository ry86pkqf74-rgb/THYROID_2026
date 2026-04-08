# Live state refresh — MotherDuck operational truth

**UTC session:** `20260408T074310Z`  
**Study folder:** `studies/live_state_refresh_20260408_074310/`  
**Git HEAD (capture):** `3f54a52e14a2c8d9d957285a9897c348371e54d0` (see `git_head.txt`)  
**Attribution:** `MOTHERDUCK_CUSTOM_USER_AGENT=thyroid_live_state_refresh/20260408T074310Z`, `MOTHERDUCK_SESSION_HINT=thyroid_live_state_refresh_20260408T074310Z`, `MD_READ_SCALING_SESSION_HINT=thyroid_live_state_refresh_ro_20260408T074310Z`

## Execution summary (read-only on catalog)

| Step | Result |
|------|--------|
| Token probe | RW: `env:MOTHERDUCK_TOKEN`; RS: `secrets.toml:MD_READ_SCALING_TOKEN` (both resolved; length logged in `token_source_probe.txt` only) |
| `smoke_test_md_connection.py --md` | PASS → prod `Thyroid 2026` |
| `144_md_repo_current_state_summary.py --md` | Wrote `CURRENT_MOTHERDUCK_REPO_STATE_live_snapshot.md` |
| `119_md_formalization_validate.py --md --release-mode` | **36 PASS / 3 WARN / 0 FAIL** → `formalization_release_mode/validation_report.md` |
| `120_review_queue_triage.py --md` | Bundle under `triage_exports/review_queue_triage_20260408_074435/` |
| `collect_evidence.py` | **read_scaling** path succeeded; SELECT-only CSVs in this folder |

**Read-scaling note:** An earlier attempt hit `Catalog write-write conflict` on DuckLake `USE`; retry succeeded. RS tokens can race on attach — fall back to RW for collectors if needed (`collect_evidence.py` implements RW fallback).

## Live metrics (prod `Thyroid 2026`)

| Item | Evidence file | Value |
|------|---------------|--------|
| MRQ total | `mrq_status_distribution.csv` | 5,622 rows |
| NULL `verification_status` | `motherduck_metrics.csv` | **0** |
| MRQ distribution | `mrq_status_distribution.csv` | `auto_accepted_standard` 3081; `auto_accepted_critical_sample_ok` 1646; `auto_accepted_informational` 893; `confirmed_correct` **2** |
| `promotion_review_decisions` | `motherduck_metrics.csv` | **4** rows (Check 5b: all have `decision_batch_id`) |
| Specimen/FHIR QA | `specimen_fhir_diag_counts.csv` | `broken_fhir_refs=0`, `high_tier_null_spec=0`, no dup/orphan counts |
| Lab waves | `lab_wave_distribution.csv` | `wave_tgab_structured_ehr` 39,005; `wave_tg_structured_ehr` 37,966; **`final_institutional_20260407` 989** |
| Analyte groups | `lab_analyte_distribution.csv` | `thyroid_tumor_markers` 76,971; `thyroid_function` 515; `metabolic_panel_nlp_canonical` 284; `metabolic_panel_postop_structured` 190 |
| Release manifest (chronological) | `release_manifest_latest.csv` | Newest `created_at`: **`20260408r3`** (2026-04-08 05:20:40) |
| Cross-env row counts | `env_row_counts_probe.csv` | dev/prod `longitudinal_lab_canonical_v1` **77,960**; qa **76,971**; MRQ **5,622** all envs |
| `MD_INFORMATION_SCHEMA` | `md_information_schema_*.csv` | Exported (see files) |
| `QUERY_HISTORY` (UA filter) | `query_history_filtered.csv` | **Not available** to this identity: `MDExternalException: Query history is only available to organization admins` |

## Code fix bundled with this refresh

- **`scripts/119_md_formalization_validate.py` — Check 9 “latest” release manifest** now orders by **`created_at DESC`** first so suffix tags like `20260408r3` surface as the newest row. Previously `TRY_CAST(release_tag AS BIGINT)` caused numeric tags (e.g. `20260411`) to win over newer resnapshot tags that do not cast to integer.

## Reconciliation vs checked-in artifacts

| Artifact | Stale? | Notes |
|----------|--------|--------|
| `README.md` | **Mostly current** | Describes `auto_accepted_*` MRQ posture, `final_institutional_20260407`, PASS WITH WARN; aligns with this run. Minor: line ~29 “blocked by synthetic MRQ” is shorthand — live **Check 5b** reports **no** synthetic-placeholder `verification_status`; queue is **tier auto-accept**, not clinician review. |
| `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` | **Partially stale** until regenerated on your HEAD | Machine-generated **2026-04-08** snapshot in repo may predate your latest commit; **live snapshot** in this folder (`CURRENT_MOTHERDUCK_REPO_STATE_live_snapshot.md`) matches this run. |
| `studies/20260407_publication_signoff_live/final_verdict_memo.md` | **Partially stale** | Banner and supersession links remain **correct** (specimen FAIL superseded; lab wave closed). **Executive snapshot** still describes historical `SYNTHETIC_*` MRQ dominance — **superseded by live**: MRQ is **`auto_accepted_*`** + 2 `confirmed_correct`; `promotion_review_decisions` is **4** rows (not 2). |
| `studies/20260407_publication_signoff_live/live_audit_memo.md` | **Partially stale** | Historical `broken_fhir_refs=10139` narrative remains for audit trail; **live** diagnostics **clean** (this run + `119`). |
| `studies/20260407_publication_signoff_live/mrq_reconciliation_memo.md` | **Stale** on §2 table | Live verification_status distribution **does not** show `SYNTHETIC_AUTOMATION_ONLY_*` (see `mrq_status_distribution.csv`). Memo’s **2026-04-08** header update points to newer studies; keep memo for process history but **do not** cite the §2 counts as current. |
| `studies/20260407_publication_signoff_live/lab_coverage_memo.md` | **Stale** | States no `final_institutional*` wave; **live** has **`final_institutional_20260407`** (989 rows) and expanded analyte groups beyond thyroid_tumor_markers-only. |

## Strict verdict — release readiness (2026-04-08 UTC)

| Dimension | Verdict |
|-----------|---------|
| **Structural / formalization automation (`119 --release-mode`)** | **PASS WITH WARNINGS** (0 FAIL) |
| **Manuscript / org governance** | **HOLD** — MRQ is almost entirely **`auto_accepted_*`** (not human adjudication); genomic/specimen **review burden** WARN (10,705 open/pending on genomic link review per `119`); `QUERY_HISTORY` attribution unavailable without org-admin |
| **Single label** | **HOLD** — operator gate green; governance and observability gaps block a blanket “signed release” without policy call |

### Current blocker list

1. **Governance:** Manuscript-class sign-off still needs explicit policy on whether **`auto_accepted_*`** MRQ tiers satisfy publication (README lens B: **no** by default).  
2. **Review burden:** Specimen-adjacent **genomic_link_review** open/pending **10,705** (`119` WARN).  
3. **Molecular data quality WARNs:** `panel_version` sparse; assay dictionary mismatch (`119` WARN).  
4. **Telemetry:** **`MD_INFORMATION_SCHEMA.QUERY_HISTORY`** filtered by custom UA **blocked** for non–org-admin tokens (`query_history_filtered.csv`).  
5. **Read-scaling:** Intermittent DuckLake attach conflict documented — use RW fallback or retry for automation.

### Exact next prompt to run

```text
Regenerate org-visible telemetry and the release gate bundle: ensure a MotherDuck **organization admin** token/session (or UI export) for QUERY_HISTORY, then run:

  source studies/live_state_refresh_20260408_074310/session_environment_exports.sh
  .venv/bin/python scripts/148_thyroid2026_release_gate.py --md --md-env prod

If manuscript policy requires human-reviewed MRQ, run the hydrate path in docs/review_queue_triage_export.md with a CSV-backed verification_status set **before** treating governance as closed.
```
