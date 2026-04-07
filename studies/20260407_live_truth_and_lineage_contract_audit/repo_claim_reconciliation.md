# Repo claim reconciliation (live session 2026-04-07)

## README.md (top “source of truth”)

| Claim | Live reconciliation |
|--------|---------------------|
| Live signoff folder is authoritative vs older validation trees | **PASS** — this audit confirms **rerun** `119 --release-mode` in `studies/20260407_live_truth_and_lineage_contract_audit/119_release_validation/`; do not rely on stale copies. |
| “119 latest run failed specimen/FHIR diagnostics” | **STALE vs this session** — current live run: **PASS WITH WARNINGS**, `broken_fhir_refs=0` (WARN on genomic review burden remains). Older `studies/20260407_publication_signoff_live/validation_report.md` shows **FAIL** with `broken_fhir_refs=10139`. **Update narrative** to cite **latest** log or keep both with timestamps. |
| Manifest tag **20260409** | **PASS** — `qa.release_manifest` latest `release_tag=20260409` matches README table (`audit_run_meta.json`). |
| Synthetic MRQ dominates | **PASS** — live `SYNTHETIC_…` = **5620** / 5622 rows (2 `confirmed_correct`) — aligns with `mrq_reconciliation_memo.md`. |
| Non-Tg institutional lab wave pending | **PASS** — not re-ingested in this session; `lab_coverage_memo.md` still directionally correct (Tg-family waves only). |

## `studies/20260407_publication_signoff_live/`

| Memo / artifact | Live reconciliation |
|-----------------|----------------------|
| `validation_report.md` | **Historical** — specimen diagnostic **FAIL** + `broken_fhir_refs=10139` **not** reproduced in this audit’s live `119` run (**WARN only**, `broken_fhir_refs=0`). Preserve file but **supersede** with pointer to `119_release_validation/` in this folder or refresh memo. |
| `mrq_reconciliation_memo.md` | **PASS** on synthetic MRQ thesis; counts drift by **2** rows vs live (**5620** vs memo **5620** — consistent within rounding none; memo said 5622 total — live total **5622** with 5620 synthetic + 2 confirmed). |
| `lab_coverage_memo.md` | **PASS** directionally — no `final_institutional*` wave verified. |

## `docs/motherduck_database_contract_v1.md`

| Claim | Live reconciliation |
|--------|----------------------|
| `qa.release_manifest` “immutable release snapshot metadata” | **PASS** semantically; **PARTIAL** column naming — contract prose elsewhere may imply `materialized_at`; **live** uses **`created_at`**. SQL in new audits should use `created_at`. |
| DuckLake caveat | **PASS** — `md_information_schema.databases.type = DUCKLAKE` for `Thyroid 2026`. |

## `docs/final_master_database_contract.md`

| Claim | Live reconciliation |
|--------|----------------------|
| Traceability for `master_fact_long_verified_v1` | **PASS** pattern — joinability to `research_id`, `source_object_id` / `note_row_id`, `extraction_run_id` remains the **stated** contract; sparse `entity_date` is **population** visibility, not missing keys. |

## Enumerated verdict

**Documentation state:** Mixed — **core README posture is sound**, but **119 FAIL vs PASS** language must be **timestamped** to avoid contradictions.

**Live technical state:** **Promotion mechanics + manifest + canonical non-null core IDs PASS**; **WARN** on specimen-adjacent review burden; **governance** still **synthetic-dominant**.
