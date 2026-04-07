# Release candidate audit — validation report

**Audit date:** 2026-04-07 (America/New_York timestamps in some MotherDuck UI exports; validation rerun UTC 2026-04-07T04:37Z).  
**Repo:** THYROID_2026  
**Target:** Live MotherDuck database **`Thyroid 2026`** only — `connect_md_or_file(..., md=True, fail_closed=True)`. **No local DuckDB fallback.**

## Methods (existing assets)

| Asset | Use |
|--------|-----|
| `scripts/126_release_candidate_motherduck_audit.py` | Evidence pack: MD info schema, parity matrix, grain check, queue/manifest snapshots, master view counts |
| `scripts/119_md_formalization_validate.py --release-mode` | Strict release gate re-run; report under `formalization_rerun/validation_report.md` |
| `docs/motherduck_database_contract_v1.md` | Contract rules (provenance, schemas) |
| `docs/motherduck_release_runbook_v2.md` | Operational intent |
| `studies/20260407_formalization_validation_release_mode/validation_report.md` | **Historical** — superseded; see §Contradictions |

## Executive summary

Automated release-mode formalization **PASS** (16/16) on live MotherDuck at rerun time. Staging parity, canonical row parity with local artifacts, quarantine counts, review queue (zero pending), load inventory, release schemas, and manifest presence all **PASS**.

Independent RC audit surfaced a **contract-level provenance gap**: a large fraction of `main.canonical_extracted_fact_long_v2` rows lack `extraction_run_id`, and **24,421 / 123,577** `main.master_fact_long_verified_v1` rows still lack a non-empty `extraction_run_id` after view-level backfill. Under `docs/motherduck_database_contract_v1.md` §3, `extraction_run_id` is marked **required** for entity tables in `main`.

**Final verdict (single):** **NOT RC READY**

---

## Evidence index (this folder)

| Deliverable | File |
|-------------|------|
| Row-count reconciliation (23 v2 domains × local / v2_stage / main) | `row_count_reconciliation.md` |
| Note-row-id grain | `grain_note_row_id.md` |
| Schema inventory (main stems from registry) | `schema_inventory.md` |
| Raw MD query snapshots (session, info schema, queues) | `snapshot_metadata.md` |
| DB type, snapshots, schema inventory | `database_attachment_and_snapshots.md` |
| Release manifest | `release_manifest_summary.md` |
| Review queue | `review_queue_summary.md` |
| Master views + traceability | `master_view_validation.md` |
| Blockers | `unresolved_blockers.md` |
| Formalization machine report | `formalization_rerun/validation_report.md` |
| Query history attribution | `query_history_notes.md` |

---

## Contradictions and resolution

1. **Prior formalization report vs live today**  
   - *Prior:* `studies/20260407_formalization_validation_release_mode/validation_report.md` → **BLOCKED** (5,622 MRQ pending).  
   - *Live:* `119 --release-mode` → **PASS** (0 pending; 16,866 reviewed).  
   - **Resolution:** Queue was cleared after the prior run; prior report is **historic only**. Current truth is the live rerun artifact in `formalization_rerun/`.

2. **Uniform 11,037 row count across all 23 domains**  
   - *Concern:* Loader duplication vs cohort mismatch.  
   - **Resolution:** Local parquets, `v2_stage`, and `main` all match per stem; `COUNT(*) = COUNT(DISTINCT note_row_id)` for every stem on both schemas — **one row per note** (11,037 notes). Not a mapping defect.

3. **“Latest” release tag vs newest `release_YYYYMMDD` schema**  
   - *Observation:* Scalar `release_tag` on master views = **`20260406`** (latest manifest **insert** time); snapshot schemas exist through **`release_20260409`**.  
   - **Resolution:** By design of `125` / `qa.release_manifest` ordering (`ORDER BY created_at DESC`), not by max tag integer. Documented in `release_manifest_summary.md`.

4. **Sparse `reviewer_status` on master facts**  
   - *Observation:* 97,787 / 123,577 facts have NULL `reviewer_status`.  
   - **Resolution:** Expected for facts whose `(research_id, source_domain)` never received an MRQ row; **integrity check:** **0** facts with NULL status where an MRQ row exists for that pair (`master_view_validation.md`).

5. **Formalization PASS vs extraction_run_id gap**  
   - *Observation:* Release validator PASS does not enforce run-id population.  
   - **Resolution:** Treated as **orthogonal**: validator green **does not** clear the contract/provenance issue (`unresolved_blockers.md`).

---

## FINAL_VERDICT

**NOT RC READY**

Rationale: `extraction_run_id` is required under the signed database contract for `main` entity/fact data, but **44.9%** of canonical long facts and **19.8%** of analyst master-view facts still lack a usable `extraction_run_id` on live MotherDuck. Close under governance (backfill/waiver + optional `119` check) before RC sign-off.
