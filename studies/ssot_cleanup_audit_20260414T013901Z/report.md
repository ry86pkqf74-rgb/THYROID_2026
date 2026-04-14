# SSOT cleanup audit — read-only (2026-04-14 UTC)

## Executive summary

Live **MotherDuck** catalog **`Thyroid 2026`** with schemas **`main`** (promoted facts, labs, presentation views) and **`qa`** (governance, `release_manifest`, `manual_review_queue`) is the **operational canonical surface** for formalized v2 extraction and release gates. **Checked-in** artifacts (`exports/release_manifests/LATEST_MANIFEST.json`, dated `studies/*/EVIDENCE_PACK.md`, duplicate copies of `CURRENT_MOTHERDUCK_REPO_STATE.md`, March manuscript bundles) are **not** interchangeable with live counts: this run shows **55,500** canonical fact rows on prod while [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) still records **123,577** from an earlier capture — treat the evidence pack as **historical** unless re-run after promotion. **`119 --release-mode`** on this run: **35 PASS / 4 WARN / 0 FAIL** (report: [`studies/20260413_motherduck_formalization/validation_report.md`](../20260413_motherduck_formalization/validation_report.md)). The analyst presentation views join MRQ at **(research_id, domain)** grain, **not** per `fact_id`; duplicate natural-key groups exist (542 groups) and are dominated by **labs** and multi-entity note rows — **not** proof of accidental double promotion without domain review.

**Grounded assumptions (no DB mutations this pass):** Token resolved via `motherduck_client.get_token()` / `motherduck.local.toml`; read-only `SELECT` and validation scripts only; branch `ssot-audit-20260414` from `f5c606f`.

---

## Is live MotherDuck clearly the canonical source of truth today?

**Yes — for promoted cloud analytics and release automation**, with explicit boundaries:

| Scope | Canonical today | Not canonical |
|-------|-----------------|---------------|
| Promoted facts + labs + presentation views | `main.*` on MotherDuck | Local `thyroid_master.duckdb` (DVC-tracked file artifact) |
| Release ordering / promotion history | `qa.release_manifest` (live) | `exports/release_manifests/LATEST_MANIFEST.json` (March 2026 point-in-time pointer) |
| Automation verdict | Fresh `119 --release-mode` output under `studies/` | Older `119` folders (e.g. 20-check era) |
| Manuscript “human-reviewed” sign-off | Policy + `qa.promotion_review_decisions` / MRQ substance | `auto_accepted_*` MRQ rows alone |

---

## Canonical objects table

| Object | Schema | Role | Freshness / SSOT source |
|--------|--------|------|-------------------------|
| `canonical_extracted_fact_long_v2` | main | Promoted long facts | Live MotherDuck; parity vs `processed/` parquets checked by `119` |
| `canonical_fact_quarantine_v2` | main | Quarantined rows | Live |
| `master_fact_long_verified_v1` | main | Analyst view: facts + MRQ + release tag | **View** — derived from canonical + `qa.manual_review_queue` + `qa.release_manifest` |
| `master_patient_rollup_verified_v1` | main | Per-patient rollup | **View** |
| `master_source_lineage_v1` | main | Lineage presentation | **View** |
| `longitudinal_lab_canonical_v1` | main | Lab timeline | Live |
| `longitudinal_lab_deduped_v` | main | Deduped lab consumption | **View** |
| `qa.release_manifest` | qa | Immutable release tags + git_sha + timestamps | **Append-only live** — authoritative vs repo JSON |
| `qa.manual_review_queue` | qa | Review queue rows | Live; **single `run_label` on prod today** (see audit.json) |
| `v2_stage.*` | v2_stage | Staging before promotion | **Not** final truth until promoted to `main` |

---

## Mismatch matrix

| Artifact A | Artifact B | Finding |
|--------------|-------------|---------|
| `git rev-parse HEAD` = `f5c606fb3fbf70e6ea2b9ea623bd011002fcc3df` | `exports/release_manifests/LATEST_MANIFEST.json` → `git_sha` **8c18892** | Checked-in manifest is **stale** vs current repo HEAD |
| `exports/release_manifests/LATEST_MANIFEST.json` | `qa.release_manifest` latest rows (e.g. tag `20260408r4`, sha `d9b9dc9`) | **Different promotion story** — live table is authoritative |
| [`EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) `canonical_extracted_fact_long_v2` = **123,577** | Live SQL `COUNT(*)` = **55,500** | **Large drift** — evidence pack is **historical snapshot**, not current prod row count |
| [`EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) `master_patient_rollup_verified_v1` = **5,574** | Live **5,141** | Drift — same explanation |
| `RELEASE_NOTES.md` (2026-04-07) MRQ **11,244** / two run labels | Live MRQ **5,622** rows, **one** `run_label` | Older narrative **superseded** by current queue hydration |
| `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` **Commit SHA** | After `144 --md`, matches **f5c606f** | **Fresh** on this run |
| DVC `thyroid_master.duckdb` | Cloud `main` | **Separate** — do not cite local file as prod |

---

## Stale-doc list (can be mistaken for current)

| Path | Why stale / risk | Prefer instead |
|------|------------------|----------------|
| [`exports/release_manifests/LATEST_MANIFEST.json`](../../exports/release_manifests/LATEST_MANIFEST.json) | March 2026 SHA; not updated on every promotion | `SELECT * FROM qa.release_manifest ORDER BY created_at DESC` on live |
| [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) | Row counts from 2026-04-07 capture; differs materially from live | Live counts + new evidence pack after promotion |
| [`studies/20260409_final_master_release/EVIDENCE_PACK.md`](../20260409_final_master_release/EVIDENCE_PACK.md) | Superseded by 20260411 for that era | 20260411 pack **or** live SQL |
| [`docs/REPO_STATUS.md`](../../docs/REPO_STATUS.md) | Mixes April narrative with **March 13** table labeled historical | README “Source of truth” + live `119` |
| [`RELEASE_NOTES.md`](../../RELEASE_NOTES.md) §2026-04-07 | MRQ 11,244 / dual run_label story | Live `qa.manual_review_queue` counts (see `audit.json`) |
| Nested [`studies/*/CURRENT_MOTHERDUCK_REPO_STATE.md`](../20260407_repo_delta_gap_audit/CURRENT_MOTHERDUCK_REPO_STATE.md) (4 copies) | Point-in-time exports; easy to open wrong file | Root [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md) after `144 --md` |
| [`README.md`](../../README.md) historical status table | Dated **2026-04-08** row-level MRQ counts | Current `119` report + governance studies |

---

## Stale-data-risk list

1. **Citing `LATEST_MANIFEST.json` as “current release SHA”** — it tracks an old manuscript-era id.
2. **Using EVIDENCE_PACK row counts in manuscript text** without reconciling to live `canonical_extracted_fact_long_v2` — demonstrated **123,577 vs 55,500** gap.
3. **Equating `master_*_verified_v1` reviewer columns with per-fact clinician review** — MRQ join is **per (research_id, domain)** in [`scripts/125_master_verified_views.py`](../../scripts/125_master_verified_views.py).
4. **Confusing DVC local DuckDB with MotherDuck prod** — different attachment and promotion state.
5. **Interpreting duplicate-key probe as “bad joins”** — 542 groups; top duplicates are **labs** / same `source_object_id` with multiple fact rows (multi-analyte or extraction grain), not necessarily promotion bugs.

---

## Does `CURRENT_MOTHERDUCK_REPO_STATE.md` cover the whole final-master surface?

**No.** Generator [`scripts/144_md_repo_current_state_summary.py`](../../scripts/144_md_repo_current_state_summary.py) explicitly frames the file as **reconciliation** of checked-in artifacts with **catalog probe + specimen/FHIR row counts + `qa.release_manifest` sample + checked-in manifest** — not a full census of all `main` final-master tables. It is **not** a substitute for `119 --release-mode` or for querying `canonical_extracted_fact_long_v2` / lab tables directly.

---

## Does the verified view layer prove per-fact review?

**No.** `master_fact_long_verified_v1` builds `review_lookup` from `qa.manual_review_queue` with `ROW_NUMBER() ... PARTITION BY research_id, domain`. Reviewer status **propagates at domain grain** to every fact row in that domain for the patient — **not** per `fact_id`. Technical traceability (extraction run, release tag) is row-level; **governance verification is coarser**.

---

## Which artifacts should be marked historical rather than current?

- `exports/FINAL_PUBLICATION_BUNDLE_20260313/` and Zenodo DOI snapshot (per README).
- `exports/release_manifests/release_8c18892_20260315_170027.json` and `LATEST_MANIFEST.json` pointer (unless regenerated).
- `studies/20260409_final_master_release/EVIDENCE_PACK.md`.
- `studies/20260411_final_master_release/EVIDENCE_PACK.md` — **use as dated operator evidence**, not live row-count SSOT.
- Older `119` validation trees under `studies/20260407_*` (20-check era / signoff snapshots).
- Duplicate nested `CURRENT_MOTHERDUCK_REPO_STATE.md` copies under dated study folders.
- `RELEASE_NOTES` MRQ 11,244 narrative where superseded by single-run-label queue.

---

## Recommended remediation plan

1. **Documentation:** Add a one-line **“as-of”** banner at top of `exports/release_manifests/LATEST_MANIFEST.json` via companion `README.md` in that directory (JSON cannot hold comments) stating **live SSOT = `qa.release_manifest`**.
2. **Regenerate or annotate** `studies/20260411_final_master_release/EVIDENCE_PACK.md` with a bold **superseded-for-row-counts** notice pointing to live SQL or a new pack after next promotion.
3. **CI / pre-commit optional:** Fail or warn if `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` commit SHA ≠ `git rev-parse HEAD` **when** that file is touched (or weekly staleness check).
4. **Index:** Add `studies/SSOT_INDEX.md` linking: live contract, `119` latest report, `144` output, governance reports — **single navigation hub** (small scope).
5. **125 docstring / docs:** Already states domain-level join; add cross-link in `docs/motherduck_database_contract_v1.md` “Presentation views” subsection.

---

## Exact files / scripts to change (remediation)

| Target | Change |
|--------|--------|
| [`exports/release_manifests/README.md`](../../exports/release_manifests/README.md) | Create or extend: state `LATEST_MANIFEST.json` is pointer-only; live = `qa.release_manifest` |
| [`README.md`](../../README.md) | Optional: shorten duplicate “historical table” or add link to this audit |
| [`studies/20260411_final_master_release/EVIDENCE_PACK.md`](../20260411_final_master_release/EVIDENCE_PACK.md) | Banner: row counts not verified against current prod |
| [`docs/motherduck_database_contract_v1.md`](../../docs/motherduck_database_contract_v1.md) | Subsection: verified views = domain-level MRQ |
| [`docs/REPO_STATUS.md`](../../docs/REPO_STATUS.md) | Optional: single link to this audit at top |
| [`scripts/125_master_verified_views.py`](../../scripts/125_master_verified_views.py) | Docstring already accurate; optional comment on PARTITION BY |

---

## Commands run (evidence)

```text
git fetch && git pull --ff-only
git checkout -b ssot-audit-20260414
.venv/bin/python scripts/144_md_repo_current_state_summary.py --md --also-write studies/CURRENT_MOTHERDUCK_REPO_STATE.md
.venv/bin/python scripts/119_md_formalization_validate.py --md --release-mode   # exit 0
.venv/bin/python scripts/125_master_verified_views.py --md --dry-run              # exit 0
```

Read-only SQL: see [`audit.json`](audit.json) (`sql.*`).

---

## Duplicated / stale MRQ snapshots and `run_label`

Live prod (this run): **one** `run_label` — `20260407_tier_policy_review_gate` (**5,622** rows). Older docs (`RELEASE_NOTES`, April 2026 signoff text) referencing **11,244** rows or **two** run labels describe a **prior** hydration — **stale** for current truth. No duplicate MRQ “snapshots” in-repo beyond dated triage exports under `exports/` (gitignored) and mirrored `studies/manuscript_*` reports.

---

## Audit artifacts

| File | Purpose |
|------|---------|
| [`audit.json`](audit.json) | Machine-readable: git SHA, script outputs, full SQL results, `duplicate_natural_key_groups` |

---

*End of report.*
