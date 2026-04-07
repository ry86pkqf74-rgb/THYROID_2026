# Publication signoff — operator memo

**Date (authoritative session):** 2026-04-07  
**Workspace:** THYROID_2026  
**MotherDuck catalog:** `Thyroid 2026` (prod) via `scripts/119_md_formalization_validate.py --md-env prod`

## Verdict: **HOLD**

**Live catalog formalization (release-mode)** is **PASS WITH WARNINGS** (0 FAIL). Full publication **signoff is not closed** because repository MRQ CSV prerequisites checked here are **unsafe for a publication `126` hydrate**, the **final institutional chemistry lab file was validated-only (no append)**, and **specimen-adjacent review backlog** triggers a release-mode WARN that needs an explicit policy call.

---

## 1) Human-reviewed MRQ / signoff CSV (ingest path)

| Evidence | Result |
|----------|--------|
| **Prod** `qa.manual_review_queue` via **119 Check 5 / 5b** | **PASS** — 11,244 rows reviewed; **0** synthetic-blocked `verification_status`; **0** pending |
| **`studies/v2_domain_promotion_gate_post_signoff_20260407/manual_review_queue.csv`** | **NOT READY** — **5,622** rows; **all** `verification_status` blank |
| **`studies/20260409_final_master_release/mrq_hydrate_gate/manual_review_queue.csv`** | **NOT READY** — **5,620** rows with publication-blocked synthetic placeholder statuses (`docs/publication_governance_gate.md`) |

**Operator action:** Before any **`126 --release-mode --hydrate-mrq-from …`**, use a gate directory whose `manual_review_queue.csv` is **fully human-reviewed** (no blanks, no synthetic placeholders). Prefer **exporting current prod MRQ** after review, or a curated gate folder known to match prod truth. Using the paths above for hydrate would **fail `assert_mrq_csv_fully_reviewed`** or **wipe good prod state** (126 clears `qa.manual_review_queue` before hydrate).

**PHI:** No patient identifiers or note text printed; only row counts and status tallies.

---

## 2) Final institutional non-Tg lab CSV (append schema)

| Item | Result |
|------|--------|
| **File** | `exports/incoming/final_institutional_chemistry_20260407.csv` |
| **Rows** | **989** |
| **Contract** | Matches `scripts/127_analyst_institutional_lab_append.py` (required: `research_id`, `lab_date`, `value_raw`, `source_lineage_key`; name via `lab_name_raw` / `lab_name_standardized`) |
| **Execution** | **`127` dry-run only** — **no MotherDuck write** in this session |

**Operator action:** To load: run **`127_analyst_institutional_lab_append.py --md`** with the same `--ingestion-wave` you intend for **`126`** (e.g. `final_institutional_chemistry_20260407`), ideally **after** `117` in a full **`126`** execution per runbook. Do **not** use synthetic-fill shortcuts in publication mode.

---

## 3) Synthetic-only MRQ in publication path

| Surface | Result |
|---------|--------|
| **Prod DB** (119 Check 5b) | **PASS** — no synthetic placeholders in `qa.manual_review_queue` |
| **Repo CSV** (20260409 hydrate gate) | **FAIL** if used as publication hydrate — majority synthetic-blocked statuses |

---

## 4) Specimen / FHIR QA diagnostics (“green enough”)

| Check | Result |
|-------|--------|
| **119** — specimen/FHIR tables, `qa.val_specimen_contract_v1`, `qa.val_specimen_genomic_binding_v1`, combined diagnostics | **PASS** (no FAIL rows; diagnostics **clean**) |
| **119 WARN** — specimen-adjacent review burden | **WARN** — `genomic_link_review` **open/pending = 10,705**; `specimen_merge_review` **open/pending = 1** |

**Operator action:** Decide whether the **genomic_link_review** backlog is **acceptable for release** under your governance policy. Core specimen contract validators are green; the open review queue is a **volume / policy** question, not a structural FAIL in this run.

---

## 5) Execution performed (PHI-safe)

1. **127** institutional lab append — **MotherDuck dry-run** (contract + counts only).
2. **119** — `--md-env prod --release-mode` → `studies/20260407_publication_signoff_final/validation_run/validation_report.md` (**VERDICT: PASS WITH WARNINGS**).
3. **126** — `--dry-run` with lab CSV + ingestion wave; **no `--hydrate-mrq-from`** (would normally be required for a real final release).
4. **144** — `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` **refreshed**.

No production promotion, snapshot, or MRQ replace was executed.

---

## Remaining blockers (exact)

1. **Repository MRQ CSV** — No checked-in gate folder in this audit was suitable for a **publication** `126` hydrate (blank or synthetic placeholders as above). **Blocker for any scripted re-hydrate from repo.**
2. **Institutional lab** — **989** rows ready but **not appended**; **blocker** only if publication requires that wave on prod **before** signoff.
3. **Genomic link review backlog** — **10,705** open/pending; **policy WARN** from **119** — confirm acceptability for your release memo.

**Not a blocker for “read-only catalog health”:** live prod **119** release-mode **passed** with **warnings** only.

---

## Deliverables (this folder)

- `operator_memo.md` (this file)  
- `validation_manifest.json`  
- `metrics.csv`  
- `validation_run/validation_report.md` (from **119**)  
- `119_release_mode_console.log`
