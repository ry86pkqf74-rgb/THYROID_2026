# Manuscript blocker rebaseline — evidence package

**Generated (UTC):** 2026-04-08T07:35–07:36Z  
**Catalog:** `Thyroid 2026` (MotherDuck prod attach, RW token from local `motherduck.local.toml` / env per `motherduck_client.get_token()`).

This package separates **structural / automation health** from **manuscript governance sign-off** and documents **non-Tg lab** posture on the live catalog.

---

## 1) Manual review queue truth (`qa.manual_review_queue`)

**Export:** [`review_queue_triage_20260408_073550/`](review_queue_triage_20260408_073550/) (script `120_review_queue_triage.py --md`).

| Metric | Value |
|--------|------:|
| Total rows | 5,622 |
| Pending (`verification_status` NULL) | 0 |
| Structurally “complete” (all non-NULL status) | 5,622 |

### Manuscript-quality tiers (`counts_manuscript_quality_tiers.csv`)

Mutually exclusive buckets (script 120; aligned with `docs/publication_governance_gate.md` for tier **B**).

| Tier | n_rows | Interpretation |
|------|-------:|----------------|
| `C_automation_tier_policy_only` | **5,620** | `verification_status` matches `auto_accepted_*` — **tier-policy automation**, not human identity on the row |
| `E_reviewed_status_without_reviewer_timestamp` | **2** | `confirmed_correct` without both non-blank `reviewer` and `reviewed_at` |
| `D_human_review_identity_present` | **0** | Conservative “human-reviewed signature” bucket |
| `B_synthetic_placeholder_*` | **0** | Would block `119 --release-mode` CHECK 5b |
| `A_pending_*` | **0** | Would block CHECK 5 in strict mode |

### Verification status histogram (`counts_by_verification_status.csv`)

| verification_status | n_rows |
|---------------------|-------:|
| `auto_accepted_standard` | 3,081 |
| `auto_accepted_critical_sample_ok` | 1,646 |
| `auto_accepted_informational` | 893 |
| `confirmed_correct` | 2 |

The two `confirmed_correct` rows lack reviewer timestamp identity → tier **E**, not **D**.

### Manuscript-signoff takeaway

- **Release-mode automation:** `119 --release-mode` **PASS WITH WARNINGS** — CHECK 5 and **5b (synthetic placeholder)** both **PASS** (see §4).
- **Governance / publication language:** The queue is **not** “human-adjudicated” in bulk — **99.96%** of rows are **automation tier policy** (`auto_accepted_*`). Treat README / memos as **structurally populated + automation-governed**, not **clinician-reviewed MRQ**, unless operators hydrate from a **human-reviewed CSV** per [`docs/review_queue_triage_export.md`](../../docs/review_queue_triage_export.md).

---

## 2) Human adjudication hydration path

**No production hydrate was executed in this rebaseline** (would overwrite `qa.manual_review_queue` per batch policy; requires an authorized reviewed gate directory).

**Documented operator path** (append-only provenance on `promotion_review_decisions`; MRQ replace semantics for full `126`):

1. Prepare **`manual_review_queue.csv`** (complete `verification_status`; no synthetic placeholders for publication).
2. **`scripts/114_qa_schema_setup.py --md --hydrate-from <gate_dir>`** — loads MRQ via `hydrate_manual_review_queue()`; **DELETE** by `run_label` then **INSERT** for that gate only.
3. **`scripts/126_final_master_release.py --md --hydrate-mrq-from <dir>`** — full-release orchestration; enforces `assert_mrq_csv_fully_reviewed` when `--release-mode`.
4. Re-run **`scripts/119_md_formalization_validate.py --md --release-mode`**.

Details: [`docs/review_queue_triage_export.md`](../../docs/review_queue_triage_export.md) § *Human-reviewed MRQ hydration*.

---

## 3) Non-Tg lab availability (live `main.longitudinal_lab_canonical_v1`)

Query time: 2026-04-08 UTC (same session as this report).

### Distinct `lab_name_standardized`

| Analyte | Rows | Patients | `ingestion_wave` | `data_completeness_tier` |
|---------|-----:|---------:|------------------|--------------------------|
| `thyroglobulin` | 37,966 | 3,057 | `wave_tg_structured_ehr` | `current_structured` |
| `anti_thyroglobulin` | 39,005 | 3,170 | `wave_tgab_structured_ehr` | `current_structured` |
| `tsh` | 515 | 413 | `final_institutional_20260407` | `current_structured` |
| `pth` | 200 | 184 | `final_institutional_20260407` | `current_structured` |
| `calcium` | 188 | 166 | `final_institutional_20260407` | `current_structured` |
| `vitamin_d` | 86 | 82 | `final_institutional_20260407` | `current_structured` |

### Operational gap (honest blocker framing)

- **Not a “missing institutional wave” blocker** on prod: `final_institutional_20260407` is present (989 rows total in that wave on this snapshot).
- **Still a manuscript / analysis blocker** for any claim requiring **population-scale non-Tg longitudinal panels**: TSH/PTH/Ca/vitD cover **hundreds of patients**, vs **~3k** for structured Tg/TgAb — **source-limited enrichment**, not a schema gap.
- **Absent analytes** on this catalog (no rows): e.g. **free T4**, **free T3**, **albumin**, **ionized calcium** as standardized names — consistent with [`docs/lab_layer_scaffold_plan_20260313.md`](../../docs/lab_layer_scaffold_plan_20260313.md) placeholders; **future institutional extract** must extend the contract if those endpoints are required.

Ingestion contract / runner reference: `scripts/127_*` institutional ingest (see `studies/20260407_institutional_lab_wave_closeout/` and `README` institutional lab bullet).

---

## 4) Release-mode validation effect (`119 --md --release-mode`)

**Output directory:** [`119_release_validation/`](119_release_validation/)

**Verdict:** **PASS WITH WARNINGS** — 36 PASS / 3 WARN / 0 FAIL.

**Relevant gates:**

| Check | Result | Note |
|-------|--------|------|
| Review queue | PASS | 0 pending |
| Review queue (synthetic placeholder) | PASS | 0 synthetic-blocked statuses |
| Promotion decision provenance | PASS | 4 rows; all `decision_batch_id` present |
| Specimen + analytic FHIR | PASS | diagnostics clean |
| Specimen-adjacent review burden | **WARN** | genomic linkage review open/pending volume |
| Molecular assay/panel_version | WARN | dictionary / panel metadata |
| Molecular assay_name dictionary | WARN | expected for some panels |

**Conclusion:** Specimen/FHIR **structure** and formalization **automation** are healthy; **WARN** items are **policy / metadata / review burden**, not NULL-queue or synthetic-placeholder failures.

---

## 5) Next concrete actions

1. **Manuscript governance:** If publication policy requires **human** MRQ, export worklists from script **120**, review out-of-band, then **hydrate** via **114/126** with a **non-synthetic** CSV and re-run **119**.
2. **Genomic/specimen WARN:** Decide disposition for **`genomic_link_review`** open/pending volume (10705 on this run) — operational review, not a validator coding task.
3. **Non-Tg labs:** Scope the next **institutional extract** row (analytes + date fidelity); do **not** claim full thyroid panel coverage from current `longitudinal_lab_canonical_v1` alone.

---

## Artifact index

| Path | Description |
|------|-------------|
| `review_queue_triage_20260408_073550/` | Full **120** bundle (CSVs + `summary.md`) |
| `119_release_validation/validation_report.md` | Frozen **119 --release-mode** report |
