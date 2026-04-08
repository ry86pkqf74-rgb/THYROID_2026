## Blockers & hypotheses matrix

| Topic | Checked-in signoff folder (`20260407_publication_signoff_live`) | Fresh live-safe evidence (2026-04-08 session) | Verdict |
|-------|-------------------------------------------------------------------|--------------------------------------------------|---------|
| **H1: Synthetic MRQ blocks release** | `mrq_reconciliation_memo.md`: 5,620 `SYNTHETIC_…` vs 2 `confirmed_correct` | **`119 --release-mode` Check 5b PASS:** “no synthetic-placeholder verification_status in qa.manual_review_queue”. MRQ total **11,244** reviewed, **0** pending. | **Hypothesis REFUTED for current prod** — live MRQ no longer matches April 7 triage snapshot; **treat signoff memos as historical** unless re-exported. |
| **H1: Missing final institutional non-Tg lab wave** | `lab_coverage_memo.md`: only `wave_tg*` rows | Live SQL: `final_institutional_20260407` **989 rows** in `main.longitudinal_lab_canonical_v1`. | **REFUTED** for live catalog (memo stale). |
| **H1: Specimen/FHIR QA failure** | In-folder `validation_report.md`: FAIL `broken_fhir_refs=10139` | Later supersession + **this `119 --release-mode`:** Check 13 **PASS**, diagnostics **clean**; WARN on **review burden** (`genomic_link_review` open/pending **10705**). Structural `119` (no `--release-mode`) also shows specimen QA **clean**. | **FAIL state REFUTED** for current prod; **WARN burden** remains. |
| **H2: Multimodal partial** | N/A | **129** docstring: imaging ↔ FNA linkage only; **128** builds `mm_contract_dev` star schema + validators. No single `main` promoted chain object. | **CONFIRMED** — partial pipeline, not full E2E canonical chain in `main`. |
| **H3: `mm_contract_dev` not `main` release surface** | Runbooks / tests use `mm_contract_dev` | **128** default schema `mm_contract_dev`; **148** searches `mm_contract_dev` then `main`. | **CONFIRMED** — dev-scoped convention; **main** may hold copies only when explicitly aligned (`--contract-schema`). |
| **H4: `data_dictionary.md` legacy** | AGENTS notes legacy role | Contract SSOT: `docs/motherduck_database_contract_v1.md`. | **CONFIRMED** — do not treat `data_dictionary.md` as MotherDuck SSOT. |
| **H5: Make fails if token only in secrets.toml** | Older anecdotal | **Makefile** uses Python `get_token()` → reads **secrets.toml**. **Verified:** `make md-v2-gate-md-dryrun` **succeeded** with env vars **unset**, `token_mode: secrets.toml:MOTHERDUCK_TOKEN`. | **REFUTED** for current Makefile. |
| **H6: DuckLake rollback** | `docs/motherduck_sandbox_clone_runbook.md` | **`130 inspect`:** prod type **DUCKLAKE**; `DATABASE_SNAPSHOTS` rows with **`snapshot_name` NULL** (automatic history). **`130 prepromote-backup --label audit_probe_20260407` (dry-run):** `CREATE DATABASE "…PrePromote…" FROM "Thyroid 2026"`. | **CONFIRMED** — do not rely on **named** snapshot semantics for prod DuckLake; **zero-copy clone** is the documented rollback handle. |
| **H7: Pro / read-scaling** | N/A | `read_scaling_token_mode: **none**`; `connect_read_scaling()` raises **RuntimeError** (expected). **136** `reader`/`writer --dry-run` print SQL only. | **RW available**; **Business read-scaling token not present** on this machine — cannot prove org tier from token label; capability **not demonstrated** for RS path. |

## `119 --release-mode` latest fail reasons (this workspace)

1. **Canonical parquet local row counts** — `local=-1` for three canonical artifacts (missing local files in this checkout), while MD counts match expectations on cloud side for downstream checks.

## `qa.promotion_review_decisions` substance (release-mode check)

- **Fresh run:** **3** rows; **all** have non-empty `decision_batch_id` (Check 5b PASS). *Contrast:* `mrq_reconciliation_memo.md` cited **2** rows and NULL batch — **stale** vs live.

## `129` scope

- **Imaging ↔ FNA** linkage tables + audit/review queues — **not** a full multimodal chain through Bethesda, molecular results, and pathology in one promoted object graph.
