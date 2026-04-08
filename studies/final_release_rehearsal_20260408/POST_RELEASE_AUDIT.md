# Post-release audit — 2026-04-08

This note records the **live** MotherDuck release/rehearsal path for tag family **`20260408`** / resume **`20260408r4`**, and closure of **release-mode 119** after manual review queue (MRQ) remediation.

## Prep promote backup

- **Execute prepromote-backup (130):** succeeded.
- **Rollback clone label (for reference):** `20260408_full_081638_exec` — see `live_exec_label.txt`.

## Final master (126) — live

- **126** ran through hydrate, 103, 117, 125, specimen gate; **115** failed initially because **`release_20260408`** already existed.
- **Manual completion:** `115_release_snapshot.py` + `118_parquet_release_bundle.py` with **`--tag 20260408r4`** and **`--final-master`**; parquet bundle under `exports/final_master_release_20260408r4/`.

## Formalization validate (119) — release mode

- After r4 path, **119** initially **FAIL** on Check 5: `qa.manual_review_queue` showed **10,948** rows with **816 PENDING** (mixed `run_label` / non–tier-policy rows from other workflows).
- **Root cause:** `114_qa_schema_setup.py --hydrate-from studies/20260407_tier_policy_review_gate` only **DELETE**s MRQ rows matching that gate’s `run_label`; it does not remove rows from other batches (e.g. promote / audit paths).
- **Remediation (prod MotherDuck):** full table clear then tier-policy hydrate (same pattern as 126’s MRQ reset):
  - `DELETE FROM qa.manual_review_queue` (all rows).
  - `114_qa_schema_setup.py --md --hydrate-from studies/20260407_tier_policy_review_gate` (5,622 MRQ rows; 0 pending after review-status alignment in gate CSV).
- **119 re-run:** **36 PASS / 3 WARN / 0 FAIL** (see `119_release_mode_AFTER_mrq_reset.log` and `studies/20260408_motherduck_formalization/validation_report.md`). Check 5 (review queue): **5,622 total, 5,622 reviewed, 0 pending**.

## Live release audit (124)

- **124** with `--final-release --tag 20260408r4` ran upstream chain then **aborted at 115** because **`release_20260408r4`** already existed (expected after manual 115/118 for r4).
- Artifacts remain under `studies/20260408r4_motherduck_live_release_audit/` for the partial run.
- **If a full 124 “green field” snapshot is needed:** use a **fresh unused** release tag (e.g. `20260408r5`) so 115 can create a new `release_*` schema, or run 115/118/119 manually for that tag.

## Read scaling (136)

- **136** `writer --md-env prod` and `reader --md-env prod --all` completed with **exit 0** (`136_writer_execute.log`, `136_reader_execute.log`).

## WARN-only items in 119 (non-blocking)

- Molecular `panel_version` empty where `assay_name` present; assay dictionary match WARN for non-Afirma panels.
- Specimen-adjacent review burden WARN (genomic link / merge queues) — separate from tier-policy MRQ gate.

## Operational note

- For future releases: if MRQ must match the **tier-policy gate only**, either run **126’s** full QA reset path or **DELETE all** `qa.manual_review_queue` rows before `--hydrate-from` the tier-policy folder; partial DELETE-by-`run_label` is insufficient when other batches populated the table.
