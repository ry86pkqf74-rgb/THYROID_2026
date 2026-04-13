# DICOM promotion / documentation reconciliation (2026-04-13)

## What was reconciled

- **README:** New concise **DICOM header ingest layer** section plus a Key references row pointing to the runbook and design memo. States additive relationship to `imaging_nodule_master_v1`, scripts **128** and **129**, default export-only behavior, supported inputs (flattened exports + metadata-only `.dcm`), and that canonical MotherDuck presence requires operator materialization/promotion.
- **`docs/motherduck_database_contract_v1.md`:** New **DICOM header ingest layer (v1)** subsection listing `dicom_header_ingestion_provenance_v1`, `dicom_study_header_v1`, `dicom_series_header_v1`, `dicom_imaging_link_exact_v1`, `dicom_link_review_queue_v1`, with explicit **repo-implemented vs canonical-live** language (no unverified live-DB claim).
- **`docs/dicom_header_ingest_runbook.md`:** Short **status vs canonical contract** note at the top; operator steps below unchanged.
- **`studies/20260408_dicom_header_ingest/design_memo.md`:** Objective reconciled so the first draft’s flattened-export focus is preserved, **raw `.dcm`** support is described as a **later addition**, and **no pixel decode** / deterministic additive behavior are explicit.

## Additive / not canonical-live (unless verified and promoted)

- DICOM DDL and script **150** define the **`dicom_*_v1`** objects; **default runs** write **Parquet exports only**. **`main` canonical contract** for these objects requires explicit **`--write-db`** materialization and operator promotion — **not asserted live in this task** (no MotherDuck read verification run was required for doc updates; **no DB writes** were performed).

## Script 129 (imaging↔FNA linkage)

- **Outcome:** **Intentionally DICOM-blind** (preferred default). A short **module docstring** note states that **150** / `dicom_*_v1` do not feed this script and that `optional_attach_dicom_to_imaging_nodule_frame()` remains optional/no-op without `dicom_study_header_v1`.
- **No** guarded hook or code path was added (avoids any risk of linkage behavior drift).

## Files changed

| File |
|------|
| `README.md` |
| `docs/motherduck_database_contract_v1.md` |
| `docs/dicom_header_ingest_runbook.md` |
| `studies/20260408_dicom_header_ingest/design_memo.md` |
| `scripts/129_imaging_fna_linkage_mm_v1.py` (docstring only) |
| `studies/20260413_dicom_promotion_reconciliation/report.md` (this file) |

## Validation commands and outputs

Commands (run from repo root, `.venv`):

```bash
.venv/bin/python -m py_compile scripts/129_imaging_fna_linkage_mm_v1.py
.venv/bin/python -m mypy --ignore-missing-imports scripts/129_imaging_fna_linkage_mm_v1.py
.venv/bin/python -m pyflakes scripts/129_imaging_fna_linkage_mm_v1.py
.venv/bin/python -m pytest tests/test_dicom_header_ingest.py -q
```

**Outputs (2026-04-13):**

- `py_compile`: exit code **0** (no output).
- `mypy --ignore-missing-imports scripts/129_imaging_fna_linkage_mm_v1.py`: `Success: no issues found in 1 source file`
- `pyflakes scripts/129_imaging_fna_linkage_mm_v1.py`: exit code **0** (no output).
- `pytest tests/test_dicom_header_ingest.py -q`: `20 passed in 0.91s`

**Database:** No MotherDuck or local DuckDB connections or writes were performed for this documentation reconciliation task.

## Limitations / follow-up

- To claim **canonical-live** `dicom_*_v1` in MotherDuck `main`, run read-only inventory (e.g. `information_schema` / table list) or `144`/operator checklist and cite row evidence in a study note.
- Optional future work: thin CI doc check that README and contract DICOM subsection stay in sync (not part of this pass).

## Git

Final commit SHA and GitHub blob URLs: see end of session (`git rev-parse HEAD` after push; `main` blob URLs below).
