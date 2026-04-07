# Specimen + FHIR — release / reviewer ops report

**Timestamp (UTC):** 2026-04-07T07:55:00+00:00  
**Commit SHA:** `6968264dbeda88effe111d669be0844f9dead35b`  
**Scope:** QA views (`142`), validation hooks (`119`), CI pytest, current-state summary (`144`), documentation, MotherDuck deploy with `custom_user_agent=specimen_fhir_release_ops_v1`.

---

## Commands run (exact)

```bash
cd "$(git rev-parse --show-toplevel)"
python3 -m py_compile scripts/119_md_formalization_validate.py \
  scripts/138_md_specimen_fhir_layer.py \
  scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py \
  scripts/144_md_repo_current_state_summary.py

python3 -m pytest \
  tests/test_specimen_fhir_layer.py \
  tests/test_specimen_genomics_binding.py \
  tests/test_specimen_fhir_qa_diagnostics.py -q --tb=short
# outcome: 10 passed (suite) + 2 passed (diagnostics-only re-run during iteration)

python3 scripts/144_md_repo_current_state_summary.py
python3 scripts/144_md_repo_current_state_summary.py --md

python3 scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md
# outcome: OK — applied 142_specimen_fhir_qa_diagnostics_ddl.sql (UA=specimen_fhir_release_ops_v1)

python3 scripts/119_md_formalization_validate.py --md
# outcome: 23 PASS / 2 WARN / 0 FAIL (structural mode)
# Check 13 notes:
#  - WARN Specimen/FHIR QA diagnostics — broken_fhir_refs=10139; focus-table scans unavailable
#  - WARN specimen-adjacent review burden — genomic_link_review open/pending=9952; merge queue COUNT unavailable
```

---

## MotherDuck outcomes

| Item | Result |
|------|--------|
| **Catalog** | `Thyroid 2026` (`current_database()`) |
| **142 deploy UA** | `specimen_fhir_release_ops_v1` (scripts 143 + second connection in 138) |
| **Named snapshot (this session)** | No new `CREATE SNAPSHOT` — not re-running full `138` here; pattern remains `specimen_fhir_pre_<UTC>` in `138_md_specimen_fhir_layer.py` before DDL |
| **Latest `qa.release_manifest` rows (from 144)** | `20260407_final2`, `20260407_final`, `20260406` (see `studies/CURRENT_MOTHERDUCK_REPO_STATE.md`) |

### MotherDuck internal errors observed (for support / triage)

- Full aggregation / `COUNT(*)` on `main.specimen_tumor_focus_v1` — e.g. error ID `b1f67f87`.
- `SELECT COUNT(*) FROM qa.specimen_merge_review_queue_v1` — e.g. `0b912da1`.
- Earlier `SELECT * FROM qa.v_diag_specimen_provenance_summary_v1` (replaced with split views + removal of focus-heavy views).

Validator now **WARN**s when focus scans or merge-queue counts are unavailable instead of hard-failing.

### Live diagnostics (sample, post-142 deploy)

- `qa.v_diag_specimen_fhir_broken_refs_v1`: ~10.1k rows — legacy/stale FHIR JSON (e.g. `Patient/Patient/...`, missing `collection` vs current tail DDL). **Remediation:** re-run `scripts/138_md_specimen_fhir_layer.py --md` after contract freeze or accept WARN until rebuild.
- Genomic link review queue: 9.9k+ `open` rows (burden view).

---

## Artifacts

| Path | Role |
|------|------|
| `scripts/sql/142_specimen_fhir_qa_diagnostics_ddl.sql` | `qa.v_diag_*` views |
| `scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py` | Standalone deploy (fail-closed UA) |
| `scripts/144_md_repo_current_state_summary.py` | `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` |
| `docs/specimen_fhir_contract_review.md` | Reviewer / release contract |
| `.github/workflows/ci.yml` | Specimen pytest in `multimodal-tests` job |

---

## Commit reference

Authoritative SHA for this bundle: `6968264dbeda88effe111d669be0844f9dead35b` (short `6968264`).
