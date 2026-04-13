# Reviewer read-scaling, CI, and documentation closure (2026-04-13)

## Git SHA

**Commit:** `276bbac836d295faad7d9e9797c4a8abadb19a51`

## Scope (this deliverable)

- Hardened **connection-mode validation** and **`MD_READ_SCALING_SESSION_HINT`** precedence for `scripts/120_review_queue_triage.py` and `scripts/151_specimen_genomic_review_queue_export.py` (aligned with `scripts/141_fhir_specimen_json_export.py` three-way split: `--md` | `--read-scaling` | local file).
- **141** read-scaling path now uses **`utils.md_connect.connect_read_scaling_fail_closed`** (single fail-closed implementation).
- **Offline tests:** mutual exclusion (`--md` + `--read-scaling`), `--md-sa` without `--md`, and kwargs forwarded to `connect_read_scaling_fail_closed` for 120 / 151.
- **Docs:** `docs/motherduck_database_contract_v1.md` (default specimen/FHIR writer UA), `docs/review_queue_triage_export.md` (151 + refresh), `README.md` (historical vs 2026-04-13 posture).

## Files changed

- `scripts/120_review_queue_triage.py`
- `scripts/151_specimen_genomic_review_queue_export.py`
- `scripts/141_fhir_specimen_json_export.py`
- `tests/test_120_review_queue_triage.py`
- `tests/test_151_specimen_genomic_review_export.py`
- `docs/motherduck_database_contract_v1.md`
- `docs/review_queue_triage_export.md`
- `README.md`
- `studies/reviewer_read_scaling_ci_doc_closure_20260413T230456Z/report.md`

## Read-scaling reviewer ops — executed vs blocked

**Blocked:** `motherduck_client.get_read_scaling_token()` returned **no** token in this workspace session (`MD_READ_SCALING_TOKEN` / `MOTHERDUCK_READ_SCALING_TOKEN` unset; no resolvable read-scaling key in `motherduck.local.toml` / `.streamlit/secrets.toml` visible to the check). **No fake live run.**

### Blocker memo — what an org admin / reviewer needs

1. **Obtain** a MotherDuck **read-scaling** (Business / dashboard read-only) token from MotherDuck org settings — **not** the RW `MOTHERDUCK_TOKEN` / `MD_SA_TOKEN`.
2. **Set** in environment or gitignored config (see `motherduck.local.toml.example`):
   - `MD_READ_SCALING_TOKEN` (or `MOTHERDUCK_READ_SCALING_TOKEN`)
   - Optional: `MD_READ_SCALING_SESSION_HINT` for stable reader affinity
3. **Writer snapshot** (RW token): after ETL/promotion, operator runs:
   ```bash
   .venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py writer --md-env prod --prefer-sa
   ```
4. **Reader refresh** (read-scaling token only):
   ```bash
   .venv/bin/python scripts/136_md_read_scaling_snapshot_refresh.py reader --md-env prod
   ```
   (or `REFRESH DATABASE` on the read-scaling connection per MotherDuck docs.)
5. **Reviewer exports** (least privilege):
   ```bash
   .venv/bin/python scripts/141_fhir_specimen_json_export.py --read-scaling --output-root exports
   .venv/bin/python scripts/120_review_queue_triage.py --read-scaling --output-root exports
   .venv/bin/python scripts/151_specimen_genomic_review_queue_export.py --read-scaling --output-root exports
   ```

**Fallback:** A **restricted hidden share** with READ-only ACL and the same fail-closed `PRAGMA database_list` MotherDuck attach check may be acceptable **if** org policy permits and the share sees the same catalog after promotion — prefer documented read-scaling tokens for reproducibility.

## `studies/CURRENT_MOTHERDUCK_REPO_STATE.md`

**Not refreshed** in this task (no live `--md` / `--read-scaling` verification run that would require a new `144` snapshot). File remains **point-in-time** per its header.

## CI coverage (before / after)

- **Before:** `.github/workflows/ci.yml` job **`llm-extraction-gold`** already ran `tests/test_151_specimen_genomic_review_export.py` alongside the other offline specimen/FHIR tests (see workflow pytest list).
- **After:** Same job list; **no workflow file change** required. Coverage **confirmed** for `test_151` + extended 120/151 tests.

## Docs drift resolved

- **Default specimen/FHIR writer UA:** `specimen_fhir_ref_integrity_v2` documented as current default; `specimen_fhir_release_truth_v2` / `v1` labeled historical in `docs/motherduck_database_contract_v1.md`.
- **README:** Lower “Status (2026-04-08)” table relabeled as **historical point-in-time** with pointers to **2026-04-13** live parity and governance reports.
- **Review queue / 151:** `docs/review_queue_triage_export.md` documents `--read-scaling`, env vars, and `136 … reader` / `REFRESH DATABASE` expectations.

## Commands run (validation)

```bash
git fetch origin && git pull --ff-only origin main
python -m py_compile scripts/120_review_queue_triage.py scripts/151_specimen_genomic_review_queue_export.py scripts/141_fhir_specimen_json_export.py
ruff check scripts/120_review_queue_triage.py scripts/151_specimen_genomic_review_queue_export.py scripts/141_fhir_specimen_json_export.py tests/test_120_review_queue_triage.py tests/test_151_specimen_genomic_review_export.py --select F
mypy   # per pyproject.toml scoped files (pre-existing errors in utils/tests may fail locally)
python -m pytest tests/test_120_review_queue_triage.py tests/test_151_specimen_genomic_review_export.py \
  tests/test_specimen_identity_layer.py tests/test_specimen_fhir_layer.py tests/test_specimen_fhir_qa_diagnostics.py \
  tests/test_specimen_genomics_binding.py tests/test_specimen_fhir_scripts_offline.py -v --tb=short
python scripts/120_review_queue_triage.py --help
python scripts/151_specimen_genomic_review_queue_export.py --help
python -c "from motherduck_client import get_read_scaling_token; print('SET' if get_read_scaling_token() else 'MISSING')"
```

## Publication / human review

**No fabricated human-review completion.** Remaining manuscript/publication blockers for **governance** sign-off are **external human review** where policy requires named reviewers — not missing exporter wiring. `auto_accepted_*` and automation-only tiers are **not** human-reviewed signoff (see `docs/publication_governance_gate.md` and triage tier CSVs).
