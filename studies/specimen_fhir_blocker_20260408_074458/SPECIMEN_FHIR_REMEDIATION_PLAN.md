# Remediation plan — specimen / FHIR vs `119 --release-mode`

## Production (sign-off target)

**Current state:** No specimen/FHIR remediation required for logic or 142 diagnostics; release-mode already **PASS WITH WARNINGS** with specimen **clean** and only **expected** review-burden WARNs.

**Optional hygiene:**

- Keep **`motherduck.local.toml`** (from `motherduck.local.toml.example`) for local token loading per project policy (this session used env-only token).
- After any future change to `138_specimen_fhir_tail_ddl.sql` or `142_specimen_fhir_qa_diagnostics_ddl.sql`, run **`scripts/138_md_specimen_fhir_layer.py --md`** then **`scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md`** on **prod**, then re-run **119** (or rely on release orchestration flags per runbook).

**Prod-safe follow-up command sequence (verification only):**

```bash
cd /path/to/THYROID_2026
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env prod --release-mode \
  --output-dir studies/$(date -u +specimen_fhir_verify_%Y%m%dT%H%M%SZ)
```

## QA sandbox (rehearsal)

**Gap:** `main.synoptic_tumor_long_v1` **missing** → Check 13 skipped; specimen/FHIR not exercised.

**Recommended order (operator — not run in this prompt on prod):**

1. Materialize **`synoptic_tumor_long_v1`** (scripts **108** / **109** as per pipeline docs) into **QA**.
2. **`scripts/138_md_specimen_fhir_layer.py --md --md-env qa`** (or equivalent env targeting per your bootstrap).
3. **`scripts/140_md_specimen_genomics_binding.py --md`** on QA.
4. **`scripts/143_md_specimen_fhir_qa_diagnostics_deploy.py --md`** on QA.
5. Re-query `qa.v_diag_*` and rerun:

```bash
.venv/bin/python scripts/119_md_formalization_validate.py --md --md-env qa --release-mode \
  --output-dir studies/specimen_fhir_blocker_<stamp>/qa_release_mode
```

**Note:** QA may still fail **119** for **canonical parquet parity** (local `processed/*.parquet` vs sandbox row counts), **MRQ governance**, or **molecular_testing** spine until the full QA clone/bootstrap matches rehearsal expectations — those are **outside** specimen/FHIR DDL.

## Historical `broken_fhir_refs` spike

Already documented: stale **`fhir_*_v1`** JSON vs current reference rules in `142` — fix was **rebuild 138** (FHIR tail) and **deploy 142/143**, not a change to broken-ref detection logic.
