# Validation summary

## Pytest (required set)

**Command:** see `commands_run.md`.

**Result:** **37 passed**, 1 deprecation warning (`dateutil` / `utcfromtimestamp` from dependency), **0 failed**.

**Scope:** Validates **129** linkage logic, **128** contract IDs / strict-release behavior, and specimen/FHIR layer tests **as implemented in repo** (fixtures / mocked connections per test files).

## Read-only local catalog probe

**Database:** workspace `thyroid_master.duckdb` (read-only connection).

**Purpose:** Illustrate that **local file ≠ MotherDuck `main`** completeness.

**Observed:**

- **Schema:** only `main` (no `mm_contract_dev` in this file).
- **Present (non-zero rows):** `fna_episode_master_v2`, `molecular_test_episode_v2`, `tumor_episode_master_v2`, `synoptic_tumor_long_v1`.
- **Absent:** `imaging_nodule_master_v1`, `preop_surgery_linkage_v3`, `fna_molecular_linkage_v3`, `surgery_pathology_linkage_v3`, `imaging_fna_linkage_mm_v1`, `path_synoptics_encounter_qc_v1`, `specimen_master_v1`, `specimen_genomic_assay_v1`.

**Interpretation:** Cannot validate end-to-end chain **on this local file**; evidence for **MotherDuck** must come from operator runs + contract docs, not this snapshot alone.

## MotherDuck live inspection

**Not executed** in this audit session: workspace lacks committed `.streamlit/secrets.toml` (only `.streamlit/config.toml` present). Token is expected via **environment variables** or **gitignored** secrets file per `motherduck_client.get_token()` — **no token values logged**.

**Read-only recommendation for follow-up:** attach with RW or RO token and run `information_schema` + `COUNT(*)` on the tables listed in `canonical_chain_matrix.md` (no note text, no PHI exports).

## Release gate alignment

- **`scripts/148_thyroid2026_release_gate.py`** multimodal section expects **`mm_contract_dev` or `main`** to contain blocking **`val_*_mm_v1`** tables and requires them **empty** for PASS on those checks (`Severity.FAIL` when non-empty) — see source ~L663–711.
