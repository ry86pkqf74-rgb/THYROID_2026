# THYROID_2026 — MotherDuck staging report

**Date (UTC):** 2026-04-07  
**Session attribution:** `MOTHERDUCK_SESSION_HINT=THYROID_2026`, `MOTHERDUCK_CUSTOM_USER_AGENT` set per script (116 / 103 / 142).  
**Target:** MotherDuck catalog **`Thyroid 2026 Molecular Dev 20260407`** (`MOTHERDUCK_ENV=dev` per `config/motherduck_environments.yml`).  
**Schemas touched (write):** `v2_stage` only for this task — **no** `main` promotion, **no** `release_*` snapshot finalization.

---

## Preflight (token and mapping)

| Check | Result |
|--------|--------|
| `MD_SA_TOKEN` (shell env) | MISSING (len 0) |
| `MOTHERDUCK_TOKEN` (shell env) | MISSING (len 0) |
| `motherduck_token` (shell env) | MISSING (len 0) |
| Effective RW auth | Resolved via `motherduck_client.get_token()` → **`.streamlit/secrets.toml`** when env empty (no token values logged or stored in repo). |

**Catalog / schema mapping (repo SSOT):**

- Environments: `config/motherduck_environments.yml` — **dev**, **qa**, and **prod** use **separate** database names (not a single shared DB). Staging used **`dev`** to avoid touching production `main`.
- Within a catalog, staging plane is **`v2_stage`**; canonical promoted surface is **`main`** (`docs/motherduck_database_contract_v1.md`).

**Isolation note:** Because dev/qa/prod are distinct databases, workspace-wide attachments are not assumed; all writes were qualified as `v2_stage.*`.

---

## Commands executed

From repo root `THYROID_2026/`, after exporting attribution env vars:

```bash
export MOTHERDUCK_ENV=dev
export MOTHERDUCK_SESSION_HINT=THYROID_2026
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/116_stage_loader;kind=ingest"
.venv/bin/python scripts/116_md_stage_loader.py --md
```

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/103_fact_lineage;kind=materialize"
.venv/bin/python scripts/103_fact_lineage_materialize.py --md --md-schema v2_stage
```

```bash
export MOTHERDUCK_CUSTOM_USER_AGENT="THYROID_2026_molecular/142_staging_qc;kind=validate"
.venv/bin/python scripts/142_md_staging_qc.py --md
```

---

## Tables materialized (staging)

| Object | Rows (QC) | Notes |
|--------|-----------|--------|
| `v2_stage.note_entities_llm_*` (30 domains) | 11,037 per table (raw fleet grain) | Loaded from `processed/output/v2_parquets/` via `116`; transaction committed. |
| `v2_stage.load_inventory` | 210 cumulative history rows | `BOOL_AND(row_match)` **PASS** for recorded loads. |
| `v2_stage.canonical_extracted_fact_long_v1` | 68,077 | Script 103 clean v1 facts. |
| `v2_stage.canonical_fact_quarantine_v1` | 0 | |
| `v2_stage.canonical_extracted_fact_long_v2` | 123,577 | Expanded entity-level v2 facts. |
| `v2_stage.canonical_fact_quarantine_v2` | 199 | Review / quarantine path. |
| `v2_stage.note_extraction_runs` | 5 | Run telemetry. |

**Code change:** `103_fact_lineage_materialize.py` now supports `--md-schema v2_stage` so canonical/quarantine tables can land in **staging only** instead of `main` (default remains `main` when `--md-schema` omitted).

---

## QC gates (`reports/motherduck_stage_counts.csv`)

| Gate | Result |
|------|--------|
| Null `research_id` in `v2_stage.canonical_extracted_fact_long_v2` | **PASS** (0) |
| Duplicate `fact_id` groups | **PASS** (0 groups) |
| `v2_stage.load_inventory` row_match rollup | **PASS** (`all_row_match=True`) |
| Domain coverage | See CSV (`domain_coverage` rows); largest domains include procedures, complications, operative_detail, imaging, pathology, etc. |

**Multimodal / release-path objects (read-only probe on `main` in dev):**

| Object | Present in `duckdb_tables()` |
|--------|------------------------------|
| `main.imaging_fna_linkage_mm_v1` | Yes (1) |
| `main.multimodal_contract_mm_v1` | No |
| `main.imaging_fna_linkage_mm_v1_validation` | No |

Scripts **128 / 129 / 130** were **not** run: they materialize or assume `main` multimodal contract surfaces and are outside **staging-only** scope for this task.

---

## Operational caveats

1. **Local episode linkage in 103:** `_load_episode_source` uses local parquet / `thyroid_master.duckdb`, not MotherDuck, for operative/pathology/imaging episode anchors. This run logged **`multi-surgery patients: 0`** (local episode sources likely empty or slim). Canonical facts are still consistent with **local** anchors; for production-grade episode linkage, verify local DB or extend 103 to optionally read anchors from MD `main` (future work).
2. **Fleet row grain:** Domain tables on `v2_stage` hold **11,037 rows per domain** (per-note fleet layout before entity expansion in 103). This is expected for raw staged parquets; entity counts are reflected in `canonical_extracted_fact_long_v2`.

---

## Recommendation

| Verdict | Rationale |
|---------|-----------|
| **PASS** | Staging load + transactional commit succeeded; null-key and duplicate-key gates **PASS**; `load_inventory` parity **PASS**; writes confined to **`v2_stage`** on **dev** DB. |
| **HOLD** | Multimodal contract / validation objects missing on **dev** `main`; do not treat dev as multimodal-complete. Promotion / 128–130 not executed. |

---

## Artifacts

- `reports/motherduck_stage_counts.csv` — machine-readable QC.  
- `docs/motherduck_operator_runbook.md` — repeat-run instructions.  
- `scripts/142_md_staging_qc.py` — reproducible QC exporter.
