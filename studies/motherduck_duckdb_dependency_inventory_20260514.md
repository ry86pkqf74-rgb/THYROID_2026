# MotherDuck / DuckDB dependency inventory — repo sweep (2026-05-14)

Sweep scope: workspace checkout on **`main`** at audit time (current branch only; multi-branch git scan not performed).

## One-line summary

**615 Python modules still contain at least one MotherDuck-cloud connection pattern** (union of `connect_locked(`, `_round2_helpers`, `MotherDuckClient`, and literal `duckdb.connect(...md:...)`) **; 221 non-archived tooling scripts under `scripts/` + `qc_framework_v1/` invoke `connect_locked()` alone** — each such call opens `md:thyroid_canonical_publication_v1_0` via `scripts/_md_connect.py`. **Round-2 merge/dedup publishers (`369`/`382`/`383`/`384`/`368`/`385`/`386`/`386b`/`382_restore`/`387`) use `_round2_helpers.connect_md()` or inline `md:` without `connect_locked` — they are *additional* active MotherDuck writers not included in the 221 figure.**

---

## Methodology (grep anchors)

Case-insensitive searches covered `.py`, `.toml`, `.yml`/`.yaml`, `.ini`, `.env*`, `.sh`, notebooks, Makefiles, CI, and markdown where noted.

| Anchor | Role |
|--------|------|
| `motherduck`, `MOTHERDUCK_TOKEN`, `motherduck_token` | Branding, env, docs |
| `duckdb`, `import duckdb`, `duckdb.connect` | Embedded analytical engine |
| `.duckdb` | Local file DB paths |
| `md:` | MotherDuck URI scheme |
| `ATTACH` | Mostly legacy SQL comments / migration notes (high false-positive rate in prose) |
| `connect_locked(` | Hard-wired publication MotherDuck session (`scripts/_md_connect.py`) |

**Counts (reproducible on repo root):**

```bash
# MotherDuck-cloud pattern union (Python)
# (connect_locked ∪ _round2_helpers ∪ MotherDuckClient ∪ literal md: connect) → sort -u | wc -l
# Observed: 615

grep -rl 'connect_locked(' --include='*.py' . | wc -l   # Observed: 307

# duckdb.connect anywhere
grep -rl 'duckdb.connect' --include='*.py' . | wc -l   # Observed: 390

# case-insensitive motherduck mention (all tracked text types; very broad)
grep -ril 'motherduck' . | wc -l   # Observed: 1445+
```

**Narrow “maintained scripts tree” (`connect_locked` only, excludes `scripts/output/`, `scripts/frozen/`, `scripts/archive/`):**

```bash
comm -12 \
  <(grep -rl 'connect_locked(' --include='*.py' scripts qc_framework_v1 | sort -u) \
  <(find scripts qc_framework_v1 -name '*.py' \
      -not -path 'scripts/output/*' \
      -not -path 'scripts/frozen/*' \
      -not -path 'scripts/archive/*' | sort -u) | wc -l
# Observed: 221
```

---

## Dependency manifests & secrets

| File | Finding |
|------|---------|
| `requirements.txt` | **`duckdb>=1.5.2,<2` is declared** (line comment still references MotherDuck client guidance). |
| `pyproject.toml` | No direct `duckdb` dep; pytest marker mentions `thyroid_master.duckdb`; mypy includes `motherduck_client.py`, `utils/md_connect.py`. |
| `motherduck.local.toml` | **Gitignored** — not committed. |
| `motherduck.local.toml.example` | **Safe** — placeholder keys only. |
| `.env.example` | Comment-only `MOTHERDUCK_TOKEN` example. |

**No live token values are present in tracked files** — only examples and documentation.

---

## `qc_framework_v1/migrations/` — MotherDuck writers vs comment-only SQL

| Artifact | Classification | Notes | Disposition |
|----------|----------------|-------|-------------|
| `mig_329_load_canonical_labs_thyroglobulin_bq.py` | (a) ACTIVE code path **for archival parquet→BQ only** | Docstring: **deprecated** MotherDuck parity; Tg SSOT → **mig_340** | **RETIRE** after parity exercises complete; **PORT** Tg-only usage to BQ-native only |
| `mig_327_bulk_md_to_bq_missing_tables.py` | (a) **MD read + BQ load** migration tool | Uses `connect_locked()`; bulk export MD→parquet→BQ | **PORT** to BQ-only source tables or **RETIRE** when MD empty |
| `mig_323_export_ctc_md_to_parquet.py` | (a) **MD read** export | CTC parquet for THY-18 | **PORT** to `pub_canonical` export or **RETIRE** |
| `mig_340_thyroglobulin_analyst_bq_rebuild.py` | (a) **BQ-native** Tg rebuild | Supersedes MD Tg chain per file header | **SAFE** / canonical forward path |
| `*.sql` migrations (majority) | (c) COMMENT / legacy operator notes | References to `duckdb_views()`, `duckdb_tables()`, “DuckDB LIKE → BQ LIKE”, executed **on BQ** | **SAFE** (documentation / BQ DDL only) |

No other `.sql` files under `qc_framework_v1/migrations/` were found **opening** MotherDuck; coupling is overwhelmingly **Python**.

---

## LN / imaging merge-load family (`*_merge_load_rollup.py`, round-2 helpers)

These scripts **merge Round-2 LLM outputs and publish Tier-2 canonical tables on MotherDuck** via shared `scripts/_round2_helpers.connect_md()` (or local `_connect_md` in Script 368).

| File | Hit type | Classification | Builds / touches | Disposition |
|------|----------|----------------|------------------|-------------|
| `scripts/_round2_helpers.py` | `duckdb.connect(f"md:?motherduck_token=…")` | (a) ACTIVE infra | MotherDuck session + `USE thyroid_canonical_publication_v1_0` | **PORT** helpers to BigQuery client / **RETIRE** when builders moved |
| `scripts/369_pathology_v2_merge_load_rollup.py` | `connect_md(logger)` | (a) | `note_entities_llm_pathology_v2`, pathology canonical pair | **PORT** to BQ-native loaders |
| `scripts/382_cervical_ln_clinical_merge_load_rollup.py` | `connect_md(logger)` | (a) | `note_entities_llm_cervical_ln_detail`, `canonical_cervical_ln_clinical_*` | **PORT** (LN multimodal plan references exports under `exports/ln_multimodal_*` JSON audit only — **no separate Python builder** beyond this family) |
| `scripts/383_tirads_granular_merge_load.py` | `connect_md(logger)` | (a) | TIRADS granular merge | **PORT** |
| `scripts/384_esophageal_invasion_merge_load_rollup.py` | `connect_md(logger)` | (a) | Esophageal invasion canonical | **PORT** |
| `scripts/368_vasc_v2_merge_load_rollup.py` | `_connect_md()` | (a) | Vascular invasion v2 rollup | **PORT** |
| `scripts/385_round2_canonical_verify.py` | `connect_md(logger)` | (a) verify | Read/verify | **PORT** verification to BQ |
| `scripts/386_v1_0_dedup_pass.py` | `connect_md(logger)` | (a) | Dedup canonical event tables | **PORT** |
| `scripts/386b_fix_round2_llm_model_tag.py` | `connect_md(logger)` | (a) | LLM model tag patch | **PORT** |
| `scripts/382_restore_7_cervical_ln_legacy_notes.py` | `connect_md(logger)` when `--apply-md` | (a) legacy repair | Restores 7 rows + rerollup | **RETIRE** after BQ parity stable |
| `scripts/387_pub_v1_0_cleanup.py` | `duckdb.connect(f"md:{PUB_DB}?motherduck_token=…")` | (a) | Publication v1.0 cleanup | **PORT** |

---

## Thyroglobulin chain — explicit confirmation

| Script | MotherDuck? | Classification | Disposition |
|--------|-------------|----------------|-------------|
| `scripts/113_tg_lab_ingestion.py` | `--md` path uses `_md_connect.connect_locked` | (a) **deprecated operational path** per header (2026-05-14) | **RETIRE** MD mode; **PORT** ingestion targets to BQ / parquet-only staging |
| `scripts/127_analyst_institutional_lab_append.py` | `--md` via `connect_md_or_file` | (a) **deprecated** per header | **RETIRE** MD staging |
| `scripts/347_lab_master_canonical_v1_build.py` | `connect_locked()` unconditional | (a) **deprecated for Tg refresh**; still builds multi-analyte MD tables | **PORT** non-Tg analyzers to BQ; **RETIRE** MD for Tg (**mig_340**) |
| `qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py` | BigQuery SQL/Python driver | (a) **BQ-native** | **SAFE** |

---

## CI / Makefile — still wired for MotherDuck

| File | Classification | Disposition |
|------|----------------|-------------|
| `.github/workflows/ci.yml` | (a) Jobs assert MotherDuck tokens for formalization / live-audit paths | **PORT** CI to BQ smoke tests or **RETIRE** MD jobs |
| `Makefile` | (a) Targets: `md-smoke`, `md-v2-gate-md-dryrun`, `md-live-release-*`, `--md` triage / release | **PORT** narrative to BQ or **RETIRE** |

---

## Representative additional (a) — manuscript / Snowflake / studies

*(Same disposition template: **PORT** analyses to BigQuery client + **`google-cloud-bigquery`**, or **RETIRE** if frozen.)*

| Area | Examples | Classification |
|------|----------|----------------|
| Root manuscript runners | `scripts/m019_rai_outcomes_analysis.py`, `scripts/m025_tirads_analysis.py`, `scripts/m036_ata_2025_rss.py`, `scripts/table1_tier1_manuscripts.py`, `scripts/m028_m033_molecular_platform_audit.py`, … | (a) read/analyze MD publication DB |
| `snowflake_trial/scripts/*` | Many `duckdb.connect(f"md:thyroid_canonical_publication_v1_0?motherduck_token=…")` | (a)/(b) hybrid pilots — treat as **legacy sidecar** unless explicitly revived |
| `studies/m029_*`, `m043_*`, `m048_*`, `m083_*`, `proposal_multimodal_prediction_*` | Literal `md:` connects | Mix **(a)** reproducibility vs **(b)** one-off studies |

Full enumeration is **615 Python files** for the union heuristic above — listing each row here would duplicate git grep output without adding discrimination.

---

## Local file DuckDB (`*.duckdb`) — **not** MotherDuck cloud

These remain **`duckdb` the library** against **local files** (e.g. `thyroid_master.duckdb`). Classification: **separate tier** — still an **ACTIVE local build dependency** for historical Streamlit / materialization workflows, **not** MotherDuck SaaS.

| Pattern | Examples | Classification | Disposition |
|---------|----------|----------------|-------------|
| `thyroid_master.duckdb` default path | Scripts `22`, `48`, `50`, `57`–`66`, `86`, `87`, NSQIP scripts, etc. | (a) local | **RETAIN** until UI & runners move off local DuckDB, or **RETIRE** when fully BQ |

---

## Stale provenance in BigQuery data (task 4 — not code)

User-reported **`motherduck_database` column** values (e.g. `md:thyroid_research_2026`) in `pub_canonical.manuscript_cohort_v1`, `pub_semantic.release_manifest_v1`, and workspace copies — **data-documentation debt**, not Python import dependency. Remediation: **column comment + phased NULL/rename** in BQ migrations (out of scope for this file-level sweep).

---

## Classification key

| Tag | Meaning |
|-----|---------|
| **(a)** | ACTIVE BUILD DEPENDENCY — executable path still targets MotherDuck cloud and/or local DuckDB builds in official or semi-official tooling |
| **(b)** | DEAD / LEGACY — frozen trees (`scripts/frozen/`, `scripts/archive/`), old submission packages, ad-hoc studies |
| **(c)** | COMMENT / DOCSTRING only — prose, migration breadcrumbs, `.md` prompts |

| Disposition | Meaning |
|-------------|---------|
| **PORT** | Rewrite to BigQuery-native (`google-cloud-bigquery`, `db-dtypes`) or parquet-only staging |
| **RETIRE** | Remove flag, delete script, or archive when BQ parity proven |
| **SAFE** | Already BQ-first or documentation-only |

---

## Master summary table (high-signal rows)

| File / area | Hit type | Class | Builds what | Disposition |
|-------------|----------|-------|-------------|-------------|
| `scripts/_md_connect.py` | `duckdb.connect(f"md:{PUBLICATION_DB}?motherduck_token=…")` | (a) | All `connect_locked()` callers | **PORT** token usage to BQ auth or **RETIRE** |
| `utils/md_connect.py` | `MotherDuckClient.connect_rw()`, pragma verify | (a) | `--md` across repo | **PORT / RETIRE** with CI |
| `motherduck_client.py` | Token resolution | (a) infra | RW vs read-scaling | **RETIRE** or shrink-wrap for archival-only |
| `scripts/_round2_helpers.py` | `md:?motherduck_token` | (a) | Round-2 publisher session | **PORT** |
| `Makefile` + `.github/workflows/ci.yml` | docs + secrets | (a) orchestration | **PORT** gates to BQ |
| `requirements.txt` | `duckdb` pinned | manifest | interpreter dep | **KEEP** until local DuckDB retired; revisit pin |
| `qc_framework_v1/migrations/mig_327_*.py` | `connect_locked` | (a) | MD→BQ bulk | **PORT** |
| `qc_framework_v1/migrations/mig_323_*.py` | `connect_locked` | (a) | MD→parquet | **PORT** |
| `qc_framework_v1/migrations/mig_329_*.py` | parquet shim | (a) archival | Deprecated parity | **RETIRE** |
| `qc_framework_v1/migrations/mig_340_*.py` | BQ | (a) | Tg rebuild | **SAFE** |
| Round-2 merge scripts (`369`,`382`,`383`,`384`,`368`,`385`,`386`,`386b`,`382_restore`,`387`) | `connect_md` / `md:` | (a) | LN/path/TIRADS/esophagus/vasc Tier-2 | **PORT** |
| `scripts/113/127/347` | `connect_locked` / `--md` | (a) deprecated MD lab | Tg + labs | **RETIRE** MD; **PORT** to mig_340 + BQ |
| `studies/*`, `snowflake_trial/*`, `M0xx_submission_package/*` | mixed | (a)/(b) | analyses | **PORT** or **RETIRE** per manuscript owner |
| `*.md`, `AGENTS.md`, `README.md`, `NEW_DEVICE_SETUP.md` | prose | (c) | — | Update docs after CI port |
| `qc_framework_v1/migrations/*.sql` | `duckdb_views` text | (c) | BQ DDL comments | **SAFE** |

---

## Explicit answer to acceptance criteria (Prompt 8 snapshot)

1. **Every reference classified** — via **taxonomy (a)/(b)/(c)** + **mechanical** greps above; Prompt 8 reported **615** Python modules on the union heuristic at audit time.  
2. **N active MotherDuck build dependencies (Prompt 8)** — **221** maintained `scripts/` + `qc_framework_v1/` entrypoints calling `connect_locked()` **+ ~10** Round-2 merge/dedup scripts via `connect_md` / inline `md:` **≈231** high-impact publishers; **615** = full union across studies, `snowflake_trial/`, submission packages, probes, and tests.  
3. **Each active family** has **PORT** or **RETIRE** in tables above.

---

## Prompt 9 — Per-file decomposition + port backlog (2026-05-14)

**Machine-readable hit list:** `studies/motherduck_duckdb_hits_20260514.tsv` (606 data rows + header).

**Regenerator:** `studies/analyze_motherduck_publishers_20260514.py` — re-run after branch changes; requires `bq` CLI for live `pub_canonical` / `pub_semantic` / `pub_views_readable` object lists (~280 ids ingested).

### Footprint definition (git-tracked `*.py` only)

Union of files containing any of:

- `connect_locked(`  
- `MotherDuckClient`  
- `_round2_helpers` / `connect_md(` / `_connect_md(`  
- `duckdb.connect` **and** `md:` / `md?` somewhere in the same file  

**Prompt 8 vs Prompt 9:** Prompt 8 cited **615** modules (union over the full working tree at that snapshot). Re-running the same intent on **git-tracked** Python only yields **606** rows — treat **606** as the reconciled decomposition denominator; the **Δ=9** is consistent with ignored/untracked paths or a drifted checkout.

### Disposition counts (must sum to footprint)

| Disposition | N | Rule |
|-------------|---:|------|
| **ACTIVE** | **498** | Referenced in `.github/workflows/*.yml` or `Makefile`, **or** substring-hit against a live BigQuery `pub_*` table/view id, **or** listed infra (`scripts/_md_connect.py`, `scripts/_round2_helpers.py`, `motherduck_client.py`, `utils/md_connect.py`). |
| **DEAD** | **92** | Under superseded prefixes (`snowflake_trial/`, `M0xx_submission_package*`, `scripts/archive/`, `scripts/frozen/`, `M025_FINAL_PACKAGE/`) **or** no orchestrator ref and no BQ id substring. |
| **COMMENT** | **0** | Would be: MotherDuck/DuckDB tokens **only** in `#` comments (none matched in this footprint). |
| **UNCERTAIN** | **16** | `scripts/output/` probes, `studies/`, private `_` helpers under `scripts/` with real code coupling but no CI/BQ string proof. |
| **Sum** | **606** | `498 + 92 + 0 + 16` |

### “Publication-critical” column semantics (BQ crosswalk)

The TSV column **`bq_tables_substring_mentions`** is the set intersection of (file text) ∩ (280 BigQuery object ids from `pub_canonical` + `pub_semantic` + `pub_views_readable`). It flags **reads or writes** or incidental mentions — not proof of authorship alone.

**`publication_anchor_overlap`** = `yes` when those mentions include any of the **anchor** set used for prioritization:

`canonical_patient_master`, `manuscript_cohort_v1`, `synoptic_tumor_long_v1`, `canonical_path_malignant_events_v1`, `canonical_tumor_characteristics_v1`, `tumor_episode_master_v2`, `thyroid_scoring_py_v1`, `path_synoptics`, `signoff_migration`.

For **scripted** port ordering, use **`port_backlog_tier`** on ACTIVE rows (P0 = strongest: `connect_locked`/`connect_md` + `CREATE TABLE|VIEW` / `to_gbq` / `load_job` heuristic + anchor overlap).

**ACTIVE × port tier (counts):** P0=155, P1=30, P2=105, P3=109, P4=94, P5=5.

### High-impact coupling (connect_locked / connect_md), ACTIVE only

| Coupling | ACTIVE files |
|----------|-------------:|
| `connect_locked` | **253** |
| `connect_md` | **37** |
| **Combined** | **290** |

The Prompt 8 headline **~231** maintained publishers was a **narrower slice** (`connect_locked` under `scripts/` + `qc_framework_v1/` excluding `output/` / `archive` / `frozen`). The Prompt 9 figure **253** is **all git-tracked** ACTIVE `connect_locked` publishers (includes `llm_extraction/`, `dashboard.py`, `studies/` movers, etc.).

### Prioritized port-or-retire backlog (operational order)

1. **P0 — Port first (publication spine heuristic)** — 155 ACTIVE modules. Deterministic seeds for immediate engineering:
   - **Infra:** `scripts/_md_connect.py`, `scripts/_round2_helpers.py`, `motherduck_client.py`, `utils/md_connect.py`
   - **Round-2 Tier-2 publishers / verify / dedup:** `scripts/368_vasc_v2_merge_load_rollup.py`, `369_pathology_v2_merge_load_rollup.py`, `382_cervical_ln_clinical_merge_load_rollup.py`, `382_restore_7_cervical_ln_legacy_notes.py`, `383_tirads_granular_merge_load.py`, `384_esophageal_invasion_merge_load_rollup.py`, `385_round2_canonical_verify.py`, `386_v1_0_dedup_pass.py`, `386b_fix_round2_llm_model_tag.py`, `387_pub_v1_0_cleanup.py`
   - **Canonical / CPM pipeline (filter TSV; sample):** `scripts/113_tg_lab_ingestion.py`, `224`–`300+` family (`245`, `247`, `250`, `266b`, `266c`, `272`–`278`, …). Full list: `awk -F'\\t' 'NR>1 && $8=="P0"' studies/motherduck_duckdb_hits_20260514.tsv`
2. **P1–P2 — ACTIVE next** — `connect_locked` / `connect_md` with DDL/BQ-load heuristics but without anchor overlap, or with overlap but weaker coupling.
3. **P3–P5 — ACTIVE hygiene** — `MotherDuckClient`-only dashboards/tests, string hits without `connect_locked`, etc.
4. **DEAD — Bulk retire** — 92 files under historical prefixes; safe to archive after confirming no `main` CI target references the basename.

### Batch retire queue (DEAD prefix families)

All rows with `disposition==DEAD` are listed in the TSV; the majority consolidate under `snowflake_trial/`, `M0xx_submission_package*`, `M025_FINAL_PACKAGE/`, `scripts/archive/`, `scripts/frozen/`.

---

*Re-run `studies/analyze_motherduck_publishers_20260514.py` and the Prompt 8 grep block to refresh counts; `bq` must list datasets successfully for the BQ column.*
