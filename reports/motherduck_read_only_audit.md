# THYROID_2026 — MotherDuck read-only operational audit

**Generated (UTC):** 2026-04-07T12:27:08Z

## Executive snapshot (production catalog)

| Metric | Value |
|--------|------:|
| `main.canonical_extracted_fact_long_v2` — canonical_clean_rows | 123,577 |
| `main.canonical_fact_quarantine_v2` — canonical_quarantine_rows | 199 |
| `v2_stage.canonical_extracted_fact_long_v2` — staging_clean_rows | _(missing)_ |
| `v2_stage.canonical_fact_quarantine_v2` — staging_quarantine_rows | _(missing)_ |
| `qa.manual_review_queue` — pending (NULL verification_status) | 0 |
| `qa.manual_review_queue` — total rows | 5,622 |

_Full per-environment metrics:_ see CSV below.

## Method & constraints

- **Session hint:** `MOTHERDUCK_SESSION_HINT` = `THYROID_2026`
- **Attribution:** `MOTHERDUCK_CUSTOM_USER_AGENT` set to project audit string (see runbook).
- **Read-scaling token source:** `none`
- **Read/write token source (if used):** `secrets.toml:MOTHERDUCK_TOKEN`
- **Queries:** `SELECT` / metadata only — no DDL/DML, no `ATTACH`/`DETACH` in this script.
- **PHI:** report contains counts, schema names, release tags, and git SHAs only — not clinical narratives.

## Environment summary

### `dev`

- **Database:** `Thyroid 2026 Molecular Dev 20260407`
- **Expected (config):** `Thyroid 2026 Molecular Dev 20260407`
- **Connection mode:** `read_write`
- **Schemas (information_schema):** 10 total
- **Release schemas:** release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260408, release_20260409
- **qa.release_manifest (latest):**
  - `20260407_final2` | `4ad9052` | 2026-04-07 05:11:41.171561
  - `20260407_final` | `4ad9052` | 2026-04-07 05:08:12.328508
  - `20260406` | `4b2d076` | 2026-04-07 04:07:52.519215
  - `20260409` | `b77b4be` | 2026-04-07 02:05:07.189573
  - `20260408` | `b77b4be` | 2026-04-07 02:03:20.732093
- **Multimodal validation schema:** `mm_contract_dev`

### `qa`

- **Database:** `Thyroid 2026 Molecular QA 20260407`
- **Expected (config):** `Thyroid 2026 Molecular QA 20260407`
- **Connection mode:** `read_write`
- **Schemas (information_schema):** 10 total
- **Release schemas:** release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260408, release_20260409
- **qa.release_manifest (latest):**
  - `20260407_final2` | `4ad9052` | 2026-04-07 05:11:41.171561
  - `20260407_final` | `4ad9052` | 2026-04-07 05:08:12.328508
  - `20260406` | `4b2d076` | 2026-04-07 04:07:52.519215
  - `20260409` | `b77b4be` | 2026-04-07 02:05:07.189573
  - `20260408` | `b77b4be` | 2026-04-07 02:03:20.732093
- **Multimodal validation schema:** `mm_contract_dev`

### `prod`

- **Database:** `Thyroid 2026`
- **Expected (config):** `Thyroid 2026`
- **Connection mode:** `read_write`
- **Schemas (information_schema):** 10 total
- **Release schemas:** release_20260406, release_20260407, release_20260407_final, release_20260407_final2, release_20260408, release_20260409
- **qa.release_manifest (latest):**
  - `20260407_final2` | `4ad9052` | 2026-04-07 05:11:41.171561
  - `20260407_final` | `4ad9052` | 2026-04-07 05:08:12.328508
  - `20260406` | `4b2d076` | 2026-04-07 04:07:52.519215
  - `20260409` | `b77b4be` | 2026-04-07 02:05:07.189573
  - `20260408` | `b77b4be` | 2026-04-07 02:03:20.732093
- **Multimodal validation schema:** `mm_contract_dev`

## Query history (org-level, PHI-safe aggregate)

_Last 14 days, filtered to THYROID session/user-agent tokens; no SQL text included._

| user_agent | queries |
|---|---:|
| `duckdb/v1.4.4(osx_arm64) python/3.14` | 1126 |

## Findings (severity-ranked)

- **HIGH:** Multimodal blocker `mm_contract_dev.val_ambiguous_multimodal_linkage_mm_v1` has 3989 rows on [dev, prod, qa] (strict-release expects 0).
- **HIGH:** Multimodal blocker `mm_contract_dev.val_imaging_fna_contract_blockers_mm_v1` has 3093 rows on [prod] (strict-release expects 0).
- **HIGH:** Multimodal blocker `mm_contract_dev.val_nodes_invariant_mm_v1` has 2957 rows on [dev, prod, qa] (strict-release expects 0).
- **HIGH:** Multimodal blocker `mm_contract_dev.val_preop_temporal_order_mm_v1` has 3 rows on [dev, prod, qa] (strict-release expects 0).
- **HIGH:** Multimodal blocker `mm_contract_dev.val_side_lobe_mismatch_mm_v1` has 36 rows on [dev, prod, qa] (strict-release expects 0).
- **MEDIUM:** Multimodal validation object missing on [dev, qa]: `mm_contract_dev.val_contract_required_join_keys_mm_v1` (present elsewhere or not deployed).
- **MEDIUM:** Multimodal validation object missing on [dev, qa]: `mm_contract_dev.val_imaging_fna_contract_blockers_mm_v1` (present elsewhere or not deployed).
- **MEDIUM:** qa: `v2_stage.canonical_extracted_fact_long_v2` not materialized, while dev has staging facts — staging/replica plane may be incomplete for this catalog.
- **MEDIUM:** prod: `v2_stage.canonical_extracted_fact_long_v2` not materialized, while dev has staging facts — staging/replica plane may be incomplete for this catalog.

## Machine-readable metrics

See [`motherduck_read_only_metrics.csv`](motherduck_read_only_metrics.csv).
