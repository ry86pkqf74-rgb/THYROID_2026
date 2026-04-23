# Script 398 — Phase 0 probe (CPM T/N/M cross-source disagreement audit)

## Halt gates (H1–H8)

| all_pass | True |

## Counts (must match for apply)

- **T disagreements:** 363 (expected 363)
- **N disagreements:** 2055 (expected 2055)
- **M disagreements:** 1838 (expected 1838)
- **Total:** 4256 (expected 4256)
- **CPM total (H4):** 10871
- **manuscript_workspace (H2):** present
- **Target table present:** False  rows=-1  idem=False
- **H5 (no CPM UPDATE in apply SQL):** True
- **H6 (column list vs spec if table exists):** True
- **H7 duplicate risk (0 required):** 0
- **H8 dominant columns:** True

## DTC vs non-DTC (PTC,FTC,HCC,DTC_NOS) — per-axis disags

- **dtc_t:** 317
- **non_t:** 46
- **dtc_n:** 1900
- **non_n:** 155
- **dtc_m:** 1689
- **non_m:** 149

## Top disagreement patterns (current CPM) — T / N / M

### Axis T
- `T3a↔T3b`: 135
- `T3b↔T4a`: 64
- `T1a↔T3a`: 31

### Axis N
- `N1a↔Nx`: 1423
- `N1a↔N1b`: 541
- `N0↔Nx`: 63

### Axis M
- `M0↔M1`: 1838

## Planned materialization (read-only; no CPM writes)

```sql
CREATE TABLE "thyroid_canonical_publication_v1_0"."manuscript_workspace"."cpm_tnm_cross_source_disagreements_v1" (
  research_id           VARCHAR NOT NULL,
  diagnosis_primary     VARCHAR,
  age_at_surgery        BIGINT,
  axis                  VARCHAR NOT NULL,
  primary_value         VARCHAR NOT NULL,
  v2_value              VARCHAR NOT NULL,
  dominant_value        VARCHAR,
  disagreement_pattern  VARCHAR NOT NULL,
  current_stage_group   VARCHAR,
  stage_group_corrected  VARCHAR,
  path_stage_raw        VARCHAR,
  snapshot_ts           TIMESTAMP NOT NULL,
  PRIMARY KEY (research_id, axis)
);

-- INSERTs T, N, M (see script constants INSERT_T_SQL / N / M)
```

---HASH-BOUNDARY---

## Generation footer (excluded from PROBE_REPORT_SHA256)

Written UTC: 2026-04-23T03:28:01.713822+00:00
