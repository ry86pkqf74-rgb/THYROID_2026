# Canonical Metrics Governance Framework

**Effective:** 2026-03-15  
**Owner:** THYROID_2026 Data Team  
**Scope:** All manuscript-facing documents, release artifacts, dashboard summaries, and supplement tables

---

## 1. Purpose

This document establishes the governance rules for the THYROID_2026 canonical metrics registry. It ensures that every number cited in manuscripts, dashboards, release notes, and supplement tables originates from a **single, version-controlled, MotherDuck-verified** source of truth.

## 2. Architecture

```
┌────────────────────────────┐
│  MotherDuck (prod)         │
│  canonical_metrics_        │
│  registry_v1               │ ← Single table, ~40 metrics
└───────────┬────────────────┘
            │ scripts/100_canonical_metrics_registry.py
            ▼
┌────────────────────────────┐
│  Exports (frozen)          │
│  exports/canonical_metrics │
│  _registry_YYYYMMDD_HHMM/ │ CSV + JSON + manifest.json
└───────────┬────────────────┘
            │
            ▼
┌────────────────────────────┐
│  Consumers                 │
│  • dashboard.py            │
│  • README.md               │
│  • docs/*.md               │
│  • release manifests       │
│  • manuscript tables       │
└────────────────────────────┘
```

## 3. Metric Lifecycle

### 3.1 Registration

Every metric must be registered in `METRIC_DEFS` within `scripts/100_canonical_metrics_registry.py`. A metric definition includes:

| Field | Required | Description |
|---|---|---|
| `metric_id` | ✓ | Stable snake_case identifier, never renamed |
| `metric_name` | ✓ | Human-readable name |
| `metric_group` | ✓ | Grouping category |
| `canonical_sql` | ✓ | Exact SQL returning a single value |
| `canonical_table` | ✓ | Primary source table |
| `numerator_def` | ✓ | What the numerator counts |
| `denominator_def` | ✓ | What the denominator counts, or "N/A" |
| `use_tier` | ✓ | `primary` / `descriptive` / `sensitivity` / `prohibited` |
| `source_limitation` | | Description of any known data gap |

### 3.2 Verification

- Run `scripts/100_canonical_metrics_registry.py --env prod --write` to live-verify all metrics.
- Registry table stores `last_verified_at` and `git_sha` for provenance.
- **Staleness rule:** a registry older than 7 days triggers a WARN gate in release promotion.

### 3.3 Promotion

```
dev  ──── 100 --env dev --write ────►  Verify all metrics resolve
  │
qa   ──── 100 --env qa --write  ────►  Compare dev ↔ qa counts
  │
prod ──── 100 --env prod --write ───►  Final canonical values
```

## 4. Use Tier Rules

| Tier | Where it may appear | Review required |
|---|---|---|
| `primary` | Manuscript abstract, tables, figures, key results | No — auto-approved |
| `descriptive` | Methods, supplement, internal dashboard | No |
| `sensitivity` | Sensitivity analyses only | Must note conditional use |
| `prohibited` | NOWHERE — known-bad or superseded value | Must not appear anywhere |

### 4.1 Prohibition Enforcement

When a metric's `use_tier` is changed to `prohibited`, all consumers must be audited. The prior value must be replaced with a reference to the correcting metric.

## 5. Drift Detection

### 5.1 Continuous Checks

`check_metric_drift()` in `100_canonical_metrics_registry.py` compares live MotherDuck values against the materialized registry:

- **tolerance:** 1% for row-count metrics, 0% for molecular flags
- **PASS:** within tolerance
- **DRIFT:** exceeds tolerance → blocks release
- **ERROR:** query fails → blocks release

### 5.2 Integration Points

| Consumer | Enforcement mechanism |
|---|---|
| Release gate (91) | Calls `check_metric_drift()` as G7 |
| Dashboard overview | Reads from `canonical_metrics_registry_v1` table |
| Manuscript freeze (90) | Validates key counts against registry |
| CI/CD | Can import and call `check_metric_drift()` |

### 5.3 Staleness Checks

`check_staleness_days()` returns STALE if the registry was last verified more than `max_days` ago (default: 7). Integrated into release promotion gates.

## 6. Cross-Source Discrepancy Resolution

Known discrepancies and their canonical resolution:

| Metric | Incorrect Source | Canonical Source | Note |
|---|---|---|---|
| BRAF+ | `extracted_braf_recovery_v1` (376) | `patient_refined_master_clinical_v12` (546) | FP-corrected + multi-source |
| RAS+ | `extracted_molecular_refined_v1` (292) | `patient_refined_master_clinical_v12` (337) | Phase 11+13 recovery |
| TERT+ | `extracted_molecular_refined_v1` (96) | `patient_refined_master_clinical_v12` (108) | mol_test_episode_v2 propagation |

**Rule:** The `patient_refined_master_clinical_v12` is always authoritative for molecular marker counts.

## 7. Adding a New Metric

1. Add a `MetricDef(...)` to `METRIC_DEFS` in `scripts/100_canonical_metrics_registry.py`.
2. Run `--env dev --write` to verify.
3. Promote through qa → prod.
4. Update any consumer docs that reference the new metric.
5. Commit with message pattern: `feat(metrics): add {metric_id} to canonical registry`.

## 8. Removing a Metric

1. Set `use_tier` to `prohibited`.
2. Add a note in `source_limitation` explaining why.
3. Do NOT delete the `MetricDef` — keep it for audit trail.
4. Update consumers to remove references.

## 9. Emergency Override

If a metric must be updated mid-release (e.g., data correction):

1. Update `SOURCE SQL` in the MetricDef.
2. Run `--env prod --write` with explicit `--write` flag.
3. Document the override in RELEASE_NOTES.md.
4. Re-run release gate (`scripts/91_promotion_gate.py --env qa --target prod`).

---

*This governance framework is enforced by `scripts/100_canonical_metrics_registry.py` and integrated into the promotion/release pipeline via gate G7.*
