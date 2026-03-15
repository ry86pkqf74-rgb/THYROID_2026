# MotherDuck Business Optimization — Final Engineering Pass
> Updated: 2026-03-14 | Scripts: 83, 84, 85 | Config: config/motherduck_environments.yml

---

## 1. Environment Strategy

The production THYROID_2026 project uses three MotherDuck Business databases:

| Environment | Database | Purpose |
|-------------|----------|---------|
| **dev** | `thyroid_research_2026_dev` | Iterative development, experimental ETL |
| **qa** | `thyroid_research_2026_qa` | Promotion gate validation |
| **prod** | `thyroid_research_2026` | Live production; served via RO share to Streamlit |

Full configuration: `config/motherduck_environments.yml`

### Promotion Workflow

```
dev  →  (gate-check)  →  qa  →  (gate-check)  →  prod
```

**Gate check command** (script 83):
```bash
.venv/bin/python scripts/83_motherduck_env_strategy.py --mode gate-only
```

**Promote dev → qa**:
```bash
.venv/bin/python scripts/83_motherduck_env_strategy.py --mode promote --from-env thyroid_research_2026_dev --to-env thyroid_research_2026_qa
```

**Promotion gates** (8 tables, minimum row counts):

| Table | Min rows | Purpose |
|-------|---------|---------|
| `master_cohort` | 10,500 | All surgical patients |
| `manuscript_cohort_v1` | 10,500 | Frozen manuscript cohort |
| `operative_episode_detail_v2` | 8,000 | Surgery episodes |
| `rai_treatment_episode_v2` | 800 | RAI episodes |
| `molecular_test_episode_v2` | 9,000 | Molecular test rows |
| `survival_cohort_enriched` | 40,000 | Survival analysis cohort |
| `thyroid_scoring_py_v1` | 10,500 | Scoring per patient |
| `longitudinal_lab_canonical_v1` | 35,000 | Lab canonical layer |

**Metric checks** (canonical values must be in range):

| Metric | Min | Max |
|--------|-----|-----|
| `surgical_cohort` | 10,500 | 11,500 |
| `cancer_cohort` | 3,900 | 4,300 |
| `rai_patients` | 700 | 1,000 |
| `dedup_episodes` | 9,000 | 9,800 |

---

## 2. Duckling Sizing

| Tier | Recommended For | Cost Sensitivity |
|------|----------------|-----------------|
| **Pulse** | Streamlit dashboard (read-only via RO share) | Lowest |
| **Standard** | Script 26 materialization, scripts 71/82, validation runs | Moderate |
| **Jumbo** | Full pipeline rebuild (scripts 22–27), hardening passes (78, 80, 81–85) | Highest — downgrade after run |

**Guidance**: Keep Streamlit Cloud connection using Pulse.  Upgrade to Standard for
any materialization run.  Upgrade to Jumbo only for a full schema rebuild, then
downgrade immediately to avoid burning budget on idle capacity.

---

## 3. Service Account Auth

All scripts use the following token resolution chain:

```python
token = os.environ.get("MOTHERDUCK_TOKEN")
if not token:
    import toml
    token = toml.load(".streamlit/secrets.toml")["MOTHERDUCK_TOKEN"]
```

| Context | Token source |
|---------|-------------|
| Local development | `.streamlit/secrets.toml` |
| GitHub Actions CI | `secrets.MOTHERDUCK` (registered as repo secret) |
| Streamlit Cloud | Streamlit secrets management |

**Never hardcode tokens in script files.**  Plan to migrate from personal tokens to
MotherDuck team/service-account tokens when available in MotherDuck Business.

---

## 4. Query Observability

**Script 84** (`84_query_observability.py`) benchmarks 24 key queries:

- 4 dashboard-facing queries (latency threshold: 10 s)
- 5 materialization queries (latency threshold: 30 s)
- 11 canonical manuscript metric queries (threshold: 5 s)
- 4 dashboard smoke tests

Outputs:
- `val_query_benchmark_v1` table (MotherDuck)
- `exports/final_md_optimization_20260314/query_benchmark_*.csv`

Run periodically after materialization passes to catch query regressions:

```bash
.venv/bin/python scripts/84_query_observability.py --md
```

---

## 5. Materialization Performance Audit

**Script 85** (`85_materialization_performance_audit.py`) audits the 20 hot-path
tables in MATERIALIZATION_MAP:

- Reports table type (TABLE vs VIEW)
- Measures scan latency for each table
- Detects MATERIALIZATION_MAP duplicates (found: `md_imaging_fna_linkage_v3` ×2)
- Generates sort key recommendations based on query patterns

**Known fix applied in this pass**: Removed duplicate `md_imaging_fna_linkage_v3`
entry from MATERIALIZATION_MAP at line 287 in script 26 (first canonical definition
at line 221 is preserved).

Outputs:
- `val_materialization_perf_v1` table
- `exports/final_md_optimization_20260314/materialization_perf_*.csv`
- Sort key recommendation JSON

---

## 6. CI Hardening Summary

`.github/workflows/ci.yml` now contains three jobs:

| Job | Description |
|-----|-------------|
| `lint-and-syntax` | Syntax check all scripts/app/utils/notes_extraction, pyflakes |
| `unit-tests` | Full `pytest tests/` run |
| `motherduck-ci` | Gated by `secrets.MOTHERDUCK`; runs 6 check steps |

**New checks in `motherduck-ci`**:
1. RO share accessible + count > 10,000
2. 12 canonical tables exist
3. 11 manuscript metric ranges (bounds ±10% of frozen values)
4. Zero row multiplication in patient/episode tables
5. Core column non-null thresholds (research_id = 0% null)
6. Dashboard critical-path query smoke tests

**Trigger paths expanded** from `dashboard.py, motherduck_client.py, requirements.txt`
to also trigger on `scripts/**, app/**, utils/**, notes_extraction/**`.

---

## 7. Deployment Order (Updated)

```
22 → 23 → 24 → 25 → 71 (operative NLP) → 26 --md
                             ↓
                     81 (NLP validate) → 82 (provenance) → 83 (env check) → 26 --md
```

After any major deployment, run the full observability suite:
```bash
.venv/bin/python scripts/84_query_observability.py --md
.venv/bin/python scripts/85_materialization_performance_audit.py --md
.venv/bin/python scripts/29_validation_engine.py --md
```

---

## 8. MATERIALIZATION_MAP Changes

**Script 26 updates in this pass**:
- Removed duplicate `("md_imaging_fna_linkage_v3", ...)` entry at former line 287
- Added 7 new `md_val_*` entries for scripts 81–85 outputs:
  - `md_val_operative_nlp_propagation_v1`
  - `md_val_recurrence_provenance_v2`
  - `md_val_rai_provenance_v2`
  - `md_val_molecular_provenance_v2`
  - `md_val_provenance_hardening_summary_v1`
  - `md_val_query_benchmark_v1`
  - `md_val_materialization_perf_v1`

MATERIALIZATION_MAP now contains **138 entries** (was 131 + 7 new = 138; duplicate removed = net 137 unique entries).
