# MANUSCRIPT_READY_CHECKLIST — THYROID_2026

One-click verification steps for manuscript submission and peer-review reproducibility.
Run each block from the repo root using `.venv/bin/python`.

---

## 1 · GitHub Tag Verification

```bash
git tag -l v2026.03.10-publication-ready
# Expected output: v2026.03.10-publication-ready

git log --oneline -5
# Most recent commits should include the publication commit

git remote get-url origin
# Expected: https://github.com/ry86pkqf74-rgb/THYROID_2026
```

**Pass criteria:** Tag exists locally and on remote (`git ls-remote --tags origin | grep v2026`).

---

## 2 · Streamlit Dashboard Banner

Open https://thyroid2026-n2hrol9ntiffy4nmedp2zs.streamlit.app/ or locally at http://localhost:8501.

Check for the blue `st.info` banner at the top:
> **Publication-ready v2026.03.10** — local DuckDB backup available · Release Notes

**Pass criteria:** Banner visible on every page load, all tabs render without errors.

---

## 3 · DVC Status

```bash
dvc status
# Expected: "Data and pipelines are up to date."

# Verify sorted parquets are DVC-tracked:
ls processed/*_sorted.parquet.dvc
# Expected: 5 .dvc sidecar files

dvc doctor
# Expected: No version warnings
```

---

## 4 · Local DuckDB Backup

```bash
.venv/bin/python -c "
import duckdb
con = duckdb.connect('thyroid_master_local.duckdb', read_only=True)
tbls = con.execute('SHOW TABLES').fetchdf()
print(f'Tables in local backup: {len(tbls)}')
print(tbls.head(10))
con.close()
"
# Expected: ≥10 tables
```

---

## 5 · Publication Bundle Contents

```bash
ls exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414/
ls exports/FINAL_RELEASE_v2026.03.10_20260310_0529/
ls exports/manuscript_cohort_20260310_0659/

# Verify manifest exists:
cat exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414/manifest.json | python -m json.tool
```

**Pass criteria:** Manifest contains `git_sha`, `row_counts`, and `created_at`.

---

## 6 · MotherDuck Connectivity

```bash
.venv/bin/python -c "
from motherduck_client import get_connection
con = get_connection()
r = con.execute('SELECT COUNT(*) FROM thyroid_research_2026.advanced_features_v3').fetchone()
print(f'advanced_features_v3 rows: {r[0]}')
con.close()
"
# Expected: 11,000+ rows
```

---

## 7 · V2 Pipeline Tables

```bash
.venv/bin/python scripts/30_readiness_check.py --md 2>&1 | tail -20
# Expected: All critical tables present, readiness score ≥90%
```

---

## 8 · Validation Run

```bash
.venv/bin/python scripts/29_validation_runner.py --md --skip-export 2>&1 | tail -10
# Expected: ≥12/14 views pass; reconciliation gap summary printed
```

---

## 9 · Test Suite

```bash
.venv/bin/python -m pytest tests/ -q --tb=short 2>&1 | tail -15
# Expected: All tests pass (or only known-skipped)
```

---

## 10 · Reproducibility Check (Proposal 2 ETE)

```bash
.venv/bin/python studies/proposal2_ete_staging/audit_reproduce.py 2>&1 | tail -10
# Expected: Audit complete, all key statistics within tolerance
```

---

## 11 · CITATION.cff Valid

```bash
python -c "import yaml; d=yaml.safe_load(open('CITATION.cff')); print(d['title'], d['version'])"
# Expected: THYROID_2026 and version string
```

---

## 12 · Trial-Downgrade Readiness

When MotherDuck Business trial expires (~18 days), the app will fall back to:
1. **Local DuckDB:** `thyroid_master_local.duckdb` (set `USE_LOCAL_DUCKDB=1`)
2. **Parquet fallback:** `exports/THYROID_2026_PUBLICATION_BUNDLE_20260310_0414/*.parquet`
3. **DVC pull:** `dvc pull processed/` to restore sorted parquets

```bash
# Test local fallback:
USE_LOCAL_DUCKDB=1 .venv/bin/streamlit run dashboard.py
```

---

## Checklist Summary

| # | Check | Expected |
|---|-------|----------|
| 1 | Git tag | `v2026.03.10-publication-ready` on origin |
| 2 | Dashboard banner | Blue `st.info` banner on all pages |
| 3 | DVC status | Up to date, 5 sorted parquet sidecars |
| 4 | Local DuckDB | ≥10 tables in backup |
| 5 | Publication bundle | `manifest.json` with git SHA |
| 6 | MotherDuck | `advanced_features_v3` ≥11,000 rows |
| 7 | V2 pipeline | Readiness score ≥90% |
| 8 | Validation run | ≥12/14 views pass |
| 9 | Test suite | All pass / known-skipped only |
| 10 | ETE reproducibility | Audit within tolerance |
| 11 | CITATION.cff | Valid YAML with title + version |
| 12 | Trial downgrade | Local fallback tested |

---

*Generated: 2026-03-10 | Repository: THYROID_2026 | Tag: v2026.03.10-publication-ready*
