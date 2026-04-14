> **HISTORICAL / SUPERSEDED:** This document is a point-in-time snapshot from its generation date. For current canonical state, see [`docs/final_source_of_truth_contract.md`](../../docs/final_source_of_truth_contract.md) and [`studies/CURRENT_MOTHERDUCK_REPO_STATE.md`](../CURRENT_MOTHERDUCK_REPO_STATE.md). Row counts cited here may no longer match live MotherDuck.

# Specimen + analytic FHIR release truth — 2026-04-08

## Git + catalog

| Field | Value |
|-------|-------|
| **git SHA (MotherDuck query + `119` run)** | `c6ef439583320aebbff811d1865e4b38a0075aa4` |

_Recording commit for this folder: use `git log -1 -- studies/specimen_fhir_release_truth_20260408T065318Z` after pull._
| **current_database()** | `Thyroid 2026` |
| **Writer UA / session** | `specimen_fhir_release_truth_v1` (see `utils/md_pipeline_attribution.specimen_fhir_release_writer_attribution`) |

## CREATE SNAPSHOT attempt (writer catalog)

- **Command-style probe:** `CREATE SNAPSHOT … OF "Thyroid 2026";`
- **Result:** **Not supported** — `InvalidInputException: Database is not a native duckdb database so it does not have snapshots`
- **Interpretation:** Catalog behaves as **MotherDuck-managed / non-native** for named snapshots; continue using repo **full-rebuild / OR REPLACE** posture (per `138` / contract docs).

## Live qa.release_manifest (latest by `created_at`, top 3)

From `144` live introspection:

| release_tag | git_sha | created_at |
|---------------|---------|------------|
| `20260408r3` | `a593544` | 2026-04-08 05:20:40.752314 |
| `20260408r2` | `a593544` | 2026-04-08 05:18:04.189662 |
| `20260411` | `de13c33` | 2026-04-07 19:15:39.106720 |

**Note:** `119` Check 9 still reports “latest: 20260411” by its internal ordering rule; **timestamp ordering** on this catalog shows **`20260408r3`** as the most recent row. Operators should treat **both** tags as live and reconcile ordering semantics in `119` if needed.

## Check 13 (specimen + analytic FHIR) — PASS / WARN / FAIL

From `119_md_formalization_validate.py --md --release-mode` (this run):

| Row | Status | Detail |
|-----|--------|--------|
| Specimen/FHIR tables present | **PASS** | 10 objects found |
| Specimen master fingerprint uniqueness | **PASS** | distinct fingerprints |
| qa.val_specimen_contract_v1 | **PASS** | no FAIL rows |
| qa.val_specimen_genomic_binding_v1 | **PASS** | no FAIL rows |
| Specimen/FHIR QA diagnostics (142) | **PASS** | clean |
| Specimen-adjacent review burden | **WARN** | genomic_link_review open/pending=10705; specimen_merge_review open/pending=1 |

**Other release-signoff rows (same `119` run):** **PASS** on release manifest, presentation views (`master_*_verified_v1`), molecular contract (with **WARN** on assay/panel pairing and dictionary match), MRQ governance. **Verdict:** `PASS WITH WARNINGS` (3 WARN total across full suite; 0 FAIL).

## Row counts (main)

| Table | Rows |
|-------|-----:|
| specimen_master_v1 | 10,139 |
| specimen_tumor_focus_v1 | 11,103 |
| specimen_genomic_assay_v1 | 10,862 |
| fhir_bundle_specimen_export_v1 | 10,139 |

## Checked-in vs live manifests

- **GitHub:** `exports/release_manifests/LATEST_MANIFEST.json` remains **`release_8c18892_20260315_170027`** (point-in-time manuscript-era bundle).
- **Live:** `qa.release_manifest` rows above — **do not** assume `LATEST_MANIFEST.json` matches MotherDuck; use live `qa.release_manifest` or regenerate exports when cutting a new frozen bundle.

## Telemetry (`MD_INFORMATION_SCHEMA`)

- **QUERY_HISTORY:** Top `user_agent` values in this environment were **default DuckDB driver strings** (e.g. `duckdb/v1.4.4…`), not `specimen_fhir_release_truth_v1`.
- **RECENT_QUERIES** filtered by `user_agent = 'specimen_fhir_release_truth_v1'`: **no rows returned** in the sampled window (either propagation delay, aggregation policy, or custom UA not stored in these views the same way).
- **Conclusion:** Custom UA is set on connections per `motherduck_client`; query-history excerpts may not expose it reliably — do not invent telemetry.

## Remaining blockers (honest)

1. **Manuscript / governance:** Synthetic/automation-only MRQ postures **do not** equal human sign-off; README posture unchanged on that axis.
2. **Operational review burden:** WARN on genomic link review queue volume; specimen merge queue small (1 open/pending).
3. **Molecular dictionary / panel_version:** non-blocking WARNs in Check 12.
4. **Named DB snapshots:** not available on this catalog — rely on `release_*` schemas + `qa.release_manifest` + procedural rebuilds.

## Commands run (exact)

```bash
cd /Users/loganglosser/THYROID_2026
TS=20260408T065318Z
STUDY="studies/specimen_fhir_release_truth_${TS}"
mkdir -p "$STUDY"

.venv/bin/python scripts/144_md_repo_current_state_summary.py --md \
  --output "$STUDY/motherduck_repo_state.md"
cp "$STUDY/motherduck_repo_state.md" studies/CURRENT_MOTHERDUCK_REPO_STATE.md

.venv/bin/python scripts/119_md_formalization_validate.py --md --md-sa --release-mode \
  --md-user-agent specimen_fhir_release_truth_v1 \
  --md-session-hint specimen_fhir_release_truth_v1 \
  --output-dir "$STUDY/119_release_validation"
# Console log: $STUDY/119_console.log (see repo)

# Post-template refresh of 144 (after script edits):
.venv/bin/python scripts/144_md_repo_current_state_summary.py --md \
  --output "$STUDY/motherduck_repo_state.md"
cp "$STUDY/motherduck_repo_state.md" studies/CURRENT_MOTHERDUCK_REPO_STATE.md
```

**Remediation not required this session:** Check 13 **PASS**; `143` / `138` not re-run.

## Related artifacts

- `motherduck_repo_state.md` — live counts + manifest excerpt
- `119_release_validation/validation_report.md` — full 39-check report
- `119_console.log` — stdout from validator
