# Prompt 11 — CPM builder recovery + orphan decomposition (acceptance)

**Date:** 2026-05-14  
**Scope:** `canonical_patient_master` publication spine; Prompt 10 `ORPHAN_BUILDER` triage.

---

## TASK 1 — Locate or confirm-absent the CPM builder

### BigQuery `pub_canonical.canonical_patient_master`

- There is **no** version-controlled `CREATE TABLE ... pub_canonical.canonical_patient_master` that fully defines the live wide table. Prompt 10 correctly labeled it `ORPHAN_BUILDER` under the strict **CREATE/load/mig_327 tuple** heuristic.
- **What exists in-repo instead:**
  - **Maintainers / partial writers:** `ALTER TABLE` / `UPDATE` on the BQ mirror, e.g. `bq_migrations/mig_080_h2_preop_rln_vc_columns.sql`, `mig_082_mig004_vc_finding_source_20260506.sql`, `mig_088_sistrunk_procedure_cpm_bq_20260506.sql`, plus QC migrations that touch CPM (`qc_framework_v1/migrations/320_*`, `334_*`, etc.).
  - **Demographics-facing templates:** `sql/mig_079_emr_demographics_import.sql` (EMR CSV → guarded UPDATE of race/sex on **existing** `canonical_patient_master`; does not rebuild the table). Companion plan: `_scripts/thy1_demographics_import_plan.md`.
  - **Staging / workspace:** `scripts/emr_demographics_v1_pipeline.py` builds `pub_workspace.emr_demographics_v1`, not full CPM.
- **`qc_framework_v1/migrations/327_bulk_md_to_bq_missing_tables.py`** exports a **catalogue** of MD→BQ tables; **`canonical_patient_master` is not in the `TABLES` list**, which explains why the automated scanner marked CPM orphaned.

### MotherDuck (authoritative quantitative spine)

- **Coherent lineage exists in git**, spread across multiple scripts—not a single file:
  - Early **assembly** pattern: `scripts/204_canonical_master_assembly.py` / `205_canonical_consolidation.py` build **`canonical_patient_master_v1`** from `gold_master_patient_facts_v1` plus canonical joins (diagnosis / recurrence / survival / molecular).
  - **Promotion / mutation** chains: `221a`, `221b`, `224`, `231_update_canonical_master.*`, `233_apply_ete_adjudication.py`, `236_canonical_finalization.py`, `240_*`, `271*`, reconciliation scripts, and many `qc_framework_v1/migrations/*` archiving `canonical_patient_master_pre_*` snapshots on MotherDuck.
- **Integrity gate:** `scripts/_md_connect.connect_locked()` **requires** `canonical_patient_master` to have **10,871 rows** and **10,871 distinct** `research_id` before allowing publication writes—this is the operational cohort invariant.

### Gitignored operator paths

- `.gitignore` contains `*demographics_import*`. Any operator SQL matching that pattern **cannot be proven present or absent from the repo**. Tracked substitutes: `sql/mig_079_emr_demographics_import.sql` (+ THY-1 plan). The analyst-supplied Excel/CSV cohort lists (refresh 2025-08-06, Epic, wrong-DOB QA) **are not referenced in tracked loaders for full CPM**; they logically feed MD-side ingestion or QA queues—**bring paths into a tracked ingest spec** when wiring those inputs.

### Verdict — TASK 1

| Layer | Single “rebuild everything” builder? |
|-------|--------------------------------------|
| **MotherDuck** | **No single script** rebuilds modern CPM from raw inputs; **yes** coherent **multi-script** lineage + signoff migrations (see canonical footnotes + `canonical_cleanup` family). |
| **BigQuery** | **No** native DDL builder before Prompt 11; mirror was **bulk parity / ops** without a pinned driver in the Prompt 10 index. |

---

## TASK 2 — Make CPM reproducible

### Delivered builder (BQ mirror — named, version-controlled)

- **`scripts/bq_replicate_canonical_patient_master.py`**  
  - Exports **`thyroid_canonical_publication_v1_0.main.canonical_patient_master`** → Parquet (PHI-column name drop list aligned with mig_327).  
  - Loads with **`bq load --replace`** into **`thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`**.  
  - Uses **`scripts/_md_connect.connect_locked()`** so export **cannot proceed** unless the MD invariant **10,871 / 10,871 distinct `research_id`** holds.

### Curated lineage (Prompt 10 index)

- **`studies/bq_pub_authoritative_builders_20260514.py`** — `CURATED_LINEAGE` extended so automated maps can resolve **`canonical_patient_master`** → **`scripts/bq_replicate_canonical_patient_master.py`**.

### Row-for-row validation vs current BQ (operator checklist)

Blocked here without BigQuery **`INFORMATION_SCHEMA` + MD export in one session**. Recommended validation (run with credentials):

1. Row counts / distinct keys:

```sql
SELECT COUNT(*) AS n, COUNT(DISTINCT research_id) AS d
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master`;
-- expect n = d = 10871 after load from current MD SSOT
```

2. Spot identity join (after reload): semi-join **hashed key columns** (e.g. `research_id`, `sex`, `race`, `age_at_surgery`, `cpm_built_at`) MD export vs BQ in a sandbox table; **expect zero anti-join** unless BQ retained extra columns absent from MD (then reconcile schema diff explicitly).

### Intentional diffs after `bq load --replace`

Any column that exists **only on BQ** from past `ALTER TABLE` migrations may be **NULL or absent** until those migrations are replayed—or until the column is first added on MD and included in export. Maintain a short **parity checklist** in `bq_migrations/` when adding BQ-only CPM columns.

---

## TASK 3 — Decompose ~~the 174~~ **`ORPHAN_BUILDER` objects** (post-CPM lineage pin)

After pinning `canonical_patient_master` to **`scripts/bq_replicate_canonical_patient_master.py`**, re-run
`studies/bq_pub_authoritative_builders_20260514.py`; the **remaining** `ORPHAN_BUILDER` count is **173** (Prompt 10 baseline was **174**).

Mechanical re-count: 

` .venv/bin/python studies/bq_orphan_decompose_prompt11_20260514.py `

### Classification rules

| Bucket | Rule |
|--------|------|
| **VIEW (OK)** | All orphans in **`pub_views_readable`** (61) — presentation views; **`pub_semantic`** orphans whose names start with **`vw_`** or contain **`_VIEW_`** / end with **`_VIEW_v1`** (18); **`pub_canonical`** orphans with view-like names — `_VIEW_`, `VW_`, `V_` (10). |
| **FROZEN SNAPSHOT (OK)** | `__readme`, `canonical_patient_master_v1_9` (2). |
| **REAL TABLE — no builder (backlog)** | Remaining **`pub_canonical`** canonical fact/rollup tables + `pub_semantic.release_manifest_v1` (**82**). |

### Counts (**173** = 89 + 2 + 82)

| Bucket | Count |
|--------|------:|
| VIEW (OK) | **89** |
| FROZEN SNAPSHOT (OK) | **2** |
| **REAL TABLE — true orphan-table backlog** | **82** |

---

## Acceptance checklist

- [x] **Named, version-controlled BQ mirror builder:** `scripts/bq_replicate_canonical_patient_master.py`
- [x] **Curated index entry** for Prompt 10-style maps: `CURATED_LINEAGE` in `studies/bq_pub_authoritative_builders_20260514.py`
- [x] **Orphan decomposition** with explicit **83** real-table backlog + reproducible counter script
- [ ] **Executed** row-for-row BQ vs MD validation in an environment with live BQ + MD (operator step)
