# Master Verification Plan — `thyroid_canonical_publication_v1_0`

**Authored:** 2026-04-27  by Logan Glosser (drafted with Claude / Cowork)
**Last commit:** see `git log --oneline qc_framework_v1/MASTER_VERIFICATION_PLAN.md`
**Goal:** every column in every base table of `main` and `manuscript_workspace` is verified against the source-of-truth Excel/notes data. The result is a fully clean canonical V1_0 database that is the trusted substrate for ALL downstream manuscripts (ETE, recurrence, molecular, ATA risk, surveillance, complications, etc.).

This document is the canonical reference for the verification effort. It supersedes the per-manuscript adjudication patterns in earlier migrations (mig_61, mig_61c, mig_62) — those are preserved as intermediate state, but the new ground rules are everything below.

---

## 1. Scope

### In scope

- `main` base tables: **105 tables** containing manuscript-relevant data
- `manuscript_workspace` base tables: **79 tables** for adjudication queues, audits, and persisted analyses
- **Total in scope: 184 base tables / 5,496 in-scope columns** (after excluding `_archived_*`, `_pre_cleanup_*`, `_legacy*`, `_pre_*` snapshots)

### Out of scope

- **Views** in any schema. Views are derived; they auto-correct when their base tables are correct. We will spot-check views as a final acceptance step but they are not individually adjudicated.
- `archive_*`, `*_legacy`, `views_readable`, `raw`, `cpm_tirads_legacy`, `tier2_legacy`, `molecular_legacy`, `us_legacy`, `verify_legacy`, `note_entities_llm_legacy`, `manuscript_workspace_legacy`, `llm_invasion_legacy`. These are deprecated snapshots.

---

## 2. Source of truth

The canonical sources from which this DB was loaded:

| Source file | DB mirror |
|---|---|
| `All Diagnoses & synoptic 12_1_2025.xlsx` (sheet `synoptics + Dx merged`) | `main.path_synoptics` |
| `Notes 12_1_25.xlsx` | `main.clinical_notes_long`, `main.note_entities_llm_*` |
| Imaging Excel exports | `main.ct_imaging`, `main.mri_imaging`, `main.nuclear_med`, `main.canonical_us_*_v2` |
| NSQIP enrichment | `main.nsqip_enrichment`, `main.nsqip_patient_summary` |
| Lab CSVs | `main.canonical_labs_*_v1`, `main.tg_*` |
| RAI episodes | `main.rai_treatment_episode_v2` |

When verifying a `source` column, the source file (not the DB) is authoritative. When verifying a `derived` column, the upstream column is authoritative.

---

## 3. Verification categories

Every column in scope is auto-classified into one of four categories:

| Category | Definition | Verification method |
|---|---|---|
| **`na_provenance`** | Build/audit metadata, identifiers, source pointers (`build_script`, `*_id`, `source_workbook`, `extracted_at`, etc.) | Auto-skipped. Marked `na` immediately. |
| **`derived`** | Value is a deterministic function of other columns in the same DB (e.g., `*_clean`, `*_normalized`, `*_final`, `is_*`, `has_*`, `days_to_*`, `n_*`, `worst_*`, `_v2`/`_v3` versions) | Re-run the derivation rule; verify it matches stored value for 100 % of rows. Certify the rule once → all rows verified. If mismatches found, flip those rows to `adjudicated`. |
| **`source`** | Raw value loaded directly from upstream Excel/CSV/note (e.g., `*_raw`, `*_text`, `*_native`, columns in `path_synoptics`, `ct_imaging`, etc.) | Sample 10–20 rows; output a CSV showing DB value vs raw source text. Logan eyeballs. If 100 % match → mark verified. If mismatches → flip to `adjudicated`. |
| **`adjudicated`** | Value requires clinical judgment, cleanup, or LLM extraction (everything not in the above three) | Per-row review. Claude generates CSV (research_id, current_value, source_field, evidence_snippet, your_corrected_value, your_note). Logan fills from source. Claude bulk-updates DB, verifies, commits a sign-off migration. |

### Auto-classification rules (in priority order)

```
1. na_provenance: column_name IN <provenance list> OR column_name = 'research_id'
                  OR column_name LIKE '%_id' OR column_name LIKE '%_uid'
2. derived: column_name LIKE one of these patterns:
   '%_clean', '%_normalized', '%_derived', '%_resolved', '%_recomputed',
   '%_calculated', '%_grouped', '%_final', '%_v2'..'%_v5', '%_rebound',
   '%_rebind', 'days_to_%', '%_days', 'is_%', 'has_%', 'any_%', 'n_%',
   '%_score', '%_summary', '%_status', '%_count', 'worst_%'
3. source: column_name LIKE '%_raw', '%_text', '%_native', '%_quote',
   'evidence_%', 'reasoning', '%_findings', 'original_%'
   OR table is a raw mirror (path_synoptics, ct_imaging, mri_imaging, nuclear_med,
   manuscript_cohort_v1, clinical_notes_long, data_dictionary_v279,
   nsqip_enrichment, nsqip_patient_summary, rai_treatment_episode_v2, note_entities_*)
4. adjudicated: everything else
```

The classifier intentionally errs on the side of `adjudicated` — better to over-flag than miss a clinically meaningful column.

---

## 4. Registry tables (the source of truth for verification status)

### `main.canonical_column_verification_registry_v1` — one row per column

```
schema_name, table_name, column_name, data_type, ordinal_position
category               -- source | derived | adjudicated | na_provenance
upstream_source        -- if derived, which base column
verification_status    -- not_started | in_review | verified | failed | na
verified_by            -- 'auto' | 'logan' | 'claude_inline' (legacy)
verified_ts
verification_method    -- e.g., auto_provenance_skip, sample_10_rows, csv_review_full,
                       --       derivation_rule_certified, etc.
batch_id               -- groups columns we adjudicate together (e.g., 'mig_64_fna_pilot')
notes
registered_ts
```

### `main.canonical_table_signoff_registry_v1` — one row per table

```
schema_name, table_name, n_columns_total, n_verified, n_not_started, n_failed, n_na
table_status           -- in_progress | verified | blocked | not_started
signed_off_ts
signoff_migration      -- e.g., 'qc_framework_v1/migrations/64_table_signoff_canonical_fna_events_v1.sql'
priority_tier          -- pilot | tier1_anchor | tier1_events | tier1_source |
                       --   tier2_canonical | tier2_rollups | tier3_extraction | tier3_helper
notes
registered_ts
```

### Update rule

Every adjudication batch ends with:
1. `UPDATE canonical_column_verification_registry_v1 SET verification_status='verified', verified_by='logan', verified_ts=CURRENT_TIMESTAMP, batch_id='<batch>' WHERE ...`
2. `UPDATE canonical_table_signoff_registry_v1` recompute counts.
3. If table is now fully verified, set `table_status='verified'`, `signed_off_ts`, `signoff_migration`.

---

## 5. Priority order

Tables are processed in this sequence:

| Tier | Tables | Cols | Why this order |
|---|---|---|---|
| **pilot** | 1 (`canonical_fna_events_v1`) | 40 | Pilot the workflow on a manuscript-critical, well-bounded table before scaling |
| **tier1_anchor** | 1 (`canonical_patient_master`) | 1,592 | Used by every downstream view/manuscript. Most cols here are sub-classified into derived (auto) once we tier them. Real adjudication load probably ~50–100 cols. |
| **tier1_events** | 18 `canonical_*_events_v1` | 466 | Event-grain truth. Verifying these auto-verifies most of tier2_rollups. |
| **tier1_source** | 12 (`path_synoptics`, `ct_imaging`, `mri_imaging`, `nuclear_med`, `clinical_notes_long`, `manuscript_cohort_v1`, `rai_treatment_episode_v2`, `canonical_us_*_v2`, `nsqip_*`) | 909 | Raw mirrors. Mostly `source` category — sample-based verification. |
| **tier2_canonical** | 16 (`canonical_*` not in tier1) | 333 | Misc canonical (specimen, complications, labs, medications, pmh, psh, molecular, frozen section, T4b, vascular, airway, esophageal, etc.) |
| **tier2_rollups** | 19 `canonical_*_patient_rollup_v1` | 616 | Patient-level rollups. Most cols are `derived` once we tier them — auto-verify when their event table is verified. |
| **tier3_extraction** | 17 `note_entities_*` | 372 | LLM extraction outputs. Mostly `na_provenance`. The `parsed_json` and `result_json` are not row-by-row verified — they're audited via the canonicals they feed. |
| **tier3_helper** | 91 (queues, audits, validation, dive maps, etc.) | 1,168 | Workspace helpers. Often skip-eligible — many will be reclassified `na` after one look. |

Within each tier, tables are processed alphabetically except where a known dependency requires re-ordering (e.g., process events before their rollups).

---

## 6. Per-table workflow

For each table, in order:

### Step A — Re-tier the columns (15 min, Claude)

The auto-classification is a starting point. Claude reviews each column in the table and refines:
- Reclassifies `adjudicated` → `derived` where the column is actually a deterministic function of another column.
- Reclassifies `adjudicated` → `na_provenance` where the column is metadata not surfaced in this rule (e.g., new audit columns added after the rules were written).
- Adds `upstream_source` pointers for `derived` columns.

Logan signs off the tier in 1 minute (yes / fix-this / fix-this).

### Step B — Verify the easy categories automatically

- `na_provenance` → marked `verified` immediately, `verified_by='auto'`, `verification_method='auto_provenance_skip'`.
- `derived` → Claude runs the derivation rule against the stored value for 100 % of rows. If it matches → mark verified, `verification_method='derivation_rule_certified'`. If mismatches found → those specific rows flip to a `derived_failed` status and become `adjudicated`.
- `source` → Claude pulls 10 random rows + 10 NULL/edge-case rows (20 total), generates a CSV showing DB value vs raw source text. Logan eyeballs (~3 min). If all match → mark verified. If mismatches → flip those rows to `adjudicated`.

### Step C — Adjudicate the hard category

For each `adjudicated` column:

1. **Claude generates the CSV** at `verification_csvs/<table>/<column>_<batch_id>.csv`:

   ```csv
   research_id,current_value_in_db,source_sheet,source_column,evidence_snippet,your_corrected_value,your_note
   ```

   Cap at 200 rows per CSV. If a column has more, partition by sub-cohort or by current_value (so Logan can review systematically).

2. **Logan fills the CSV** from the source Excel.

3. **Claude bulk-updates** MotherDuck:
   - Loads the filled CSV
   - Updates rows where `your_corrected_value IS NOT NULL` and differs from `current_value_in_db`
   - Inserts an audit row in `manuscript_workspace.canonical_logan_review_log_v1` for every change
   - Updates the column's verification_status to `verified`, `verified_by='logan'`, `verification_method='csv_review_full'`, `batch_id`

4. **Claude commits** `qc_framework_v1/migrations/NN_logan_verified_<table>_<column>.sql`.

### Step D — Sign off the table

When all columns in the table are `verified` or `na`:
1. Update `canonical_table_signoff_registry_v1.table_status = 'verified'`.
2. Write `qc_framework_v1/migrations/NN_table_signoff_<table>.sql` that documents the sign-off.
3. Append the table to `qc_framework_v1/VERIFIED_TABLES.md` (append-only log).

---

## 7. Audit log

Every Logan-driven correction is logged to `manuscript_workspace.canonical_logan_review_log_v1`:

```
log_id (auto), research_id, table_name, column_name,
old_value, new_value, batch_id, change_ts,
csv_path, logan_note
```

This gives a complete reversible record. If Logan later changes his mind about a value, we can trace exactly when and why every cell was modified.

---

## 8. Stopping rules

The verification effort is **complete** when:

1. Every column in the registry has `verification_status` IN (`verified`, `na`).
2. Every table in the sign-off registry has `table_status = 'verified'`.
3. The `qc_framework_v1/VERIFIED_TABLES.md` file lists all 184 tables.
4. A final acceptance script (`qc_framework_v1/scripts/final_acceptance_verify.py`) runs:
   - All views in `main` and `manuscript_workspace` execute without error
   - Spot-checks 100 random rows across 20 random analytic views vs. their underlying tables
   - Re-runs every `derivation_rule_certified` rule and confirms 100 % match

When all four conditions pass, we tag the database as `thyroid_canonical_publication_v1_0_verified` and the verification effort is closed. The dashboard is locked.

---

## 9. Effort estimate

| Activity | Estimated time |
|---|---|
| Pilot table (`canonical_fna_events_v1`) | 1 session, ~1 hour |
| Per `tier1_events` table | 30–60 min |
| `canonical_patient_master` (tier1_anchor) | 4–6 sessions, possibly more |
| Per `tier1_source` table | 30–45 min (mostly sample-based) |
| Per `tier2_canonical` table | 30 min |
| Per `tier2_rollups` table | 15–20 min (mostly auto-verified once events are done) |
| Per `tier3_extraction` table | 15 min |
| Per `tier3_helper` table | 10–20 min (many will be skip-eligible) |
| Final acceptance run | 1 session |

**Rough total: 80–120 sessions** spread over many weeks/months. Each session is bounded — Logan can stop after any single column or table is verified without leaving inconsistent state.

---

## 10. Progress dashboard

`qc_framework_v1/VERIFICATION_PROGRESS.md` is auto-regenerated each session and committed. It shows:

- Total: X / 184 tables verified, Y / 5,496 columns verified
- Per-tier rollup
- Next-up table queue (top 5)
- Failed/blocked items needing attention

---

## 11. What this plan replaces

- The ad-hoc per-manuscript adjudication used in mig_61c (ETE inline review) and mig_62 (recurrence dual-track) — those migrations remain in the DB and are auditable, but their results will be re-verified under this plan when their respective tables come up in the queue. Specifically:
  - `main.canonical_ete_event_resolved_v1.ete_grade` — re-verified when `canonical_path_malignant_events_v1.extrathyroidal_extension` and `canonical_patient_master.ete_grade_clean` are verified.
  - `main.canonical_recurrence_resolved_v1` — re-verified when its source tables are verified.
- The Cursor LLM-batch prompts (mig_63, mig_64, mig_65) are deprioritized. We will only run an LLM batch if a `source` or `derived` column fails its automated check AND manual review by Logan would be impractical for the volume.

---

## 12. Glossary

- **Canonical column**: a column in `main.canonical_*` or other `main`/`manuscript_workspace` base table that downstream views and manuscripts rely on.
- **Source column**: a column whose value was loaded directly from an Excel/CSV file or note. The Excel file is the authority.
- **Derived column**: a column whose value is computed deterministically from other columns in the DB. The derivation rule is the authority.
- **Adjudicated column**: a column whose value requires clinical judgment, free-text interpretation, or NLP extraction. Logan is the authority.
- **Provenance column**: a column carrying build metadata, source pointers, identifiers, or audit timestamps. Not subject to clinical verification.
- **Verified**: status set when a column's contents have been confirmed to match its authority for 100 % of rows under the appropriate verification method.
- **Sign-off**: when all columns in a table reach `verified` or `na`, the table itself is signed off and a migration documents the closure.

---

## 13. Workflow files (where things live)

```
qc_framework_v1/
├── MASTER_VERIFICATION_PLAN.md             ← this document
├── VERIFICATION_PROGRESS.md                ← auto-generated dashboard
├── VERIFIED_TABLES.md                      ← append-only log
├── migrations/
│   ├── NN_logan_verified_<table>_<col>.sql ← per-batch Logan adjudications
│   ├── NN_table_signoff_<table>.sql        ← per-table sign-off
│   └── ...
└── scripts/
    ├── refresh_verification_registry.sql   ← re-pulls information_schema and re-classifies
    ├── refresh_progress_dashboard.py       ← regenerates VERIFICATION_PROGRESS.md
    └── final_acceptance_verify.py          ← runs at completion

verification_csvs/
├── <table>/
│   ├── <column>_<batch_id>.csv             ← Claude-generated, Logan-filled
│   └── ...
```

---

## 14. Initial registry seed (committed at plan creation)

The DB-side registries `main.canonical_column_verification_registry_v1` and
`main.canonical_table_signoff_registry_v1` were created and populated
2026-04-27 with the auto-classification described above. Initial counts:

| category | n_columns | initial_status |
|---|---|---|
| `adjudicated` | 2,834 | `not_started` |
| `derived` | 1,150 | `not_started` |
| `source` | 745 | `not_started` |
| `na_provenance` | 767 | `na` (auto-verified) |
| **total** | **5,496** | — |

| priority_tier | n_tables | total_cols | not_started_cols |
|---|---|---|---|
| pilot | 1 | 40 | 35 |
| tier1_anchor | 1 | 1,592 | 1,588 |
| tier1_events | 18 | 466 | 320 |
| tier1_source | 12 | 909 | 860 |
| tier2_canonical | 16 | 333 | 282 |
| tier2_rollups | 19 | 616 | 569 |
| tier3_extraction | 17 | 372 | 63 |
| tier3_helper | 91 | 1,168 | 1,012 |
| **total** | **175** | **5,496** | **4,729** |

(Note: 175 tables in registry vs 184 tables in scope = 9 tables had only provenance/identifier
columns and were auto-fully-verified at registration time.)

---

## 15. Sign-off

The verification effort begins with the pilot table immediately after this plan
lands in the repo. The pilot establishes the workflow rhythm; subsequent tables
inherit that rhythm.

Final sign-off of the entire effort produces a tagged database release:
`thyroid_canonical_publication_v1_0_verified` (snapshot in MotherDuck +
backed-up Parquet in S3 if available).

— end of plan —
