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

## 6. Per-table workflow (Protocol v2 — full-row mechanical compare)

> **Protocol revision 2026-04-27.** The four-tier shortcut documented in v1 (auto-skip
> provenance, derivation-rule-certify derived, sample-verify source, CSV-review-only
> adjudicated) is **deprecated**. Logan's directive: every cell of every column is
> verified against the original source workbook. The tier system survives only as a
> way to choose the verification *method* per column. See §6a for the protocol history.

For each table, in order:

### Step A — Map columns to source workbook + assign verification method (15 min, Claude)

Claude assigns each column to one of four verification methods:

| Method | Applies to | What Claude generates |
|---|---|---|
| `auto_no_source_counterpart` | Pure-provenance columns (identifiers, build-script outputs, ingest timestamps) AND pipeline-trace columns (`*_confidence`, `*_status`, `*_derivation_method`, `*_provider`, `*_evidence_present`, `*_rules_*`, `*_text_length`) — i.e., any column whose value is generated by the build/extraction pipeline and has no corresponding cell in the source Excel. | No CSV. **These columns remain `not_started` throughout per-column verification and are flipped to `verified` (`verification_method='auto_no_source_counterpart'`) only at Step D table sign-off, after every source/derived/adjudicated column in the table is verified.** This keeps the dashboard honest about what's been clinically reviewed and what's been auto-rubber-stamped. |
| `mechanical_source_compare` | `source` columns — values loaded directly from a source Excel cell. | Per-column CSV: every DB row joined to its source-Excel cell on a stable join key (e.g., `research_id` + episode index). String compare (with documented normalization rules) yields `MATCH` / `MISMATCH` / `NO_SOURCE_MATCH` / `DB_NULL_SOURCE_HAS` / `SOURCE_NULL_DB_HAS`. |
| `mechanical_derivation_compare` | `derived` columns — values that are deterministic functions of other DB columns. | Per-column CSV: every DB row alongside the recomputed-from-rule value. `MATCH` / `MISMATCH`. The derivation rule is documented in the CSV header. |
| `manual_source_review` | `adjudicated` columns — values requiring clinical judgment over upstream raw text (e.g., `bethesda_calculated_num` extracted from `bethesda_original_text`). | Per-column CSV: every DB row alongside the upstream raw-text snippet so Logan can confirm/correct without leaving the spreadsheet. No mechanical match flag. |

Claude also adds `upstream_source` pointers for `derived` and `adjudicated` columns
(which DB or Excel column is authoritative).

Logan reviews and signs off the column-method assignment.

### Step B — Generate the column-level verification CSVs

For every column where method ≠ `auto_no_source_counterpart`, Claude writes a CSV at:

```
verification_csvs/<table>/<column>__<batch_id>.csv
```

The CSV has **one row per DB row, no row cap.** Mismatches and edge-case flags
sort to the top so Logan can scan them first; clean `MATCH` rows sit below.

Standard columns:

```csv
research_id, <natural-key cols, e.g. fna_seq_n>, source_workbook, source_sheet, source_locator,
db_value, source_value, match_flag, your_correction, your_note
```

- `source_locator` is the row+column locator in the source Excel (e.g., `row=4231, col=Bethesda #2`) so Logan can jump straight to the cell.
- For `mechanical_derivation_compare`, the columns are `db_value`, `recomputed_value`, `match_flag`, and the rule expression is in the CSV preamble.
- For `manual_source_review`, the columns include the upstream raw-text snippet (e.g., `bethesda_original_text`) instead of `source_value` from a separate workbook.

### Step C — Logan reviews each CSV

Logan opens each CSV, reviews the mismatches that bubbled to the top, optionally
checks a sample of MATCH rows, fills `your_correction` / `your_note` for any rows
he wants changed, and saves. For columns where every row is MATCH and Logan has
no objection, sign-off is one click — no edits required.

### Step D — Bulk-update MotherDuck (via Cowork) and write the sign-off migration

**All DB writes flow through Cowork mode.** Specifically: every UPDATE/INSERT
that lands in MotherDuck during this verification effort is executed via the
`mcp__motherduck__query_rw` tool from a live Cowork session. The same SQL is
also persisted as a `.sql` file under `qc_framework_v1/migrations/` so the
change is reproducible, auditable, and replayable from the repo without a live
session. There is no separate "run this offline" step — Cowork is the runtime.

For each filled CSV:
1. Claude loads the CSV (in the Cowork session).
2. For rows where `your_correction IS NOT NULL`:
   - UPDATE the DB cell via `query_rw`.
   - INSERT an audit row in `manuscript_workspace.canonical_logan_review_log_v1`
     (old_value, new_value, batch_id, csv_path, logan_note).
3. Update the column's `verification_status='verified'`, `verified_by='logan'`,
   `verification_method=<one of the four above>`, `batch_id`.
4. Capture every executed statement in `qc_framework_v1/migrations/NN_logan_verified_<table>_<column>.sql`.

When all source/derived/adjudicated columns in the table are `verified`:
1. Flip every `auto_no_source_counterpart` column in the table from
   `not_started` to `verified` in a single statement (the deferred batch).
2. Update `canonical_table_signoff_registry_v1.table_status = 'verified'`.
3. Write `qc_framework_v1/migrations/NN_table_signoff_<table>.sql` capturing
   both the deferred-batch UPDATE and the table-status flip.
4. Append the table to `qc_framework_v1/VERIFIED_TABLES.md`.
5. Regenerate `qc_framework_v1/VERIFICATION_PROGRESS.md`.
6. Commit + push (Desktop Commander runs git on the host so sandbox lock-files
   don't block the push).

---

## 6a. Protocol revisions

| Date | From | To | Reason |
|---|---|---|---|
| 2026-04-27 | v1 (four-tier shortcut) | v2 (full-row mechanical compare) | Logan's directive: every data point in every column verified against original source; auto-skip / sample-only categories deprecated. CSV row cap removed — one CSV per column with all rows, mismatches bubbled to top. No-source-counterpart columns (provenance + pipeline trace) deferred to table sign-off rather than auto-verified upfront. All DB writes flow through Cowork mode's `query_rw` tool with mirrored migration `.sql` files under `qc_framework_v1/migrations/`. |

The v1 protocol text is preserved in git history (see `git log -- qc_framework_v1/MASTER_VERIFICATION_PLAN.md`). Migrations executed under v1 (mig_61c, mig_62) are flagged for re-verification under v2 when their respective tables come up in queue.

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
