# Cowork session continuation — Protocol v2 verification of `thyroid_canonical_publication_v1_0`

**Generated:** 2026-04-28 (post-mig_98a-apply, post-mig_97b)
**State on MotherDuck `thyroid_canonical_publication_v1_0`:** 16 / 184 tables verified (8.7%); 417 / 5,502 cols verified (7.6%); **0 failed CFs across all tables**.

## Project goal

Verify and clean every table in the canonical publication database
`thyroid_canonical_publication_v1_0` under Protocol v2. Final outcome:

1. **All canonical tables `table_status='verified'`** in
   `main.canonical_table_signoff_registry_v1`.
2. **All columns flagged `verified` or `na`** in
   `main.canonical_column_verification_registry_v1` (zero `not_started`,
   zero `failed`).
3. **Standardized values** on every analytic column (controlled
   vocabularies for status / type / modality / kind / temporal_class).
4. **Old / archived tables and columns removed** when no longer load-bearing
   (419 archive tables in `archive_pub_v1_0` schema currently — many can
   be dropped post-verification).
5. **Patient-level rollups + view layer aligned** with verified events tables.

## Working accounts + access

- MotherDuck account: **`logan.glosser.eras@gmail.com`** (publication DB lives here per `reference_protocol_v2_md_accounts.md`).
- Cowork's MotherDuck MCP is authed to `.eras` directly. Local duckdb-py CLI **must SSO as `.eras`** (browser default may pick `logan.glosser@gmail.com` which doesn't have the publication DB).
- Read-only verification reference allowed against `archive_pub_v1_0` schema (in either `thyroid_canonical_publication_v1_0` OR `"Thyroid 2026 UPdated"` databases). **Never source canonical from archive at build time** (memory: `feedback_no_cross_db_canonical_sourcing.md`).

## Standing protocol

- Use `mcp__motherduck__query_rw` (or duckdb-py with `.eras` token) for all writes; mirror in `qc_framework_v1/migrations/NN_*.sql`.
- **PHI rule**: never print clinical text outside review .xlsx files (memory: `feedback_phi_safety.md`). Review .xlsx files are gitignored except Logan-reviewed copies which can be force-added for audit.
- **Commits**: explicit-path `git add` only — NEVER `git add -A` (memory: `feedback_surgical_git_add.md`). Lint Python with `python3 -m py_compile` before commit. Author: `Logan Glosser <logan.glosser@gmail.com>`.
- **Per-round MD verification**: re-query MotherDuck for current state before recommending changes; don't trust prior summaries (memory: `feedback_motherduck_direct_check.md`).
- **VSC/Cursor agents** can be assigned non-overlapping verification tasks in parallel. Confirm any action they take by directly querying MotherDuck.
- **Execute directly when appropriate**: if the task is a clean derivation check, mass-equivalence, or auto-apply with no clinical judgment, run it via `query_rw`. If clinical judgment is needed, build an .xlsx review file at `verification_csvs/<table>/<bucket>_review__migNN.xlsx` for Logan to fill, then apply.
- **Review .xlsx format** (memory: `feedback_review_csv_formatting.md`): use openpyxl, NOT csv.QUOTE_ALL (fragile in Excel). evidence_quote in dedicated wide wrap-text column right before the decision column. Headers in row 1, data starts row 2. Decision col vocab: `ACCEPT / FLIP_TO_PRESENT / FLIP_TO_ABSENT / FLIP_TO_SUSPECTED / DELETE / RECLASS_INVASION_TYPE / NEEDS_CONTEXT / ADD`.

## Verified tables (16) — established patterns

| Table | Cols | Method | Migration |
|---|---|---|---|
| canonical_fna_events_v1 | 38 | mechanical_source + mechanical_derivation + manual_source | mig_78 → mig_96 (d2s recompute) |
| canonical_airway_invasion_events_v1 | 23 | manual_source_review + findings-vs-staging | mig_83 |
| canonical_path_malignant_events_v1 | 56 | CTC-equivalence + Script-rule re-run | mig_89 |
| canonical_operative_events_v1 | 54 | CTC-equivalence single-migration | mig_90 |
| canonical_t4b_invasion_events_v1 | 19 | per-finding Logan review + default-not | mig_92 |
| canonical_esophageal_invasion_events_v1 | 15 | per-finding Logan review | mig_93 |
| canonical_vascular_invasion_events_v1 | 22 | per-finding Logan review + extent backfill | mig_94 / 94b |
| canonical_invasion_events_v1 | 11 verified + 9 na | CTC-equivalence on UNION canonical + orphan review | mig_91 / 91b → 95 (ETE taxonomy) |
| canonical_path_benign_events_v1 | 51 verified + 4 na | NLP-flag audit + structural repair + specimen_master inherit | mig_97 / 97b |
| canonical_airway_invasion_patient_rollup_v1 | 17 verified + 3 na | derivation re-derivation against verified events | mig_95 |
| canonical_esophageal_invasion_patient_rollup_v1 | 9 + 2 na | mig_95 |  |
| canonical_t4b_invasion_patient_rollup_v1 | 10 + 3 na | mig_95 |  |
| canonical_vascular_invasion_patient_rollup_v1 | 11 + 3 na | mig_95 |  |
| canonical_invasion_patient_rollup_v1 | 44 + 3 na | mig_95 |  |
| canonical_fna_patient_rollup_v1 | 18 + 2 na | mig_95b |  |
| canonical_operative_patient_rollup_v1 | 19 + 3 na | mig_95b |  |

## Verification methodology library

1. **CTC-equivalence verification** — for canonicals built via Script-N SELECT*+filter+UPDATE chains OR UNION pipelines where a pre-Script-N archive snapshot exists with identical row count + identifying-key set. One mass-equivalence query covers dozens of cols. (mig_87 / mig_90 / mig_91.)
2. **Script-rule re-run verification** — for post-build UPDATE-derived cols, re-execute UPDATE logic as SELECT and compare to stored values. (mig_88 / mig_90b.)
3. **Derivation re-derivation against verified upstream** — for rollups, recompute aggregations against the verified events table and mass-equivalence-check. >99% match acceptable; complex derived cols deterministic-from-build-script. (mig_95 / mig_95b.)
4. **Per-finding Logan review with rule-based pre-filter** — for LLM-output canonicals: apply 12-rule clinical pre-filter (memory: `feedback_invasion_orphan_clinical_rules.md`), then surface remaining ambiguous rows in batches of 20 for Logan ACCEPT/FLIP/REJECT. (mig_80→83 / mig_91 / mig_92 / mig_93 / mig_94.)
5. **NLP-flag audit** — sample-based pattern-match validation against source free text. >95% match on common flags, >80% on rare. (mig_97 NLP audit on 38 path_benign flags.)
6. **Structural repair + source inheritance** — for cols never populated by build script, locate alternative verified source and inherit. (mig_97 surgery_episode_id repair + mig_97b synoptic_row_ix inherit from specimen_master_v1.)
7. **Orphan review with cancer/benign Rule #1 split** — for invasion-canonical orphan finding_status downgrades, split by Rule #1 (cancer-only) → BENIGN auto-accept (Script's downgrade correct for non-malignant patients), CANCER → sub-bucket by clinical rule library. (mig_91 cancer orphans 47 sub-bucketed.)

## Open work (priority queue)

### Immediate — finish canonical_complications_events_v1 (2 of 8 complication-types started)

```
Currently 'not_started' (14 not_started cols + 5 na). 1 sub-mig done:
  ✓ mig_98a: vocal_cord_paralysis (24 NEGATION_RISK reviewed; 23 FLIP + 1 ACCEPT;
              CF91 rid 5048 ADD with finding_date=2016-12-02; finding_date_source col added)
  
Pending sub-migs (one per complication type):
  ▢ mig_98b: chyle_leak           (3,028 present / 1,576 pts -- prevalence anomaly 14.5% vs clinical 1-3%; likely intra-op observation conflated with clinical leak)
  ▢ mig_98c: rln_injury           (1,150 present / 709 pts)
  ▢ mig_98d: seroma               (1,407 present / 873 pts)
  ▢ mig_98e: hematoma             (350 present / 169 pts)
  ▢ mig_98f: hypoparathyroidism   (608 present / 425 pts)
  ▢ mig_98g: hypocalcemia_clinical (11 present / 9 pts -- spot-check only)
  ▢ mig_98h: mortality            (1 present / 1 pt -- spot-check)

After all 8 sub-migs: mig_99 flips column registry (14 not_started -> verified)
+ refreshes table_signoff_registry -> 'verified'.

Pattern to reuse: existing mig_98a builder + apply scripts at
  qc_framework_v1/scripts/build_vocal_cord_paralysis_review.py
  qc_framework_v1/scripts/apply_mig_98a_vocal_cord_decisions.py
  qc_framework_v1/migrations/98a_mig_vocal_cord_paralysis_apply.md
Adapt rule library per complication type:
  - chyle_leak needs intra-op-vs-postop temporal split (likely most over-extracted)
  - rln_injury needs transient-vs-permanent permanence rule
  - seroma needs treatment-requiring vs incidental
  - hematoma needs reoperation-required vs observed-only
  - hypoparathyroidism needs lab-confirmed vs symptomatic-only
```

### Tier 1 events tables remaining (8 tables, ~190 cols)

```
canonical_path_gland_events_v1            20 cols / 28,724 rows (paired with path_malignant; CTC-equivalence likely)
canonical_frozen_section_events_v1        31 cols /  7,081 rows (Script 360 closed; CTC pattern)
canonical_parathyroid_events_v1           25 cols /  8,697 rows
canonical_pmh_events_v1                   19 cols / 12,444 rows
canonical_psh_events_v1                   19 cols /  3,919 rows
canonical_medications_events_v1           19 cols /  7,501 rows
canonical_pathology_clinical_events_v1    15 cols / 13,358 rows
canonical_cervical_ln_clinical_events_v1  15 cols /  4,493 rows
```

### Tier 2 derived canonicals (~16 tables)

Labs (Tg, calcium, PTH, TSH, vitamin D), molecular, recurrence, ETE adjudication, survival, etc.
See `qc_framework_v1/REMAINING_WORK_INVENTORY.md` table 5b.

### Tier 4 / 5 (~120 tables)

`canonical_patient_master` (1,592 cols) — deferred until Tier 1 events mostly closed (most patient_master cols are events-rollups; auto-derivable cascade).
12 raw mirror sources (path_synoptics 311 cols, manuscript_cohort_v1 151 cols, nsqip 94 cols, etc.) — sample-based verification.
91 helpers in manuscript_workspace + 17 note_entities_*. Many reclass to `na` after one look.

## Cleanup of archive layer

```
"Thyroid 2026 UPdated".archive_pub_v1_0  has 419 archive tables
   (pre-Script-N snapshots accumulated over 2026-04-15 → 2026-04-28).

Many are now redundant:
  - Pre-mig_8X / pre-mig_9X snapshots for tables now verified
  - Multiple snapshot generations for same table (pre251 / pre364 / pre368 etc.)
  - Schema-reorg snapshots already absorbed

Cleanup criteria:
  - SAFE TO DROP: archive snapshots taken pre-verification IF the post-verification
    canonical state has been reproduced in a migration SQL file (mig_NN.sql)
    such that the archive isn't needed for re-verification.
  - KEEP: most-recent pre-Script-N snapshot per table (in case Script-N is rerun).
  - KEEP: snapshots referenced in still-open carry-forwards.

Recommend: do this cleanup once all Tier 1 + Tier 2 tables are verified.
Pre-emptive cleanup risks losing a snapshot we still need.
```

## Established memory references (READ THESE)

- `feedback_motherduck_direct_check.md` — re-query MD before recommending state changes
- `feedback_no_cross_db_canonical_sourcing.md` — never `FROM archive_pub_v1_0.*` at build time
- `feedback_findings_vs_staging.md` — staging cols follow anatomic findings, never override
- `feedback_invasion_orphan_clinical_rules.md` — 6-rule clinical adjudication library + path_synoptics structured probe
- `feedback_surgical_git_add.md` — never `git add -A`
- `feedback_commit_workflow.md` — always stage/commit/push; lint Python first
- `feedback_review_csv_formatting.md` — openpyxl + .xlsx, NOT csv.QUOTE_ALL
- `feedback_phi_safety.md` — never print clinical notes; research_id only
- `reference_protocol_v2_md_accounts.md` — `.eras` account hosts publication DB
- `reference_synoptic_row_ix.md` — Script-108 pandas-load-order; never synthesize via ROW_NUMBER (inherit OK)
- `reference_view_naming_convention.md` — main.* VIEW must carry `_VIEW` in name
- `reference_canonical_naming_convention.md` — `canonical_<domain>_events_v1` / `canonical_<domain>_patient_rollup_v1`
- `reference_2digit_year_convention.md` — all YY → 20YY (Logan-ratified 2026-04-27)
- `feedback_alter_view_dependents.md` — `ALTER VIEW RENAME TO` is catalog-only; CREATE OR REPLACE dependents in same commit
- `feedback_mention_grain_partition_probe.md` — probe COUNT(*) vs COUNT(DISTINCT key) before ROW_NUMBER on mention tables
- `project_ctc_equivalence_verification_pattern.md` — CTC pattern established in mig_87
- `project_invasion_family_signoff_2026-04-28.md` — invasion family complete signoff with 12-rule clinical library

## Recent commits on origin/main (most recent first)

```
cff7ad3  fix(qc): mig_98a CF91 finding_date 2016-12-02 + onset anchor + apply  (Cursor agent)
1437549  mig_97b: close CF-PATH-BENIGN-SYNOPTIC-ROW-IX via specimen_master_v1 inherit  (Cowork)
6e811a8  feat(qc): mig_98a apply script for vocal_cord_paralysis decisions  (Cursor agent)
0351e2b  Close FNA D2S and repair path benign verification  (Cursor agent mig_96+97)
1e91f42  Add mig_98a builder for vocal_cord_paralysis complications review workbook  (Cursor agent)
9fbca90  mig_95b: close invasion family + FNA + operative rollups (15 tables verified)  (Cowork)
f945a28  mig_95: standardize ETE taxonomy and invasion rollups  (Cursor agent)
fff2456  mig_91b: canonical_invasion_events_v1 SIGNED OFF — 8th Protocol v2 table  (Cowork)
99745ce  mig_91: final dispositions extrapolated from Logan partial review + structured data  (Cowork)
acbb05e  mig_91 v3: sub-bucket cancer orphans per Logan's clinical rules  (Cowork)
aa60790  mig_91: split orphan review by Rule #1 (cancer-only)  (Cowork)
791645f  mig_91: CTC-equivalence verification of canonical_invasion_events_v1  (Cowork)
```

## Key files / artefacts

```
Migrations:           qc_framework_v1/migrations/NN_*.sql
Builders:             qc_framework_v1/scripts/build_*_review.py    (uses openpyxl + .xlsx)
Apply runners:        qc_framework_v1/scripts/apply_*_decisions.py (use duckdb-py + MD token)
Review outputs:       verification_csvs/<table>/                   (gitignored except force-added Logan-reviewed)
Master plan:          qc_framework_v1/MASTER_VERIFICATION_PLAN.md
Inventory:            qc_framework_v1/REMAINING_WORK_INVENTORY.md
Progress dashboard:   qc_framework_v1/VERIFICATION_PROGRESS.md
Verified log:         qc_framework_v1/VERIFIED_TABLES.md
Manifests:            qc_framework_v1/migrations/NN_mig_*_apply.md (per-mig human-readable plan)
```

## Working pattern per round

1. **Logan**: "next table" or specific table name.
2. **Cowork**: re-query MD for current state (row count, column types, not_started cols, build script).
3. **Cowork**: identify methodology (CTC-equivalence / Script-rule re-run / per-finding review with rule-based pre-filter / source inheritance).
4. **Cowork**: apply bulk filters / mass-equivalence checks; surface anomalies inline or in .xlsx for Logan review.
5. **Cowork**: dispatch to Cursor agent for non-overlapping scripted work in parallel (e.g., NLP-flag audit, mass-derivation, mig_NN apply scripts).
6. **Logan**: review batches of 20 rows or confirm bulk filter results ("agree" / "agree except X").
7. **Cowork or Cursor**: apply UPDATEs / DELETEs / INSERTs via `query_rw`; verify post-state on MD; write `qc_framework_v1/migrations/NN_*.sql` with full documentation.
8. **Cowork**: update VERIFIED_TABLES.md + VERIFICATION_PROGRESS.md; commit (explicit paths, Logan author); push origin/main.
9. **Cowork**: brief summary; ready for next.

## Immediate next-table candidates (Logan to pick)

1. **`canonical_path_gland_events_v1`** (20 cols / 28,724 rows) — Tier 1, paired with path_malignant; likely CTC-equivalence candidate; structural similarity to path_benign and path_malignant which are both verified.

2. **`canonical_frozen_section_events_v1`** (31 cols / 7,081 rows) — Script 360 closed (memory: `project_frozen_section_script_360.md`); CTC-equivalence pattern with archive snapshot likely.

3. **`canonical_complications_events_v1`** sub-migs continued (mig_98b chyle_leak next; reuse mig_98a builder/apply pattern). 7 sub-migs remaining; high-leverage as it's complete after.

4. **`canonical_parathyroid_events_v1`** (25 cols / 8,697 rows) — Tier 1, related to thyroidectomy parathyroid complications (cross-references hypoparathyroidism in canonical_complications_events_v1).

## Standing reminders

- **Confirm in MD before recommending**: don't trust prior summaries; re-query.
- **Execute directly when appropriate**: for clean derivation checks / mass-equivalences with no clinical judgment, run via `query_rw`. Build .xlsx for clinical judgment cases.
- **Cursor agents in parallel**: assign non-overlapping tasks; verify their work directly on MD before declaring complete.
- **Push back on bugs**: e.g., the rid 5048 finding_date issue caught in mig_98a-apply where the agent used surgery date instead of CT date — verify temporal-granularity intent is preserved.
- **0 failed CFs target**: every CF should resolve to either `verified` (with explanation) or `na` (auto-skip with rationale). 'failed' should be transient.
