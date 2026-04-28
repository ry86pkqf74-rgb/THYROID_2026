# Cowork Session Continuation Prompt — Protocol v2 verification

**Updated:** 2026-04-28 post-mig_95 (ETE taxonomy + invasion-family rollups complete)
**State on MotherDuck `thyroid_canonical_publication_v1_0`:** 13 / 184 Protocol v2 tables verified · 328 / 5,502 cols · all invasion-family rollups `table_status='verified'` confirmed via registry probe.

## Session goal

Continue clean-master-canonical verification of `thyroid_canonical_publication_v1_0` (MotherDuck account `logan.glosser.eras@gmail.com`).

**Next table to close:** CPM AJCC/ETE downstream re-derivation or `canonical_lymph_node_events_v1`, depending on whether you want to immediately propagate the corrected ETE taxonomy into staging. The invasion events + five invasion-family rollups are now closed.

## What's verified through mig_94b

| Migration | Table | Rows | Cols |
|---|---|---:|---:|
| mig_78 | canonical_fna_events_v1 | 8,050 | 38 (-1 CF) |
| mig_83 | canonical_airway_invasion_events_v1 | 3,155 | 23 |
| mig_89 | canonical_path_malignant_events_v1 | 6,689 | 56 |
| mig_90 | canonical_operative_events_v1 | 11,773 | 54 |
| mig_92 | canonical_t4b_invasion_events_v1 | 944 | 19 |
| mig_93 | canonical_esophageal_invasion_events_v1 | 188 | 15 |
| mig_94+94b | canonical_vascular_invasion_events_v1 | 3,861 | 22 |

**Final invasion staging distributions (analytics-ready):**

- airway (3,155): 138 pT4a · 575 not_pT4a · 2,442 unable_to_determine
- t4b (944): 19 pT4b · 925 not_pT4b · 0 unable_to_determine
- vascular (3,861, vi='present'=739): focal 209 · extensive 175 · minimal 57 · widely_invasive 14 · unspecified 284

## Verification methodology library (use whichever fits)

1. **CTC-equivalence verification** — Script-N SELECT*+filter+UPDATE chain canonicals; archived `_pre<N>_<timestamp>` snapshot in `archive_pub_v1_0` is value-source-of-truth. One mass-equivalence query closes dozens of cols. (mig_87 / mig_90.)
2. **Script-rule re-run verification** — post-build UPDATE-derived cols; re-execute the original UPDATE logic as SELECT and compare. (mig_88 / mig_90b.)
3. **Per-modality re-run** — multi-source UNION canonicals; mass-equivalence at `(invasion_type, source_value)` level when row-id keys are degenerate. (mig_91 establishes.)
4. **Per-finding Logan review with rule-based bulk pre-filter** — LLM-output canonicals; apply 12-rule pre-filter (below) then surface remaining 'present' rows for inline 20-row ACCEPT/FLIP/REJECT batches. (mig_80→83 airway · mig_92 t4b · mig_93 esophageal · mig_94 vascular.)

## Clinical rule library — LLM-output invasion canonicals

1. **Cancer-only** — non-tumor pathology (goiter / parathyroid adenoma / benign / hyperplasia / multinodular) → negate
2. **Compression ≠ invasion** — "compress / displace / deviate / efface / mass effect" → negate
3. **Adjacency-only** — "behind / posterior to / lateral to / abutting / adjacent to / along the / retroesophageal" + structure WITHOUT invasion verb → negate
4. **Adherent-only** — "densely / intimately adherent" without invasion / fistula / defect / muscularis / mucosa context → negate
5. **Explicit negative** — "no entrance / no injury / without compromise / no evidence of invasion / lumen not violated / not identified" → negate
6. **Procedural-only** — Maloney / Dobhoff / surgicel / NG tube placement WITHOUT invasion finding → negate
7. **Closure / wound text** — "thoroughly irrigated / hemostasis excellent / Vicryl stitches / no complications / sutured closed" → negate
8. **Iatrogenic injury** — luminal defect created by surgical dissection (not tumor) → negate
9. **Multi-structure summary** — "<carcinoma|tumor> with invasion of A, B, C, D" comprehensive shopping-list staging summaries → negate
10. **Subtype mismatch** — when evidence specifies muscularis but says "mucosa intact", do NOT propagate 'present' to mucosal column
11. **Findings-vs-staging** — staging columns follow anatomic findings, NEVER override. Two-branch staging rule: pT4a/pT4b iff ≥1 anatomic finding 'present'; not_pT4a/not_pT4b otherwise. Eliminate `unable_to_determine` when possible.
12. **LVI catch-all only** — `lvi_collapsed='present'` reserved for source-collapsed cases. Where source separates V vs L, vascular AND lymphatic must be populated separately.

## Standing protocol

- MotherDuck account: `logan.glosser.eras@gmail.com`
- All writes via `mcp__motherduck__query_rw`; mirrored in `qc_framework_v1/migrations/NN_*.sql`
- PHI: never print clinical text outside review CSVs (gitignored except Logan-reviewed xlsx force-added for audit)
- Commits: explicit-path `git add` (NEVER `-A`); lint Python before commit; author `Logan Glosser <logan.glosser@gmail.com>`
- Read-only `archive_pub_v1_0` permitted for verification; never source canonical from archive at build time
- **Every round: re-query MD for current state** (memory: `feedback_motherduck_direct_check.md`)
- Review CSVs: **.xlsx output** with openpyxl (csv.QUOTE_ALL fragile in Excel); evidence_quote in dedicated wide wrap-text column right before decision column (memory: `feedback_review_csv_formatting.md`)

## Open carry-forwards (non-blocking)

- **CF-91-LINKAGE-COL-NAME** — CLOSED by mig_95: `canonical_invasion_events_v1.linkage_ambiguous_multi_episode` → `linkage_ambiguous_multi_finding`
- **CF-91-PSID** — `canonical_path_malignant_events_v1.path_surgery_id` only 3 distinct non-null values; investigate Script 361
- **CF-91-LLM-V1-V2-DRIFT** — LLM source tables substantially reshaped post Script 363; ct/llm 477 + mri/llm 25 + most op_note/llm 120/168 source rows GONE
- **CF-91-SOURCE-ROW-ID-COLLISION** — Script 363's `note_row_id|source_line|entity_type` not unique
- **CF-91-LLM-COL-RENAME** — v2 LLM tables renamed `result_json` → `parsed_json`
- **CF-90-DATE-FORMAT** — operative_events `resolved_surgery_date` MM/DD/YYYY vs YYYY-MM-DD format
- **CF-fna-days-to-surgery** — UNBLOCKED at mig_90; can re-open

## Immediate next step — canonical_invasion_events_v1 (cross-modal UNION)

mig_91 SQL is a SKELETON with `TODO_LOGAN` sections. With 4 sibling LLM canonicals now closed, the LLM-slice verification is satisfied transitively. Steps:

1. Re-query MD: confirm 51,773 rows / 20 cols / 6 modality slices
2. Adjudicate 759 ambiguous-linkage groups (review CSV) OR defer as a column-level CF
3. Apply column registry flips per mig_91 structure (4 mech_deriv_cmp + 4 linkage + 11 auto_*_skip)
4. Refresh table_signoff_registry; update `VERIFIED_TABLES.md` + `VERIFICATION_PROGRESS.md` (8/184 tables, ~246 cols)
5. Commit + push at explicit migration path

## After invasion_events — table queue

### Tier 1: invasion-family rollups (closed by mig_95)
`canonical_airway_invasion_patient_rollup_v1` (20) · `..._esophageal_..._rollup_v1` (11) · `..._t4b_..._rollup_v1` (13) · `..._vascular_..._rollup_v1` (14) · `canonical_invasion_patient_rollup_v1` (47). Deterministic patient-grain rollups verified by re-derivation rule probe.

ETE taxonomy is now three-bucket:
- `gross_ete` = explicit gross / extensive / macroscopic
- `microscopic_ete` = explicit microscopic / minimal / focal
- `ete_present_not_further_specified` = generic present / yes / true ETE

Downstream `canonical_invasion_patient_rollup_v1` exposes `any_ete_present_not_further_specified_*` and `any_ete_*` union columns. CPM feeder flags were synced from the corrected rollup, but `canonical_patient_master.ajcc8_t_stage` was not silently rederived.

### Tier 2: remaining tier1_events canonicals (~10 tables)
path_benign (55) · path_gland (20) · frozen_section (31) · parathyroid (25) · complications (18) · pmh (19) · psh (19) · medications (19) · pathology_clinical (15) · cervical_ln_clinical (15). Most close single-mig via CTC-equivalence or Script-rule re-run.

### Tier 3: tier2 derived canonicals (~16 tables)
Labs (Tg/calcium/PTH/TSH/vitD), molecular, recurrence, ETE adjudication, survival. See `REMAINING_WORK_INVENTORY.md` table 5b.

### Tier 4: tier1 sources + anchor
- `canonical_patient_master` (1,592 cols — 4-6 sessions, deferred until tier 1 events mostly closed)
- 12 raw mirror sources (path_synoptics 311, manuscript_cohort_v1 151, nsqip 94, etc.) — sample-based v2

### Tier 5: tier3 helpers (~108 tables)
91 helpers in manuscript_workspace + 17 note_entities_*. Many reclass to `na` after one look (10-20 min each).

## Working pattern (each round)

1. Logan: "next table" or specific name
2. Cowork: re-query MD for current state
3. Cowork: identify methodology
4. Cowork: apply bulk filters / mass-equivalence; surface anomalies inline or in .xlsx
5. Logan: review batches of 20 OR confirm bulk results ("agree" / "agree except X" / specific F-list)
6. Cowork: apply via `mcp__motherduck__query_rw`; verify post-state; write `qc_framework_v1/migrations/NN_*.sql`
7. Cowork: update progress docs; commit (explicit paths, Logan author); push
8. Cowork: brief summary; ready for next

## Key files / artefacts

- Migrations: `qc_framework_v1/migrations/NN_*.sql`
- Builders: `qc_framework_v1/scripts/build_*_review.py` (openpyxl + .xlsx)
- Review outputs: `verification_csvs/<table>/` (gitignored except Logan-reviewed xlsx force-added)
- Master plan: `qc_framework_v1/MASTER_VERIFICATION_PLAN.md`
- Inventory: `qc_framework_v1/REMAINING_WORK_INVENTORY.md`
- Progress: `qc_framework_v1/VERIFICATION_PROGRESS.md`
- Verified log: `qc_framework_v1/VERIFIED_TABLES.md`

## Standing memory references

`feedback_findings_vs_staging.md` · `feedback_motherduck_direct_check.md` · `feedback_no_cross_db_canonical_sourcing.md` · `feedback_surgical_git_add.md` · `feedback_commit_workflow.md` · `feedback_review_csv_formatting.md` · `feedback_phi_safety.md` · `reference_protocol_v2_md_accounts.md` · `reference_canonical_naming_convention.md` · `project_ctc_equivalence_verification_pattern.md` · `project_invasion_canonical_mig_91_progress.md` · `project_invasion_family_signoff_2026-04-28.md` (this session)
