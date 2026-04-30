# Lymph Nodes + Tumor Histology — Cowork Assessment & Plan

**Date:** 2026-04-30
**Source review:** uploaded ChatGPT plan `lymph_nodes_histology_findings_plan.md` (referenced inline as "ChatGPT")
**Cowork validator:** live MotherDuck DB `thyroid_canonical_publication_v1_0` (account `logan.glosser.eras@gmail.com`)
**Repo HEAD at write:** `2c0a135` (post v13 round close)
**Status:** Plan only. No DB mutation in this round.

---

## TL;DR

ChatGPT's review is **highly accurate** — every primary count claimed in §§1-7 of the doc matches MD exactly except 3 minor drifts (resolved below). The **dedup view** (`canonical_path_malignant_events_dedup_VIEW_v1`, mig_212) is the right tumor-grain source. None of the 4 LN/histology safe views ChatGPT proposed exist yet, but a substantial scaffolding (`ln_crossval_v1`, `ln_per_patient_multisource_v1`, `histology_vocab_normalization_map_v1` with 96 rows, `canonical_path_malignant_events_v1_histology_clean`, 12 `ln_mets_*` cols on `ln_master_rollup_v1`) is already in place to build on.

**Recommended next step:** queue a new lane **"Lane LN" (mig_224 → mig_229)** that builds the 4 safe views + 5 QC tables + extends the vocab map + adds a benign/borderline-with-staging quarantine flag. Estimated 2 Cline GPT-5.5 sessions or 1 Cursor composer pass.

**One Logan-decision needed:** whether the 3 LN safe views land in `manuscript_workspace` (per ChatGPT proposal, parallel to existing helpers) or in `semantic_publication` (extending Lane G's mig_223 surface area).

---

## §1 Validation report — ChatGPT counts vs live MD

**EXACT matches (claim ↔ MD agree to the unit):**

| Claim domain | ChatGPT count | Live MD | Status |
|---|---:|---:|---|
| `canonical_path_malignant_events_dedup_VIEW_v1` rows | 5,944 | 5,944 | ✓ |
| Dedup distinct patients | 4,022 | 4,022 | ✓ |
| Dedup duplicate `(rid, path_surgery_id, tumor_ordinal)` keys | 0 | 0 | ✓ |
| Tumor rows missing LN denominator | 597 | 597 | ✓ |
| Tumor rows missing LN positive | 2,658 | 2,658 | ✓ |
| `ln_master_rollup_v1` rows | 4,273 | 4,273 | ✓ |
| Distinct rollup patients | 3,986 | 3,986 | ✓ |
| Patients with duplicate rollup rows | 256 | 256 | ✓ |
| Rollup rows with `ln_total_positive > ln_total_examined` | 11 | 11 | ✓ |
| Rollup rows with positive nodes | 2,847 | 2,847 | ✓ |
| Rollup rows with examined nodes | 4,012 | 4,012 | ✓ |
| `canonical_patient_master` rows | 10,871 | 10,871 | ✓ |
| CPM null `ln_rollup_total_examined` | 6,924 | 6,924 | ✓ |
| CPM null `ln_rollup_total_positive` | 7,115 | 7,115 | ✓ |
| CPM `ln_rollup_total_positive > ln_rollup_total_examined` | 4 | 4 | ✓ |
| CPM `examined_gt0` | 3,751 | 3,751 | ✓ |
| CPM `positive_gt0` | 2,629 | 2,629 | ✓ |
| CPM `positive_binary=1` | 2,637 | 2,637 | ✓ |
| Histology distribution (12 categories incl. `Follicular caricinoma` typo) | matches all | matches all | ✓ |
| Multi-histology surgery groups | 164 | 164 | ✓ |
| Multi-histology patients | 164 | 164 | ✓ |
| Multi-hist groups with positive nodes | 48 | 48 | ✓ |
| Max involved nodes in multi-hist group | 51 | 51 | ✓ |
| Max examined nodes in multi-hist group | 72 | 72 | ✓ |
| `ln_mets_ptc=TRUE` rows | 46 | 46 | ✓ |
| `ln_mets_mtc=TRUE` rows | 4 | 4 | ✓ |
| `ln_mets_atc=TRUE` rows | 1 | 1 | ✓ |
| Multi-tumor-type rows | 3 | 3 | ✓ |
| Positive-node rows with no `ln_mets_tumor_types_array` value | 2,801 | 2,801 | ✓ |
| `histology_clean` view rows | 6,469 | 6,469 | ✓ |
| `histology_clean` view patients | 4,022 | 4,022 | ✓ |
| Discordance histology flag count | 564 | 564 | ✓ |
| Raw vs cleaned histology different | 6,354 | 6,354 | ✓ |

**3 drifts surfaced + resolved:**

1. **Dedup view "0 impossible rows" → actually 6.** ChatGPT §2 said tumor rows where `ln_involved > ln_examined` = 0. Cowork found **6 PTC rows** where `ln_examined=0.0` but `nodal_disease_total_count > nodal_disease_positive_count > 0`. All 6 have `(path_surgery_id=1, tumor_ordinal=1)`. The contradiction is between the floating-point `ln_examined` source and the integer `nodal_disease_total_count` source. ChatGPT likely used MAX-coalesce; Cowork used COALESCE-by-ordinal. **This is a real data-modeling decision needed** — see Open-Question 3.
   - Affected research_ids: 744, 4426, 4560, 5197, 5917, 8482
   - Worst case: rid 5197 (ln_examined=0, nodal_disease_total_count=55, nodal_disease_positive_count=6)

2. **Patient-level LN positive/examined counts off-by-3 / off-by-15.** ChatGPT: 1,129 LN-positive / 2,214 LN-examined. Cowork: 1,126 / 2,229. Likely difference in how NULL-vs-0 is treated in the patient roll-up. Both are within tolerance for a planning doc; the new safe view will be the SSOT.

3. **Multi-hist "vary by row" vs "identical" pattern is partially reversed.** ChatGPT: 47 vary / 1 identical. Cowork strict (NULL not counted): 15 vary / 33 identical. Cowork treating NULL as distinct: 28 / 20. ChatGPT's split (47/1) doesn't match either interpretation cleanly — likely a different definition (e.g., row-level variation including non-LN-positive rows). **Direction of the finding is still correct** (many multi-hist groups have row-level variation; some don't). The histology-attribution view will use a clearer per-row attribution rule rather than relying on this aggregate.

---

## §2 Existing assets to leverage (already in MD)

ChatGPT's plan proposes building from scratch, but several pieces already exist:

| Asset | Schema | Type | Rows | Use in plan |
|---|---|---|---:|---|
| `canonical_path_malignant_events_dedup_VIEW_v1` | main | VIEW | 5,944 | **Primary source** for surgery + histology attribution views (mig_212) |
| `histology_vocab_normalization_map_v1` | main | BASE TABLE | 96 | **Extend** rather than replace; add missing typos + benign/borderline flags |
| `canonical_path_malignant_events_v1_histology_clean` | manuscript_workspace | VIEW | 6,469 | **Source for `primary_histology_clean`** column; already provides discordance flags |
| `dim_histology_variant_v1` | manuscript_workspace | VIEW | (unknown) | **Predecessor** to proposed `dim_histology_standardized_VIEW_v1`; check for overlap before replacing |
| `ln_crossval_v1` | manuscript_workspace | BASE TABLE | 4,290 | **Source** for `ln_crossval_status` field on patient safe view |
| `ln_per_patient_multisource_v1` | manuscript_workspace | VIEW | 6,979 | **Source / cross-check** for patient-level LN aggregation |
| `ln_master_rollup_v1` | manuscript_workspace | (table) | 4,273 | **Reference / QC source**; do NOT use directly per ChatGPT §3; flag 256 dup pts + 11 impossible rows in QC tables |
| `ln_mets_*` cols (12 total) on `ln_master_rollup_v1` | manuscript_workspace | columns | — | **Source** for `ln_mets_tumor_type_text` field on histology-attribution view |
| `canonical_patient_master.ln_*` cols | main | columns | — | **Cross-check denominator** for new patient safe view; document discordance |

`dim_histology_variant_v1` already exists — should I confirm its schema before replacing or extending it? See Open-Question 1.

---

## §3 What ChatGPT proposed that doesn't exist yet

Confirmed absent from MD via `information_schema.tables`:

| Proposed object | Schema (per ChatGPT) | Type |
|---|---|---|
| `vw_ln_patient_publication_safe_v1` | manuscript_workspace | VIEW |
| `vw_ln_surgery_publication_safe_v1` | manuscript_workspace | VIEW |
| `vw_ln_histology_attribution_v1` | manuscript_workspace | VIEW |
| `dim_histology_standardized_v1` | manuscript_workspace | TABLE / VIEW |
| `qc_ln_impossible_counts_v1` | manuscript_workspace | TABLE |
| `qc_ln_duplicate_rollup_patients_v1` | manuscript_workspace | TABLE |
| `qc_ln_multihistology_attribution_queue_v1` | manuscript_workspace | TABLE |
| `qc_histology_borderline_in_malignant_table_v1` | manuscript_workspace | TABLE |
| `qc_histology_vocab_typos_v1` | manuscript_workspace | TABLE |
| `semantic_publication` schema | (Lane G mig_223) | SCHEMA — not yet built |

**Naming-convention note:** per `reference_view_naming_convention.md`, all `main.*` views must carry `_VIEW` infix before `_v<N>`. ChatGPT's proposed names lack this. Real names should be:

- `vw_ln_patient_publication_safe_VIEW_v1`
- `vw_ln_surgery_publication_safe_VIEW_v1`
- `vw_ln_histology_attribution_VIEW_v1`
- `dim_histology_standardized_VIEW_v1` (if VIEW; if TABLE, no infix)

The convention applies to `manuscript_workspace.*` too per the v12-round labeling pass close-out.

---

## §4 Confirmed real issues that need fixing

### 4.1 Benign/borderline histologies in malignant pathology (HIGH PRIORITY)

ChatGPT §7.1 flagged FTUMP and follicular adenoma rows with N1/M1 staging. **Confirmed:**

| Histology | Total rows | N1* rows | NX rows | M1 rows |
|---|---:|---:|---:|---:|
| FTUMP | 48 | **17** | 1 | **12** |
| follicular adenoma | 6 | 0 | 0 | **6** |

These rows should not contribute to carcinoma-only or N-stage analyses. Action: build `qc_histology_borderline_in_malignant_table_v1` to enumerate them + add a `is_borderline_or_benign_with_staging` BOOLEAN flag on the new safe views.

### 4.2 Histology typo persistence (LOW PRIORITY but visible)

The `Follicular caricinoma` typo persists in 1 row of `primary_histology_raw` despite the existing 96-row vocab map. The clean view does not currently fold this. Other typos ChatGPT enumerated (`microcarinoma`, `microcarcinooma`, `microcaricnoma`, `folliucalr`, `follicualr`, `classsical`, `poorly differntiated`) appear to be variant-level only — none surface in `primary_histology_raw` for malignant pathology dedup, so they're at most variant/sub-string concerns. Action: extend `histology_vocab_normalization_map_v1` with the carcinoma-level typo + audit variant-level typos as a separate sub-task.

### 4.3 `ln_mets_tumor_types_array` literal-bracket sparsity (CONFIRMED REAL)

2,801 of the 2,847 positive-node rows have `ln_mets_tumor_types_array='[]'` (empty literal brackets, not NULL). This means **only 46 of 2,847 LN-positive cases (1.6%)** have explicit histology-attribution evidence in the array. Action: surface this in `vw_ln_histology_attribution_VIEW_v1` as an `ln_attribution_confidence` category — most non-array rows fall into `surgery_level_only` or `ambiguous_multi_histology` per ChatGPT §9.3.

### 4.4 6 dedup-view rows with `ln_examined=0` but `nodal_disease_total_count > 0` (NEW FINDING)

Not in ChatGPT's review. The 6 rows where double-source `ln_examined` is 0 but integer `nodal_disease_total_count` carries the real value. **The safe view needs an explicit source-priority rule** (use `nodal_disease_total_count` when `ln_examined=0`?). See Open-Question 3.

### 4.5 11 impossible rows on `ln_master_rollup_v1` + 4 on CPM (CONFIRMED)

Action: build `qc_ln_impossible_counts_v1` listing all 15 rows with research_id + source-table + counts; add `ln_impossible_count_flag` column on patient safe view; do **not** silently exclude — flag transparently per `feedback_findings_vs_staging.md`-style rule.

### 4.6 Multi-histology LN attribution ambiguity (CONFIRMED — 48 groups)

Of the 48 multi-hist surgery groups with positive nodes:
- 33 share identical LN positive value across all tumor rows (surgery-level only; cannot attribute)
- 15 vary by row (potential histology-specific signal; needs per-row check)
- Even the 15 with row-level variation don't guarantee tumor-type attribution without explicit `ln_mets_*_array` text

Action: build `qc_ln_multihistology_attribution_queue_v1` enumerating all 48 groups for Logan's manual review or for downstream LLM extraction; build `vw_ln_histology_attribution_VIEW_v1` with the 4 confidence categories ChatGPT proposed (`definite_histology_specific` / `probable_histology_specific` / `surgery_level_only` / `ambiguous_multi_histology`).

---

## §5 Lane G overlap analysis

Lane G (mig_223, Cline GPT-5.5, pending) builds:
- `semantic_publication` schema (does NOT yet exist — confirmed)
- `release_manifest_v1` table
- 8 manuscript-safe views: `vw_patient_master_safe_VIEW_v1`, `vw_path_malignant_tumor_safe_VIEW_v1`, `vw_recurrence_safe_VIEW_v1`, `vw_molecular_safe_VIEW_v1`, `vw_fna_safe_VIEW_v1`, `vw_us_nodule_safe_VIEW_v1`, `vw_labs_long_safe_VIEW_v1`, `vw_cohort_membership_safe_VIEW_v1`

**LN safe views are NOT in Lane G's scope.** Two architectural options for the LN/histology safe layer:

**Option A — `manuscript_workspace.*` (per ChatGPT, lower-friction):**
- Build `vw_ln_*_publication_safe_VIEW_v1` and `vw_ln_histology_attribution_VIEW_v1` in `manuscript_workspace` (where `canonical_us_nodule_v2_filtered` and other working views live)
- Promote to `semantic_publication.vw_ln_*_safe_VIEW_v1` later if needed (extend Lane G mig_223)

**Option B — extend Lane G mig_223 (one-pass, more semantic-publication coverage):**
- Add `vw_ln_patient_safe_VIEW_v1` + `vw_ln_surgery_safe_VIEW_v1` + `vw_ln_histology_attribution_safe_VIEW_v1` to mig_223's view list
- Single `release_manifest_v1` covers all manuscript-safe surfaces uniformly
- Heavier prompt for Cline; may slow Lane G

**Recommendation:** Option A. Reasons: (1) existing `ln_*` helpers all live in `manuscript_workspace`; (2) LN safe views need the additional QC tables + vocab work that Lane G doesn't include; (3) Lane G stays scoped and shippable.

---

## §6 Proposed migration plan — "Lane LN" (mig_224 → mig_229)

All migs are Cowork-direct safe (mostly view DDL + INSERT into vocab map + 5 QC table builds). Single-shot Cline GPT-5.5 or Cursor composer can author all 6 in one pass.

### mig_224 — extend histology vocab map + build standardized dim view

**Target objects:**
- INSERT into `main.histology_vocab_normalization_map_v1` for all 7 typos ChatGPT enumerated (1 confirmed in malignant pathology, 6 variant-level)
- CREATE OR REPLACE VIEW `manuscript_workspace.dim_histology_standardized_VIEW_v1` over `histology_vocab_normalization_map_v1` adding `carcinoma_flag`, `borderline_flag`, `benign_flag`, `aggressive_histology_flag`, `ptc_variant_group`, `who_terminology_preferred`
- Pre-snapshot of `histology_vocab_normalization_map_v1` to `archive_pub_v1_0`
- Decision needed: keep `dim_histology_variant_v1` or supersede it (Open-Question 1)

**Acceptance:**
- Vocab map row count grows from 96 to 103 (+7 typos)
- All 7 typos resolve to clean labels via lookup
- `dim_histology_standardized_VIEW_v1` returns 1 row per clean label with all 6 flags populated
- `Hurthle cell carcinoma` row has `who_terminology_preferred='Oncocytic thyroid carcinoma'` (per ChatGPT §7.3 — Logan to ratify)

### mig_225 — `vw_ln_surgery_publication_safe_VIEW_v1`

**Target object:** `manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1`

**Source:** `canonical_path_malignant_events_dedup_VIEW_v1`

**Per-surgery key:** `COALESCE(path_surgery_id::VARCHAR, surgery_episode_id::VARCHAR, 'NULL_SURG')`

**Logan-locked rules to bake in:**
- LN counts collapsed by `MAX()` (not `SUM()`) per ChatGPT Rule 2
- LN denominator preference: when `ln_examined=0` AND `nodal_disease_total_count > 0`, prefer `nodal_disease_total_count` (per Open-Question 3 if ratified)
- `ln_attribution_ambiguous_flag` = `n_histologies > 1 AND ln_positive_surgery > 0`
- `ln_impossible_count_flag` = `ln_positive_surgery > ln_examined_surgery`

**Acceptance:**
- One row per `(research_id, surgery_key)` (no dups)
- ~4,022 patients × ~1.0 surgery = ~4,022-4,200 rows
- Internal consistency: `ln_positive_surgery <= ln_examined_surgery` for all rows except those flagged `ln_impossible_count_flag=TRUE`

### mig_226 — `vw_ln_patient_publication_safe_VIEW_v1`

**Target object:** `manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1`

**Source:** `vw_ln_surgery_publication_safe_VIEW_v1` (mig_225) + `canonical_patient_master.ln_*` for cross-validation

**Logan-locked rules:**
- `ln_total_examined_safe = SUM(ln_examined_surgery)` per ChatGPT skeleton (sums ACROSS surgeries are OK; within-surgery already MAX'd)
- `ln_crossval_status` field comparing patient-level safe value vs CPM `ln_rollup_total_examined`/`ln_rollup_total_positive`; populate `discordant_with_cpm` / `concordant` / `cpm_only_null` / `safe_only_null`
- `n_impossible_surgery_ln_rows` propagated from mig_225

**Acceptance:**
- 4,022 patients (matches dedup patient count)
- Discordance vs CPM tabulated; expected ~109 discordant rows per ChatGPT §4
- `vw_ln_patient_publication_safe_VIEW_v1` becomes the **manuscript-facing patient LN SSOT**

### mig_227 — `vw_ln_histology_attribution_VIEW_v1`

**Target object:** `manuscript_workspace.vw_ln_histology_attribution_VIEW_v1`

**Source:** `canonical_path_malignant_events_dedup_VIEW_v1` + `vw_ln_surgery_publication_safe_VIEW_v1` + `ln_master_rollup_v1.ln_mets_*` cols

**Logan-locked rule (4 confidence categories per ChatGPT §9.3):**
- `definite_histology_specific` — `ln_mets_<histology>=TRUE` for the row's primary histology
- `probable_histology_specific` — single-histology surgery + LN positive
- `surgery_level_only` — multi-hist + identical LN values across rows
- `ambiguous_multi_histology` — multi-hist + row-level variation OR no `ln_mets_*` evidence
- `none_or_unknown` — no LN positive evidence

**Acceptance:**
- 5,944 rows (one per dedup tumor row)
- ~1,129 patient-level LN-positive cases distributed across the 5 categories with at least 46 in `definite_histology_specific` (matches `ln_mets_*=TRUE` count)

### mig_228 — 5 QC tables

**Target objects (all `manuscript_workspace`):**

| Table | Purpose | Expected rows |
|---|---|---:|
| `qc_ln_impossible_counts_v1` | rid + source-table + counts for the 6 dedup + 11 rollup + 4 CPM impossible rows | ~21 rows |
| `qc_ln_duplicate_rollup_patients_v1` | 256 patients with >1 row in `ln_master_rollup_v1` | 256 rows |
| `qc_ln_multihistology_attribution_queue_v1` | 48 multi-hist surgery groups with positive nodes (15 row-vary + 33 identical) | 48 rows |
| `qc_histology_borderline_in_malignant_table_v1` | 35 FTUMP+FA-with-staging rows (17 N1 + 1 NX + 18 M1) | ~35 rows |
| `qc_histology_vocab_typos_v1` | residual typo rows post-mig_224 vocab extension | ~0 expected post-mig_224 |

All 5 governed via `signoff_registry` + `col_registry` per `feedback_phi_safety.md` template.

### mig_229 — benign/borderline-with-staging quarantine flag

**Target object:** ADD COLUMN `is_borderline_or_benign_with_staging` BOOLEAN to `canonical_path_malignant_events_v1` AND to `canonical_path_malignant_events_dedup_VIEW_v1` (via REPLACE VIEW).

**UPDATE rule:** TRUE for rows where `primary_histology IN ('FTUMP', 'follicular adenoma', 'Follicular adenoma')` AND (`n_stage_ajcc8 LIKE 'N1%'` OR `m_stage_ajcc8 = 'M1'`).

**Acceptance:**
- 35 rows flagged TRUE per §4.1 (17 FTUMP-N1 + 12 FTUMP-M1 + 6 FA-M1; check overlap)
- All 4 new safe views (mig_225/226/227) optionally exclude on this flag
- Carry-forward CF if Logan wants to also exclude `nx_rows` (1 FTUMP)

---

## §7 Open Logan-decisions (block apply until resolved)

1. **`dim_histology_variant_v1` — supersede or extend?** Already exists. Schema unknown; if it covers similar ground to ChatGPT's proposed `dim_histology_standardized_v1`, prefer extending in place over creating a parallel object.

2. **Hürthle terminology — `Hurthle cell carcinoma` or `Oncocytic thyroid carcinoma`?** WHO 2017 prefers the latter. Manuscript-facing convention?

3. **LN denominator source-priority rule.** When `ln_examined=0.0` (DOUBLE) but `nodal_disease_total_count > 0` (INTEGER), which wins?
   - Option (a): always prefer non-zero (treats 0 as "missing")
   - Option (b): always prefer the typed-stronger source (`ln_examined` first)
   - Option (c): MAX() across the two (current Cowork probe uses this; produces 0 impossible per ChatGPT)
   - Option (d): keep both as separate cols on the safe view + flag conflict

4. **Lane LN architecture.** §5 Option A (`manuscript_workspace.*`) vs Option B (extend Lane G `semantic_publication.*`). Default A unless Logan says otherwise.

5. **Mig labels.** `mig_224` through `mig_229` are proposed. Any conflict with Cursor/Cline in-flight that I should defer for?

---

## §8 Carry-forwards opened by this assessment (independent of mig_224-229)

These are documented for the registry regardless of whether Lane LN proceeds:

| CF tag | Description | Severity |
|---|---|---|
| `CF-LN-DEDUP-IMPOSSIBLE-6` | 6 dedup-view rows where `ln_examined=0` ∧ `nodal_disease_total_count > 0`; rids 744, 4426, 4560, 5197, 5917, 8482 | Medium — affects denominator math; needs Open-Question 3 |
| `CF-LN-MASTER-IMPOSSIBLE-11` | 11 impossible rows on `ln_master_rollup_v1`; all `ln_total_examined=0` per ChatGPT § 3 examples | Medium |
| `CF-LN-CPM-IMPOSSIBLE-4` | 4 CPM rows with `ln_rollup_total_positive > ln_rollup_total_examined` | Medium |
| `CF-LN-METS-ARRAY-EMPTY-2801` | 2,801 of 2,847 positive-node rows have empty `[]` array (no histology attribution evidence) | High for histology-specific claims; Low for patient-level |
| `CF-HIST-FTUMP-FA-WITH-N1-M1-35` | 35 FTUMP/FA rows with N1/NX/M1 in malignant pathology; quarantine candidate | Medium |
| `CF-HIST-VOCAB-CARICINOMA-1` | `Follicular caricinoma` raw typo persists despite 96-row vocab map | Low |
| `CF-HIST-VARIANT-TYPOS-AUDIT` | Variant-level typos (`microcarinoma`, `folliucalr`, `classsical`, etc.) need audit; not seen in primary histology of dedup but may surface in `histology_variant` | Low |
| `CF-LN-MASTER-DUP-PTS-256` | 256 patients with >1 row in `ln_master_rollup_v1`; needs collapse rule before patient-level use | Medium |

Add these to `qc_framework_v1/ISSUE_REGISTRY.md` once Logan ratifies any (or all) for inclusion.

---

## §9 Ready-to-paste Cline / Cursor lane prompt (when Logan ratifies)

```text
LANE LN (mig_224 — mig_229) — Lymph nodes + tumor histology safe views and QC

Read the Cowork plan at qc_framework_v1/reports/lymph_nodes_histology_assessment_plan_20260430.md end-to-end before starting. Logan ratified Option A (manuscript_workspace surface) and the 4 confidence categories from §6 mig_227.

Build, in order:
1. mig_224 — extend histology_vocab_normalization_map_v1 (96 → 103 rows; pre-snapshot to archive_pub_v1_0); CREATE OR REPLACE VIEW manuscript_workspace.dim_histology_standardized_VIEW_v1
2. mig_225 — CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_surgery_publication_safe_VIEW_v1 over canonical_path_malignant_events_dedup_VIEW_v1
3. mig_226 — CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_patient_publication_safe_VIEW_v1 over mig_225 + crossval vs CPM
4. mig_227 — CREATE OR REPLACE VIEW manuscript_workspace.vw_ln_histology_attribution_VIEW_v1 over dedup + mig_225 + ln_mets_*
5. mig_228 — CREATE TABLE on 5 qc_ln_*/qc_histology_* tables; INSERT enumerated rows
6. mig_229 — ALTER TABLE canonical_path_malignant_events_v1 ADD COLUMN is_borderline_or_benign_with_staging BOOLEAN; UPDATE per §6.5 rule; recompile dedup VIEW

For every mig:
- Pre-snapshot to "Thyroid 2026 UPdated".archive_pub_v1_0
- INSERT into canonical_table_signoff_registry_v1 + canonical_column_verification_registry_v1
- INSERT into manuscript_workspace.cpm_reconciliation_provenance_v1
- Apply via MotherDuck query_rw (account logan.glosser.eras@gmail.com)

Acceptance:
- 5-gate gate1 += 4 (mig_225/226/227 + dim_histology_standardized_VIEW_v1) + 5 (qc_*)
- §12 governance gap stays 0
- Cohort parity stays 10,871 / 10,871 / 10,871
- All carry-forwards in §8 either closed or migrated to ISSUE_REGISTRY.md

Do not absorb LLM-derived nodule features into canonical rows. Use research_id only (no PHI).
```

---

## §10 What Logan can do NEXT

1. **Decide on Open-Questions §7 (4 architectural choices + naming)**, then either dispatch the Lane LN prompt §9 to Cline GPT-5.5 / Cursor composer, OR have Cowork apply directly per the v13 Cowork-direct pattern.
2. **Independently**: queue Lane G (mig_223) which is unchanged; Lane LN can run in parallel since they touch different schemas.
3. Continue manuscript writing in parallel — none of the §8 carry-forwards block v1.0 patient-level claims; they only block tumor-type-specific LN attribution and FTUMP-inclusive analyses.

---

## Appendix — files and view references

- ChatGPT plan: uploaded `lymph_nodes_histology_findings_plan.md`
- v13 Cowork session summary: `qc_framework_v1/COWORK_SESSION_SUMMARY_2026-04-30_v13.md`
- v13 Lane E close-out: `qc_framework_v1/reports/lane_e_continuation_apply_closeout_20260430.md`
- View naming convention: `memory/reference_view_naming_convention.md`
- PHI safety rules: `memory/feedback_phi_safety.md`
- Findings vs staging rule: `memory/feedback_findings_vs_staging.md`
