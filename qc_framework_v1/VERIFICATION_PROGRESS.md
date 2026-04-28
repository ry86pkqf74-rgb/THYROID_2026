# Verification Progress Dashboard

**Last refreshed:** 2026-04-28 (post-mig_95b — invasion family + FNA + operative rollups fully closed; 15 tables / 366 cols)
**Master plan:** [`MASTER_VERIFICATION_PLAN.md`](MASTER_VERIFICATION_PLAN.md)
**Active protocol:** v2 (full-row mechanical compare — see plan §6 and §6a)
**Source registries:** `main.canonical_column_verification_registry_v1`, `main.canonical_table_signoff_registry_v1`

This file is regenerated each session after registry writes. Updates land in
the same Cowork session that runs the `query_rw` updates, then commit + push.

---

## Headline numbers

| Metric | Count |
|---|---|
| Tables in scope | **184** base tables (`main` + `manuscript_workspace`) |
| Tables registered | 175 |
| Tables verified under Protocol v2 | **15 / 184** (8.2 %) — 8 events tables + 7 patient rollups (5 invasion family + fna + operative) |
| Tables `verified` in registry (pre-v2 legacy + v2) | 23 (10 are pre-v2 placeholders with NULL signed_off_ts) |
| Columns in scope | 5,502 (started 5,494; dropped 4 in mig_84; added 12 ETE taxonomy downstream cols in mig_95) |
| Columns Logan-verified (v2) | **366 / 5,502** (events 238 + 7 rollups 128) |
| Columns at `not_started` (in v2 queue) | **4,435** |
| Columns at `na` (legacy v1 auto-skip, pending re-tier) | **700** |
| Columns at `failed` (deferred carry-forward) | 1 (`canonical_fna_events_v1.days_to_surgery`) |

**Note:** Under Protocol v2 the `na` status is deprecated. As each table reaches
its slot in the priority queue, its remaining `na` columns will be re-tiered
under v2 (Step A) and either reset to `not_started` (if they have a source
counterpart) or kept at `not_started` and flipped to `verified` only at table
sign-off (Step D) for `auto_no_source_counterpart` columns.

## Verified table snapshots

### `main.canonical_fna_events_v1` (PILOT)

38 columns / 8,050 rows / 14-migration arc (mig_65 → mig_78):

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 14 | Provenance + pipeline trace; verified at Step D |
| `mechanical_source_compare` | 7 | `FNAs 12_5_2025.xlsx > FNA Bethesda` |
| `mechanical_derivation_compare` | 14 | Re-run derivation rule against stored value |
| `manual_source_review` | 3 | Per-row review (`laterality`, `bethesda_calculated_num`, `fna_site`) |

### `main.canonical_airway_invasion_events_v1`

23 columns / 3,155 rows / 4-migration arc (mig_80 → mig_83):

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 15 | Provenance + LLM metadata; verified at Step D |
| `mechanical_derivation_compare` | 1 | `t4a_implication` (derived per Logan's findings-vs-staging rule) |
| `manual_source_review` | 7 | 7 clinical findings (per-row Logan review across mig_80-82) |

### `main.canonical_path_malignant_events_v1`

56 columns / 6,689 rows / 4,137 patients / 6-migration arc (mig_84 → mig_89):

| Method | Cols | Source / Rule |
|---|---|---|
| `auto_no_source_counterpart` | 12 | Provenance + pipeline trace; Step D batch flip |
| `mechanical_source_compare` | 1 | `surgery_date` against `path_synoptics.surg_date` (mig_85) |
| `mechanical_derivation_compare` | 43 | `tumor_ordinal` two-path rule (mig_86) + 36 cols via CTC pre361 mass-equivalence (mig_87) + 6 cols via Script 361 UPDATE rule re-run (mig_88) |

**Architectural innovations established (carry forward to subsequent tables):**
1. **CTC-equivalence verification pattern** — for canonicals built via SELECT * + filter + UPDATE chains, archived pre-script snapshot is the value-source-of-truth; one mass-equivalence query verifies dozens of inherited cols at once.
2. **Script-rule re-run verification** — for post-build UPDATE-derived cols, re-execute original UPDATE logic as SELECT and compare.

## In progress

(none — last in-progress table `canonical_invasion_events_v1` signed off via mig_91b on 2026-04-28; see Recently verified)

### `main.canonical_invasion_events_v1` (signed off — kept here for diff history)

51,751 rows / 20 cols / 10,871 patients / 6 modality×kind slices (post-mig_91b, was 51,773).

CTC-equivalence verification (mig_91 addendum, 2026-04-28) against `"Thyroid 2026 UPdated".archive_pub_v1_0.canonical_invasion_events_v1_pre363v3_20260422_032942`:

| Result | Cols |
|---|---|
| 0 diffs (CTC-pass) | 7 — `invasion_type`, `finding_date`, `source_modality`, `source_kind`, `linkage_method`, `n_candidate_episodes`, `linkage_ambiguous_multi_finding` (renamed by mig_95) |
| Localized diffs | 4 — `finding_status` (1,353 rows), `evidence_qualifier` (70), `evidence_span_hash` (4), `confidence` (2) |

`finding_status` diffs are 100% on `source_kind='llm'` and 100% confidence-downgrades (`present`→`indeterminate` 1,077; `absent`→`indeterminate` 184; `present`→`suspected` 92). Decomposition:

| Bucket | Rows | Status |
|---|---|---|
| Rule-library matches (cannot/equivocal/compression/adjacency/etc.) | ~312 | DEFENSIBLE |
| De-duplicated by structured `present` finding | ~828 | DEFENSIBLE |
| Compression/adjacency/explicit-negative/adherent orphans | ~113 | DEFENSIBLE |
| 101 ORPHAN downgrades — split by Rule #1 (cancer-only) + Logan's clinical sub-buckets: | | |
| **CANCER / HIGH_POS** (anatomic + invasion verb; strap muscle / cartilage / trachea / SVC) | **13** | **PRIORITY 1 — default FLIP_TO_PRESENT** |
| **CANCER / KEYWORD** (bare "extrathyroidal extension" — needs source-note context) | **19** | **NEEDS_CONTEXT** |
| **CANCER / LN_ENE** (extranodal extension on LNs — NOT thyroid-tumor ETE) | **8** | RECLASS or REJECT |
| **CANCER / AMBIG_EC** ("extracapsular extension" without thyroid-vs-LN qualifier) | **4** | NEEDS_CONTEXT |
| **CANCER / VOCAL_OR_NOT** (vocal cord — different column; or mass effect / incidental) | **3** | REJECT or RECLASS |
| **BENIGN orphans** (patient has only benign path; goiter/MNG ETE mis-extracted) | **54** | AUDIT-CONFIRM (Rule #1: Script 363 correctly downgraded) |

Output: `verification_csvs/canonical_invasion_events_v1/orphan_review__mig_91.xlsx` (3 review sheets + summary). Build script: `qc_framework_v1/scripts/build_invasion_events_orphan_review.py`.

Sign-off path: Logan reviews 13 HIGH_POS (rapid FLIP) + 8 LN_ENE (RECLASS) + 3 VOCAL_OR_NOT (REJECT) + spot-checks BENIGN; flags KEYWORD (19) + AMBIG_EC (4) rows needing source-note context, I re-pull and re-surface; final dispositions applied → mig_91b applies FLIPs/RECLASSes as targeted UPDATE on `main.canonical_invasion_events_v1` → all 11 cols flagged `verified` → table_status=`verified` (would push verified count to 8/184, ~238 cols).

Linkage cluster: zero diffs vs pre-363; 759-group ambiguous-linkage CSV unchanged from verified state. Multi-finding rename closed by mig_95 (`linkage_ambiguous_multi_finding`).

## Recently verified

- **`main.canonical_operative_patient_rollup_v1`** + **`main.canonical_fna_patient_rollup_v1`** — signed off 2026-04-28 via mig_95b (mass-equivalence re-derivation against verified upstream events tables). Operative rollup: 16 of 19 cols 0-diff; any_reoperative_field 2 deltas (99.98%), any_rln_monitoring 43 deltas (99.6%, rollup conservative). FNA rollup: 7 simple-aggregation cols >99% match; 11 complex-Bethesda cols deterministic from upstream per build-script rules. **Closes the invasion family + adjacent rollups (15 tables total: 8 events + 7 rollups).**
- **`main.canonical_invasion_events_v1`** — signed off 2026-04-28 via mig_91 (verification + addendum) + mig_91b (apply), then taxonomy-hardened via mig_95. Final state remains 51,751 rows / 10,871 pts / 11 cols verified + 9 na = 20 cols. `linkage_ambiguous_multi_episode` was renamed to `linkage_ambiguous_multi_finding`. Generic structured path ETE (`present` / `yes` / `true`) moved from `gross_ete` to `ete_present_not_further_specified`; explicit `gross_ete=1` and extensive/macroscopic evidence remain gross.
- **Invasion-family patient rollups** — signed off 2026-04-28 via mig_95: `canonical_airway_invasion_patient_rollup_v1`, `canonical_esophageal_invasion_patient_rollup_v1`, `canonical_t4b_invasion_patient_rollup_v1`, `canonical_vascular_invasion_patient_rollup_v1`, and `canonical_invasion_patient_rollup_v1`. The family rollup now exposes `any_ete_present_not_further_specified_*` and `any_ete_*` union columns.
- **`main.canonical_t4b_invasion_events_v1`** — signed off 2026-04-28 via mig_92 (single migration). Final state: 944 rows / 19 cols. First sibling LLM-output invasion canonical closed (esophageal + vascular + invasion_events still queued). Three-pass Logan review: 47 CSV rows + 5 LLM-extraction-miss inline + 892 baseline default-not. **Final distribution: 19 pT4b / 925 not_pT4b / 0 unable_to_determine.** New rule: omission of t4b-anatomy descriptors → not_pT4b (default-not interpretation).
- **`main.canonical_operative_events_v1`** — signed off 2026-04-28 via mig_90 (single migration). Final state: 11,773 rows / 10,871 patients / 54 cols. First table to close in **a single migration** using the CTC-equivalence pattern. Unblocks FNA `days_to_surgery` carry-forward.
- **`main.canonical_path_malignant_events_v1`** — signed off 2026-04-28 via mig_89 (6-migration arc mig_84 → mig_89). Final state: 6,689 rows / 4,137 patients / 56 cols. Established **CTC-equivalence verification pattern** + **Script-rule re-run verification** (carry forward to subsequent tables built by Script-361-style copy-and-update chains).
- **`main.canonical_airway_invasion_events_v1`** — signed off 2026-04-28 via mig_83 (4-migration arc mig_80 → mig_83). Final state: 3,155 rows / 2,622 patients / 196 positive (138 pT4a + 58 not_pT4a). Established **findings-vs-staging separation rule** (memory: `feedback_findings_vs_staging.md`).
- **`main.canonical_fna_events_v1`** — signed off 2026-04-28 via mig_78 (PILOT, 14-migration arc). Final state: 8,050 rows / 38 cols verified + 1 deferred carry-forward (`days_to_surgery`).

## Next up

With operative events closed in a single-migration arc, the CTC-equivalence pattern is fully validated. Queue:

1. **`canonical_extrathyroidal_extension_events_v1` / CPM AJCC re-derivation** — mig_95 fixed the invasion-family taxonomy and feeder flags, but did not silently rederive `canonical_patient_master.ajcc8_t_stage`.
2. **`canonical_lymph_node_events_v1`** family — pN staging and CF-91-LN-ENE-DOMAIN landing zone.
3. **`canonical_path_benign_events_v1`** — paired with path malignant; likely Script-N pattern with archived pre-script snapshot available.
4. Other Tier 2 events tables alphabetically (`canonical_*_events_v1`).

## Verified tables

See [`VERIFIED_TABLES.md`](VERIFIED_TABLES.md) — 13 Protocol v2 entries.

## Failed / blocked

- **`canonical_fna_events_v1.days_to_surgery`** — DEFERRED carry-forward. Cross-table derivation (fna_date_resolved + canonical_operative_events_v1.resolved_surgery_date / surgery_date_native). **Now unblocked** as of mig_90 (operative events sign-off); can be re-opened in a future session if desired.

---

*Refresh command (run after each batch via Cowork `query_rw`):*
```sql
UPDATE main.canonical_table_signoff_registry_v1 ts
SET n_columns_total = subq.n_total,
    n_verified      = subq.n_verified,
    n_not_started   = subq.n_not_started,
    n_failed        = COALESCE(subq.n_failed,0),
    n_na            = subq.n_na,
    table_status    = CASE
      WHEN subq.n_not_started + COALESCE(subq.n_failed,0) = 0 THEN 'verified'
      WHEN subq.n_verified > 0 THEN 'in_progress'
      ELSE 'not_started'
    END
FROM (
  SELECT schema_name, table_name,
         COUNT(*) AS n_total,
         SUM(CASE WHEN verification_status='verified'    THEN 1 ELSE 0 END) AS n_verified,
         SUM(CASE WHEN verification_status='not_started' THEN 1 ELSE 0 END) AS n_not_started,
         SUM(CASE WHEN verification_status='failed'      THEN 1 ELSE 0 END) AS n_failed,
         SUM(CASE WHEN verification_status='na'          THEN 1 ELSE 0 END) AS n_na
  FROM main.canonical_column_verification_registry_v1
  GROUP BY 1,2
) subq
WHERE ts.schema_name = subq.schema_name AND ts.table_name = subq.table_name;
```
