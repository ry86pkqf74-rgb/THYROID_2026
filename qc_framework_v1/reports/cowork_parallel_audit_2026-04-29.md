# Cowork Parallel-Audit Findings — 2026-04-29 (late evening)

Generated while Cursor agents were running mig_167–170 in parallel. Read-only against MotherDuck `thyroid_canonical_publication_v1_0`. No data writes.

---

## §1 PM BOOLEAN cohort-uniformity back-sweep (398 verified cols)

Method: classify every verified BOOLEAN col on `canonical_patient_master` by direction (T-only / F-only / near-uniform), cross-reference with existing CF notes, surface only finds NOT already documented. Helper script: `scripts/_cowork_pm_bool_sweep_batched.py`.

### §1.1 Genuine new sneakers (need follow-up)

| col | t / f / n | reason | proposed CF |
|---|---|---|---|
| `rln_permanent_flag` | 0 / 10871 / 0 | mig_135 cluster note exists, but contradicts `comp_rln_injury_confirmed` (39 TRUE) — RLN injuries clearly exist; the refined_v2 spine appears unpopulated | `CF-mig167-RLN-FLAG-DEGENERATE-VS-COMP-RLN-39-CONFIRMED` |
| `rln_transient_flag` | 0 / 10871 / 0 | same lineage as above; same contradiction | (same CF) |
| `nsqip_hypoparathyroidism_recovered_flag` | 0 / 10871 / 0 | mate `nsqip_hypocalcemia_recovered_flag` has 80 TRUE; identical NSQIP study scope; either NSQIP doesn't track this outcome (col should be `na`) or builder bug | `CF-mig167-NSQIP-HYPOPARA-RECOVERED-DEGENERATE` |
| `biochemical_concern_flag` | 0 / 10871 / 0 | mig_134 note says "Script 224 helper... those deferred"; verified-but-deferred is the wrong status; should be `na` until Script 224 lands | `CF-mig167-BIOCHEMICAL-CONCERN-DEFERRED-PLACEHOLDER` |
| `ames_calculable_flag` | 10871 / 0 / 0 | Type-A near-uniform-TRUE; mig_155 missed the CF tag; **mig_161 §B1 already has a registry-note appendix for this** — apply mig_161 to close | already covered by mig_161 §B1 |

### §1.2 Type-A presence-flag pattern (14 finds — by-design valid, no action needed individually)

These cols are presence indicators (`*_has_data`, `op_drain_placed_any`, `mri_has_data`, `pet_has_data`, etc.) where T>0 / F=0 / N>0 is the intentional encoding ("flag present means data exists; NULL means no data for this patient"). All are cluster-noted via the parent batch. Could batch-add `CF-COWORK-PRESENCE-FLAG-PATTERN` notes for posterity, but not blocking manuscript pipeline.

### §1.3 Most "Type-B" finds are cluster-doc'd (false-positive in matcher)

50 of the 53 raw Type-B finds are covered by mig_133 / 134 / 135 / 140 cluster batch notes (e.g., `comp_*` 0-TRUE entries are valid because the canonical_complications_events_v1 SSOT yields zero patients with that specific evidence-level — clinically faithful). The matcher false-flagged them because the cluster notes don't use `TYPE-B` / `UNIFORM-FALSE` keywords. Three are real sneakers (above).

### §1.4 Near-uniform-FALSE finds (62 cols, < 1% TRUE)

Almost all are clinically-expected low-prevalence events (mortality, aggressive variant, rare LN met histology, rare TERT/RET mutations). Cluster-noted via parent batches. Not actionable individually.

---

## §2 CF backlog inventory

- **135 distinct CF tags** across the lakehouse
- **1,168 total CF appearances** (some cols carry multiple CFs)
- **771 cols** have at least one open CF (≈ half of all verified analytic cols)

### §2.1 Top-10 highest-volume CF tags

| Rank | CF tag | n_cols | n_tables | Status |
|---|---|---|---|---|
| 1 | `CF-mig136-DAYS-SEMANTIC` | 58 | 1 | open — semantic question on day-counting cols |
| 2 | `CF-117-US-EXAM-ID-PORTABILITY` | 53 | 1 | pre-Cowork ETL CF |
| 3 | `CF-117-US-LATERALITY-RAW` | 53 | 1 | pre-Cowork ETL CF |
| 4 | `CF-117-US-NODULE-RANGE` | 53 | 1 | pre-Cowork ETL CF |
| 5 | `CF-GEN07-ROM-OCR` | 41 | 1 | molecular OCR provenance |
| 6 | `CF-90-DATE-FORMAT` | 38 | 1 | **closed by mig_160 once applied** |
| 7 | `CF-87-AJCC` | 36 | 1 | manuscript-spine adjudication |
| 8 | `CF-100-DATE-RETYPE` | 29 | 5 | **closed by mig_160 once applied** |
| 9 | `CF-117-US-GLAND-PARENCHYMA` | 28 | 1 | pre-Cowork ETL CF |
| 10 | `CF-mig137-PM-MOL-DATE-RETYPE` | 27 | 1 | **closed by mig_160 once applied** |

### §2.2 What mig_160 alone closes

When mig_160 is applied, it closes these date-retype CFs, eliminating ~150 col-impact:
- `CF-100-DATE-RETYPE` (29 cols across 5 tables)
- `CF-mig137-PM-MOL-DATE-RETYPE` (27)
- `CF-mig120-PATH-MALIG-DATE-RETYPE` (14)
- `CF-119-FROZEN-ROLLUP-DATE-RETYPE` (14)
- `CF-mig146-PM-PET-FIRST-LAST-DATE-VARCHAR` (13)
- `CF-mig134-PM-LAB-DATE-ANCHOR` (13)
- `CF-mig122-RECURRENCE-FIRST-SURGERY-DATE-RETYPE` (12)
- `CF-mig130-PM-FIRST-SURGERY-DATE-RETYPE` (12)
- `CF-mig123-RECURRENCE-DATE-RETYPE` (10)
- `CF-mig133-PM-CNCLN-DATE-PARSE` (6)
- `CF-90-DATE-FORMAT` (38)

→ **~190+ col-impact CFs closed by a single mig_160 apply.**

### §2.3 High-priority manuscript-blocking CFs (not auto-closed)

- `CF-mig136-DAYS-SEMANTIC` (58 cols) — semantic ambiguity on day-counting (event-grain start? surgery anchor? LKA?). Manuscript pipeline needs a decision.
- `CF-mig138-CPM-RECURRENCE-SPINE-RESYNC-PENDING` (19) — recurrence spine work. Likely closed by mig_163b when applied.
- `CF-mig156-ANY-RECURRENCE-*` (13) — closed by mig_163b apply.
- `CF-mig150-TP-UPSTREAM-NOT-IN-MAIN` (9) — needs `canonical_us_lymph_node_v2` Tier-2 build.
- `CF-mig154-PM-VI-VS-INVASION-EVENT-PRESENT` (12) — invasion family reconcile.
- `CF-mig150-PTH-MULTI-SOURCE-DERIVATION` (7) — notes-PTH source restoration.
- `CF-mig144-PM-US-DUAL-SPINE` (7) — US v1/v2 dual-spine reconcile.

---

## §3 mig_166 verification (Path C)

Verified clean live (manuscript_workspace.canonical_cleanup_audit_v1):
- Pre-mig_166 state: 18 cols all `na` from mig_165, table_status=verified, signoff_migration→mig_165
- mig_166 elevates 15 cols `na→verified` with proper methodology lineage; keeps 3 as `na` (1 identifier + 2 audit timestamps)
- Header BOOLEAN claims match live: `is_referenced_by_view` 6/114/0 ✓, `is_referenced_by_script` 120/0/0 ✓ (CF-mig166-COHORT-NEAR-UNIFORM-TRUE), `is_identical_to_twin` 0/0/120 ✓, `has_version_twin` 0/120/0 ✓ (CF-mig166-COHORT-NEAR-UNIFORM-FALSE)
- 120 rows total ✓
- Two intentional CFs documented (Type-A presence flag + Type-B classifier-faithful)

→ **mig_166 is clean to apply via Path C.** Add to apply queue.

---

## §4 Live state snapshot (post-mig_165, pre-anything-else)

| Metric | Value |
|---|---|
| Latest commit | `2395059` (post-HYBRID prompt update) |
| gate1 / gate2 / gate3 / gate4 / gate5 | 165 / 0 / 0 / 0 / 21 |
| PM table_status | in_progress (1,441 verified / 13 na / 144 not_started / 0 failed / 1,598 total) |
| Status hist | 165 verified / 1 in_progress / 10 not_started / 176 total |
| Cohort parity | 10,871 rows / 10,871 distinct rids ✓ |

---

## §5 Recommendations

1. **Apply queue tomorrow** (in this order, with pre-snapshot per step):
   - mig_161 + mig_161b (registry notes only)
   - mig_159 (registry status flips, 27 cols)
   - mig_160 (structural ALTERs, 21 cols × 5 tables — closes ~190 col-impact CFs)
   - mig_166 (registry refinement, 18 cols)
   - After mig_152 NLP lands (Cursor), mig_162 PM finalization
   - After Logan ratifies, mig_163b HYBRID apply

2. **Open these CFs in mig_167 (or a small mig_167-cowork addendum)**:
   - `CF-mig167-RLN-FLAG-DEGENERATE-VS-COMP-RLN-39-CONFIRMED` (rln_permanent_flag, rln_transient_flag)
   - `CF-mig167-NSQIP-HYPOPARA-RECOVERED-DEGENERATE`
   - `CF-mig167-BIOCHEMICAL-CONCERN-DEFERRED-PLACEHOLDER`

3. **mig_136 days-semantic decision** — 58 cols at risk; needs Logan's ratification on the day-counting anchor (event start vs surgery vs LKA). Could be its own lane.

4. **`CF-mig150-TP-UPSTREAM-NOT-IN-MAIN`** — needs `canonical_us_lymph_node_v2` Tier-2 build; deferred.
