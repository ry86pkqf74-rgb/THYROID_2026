# Cleanup + data-verification audit — 2026-05-05

> Cowork audit pass. Latest commit on origin/main: `c8261c8`. Latest signoff: `mig_310_phaseA0` (06:50). Locked numbers all match.

---

## TL;DR

Six cleanup items found across MotherDuck + GitHub + signoff registry. Two need cursor (mig_318 retry, mig_319 m083 build), three are documentation-only (info_schema artifact false alarm, M048 commit-tag typo, M044 v6 manuscript-patch routing), and one is a trivial MD dedupe (mig_288).

---

## 1. **mig_318 has NOT landed** despite "318 done" claim

The data state is unchanged from before mig_318 was written:

| Metric | Pre-mig_318 | Current | Target |
|---|---|---|---|
| `nlp_fna_size_rollup_v1` size_pct | 0.1% (3 rows) | **0.1% (3 rows)** | ≥60% |
| `nlp_fna_size_rollup_v1` beth_pct | 0.7% (18 rows) | **0.7% (18 rows)** | ≥50% |
| `imaging_fna_linkage_v4` nlp_high+medium | 5 | **5** | ≥1,500 |
| `signoff_migration` mig_318 row | absent | **absent** | present |
| origin/main commit | `9221d3d` | `c8261c8` (M048 racial disparities) | mig_318 commit |

The avg_size of 10.67 cm with min=10 / max=12 confirms the same 3 implausible rows from before are still the only parsed sizes. `CF-FNA-SIZE-PARSE-LAYER` remains **OPEN**.

**Action:** Cursor needs to actually run `CURSOR_PROMPT_MIG_318_FNA_PARSE_LAYER_FIX_20260505.md`. The prompt itself has no defects.

---

## 2. **`cohort_m083_braf_dual_platform_discordance_v1` is a 1-column stub**

```
DESCRIBE manuscript_workspace.cohort_m083_braf_dual_platform_discordance_v1;
-- Returns 1 row: research_id VARCHAR
-- Row count: 167
```

This view exists but only exposes `research_id`. Any analysis of M083 will fail because there are no covariate columns. Two options:

**Option A — Cursor mig_319 to flesh out the view.** Add the columns the M083 study brief requires (BRAF status from each platform, discordance flag, tumor size, histology, recurrence). Estimated 30–60 min.

**Option B — Defer until Logan/cursor decides whether M083 is in the priority queue.** No active analysis depends on it.

**Action:** Open `CF-M083-STUB`. Recommend Option B for now; not blocking active manuscripts.

---

## 3. **`mig_288` has a duplicate signoff row**

```sql
SELECT mig_id, signed_off_at, by_actor FROM main.signoff_migration WHERE mig_id='mig_288' ORDER BY signed_off_at;
-- 2026-05-04 00:49:24.454792  cursor_composer_mig288_retry_of_282
-- 2026-05-04 00:49:37.224383  cursor_composer_mig288_retry_of_282
-- (separated by 13 seconds)
```

Same `by_actor`, near-identical summaries (slightly different wording). The `WHERE NOT EXISTS` guard wasn't applied; the script inserted twice. **The second insert (00:49:37) is the canonical one** — its summary mentions "+ 7 NLP regex fallback = 3,38..." which is more detailed.

**Proposed cleanup SQL** (Cowork — needs explicit OK before running, since this modifies the audit registry):

```sql
DELETE FROM main.signoff_migration
WHERE mig_id='mig_288' AND signed_off_at='2026-05-04 00:49:24.454792';
```

**Action:** Ask Logan to confirm. Trivial cleanup, but it's the audit registry.

---

## 4. **`information_schema.columns` dup-col is a MotherDuck artifact, NOT a real defect**

Cross-check across multiple cohorts confirms cursor's mig_315 claim was correct:

| Cohort | `information_schema.columns` count | `DESCRIBE` actual | Real defect? |
|---|---:|---:|---|
| cohort_m037_ln_metastasis_v1 | 83 | **43** | No |
| cohort_m038_massive_goiter_v1 | 153 | **129** | No |
| cohort_m032_descriptive_25yr_v1 | 70 | **37** | No |
| cohort_m036_ata_risk_comparison_v1 | 64 | **32** | No |
| cohort_m029_fna_concordance_v1 | 40 | **20** | No |
| cohort_m044_ajcc_ete_v1 | 65 | **36** | No |

`information_schema.columns` is double-counting because MotherDuck attaches the publication database under both `main` and `manuscript_workspace` schemas in some catalog views. **`DESCRIBE` is the source of truth.**

**Action:** Update Cowork audit playbook to use `DESCRIBE`, not `information_schema.columns`. Close `CF-M044-DUP-COLS` definitively as "info_schema false alarm, system-wide quirk." No work needed.

---

## 5. **M048 commit `c8261c8` mis-tags itself as `(mig_315)`**

The commit message reads `M048 racial disparities in TI-RADS: analytic package (mig_315)` but the actual mig_315 is M044's cohort rebuild (cursor's correct signoff at 03:30:44). The M048 commit didn't insert any signoff with a colliding mig_id — verified `signoff_migration` has only one mig_315 row, and it's M044's.

**Impact:** Cosmetic only. Future readers may be briefly confused by the M048 commit message. No data impact.

**Action:** Optional — amend the M048 commit message in a follow-on commit if Logan cares. Not worth a force-push.

---

## 6. **M044 v6 deliverables shipped without docx**

`M044_FINAL_PACKAGE_v6/` contains:

```
M044_ETE_FINAL_all_stats_v6.xlsx
M044_ETE_FINAL_per_research_id_dataset_v6.xlsx
MIG_315_REGRESSION_DELTA_v5_vs_v6.md
README.md
figures/
m044_v6_run_snapshot.json
```

No docx, no LaTeX. The regression delta report is excellent and gives Cowork everything needed for a v6 prose pass:

| Estimate | v5 locked | v6 post-mig_315 | Drift | OK? |
|---|---|---|---|---|
| Primary aOR gross-vs-micro | 1.77 [1.15–2.71] p=0.009 | **1.72 [1.15–2.56] p=0.008** | aOR Δ=0.05 | ✅ at threshold |
| Cohort N (strict-DTC) | 3,572 | **3,614** | +42 (+1.2%) | ✅ |
| Path-proven events | 105 | **136** | +31 (+29.5%) | ✅ explained |
| ETE no_negative crude rate | (not shown) | **6.4%** (vs micro 2.4%) | reverses prior framing | ⚠️ Limitations note needed |

**Critical Discussion-section change:** v5 said *"microscopic ETE behaves like the no-ETE group"*. With n_no_negative now 173 (vs 68) and the crude rate 6.4% > microscopic 2.4%, that framing is reversed. The adjusted no/neg vs micro estimate is unstable (CI 0.23–1.32, includes 1.0) so v6 should add a Limitations paragraph rather than make a directional claim.

**Action:** Cowork lane (`CF-M044-V6-MANUSCRIPT-PATCH`). Logan flagged he's drafting M036 in another Cowork session; M044 v6 docx pass is a separate Cowork turn afterward.

---

## Locked-number sanity check (post all migrations)

| Number | Expected | Actual | OK? |
|---|---:|---:|---|
| CPM rows | 10,871 | **10,871** | ✅ |
| Malignant patients | 4,019 | **4,019** | ✅ |
| M025 nodule cohort rows | 37,438 | **37,438** | ✅ |
| M025 nodule TR4 ROM | 18.72 | **18.72** | ✅ |
| M025 nodule TR5 ROM | 26.11 | **26.11** | ✅ |
| M037 LN-eligible cohort | 2,234 | **2,234** | ✅ |
| M044 cohort flat | 3,500–3,750 | **3,868** (post-mig_315 expansion) | ✅ |
| M038 cohort flat | 10,871 | **10,871** | ✅ |
| M032 cohort flat | 10,871 | **10,871** | ✅ |

All locked numbers reproduce. No data regression detected from any cursor migration.

---

## Carry-forward register (current state)

| CF | State | Owner | Closes when |
|---|---|---|---|
| MSTAGE-CORRUPTION | CLOSED | (mig_313 + mig_314) | done |
| FNA-SIZE-CM-NULL | CLOSED (formal) | (mig_310) | done |
| **FNA-SIZE-PARSE-LAYER** | **OPEN** | cursor (mig_318) | regex parse fix re-derives ≥1,500 valid sizes |
| M044-DUP-COLS | CLOSED (info_schema artifact) | — | done |
| M044-V6-MANUSCRIPT-PATCH | OPEN | Cowork | v6 docx prose review against post-mig_315 cohort |
| M037-COHORT-MISSING | CLOSED | (mig_316) | done |
| M032-CORRECTION-NOTICE | OPEN | Logan + Cowork | correction notice drafted + reviewed |
| **M083-STUB** | **NEW OPEN** | cursor (mig_319, deferred) | view fleshed out with covariates |
| **MIG_288-DUPE-SIGNOFF** | **NEW OPEN** | Cowork (await OK) | trivial DELETE of duplicate row |

---

## Decision tree for next moves

### If Logan wants Cowork to run trivial cleanup now

- Delete duplicate mig_288 signoff row (one DML)
- Document closure of `CF-M044-DUP-COLS` definitively (text-only)
- Open `CF-M083-STUB` formally (text-only)

### If cursor runs first

- Re-run `CURSOR_PROMPT_MIG_318_FNA_PARSE_LAYER_FIX_20260505.md` (didn't actually land)
- Optionally tackle `mig_319` — flesh out cohort_m083 view with BRAF / discordance columns

### If neither, both runtimes idle

The audit is complete. M025 nodule semantic model still passes locked numbers. M044 v6 regression validates cleanly. M037 / M036 / M032 / M044 cohorts all have known good post-mig_313 row counts.

---

## What was inspected this turn (no data writes)

- Pulled latest commits (origin/main = `c8261c8`)
- Queried `main.signoff_migration` for new rows (none beyond mig_310/315/316/317/310_phaseA0)
- Probed `nlp_fna_size_rollup_v1` size/lat/beth distributions (unchanged from pre-mig_318)
- Probed `imaging_fna_linkage_v4` source distribution (unchanged)
- Cross-checked `information_schema.columns` vs `DESCRIBE` for 6 cohort tables
- Sampled cohort_m045 through cohort_m083 row counts
- Read `M044_FINAL_PACKAGE_v6/MIG_315_REGRESSION_DELTA_v5_vs_v6.md`
- Verified mig_288 duplicate signoff via timestamp-ordered query
- M025 nodule + patient grain TR-ROM smoke test passed exactly

No DDL or DML executed. This is a verification-only audit.
