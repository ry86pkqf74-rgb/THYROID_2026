# Delta vs checked-in April 13 audits

Compared against:

- `studies/20260413_source_truth_completeness_audit/` (executive `CONFIRMED`, YAML criteria)
- `studies/20260413_us_lymph_node_audit/verdict.md`
- `studies/CURRENT_MOTHERDUCK_REPO_STATE.md` (was stale vs HEAD before this session)

## What still matches

| Item | April 13 / prior | This rerun |
|------|------------------|------------|
| COMPLETE → canonical key parity | 19,891 / 19,891 | **Same** |
| `missing_canonical_despite_sufficient_source` (COMPLETE) | 0 | **0** |
| `unresolved_linkage_gap` | 0 | **0** |
| `v_fna_episode_bethesda_resolved_v1` fixable gap | 0 | **0** |
| US LN strict miss lists | 0 positive / 0 negative gaps | **Not re-run**; prior CSVs unchanged |

## What changed (environment / repo)

| Item | Prior checked-in snapshot | Now |
|------|---------------------------|-----|
| `CURRENT_MOTHERDUCK_REPO_STATE.md` Commit SHA | `27630223…` (older file) | **9f3e41f…** after `scripts/144_md_repo_current_state_summary.py --md` |
| `119` release gate | Not in this folder | **2026-04-13** run: 35 PASS / 4 WARN / 0 FAIL (`studies/20260413_motherduck_formalization/validation_report.md`) |
| `152`/`153` dry-run | N/A | **152:** 23 NULL before; 0 matchable from cytology preview. **153:** 0 Phase A/B rows. |

## What remains broader-than-scoped incomplete

Per **Standard B** (this study’s `repo_scoped_vs_strong_standard.md`):

1. **Scored + Imaging_12 deterministic nodule keys** do not all appear in `imaging_nodule_master_v1` (527 + 620 unmatched).
2. **No** `serial_imaging_us` table on MotherDuck.
3. **LN** remains at **heuristic / exam-level** structured capture, not per-level structured documentation.
4. **23** FNA episodes still lack numeric `bethesda_category` in `fna_episode_master_v2` (though resolved view documents reasons).

## Answer to the prompt’s explicit question

- **Is the repo merely still “scoped confirmed”?** **Yes** — Standard A (April 13–style gates) still **PASS** on fresh evidence.
- **Is it “fully executed by the strong standard”?** **No** — Standard B **FAIL** (see `executive_verdict.md`).
