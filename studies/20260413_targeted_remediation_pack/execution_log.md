# Remediation execution log (local)

**UTC wall time:** 2026-04-13 (session)

## Steps executed

1. **`candidate_python_fixes.py --phase token-check`** — MotherDuck token loaded via `motherduck.local.toml:MOTHERDUCK_TOKEN` (length only logged by script).
2. **`scripts/151_source_truth_confirmation_v1.py --md`** — Deployed `scripts/sql/source_truth_confirmation_v1.sql` to MotherDuck (`Thyroid 2026`). Post-deploy metrics included `unresolved_linkage_gap_total: 0`, `n_resolved` Bethesda 8072 / 8119 episodes, `tirads_gaps.n_gap: 0` over 37,016 canonical nodule rows.
3. **`scripts/129_imaging_fna_linkage_mm_v1.py --md`** — Rebuilt `imaging_fna_linkage_mm_v1` (7,305 rows; 6,351 primary links). Wrote `motherduck/exports/imaging_fna_linkage_mm_v1_audit.json`.
4. **Four audits re-run** (same study folders, `motherduck_client` token):
   - `studies/20260413_source_truth_completeness_audit/run_source_truth_audit.py`
   - `studies/20260413_us_nodule_tirads_linkage_audit/run_us_nodule_tirads_linkage_audit.py`
   - `studies/20260413_us_lymph_node_audit/run_us_lymph_node_audit.py`
   - `studies/20260413_fna_bethesda_audit/run_fna_bethesda_audit.py`

## Code fix (audit robustness)

- **`run_source_truth_audit.py`**: `norm_date_str()` now treats `pd.NaT` / null timestamps safely so `imaging_nodule_master_v1` rows with missing `exam_date` do not crash `.map()`.

## Notable post-run metrics (source-truth executive verdict)

- Primary-linked nodules **6351** / **37,016** (updated from prior snapshot after DB growth / linkage run).
- See **Follow-up** below for computed per-question status and `executive_verdict.md`.

## Artifacts refreshed

CSV/MD/JSON under the four `studies/20260413_*_audit/` folders plus `motherduck/exports/imaging_fna_linkage_mm_v1_audit.json` as modified by the tools above.

## Follow-up — NOT_CONFIRMED review (computed criteria + Bethesda backfill)

- **`scripts/152_fna_episode_bethesda_backfill_from_cytology.py --md`:** JOIN `fna_cytology` → `fna_episode_master_v2` for NULL `bethesda_category` (413 rows updated on MotherDuck; **47** episodes still NULL with no matching cytology category).
- **`run_source_truth_audit.py`:** Executive verdict is no longer hardcoded; YAML lists per-question **CONFIRMED** / **NOT_CONFIRMED**. LN Q4 uses regex `\*\*PASS\b` so `**PASS (heuristic):**` matches.
- **Latest computed result:** **Q1–Q4 CONFIRMED**, **Q5 NOT_CONFIRMED** (47 null Bethesda) → **overall NOT_CONFIRMED** until those rows get a source-attributed class or explicit reason column.
