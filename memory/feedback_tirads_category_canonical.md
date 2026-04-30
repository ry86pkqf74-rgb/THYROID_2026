# TIRADS dual columns on `canonical_us_nodule_v2` (manuscript-facing)

**Ratified (Logan, 2026-04-30).** Both columns are reportable; use distinct methods sections.

| Column | Role | Definition |
|--------|------|------------|
| `acr2017_tirads_category` | **Primary** | Strict **ACR TI-RADS 2017** category derived from `acr2017_tirads_points` (Tessler et al., JACR 2017): TR1 = 0 pts, TR2 = 2 pts, TR3 = 3 pts, TR4 = 4–6 pts, TR5 ≥ 7 pts. **No ACR band for total points = 1** — such rows must have **NULL** category after cleanup (`mig_215`). |
| `updated_tirads_category` | **Sensitivity / secondary** | **Institutional / Emory “updated” TI-RADS tier** carried from legacy `tirads_category_v2` and LLM/absorption overlays (`tirads_v2_nodules_raw`, Script 377/378 paths). Parallel to strict ACR 2017; compare using `acr2017_vs_updated_concordant`. |

**Supplementary text:** Primary analyses should state ACR 2017 (`acr2017_*`); sensitivity analyses may use `updated_tirads_category` where completeness differs.

**Related migrations:** `mig_215` (size quarantine + ACR band fix), `mig_216` (column registry documentation).
