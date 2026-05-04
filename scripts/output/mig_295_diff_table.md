# mig_295 — M044 .docx v1.0 → v1.1 patch — diff table

Source of truth: `studies/m044_validation/m044_validation_summary.md` + `manuscript_outputs/v1_0_20260501/M044_READY_FOR_WRITING_BRIEF_v1_1.md`.

Re-apply (idempotent on v1.1 text): `.venv/bin/python scripts/mig_295_apply_docx_patches.py --apply`

## §1 — Per-cell numerical patches

| # | File | Old (v1.0 / residual) | New (v1.1) | Notes |
|---:|---|---|---|---|
| 1 | `02_manuscript.docx` | `4,128` | `4,012` | Primary cohort denominator (comma form) |
| 2 | `02_manuscript.docx` | `4,128/4,128` | `4,012/4,012` | Surgery-date completeness ratio |
| 3 | `02_manuscript.docx` | `(n=4128),` | `(n=4,012),` | **Follow-on:** unformatted extract count (missed if only `4,128` was replaced) |
| 4 | `02_manuscript.docx` | `(4128/4128),` | `(4,012/4,012),` | **Follow-on:** unformatted calendar-date ratio |
| 5 | `02_manuscript.docx` | `n=0/4128),` | `n=0/4,012),` | **Follow-on:** limitations surg_date_missing denominator |
| 6 | `02_manuscript.docx` | `(n = 3,789)` | `(n = 3,750)` | Strict-DTC + no-RAI (e.g. Figure 1) |
| 7 | `02_manuscript.docx` | `(n = 3,756; events = 139)` | `(n = 3,750; events = 193)` | Primary logistic n/events caption |
| 8 | `02_manuscript.docx` | `(n = 3,756)` | `(n = 3,750)` | Standalone 3-level n |
| 9 | `02_manuscript.docx` | `Cox subset n = 2,025.` | `Cox subset n = 2,511 (events = 178).` | Cox sensitivity denominator |
| 10 | `03_supplement.docx` | `4,128` / `4,128/4,128` | `4,012` / `4,012/4,012` | When present (earlier pass may have cleared) |

Headline logistic / Cox contrasts in the Multivariable section were already v1.1 **before** this pass: aOR 2.08 (1.48–2.91), HR 0.91 (0.48–1.73), pseudo-R² ≈0.140, LR χ² ≈213.7, n=3,750 / 193 events.

## §2 — Discussion sensitivity paragraph (Logan Option A)

**Location:** `02_manuscript.docx`, paragraph following the Cox + logistic Results narrative (Cf. index ~84).

**Template (with v1.1 numbers + median follow-up):**

> In sensitivity analyses, the time-to-event Cox proportional-hazards model showed no significant difference between gross and microscopic ETE (HR 0.91, 95% CI 0.48–1.73, p=0.77, n=2,511 / 178 events). This contrasts with the primary logistic regression on path-proven recurrence (aOR 2.08, p=2.5×10⁻⁵). The discrepancy reflects the difference in eligibility criteria: the Cox model includes patients with shorter follow-up windows in which late path-proven recurrences accumulate disproportionately in the microscopic stratum **(median follow-up 3.2 years in the Cox-eligible subset; empirical median of `followup_years` on the lifelines Cox frame)**, diluting the time-to-event signal. The primary logistic specification, which treats path-proven recurrence as a binary outcome over the entire observation period, is the pre-specified primary analysis.

## §3 — Known prose residuals (NOT auto-patched by mig_295)

Per-ETE-group counts/percentages in body prose (e.g. PTC 3,075 (74.5%), `4090` calendar strata) may still reflect v1.0-era typing or table snapshots; re-derive from `04_tables.xlsx` in a writing pass per `M044_READY_FOR_WRITING_BRIEF_v1_1.md`.

## §4 — Verification residual scan

After `scripts/mig_295_apply_docx_patches.py --apply`, `scripts/output/mig_295_apply_log.txt` JSON includes `residual_scan` for tokens: `4,128`, `4128`, `1.80`, `2.34`, `3,789`, `3,756`, `2,025`, `139 events`, `events = 139`.  
**Acceptance:** `residual_scan` is empty `{}` for both `.docx` paths.
