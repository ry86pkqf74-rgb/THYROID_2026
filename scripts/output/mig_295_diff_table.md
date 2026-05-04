# mig_295 — M044 .docx v1.0 → v1.1 patch — diff table

Source of truth: `studies/m044_validation/m044_validation_summary.md` + `manuscript_outputs/v1_0_20260501/M044_READY_FOR_WRITING_BRIEF_v1_1.md`.

## §1 — Per-cell numerical patches

| File | Old (v1.0) | New (v1.1) | Paragraph hits | Run hits |
|---|---|---|---:|---:|
| `02_manuscript.docx` | `4,128` | `4,012` | 9 | 9 |
| `02_manuscript.docx` | `4,128/4,128` | `4,012/4,012` | 0 | 0 |
| `02_manuscript.docx` | `(n = 3,789)` | `(n = 3,750)` | 1 | 1 |
| `02_manuscript.docx` | `(n = 3,756; events = 139)` | `(n = 3,750; events = 193)` | 1 | 1 |
| `02_manuscript.docx` | `Cox subset n = 2,025.` | `Cox subset n = 2,511 (events = 178).` | 1 | 1 |
| `03_supplement.docx` | `4,128` | `4,012` | 2 | 2 |
| `03_supplement.docx` | `4,128/4,128` | `4,012/4,012` | 1 | 1 |
| `03_supplement.docx` | `(n = 3,789)` | `(n = 3,750)` | 0 | 0 |
| `03_supplement.docx` | `(n = 3,756; events = 139)` | `(n = 3,750; events = 193)` | 0 | 0 |
| `03_supplement.docx` | `Cox subset n = 2,025.` | `Cox subset n = 2,511 (events = 178).` | 0 | 0 |
| `02_manuscript.docx` | `(n = 3,756)` *(standalone, Figure 1 caption)* | `(n = 3,750)` | 1 | 1 |

## §2 — Discussion paragraph addition (per dispatch §2 template, Logan Option A)

Inserted after the existing Discussion paragraph that mentions Cox HR 0.91:

> *In sensitivity analyses, the time-to-event Cox proportional-hazards model
> showed no significant difference between gross and microscopic ETE
> (HR 0.91, 95% CI 0.48–1.73, p=0.77, n=2,511 / 178 events). This contrasts
> with the primary logistic regression on path-proven recurrence
> (aOR 2.08, p=2.5×10⁻⁵). The discrepancy reflects the difference in
> eligibility criteria: the Cox model includes patients with shorter
> follow-up windows in which late path-proven recurrences accumulate
> disproportionately in the microscopic stratum, diluting the time-to-event
> signal. The primary logistic specification, which treats path-proven
> recurrence as a binary outcome over the entire observation period, is
> the pre-specified primary analysis.*

## §3 — Known prose residuals (NOT auto-patched by mig_295)

Per-ETE-group counts/percentages in body prose were rendered against the
v1.0 4,128-patient cohort (e.g. "microscopic ETE 2,576 (62.4%)", "73.4% female",
"1,400 (33.9%) zero follow-up", crude PP rates per group, age means, tumor-size
means).  These remain v1.0-derivative because mig_295 only patches the
headline numerical cells listed in the dispatch §1 (per the source-of-truth
brief, which routes prose re-derivation to the writing chat using the
now-v1.1 `04_tables.xlsx`).  The Discussion's primary aOR, Cox HR,
pseudo-R², LR χ², and n/events numbers are already at v1.1.

## §4 — Verification residual scan after apply

After apply, scripts/output/mig_295_apply_log.txt contains the post-patch
scan for residual instances of `4,128`, `1.80`, `2.34`, `3,789`, `3,756`,
`2,025`, and `139 events`. Acceptance: `4,128` count = 0 in both files,
Figure-1 / Figure-5 caption cells updated, no `1.80` / `2.34` regressions.
