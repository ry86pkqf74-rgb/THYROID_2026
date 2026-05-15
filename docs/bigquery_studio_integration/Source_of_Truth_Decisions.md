# Source-of-Truth Decisions Needed — thyroid-canonical-pub-2026

**Prepared:** May 14, 2026 · Cowork / BigQuery Studio Integration Plan
**Purpose:** The "competing sources of truth" pain point can't be closed by tooling — each case needs a human to decide which column is authoritative. This doc quantifies the two biggest cases on `pub_canonical.manuscript_cohort_v1` so the decision can be made from numbers, not guesses. Each decision should then be encoded in `__conventions`, enforced by the QC pipeline, and the losing column deprecated.

---

## Decision 1 — The surgery-date column (QC rule SURG01 / SURG02 · Linear THY-87)

`manuscript_cohort_v1` carries **three** surgery-date columns, all `TIMESTAMP`. Measured across all 10,871 patients:

| Column | Populated | Notes |
|---|---|---|
| `first_surgery_date` | 10,870 / 10,871 | Most complete. The column the `pub_eval` workup census currently anchors on. |
| `surg_first_date` | 8,731 / 10,871 | — |
| `surgery_date` | 8,731 / 10,871 | — |

**Disagreement pattern:**

- `surg_first_date` vs `surgery_date`: **0 disagreements** — these two are *identical duplicates* of each other. One of them is pure redundancy and should be dropped outright.
- `first_surgery_date` vs the other two: **171 patients disagree**.
- **Maximum absolute gap: 21,550 days (~59 years)** — at least a handful of rows have a corrupt/misparsed date, not just a near-miss.

**What needs to be decided:**

1. **Which column is canonical** — `first_surgery_date` (most complete) or the `surg_first_date`/`surgery_date` value (the agreeing pair)? Recommendation leans to `first_surgery_date` for completeness, *after* the 171 divergences and the extreme-gap rows are chart-reviewed.
2. The other two columns get deprecated (registered in `deprecation_registry_v1`) and eventually dropped.
3. The 171 divergent patients + the ~59-year-gap outliers go to a chart-review queue.

Until decided, every analysis that picks a surgery date is silently choosing — including the `pub_eval` census, which documents `first_surgery_date` as its anchor in `surgery_anchor_date`.

---

## Decision 2 — The lymph-node-positive count (QC rules LN01 / LN02 / LN03)

`manuscript_cohort_v1` has two lymph-node-positive columns plus an examined-count column, all `INT64`:

| Check | Count | Severity |
|---|---|---|
| `path_ln_positive_raw` and `ln_positive_final` both populated | 3,603 patients | — |
| ...of those, the two **disagree** (`LN03`) | **51 patients** | warning |
| `ln_positive_final` > `path_ln_examined_raw` — more positive than examined, impossible (`LN01`) | **10 patients** | **critical** |
| `ln_positive_final` > 0 but `path_ln_examined_raw` = 0 — numerator with no denominator (`LN02`) | **28 patients** | **critical** |

**What needs to be decided:**

1. **Which column is the canonical LN-positive count** — `path_ln_positive_raw` (raw pathology extract) or `ln_positive_final` (the derived "final" value)? The 51 disagreements need a rule, not a coin flip.
2. The **38 critical rows** (10 `LN01` + 28 `LN02`) are impossible by construction — a positive count that exceeds or lacks an examined count. These can't be left in any staging-dependent cohort; they need correction or explicit exclusion.

---

## How this gets enforced once decided

1. Write the decision into `pub_workspace`'s `__conventions` (or wherever conventions live) as a one-line rule: *"the canonical surgery date is X; the canonical LN-positive count is Y."*
2. The losing columns get a row in `deprecation_registry_v1`.
3. The QC pipeline (`cowork_qc_nonblocking_pipeline_v1`) already carries assertions for SURG01 / LN01 / LN02 / LN03 — once the canonical column is chosen, the assertion flips from "do they disagree" to "does anything still use the deprecated column", so regressions surface automatically.
4. The chart-review rows (171 surgery-date divergences, 38 critical LN rows) go to a review queue.

---

## Linear

- **THY-87** — updated with the surgery-date quantification above.
- **THY-89** (new) — the LN-positive canonical-source decision (LN01/LN02/LN03).

*All figures from a live query of `pub_canonical.manuscript_cohort_v1` on 2026-05-14. Re-run before acting — the cohort changes.*
