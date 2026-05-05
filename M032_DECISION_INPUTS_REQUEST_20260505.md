# M032 correction notice — decision inputs needed from Logan

> Cowork-side asks before drafting can begin. mig_317 (cursor) already produced the numerical exhibits; this doc lists the editorial / strategic decisions only Logan can make.

---

## Background recap

mig_317 quantified the post-mig_313 deltas to M032 v1 stage-by-era counts:

- **E (2020–2025) × Stage I**: v1 5.13% → v2 62.49% (Δpp **+57.36**)
- **E (2020–2025) × Stage IV**: v1 41.70% → v2 3.51% (Δpp **−38.19**)
- Max within-era |Δpp| **= 57.36%** (rubric threshold: 15%)

Decision: **substantive correction notice required.** Backing data in `studies/m032_era_stage_v2_post_mig313/{M032_DELTA_REPORT_v1_vs_v2.md, delta_v1_vs_v2.xlsx, m032_era_stage_v2_live.csv}`.

---

## Decisions Logan needs to make

### 1. **Submission target** — where does the correction go?

Three live options; pick one:

- **(a) Formal published correction / erratum** at the original journal — strongest scientific record; requires editor coordination and may delay publication of Volume X. **Recommended if M032 v1 is already accepted/published.**
- **(b) Self-published correction notice** on institutional repository / preprint server, with an internal addendum file in the submission package. Faster, no editor gatekeeping, lower formal weight.
- **(c) Withdraw + resubmit v2** — only viable if M032 v1 is still in submission pipeline (pre-acceptance) and not yet under DOI.

**Cowork needs:** which option, and current submission state of M032 v1 (submitted / under review / accepted / published / DOI assigned).

### 2. **Scope of correction** — what to revise

Three concentric rings:

- **(i) Numerical only** — Fig 3 stacked bar regenerated with v2 data; Table 3 cell values updated; one Methods footnote noting the upstream M-stage repair (mig_313). **Minimum acceptable.**
- **(ii) Numerical + interpretive** — also revise any Discussion sentences referencing temporal stage-trend interpretations; the "Stage IV is rising in recent eras" framing (if present) must be reversed or removed. **Recommended.**
- **(iii) Numerical + interpretive + abstract** — also touch Abstract sentences that quote stage-by-era percentages. **Required if Abstract has explicit stage-IV-trend numbers.**

**Cowork needs:** which ring; whether the Abstract has the impacted numbers (Logan likely knows from drafting it).

### 3. **Co-author concurrence** — sign-off path

A correction notice typically requires senior-author + corresponding-author concurrence before submission, plus all-author notification. Some journals require written assent from every author.

**Cowork needs:**
- Senior author / corresponding author (likely already known; Logan to confirm)
- Whether M032 has co-authors who need to be looped in before drafting begins
- Preferred channel for review (email circulated draft, shared doc, in-person)

### 4. **Timing**

- Conference abstract deadlines that quoted M032 v1 numbers — any near-term?
- Pending grant submissions referencing M032 v1?
- Logan's bandwidth in the next 1–2 weeks for review cycles?

**Cowork needs:** a deadline date or "next-meeting" target.

### 5. **Publication scope vs. reproducibility scope**

A correction notice can be:
- **Concise (1 page)** — cite the issue, point to v2 numerical exhibits, inline Fig 3 v2.
- **Extended methods note (3–5 pages)** — also document mig_313 root cause, what M-stage corruption looked like, why downstream cohorts were affected. Useful as a methodological reference cited by other Emory papers.

**Cowork needs:** preference between concise and extended.

---

## What Cowork will do once those five decisions are in

1. Draft the correction notice in the chosen format and submission target (a / b / c).
2. Regenerate Fig 3 v2 PNG/PDF from `m032_era_stage_v2_live.csv` using the same `m032_make_figures.py` style used for v1.
3. Produce a concise diff exhibit (`M032_v1_vs_v2_table.docx`) suitable for journal submission.
4. Pre-mark all manuscript locations needing edits (Discussion, Abstract, Results) for Logan + co-author review.
5. Stage everything in `M032_correction_notice_v1/` (separate from `M032_submission_package_v1_0/` which stays frozen).
6. Insert `mig_321` signoff once Logan accepts the draft.

Estimated drafting time: **2–4 hours** depending on Ring (i)/(ii)/(iii).

---

## Quick-fire checklist for Logan to fill in

```
[ ] Submission target: (a) erratum  /  (b) self-published  /  (c) withdraw+resubmit
[ ] M032 v1 current state: submitted / under review / accepted / published / DOI
[ ] Scope: ring (i) / (ii) / (iii)
[ ] Abstract has impacted stage-IV numbers? yes / no
[ ] Co-author list (or "I'll loop them in"):
[ ] Deadline / target date:
[ ] Format: concise (1 page) / extended methods note (3–5 pages)
[ ] Anything else Cowork should know:
```

When Logan provides these, Cowork takes over drafting in the next session.
