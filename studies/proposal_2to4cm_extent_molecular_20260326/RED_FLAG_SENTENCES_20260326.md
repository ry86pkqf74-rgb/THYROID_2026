# Red-flag sentences — review log — 2026-03-26

Strings or patterns that **were** risky or **remain** sensitive. Use during final author read.

---

## Addressed in this QA pass (revised or clarified)

| Location (before) | Issue | Action |
|-------------------|-------|--------|
| Intro — “choices influenced by” | Implied deterministic pathway | Rewritten to guideline framing + **[1]** only once per sentence cluster |
| Discussion — “validate our effect sizes” | “Effect sizes” can imply causal estimands | → “replicate our adjusted odds ratios” |
| Discussion — “could influence extent … causal impact” | Strong causal language | → “may co-occur … cannot infer causal effects” |
| Limitations — missingness bullet | FNA linkage not spelled out | Added **FNA/imaging linkage** explicitly |
| Limitations — completion | Operationalization not its own bullet | Added **pipeline flags / not population rate** bullet |
| `figure_legends_v1.md` Figure 2 | Garbled “versus not associated with” | Fixed to “binary outcome … associated with” |
| `AUTHOR_FILL_INS` | Wrong ref number (**13**) for completion SR | Corrected to **ref 12** |

---

## Acceptable retained phrasing (context)

| Text | Why OK |
|------|--------|
| “does **not** estimate recurrence, survival, or **causal effects** of extent” | Explicit **non**-claim |
| Reference 7 title contains “influence” | **Bibliographic** title of cited work; not our causal claim |
| “associated with,” “odds,” “confound” | Standard observational vocabulary |

---

## Remaining author-facing flags (not prose defects)

| Item | Risk | Mitigation |
|------|------|------------|
| Ref **12** stub | Citing systematic review without full record | Complete PubMed/journal entry before submission |
| Pooled completion rates in literature | Must not over-interpret vs **0/238** flags | Keep contrast language; cite completed ref 12 |
| Figure 1 truncated y-labels | Peer review / production | Relabel or redraw per `AUTHOR_FILL_INS` |

---

## Abstract-only notes

- No standalone causal claims detected; **“associational”** in Conclusions is appropriate.
- All numeric lines in abstract matched ledger in checklist document.
