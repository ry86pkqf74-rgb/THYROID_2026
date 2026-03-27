# Citation integrity sweep — 2–4 cm manuscript packet (2026-03-26)

## Files inspected (whole packet, markdown emphasis)

- **Core:** `manuscript_submission_v1.md`, `references_working_20260326.md`, `abstract_structured_v1.md`
- **Letters / QA:** `cover_letter_v1.md`, `revision_packet_v1.md`, `READY_TO_SUBMIT_STATUS_20260326.md`, `FINAL_QA_CHECKLIST_20260326.md`, `AUTHOR_FILL_INS_FOR_SUBMISSION_20260326.md`, `AUTHOR_INPUTS_REQUIRED_20260326.md`
- **Bibliography trail:** `SCHOLAR_GPT_REFERENCE_RECONCILIATION_20260326.md`, `MANUSCRIPT_GAP_LIST.md`, `MANUSCRIPT_STATE_AUDIT.md`
- **Supplements / methods:** `supplement.md`, `supplement_methods_v1.md` (no numeric citations)
- **Context (secondary):** `external_context_grok_live_literature_20260326.md`, `external_context_elicit_molecular_background_20260326.md`, `journal_fit_matrix_v1.md`, `journal_style_results.md`, `figure_legends_v1.md`, `figure_legends_v2.md`
- **Spot-check:** `manuscript_full_draft.md` (no bracket citations), `RED_FLAG_SENTENCES_20260326.md`, `findings_note.md`, `qa_reconciliation.md`

## Files edited (this sweep)

| File | Change |
|------|--------|
| `references_working_20260326.md` | §1 heading/intro clarified: items **1–10** are the active manuscript set; §1 + §2 are organizational blocks only. |
| `SCHOLAR_GPT_REFERENCE_RECONCILIATION_20260326.md` | Changelog row corrected: **7–10** are verified (removed stale “unverified”). |
| `MANUSCRIPT_GAP_LIST.md` | Resolved-items line updated: no “verified vs unverified” wording for the working file. |
| `revision_packet_v1.md` | Refs **1–6** parenthetical order aligned to **manuscript numbering** (Ringel → … → Wang). |
| `external_context_grok_live_literature_20260326.md` | **Packet citation warning** added before Grok list item 1: Ringel DOI there must not be copied; use submission manuscript / PMID 40844370. |
| `CITATION_INTEGRITY_SWEEP_20260326.md` | This report (new). |

## Findings

### Manuscript `manuscript_submission_v1.md`

| Issue | Status |
|-------|--------|
| In-text tags **[1]–[10]** vs numbered list **1.–10.** | **PASS** — each bracket number matches the same entry in the reference list. |
| Orphan / missing list entries | **None** — all cited numbers have list rows. |
| Placeholders `NEEDS AUTHOR`, `XXX`, `TBD`, `UNVERIFIED` in body or reference list | **None** in manuscript body or refs block. |
| **Vancouver “first-appearance” order** | **Advisory:** first citation sequence is **1, 2, 3, 6, 4, 5, 7, 8, 9, 10** (refs **4** and **5** first appear after **6**). Many journals accept fixed bibliography order; if the target journal requires strict order-of-appearance numbering, a **renumbering pass** would be needed (out of scope here per “no risky cascade”). |
| Semantic fit **[8]** with sentence on “population-level” lobectomy trends | **Advisory:** **[8]** is Sutton *Am J Surg* 2022 (**older adults**, 2015 ATA impact). **[3]** Worrall *Endocr Oncol.* is also cited elsewhere for post-ATA lobectomy/completion patterns — not clearly wrong; no change made. |

### `references_working_20260326.md`

| Issue | Status |
|-------|--------|
| **NEEDS AUTHOR CHECK** (Barbaro; statsmodels/SciPy software) | **Intentional** — optional/omitted from manuscript **1–10**; flags preserved for authors if Discussion or journal requires. |
| Stale structure implying **7–10** were unverified | **Fixed** in §1 intro + cross-docs (see edited files). |
| Duplicate papers under two numbers | **None** in active **1–10** list. |
| Vancouver punctuation | **OK** — consistent `doi:` / `PMID:` / `PMCID:` usage; Ringel remains PMID-only per policy (no guessed DOI). |

### Secondary / operational placeholders (not manuscript bibliography defects)

| Location | Text |
|----------|------|
| `cover_letter_v1.md` | **[JOURNAL TBD]**, corresponding author/date brackets — submission template. |
| `journal_fit_matrix_v1.md` | “verify” journal limits; **TBD (author-completed)** section; checklist “NEEDS AUTHOR CHECK” for `.bib` after optional rows resolved. |
| `READY_TO_SUBMIT_STATUS_20260326.md` | Describes optional Barbaro/software **NEEDS AUTHOR CHECK** in working file — accurate. |

### `external_context_grok_live_literature_20260326.md`

| Issue | Status |
|-------|--------|
| Incorrect Ringel **DOI** in Grok list | **Mitigated** — warning callout added; list row left unchanged so Grok output stays auditable; submission must use manuscript/PMID only. |

## Validation commands (post-edit)

```bash
rg -n "NEEDS AUTHOR CHECK|XXX|TBD|UNVERIFIED|AUTHOR CHECK|placeholder" studies/proposal_2to4cm_extent_molecular_20260326 --glob "*.md"
```

Expect: matches only operational/template lines (cover letter, journal matrix, optional Barbaro/software in `references_working`, QA docs, Grok “placeholder” section headings / verify language) — **no** unresolved tags in `manuscript_submission_v1.md` reference list or body.

```bash
rg "\[[0-9]+\]" studies/proposal_2to4cm_extent_molecular_20260326/manuscript_submission_v1.md
```

Expect: **[1]** through **[10]** only, as tabulated against the reference list.

## Submission readiness (citations)

**Yes — citation-clean for submission** for the **active** Vancouver list **1–10** in `manuscript_submission_v1.md`, with these **author-side** items still external to that list:

1. Optional Barbaro + formal software citations (**NEEDS AUTHOR CHECK** in `references_working_20260326.md` §4–5).
2. Template placeholders in cover letter / journal choice.
3. If target journal mandates **strict order-of-appearance** reference numbering, review **Vancouver first-appearance** advisory above.

**No invented bibliographic fields** were added in this sweep.
