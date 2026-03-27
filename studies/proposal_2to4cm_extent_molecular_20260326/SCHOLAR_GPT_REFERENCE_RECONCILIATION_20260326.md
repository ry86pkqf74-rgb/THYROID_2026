# ScholarGPT reference reconciliation — 2026-03-26

Bibliographic and stylistic pass **only**; **no** numeric results, cohorts, tables, or figures altered.

## Files changed

| File | Change summary |
|------|----------------|
| `manuscript_submission_v1.md` | Intro / Discussion / limitations prose; molecular “not associated” → caveat language; Vancouver list **1–10**; removed **Kim MH** and placeholder **12**; STROBE **\[4\]**; renumbered citations; incorrect ATA DOI removed |
| `abstract_structured_v1.md` | Parsimonious-model paragraph: sex vs molecular split + sparse-testing caveat (**instruction 8**) |
| `references_working_20260326.md` | Restructured: verified **1–6**, unverified **7–10**, removed/not used, optional background |
| `MANUSCRIPT_GAP_LIST.md` | Bibliography gap row + resolved/improved note |
| `READY_TO_SUBMIT_STATUS_20260326.md` | Citations **\[1\]–\[10\]**, external bib gate, next actions |
| `revision_packet_v1.md` | Reference section reflects reconciliation |
| `FINAL_QA_CHECKLIST_20260326.md` | Citation QA row **\[1\]–\[10\]** |
| `SCHOLAR_GPT_REFERENCE_RECONCILIATION_20260326.md` | This changelog |

**Not edited:** `CLAIM_SOURCE_LEDGER.md` (no citation content), `README.md`, `MANUSCRIPT_STATE_AUDIT.md` (no stale ref claims), `external_context_grok_live_literature_20260326.md`.

## References corrected (verified)

1. **Ringel et al., 2025 ATA** — PMID only; **incorrect DOI removed** (no replacement guessed).
2. **Montgomery et al.** — Kept as specified; PMID added in manuscript list for parity.
3. **Worrall et al.** — Full author list; ***Endocr Oncol*.** 2023;3(1):EO-22-0095; doi 10.1530/EO-22-0095; PMCID PMC10305631 (replaces incorrect *Thyroid* 2023 stub).
4. **STROBE** — Full author line + **STROBE Initiative**; PMID 17938396.
5. **Dhir et al.** — *Surgery* 2018;163(1):81–87; DOI + PMID.
6. **Wang et al.** — *World J Surg Oncol* 2020;18(1):310; DOI + PMCID.

## References intentionally left unresolved

**7.** Kiss A, et al. *BMC Endocr Disord*. 2023.  
**8.** Sutton W, et al. *Am J Surg*. 2022.  
**9.** Loderer T, et al. *Ann Ital Chir*. 2023.  
**10.** Hao Q, et al. *Gland Surg*. 2025.  

All tagged **NEEDS AUTHOR CHECK / UNVERIFIED** in manuscript and working file. **No** fabricated volume, pages, or DOI.

## Placeholder / removed from active use

- **Former ref 12:** Fake completion-thyroidectomy systematic review — **not** in numbered manuscript references; **not** cited in text.
- **Former ref 4 (Kim MH):** **Not** in manuscript text or formal list; listed under **Removed** in `references_working_20260326.md`.

## Narrative adjunct (ref 7)

After removing **\[7,8\]** (Wang + Kiss) from the prior literature line in favor of **\[5,6\]** (Dhir + Wang), one sentence was added so **Kiss \[7\]** remains a justified citation (variation in surgical strategy, low-risk PTC).

## Validation (target folder)

Commands and results (2026-03-26):

- `\[12\]` in `manuscript_submission_v1.md`: **none**
- `Kim MH` in `manuscript_submission_v1.md`: **none**
- `10.1177/10507256251363120` / `doi:10.1177` in `manuscript_submission_v1.md` and `references_working_20260326.md`: **none**
- In-text citation IDs present: **\[1\]–\[10\]** only, aligned with reference list order
- Quantitative strings (N=558, ORs, p-values): **unchanged** by this pass (spot-check vs prior ledger)

## Remaining author actions

1. PubMed / journal sites for **refs 7–10** (full metadata + consistent Vancouver typography).
2. Institution, IRB, funding, COI per existing `AUTHOR_*` docs.
3. Optional **Figure 1** relabel for production.

## Readiness

Suitable for **next figure / package / DOCX** assembly **after** ref **7–10** verification for submission portals that require complete bibliographies; internal numeric story unchanged and **READY** per `CLAIM_SOURCE_LEDGER.md`.
