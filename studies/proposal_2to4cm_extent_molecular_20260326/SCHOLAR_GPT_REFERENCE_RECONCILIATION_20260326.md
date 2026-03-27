# ScholarGPT reference reconciliation — 2026-03-26

Bibliographic and stylistic pass **only**; **no** numeric results, cohorts, tables, or figures altered.

## Files changed

| File | Change summary |
|------|----------------|
| `manuscript_submission_v1.md` | Intro / Discussion / limitations prose; molecular “not associated” → caveat language; Vancouver list **1–10**; removed **Kim MH** and placeholder **12**; STROBE **\[4\]**; renumbered citations; incorrect ATA DOI removed |
| `abstract_structured_v1.md` | Parsimonious-model paragraph: sex vs molecular split + sparse-testing caveat (**instruction 8**) |
| `references_working_20260326.md` | Restructured: verified **1–10** (§1 = items 1–6, §2 = items 7–10), removed/not used, optional background |
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

## References verified (manuscript 7–10) — PubMed/PMC 2026-03-26

**7.** Kiss A, Szili B, Bakos B, Ármós R, Putz Z, Árvai K, et al. Comparison of surgical strategies in the treatment of low-risk differentiated thyroid cancer. *BMC Endocr Disord*. 2023 Jan 26;23(1):23. doi:10.1186/s12902-023-01276-8. PMID: 36703169. PMCID: PMC9881362.

**8.** Sutton W, Crepeau PK, Canner JK, Karzai S, Segev DL, Mathur A. Impact of the 2015 American thyroid association guidelines on treatment in older adults with low-risk, differentiated thyroid cancer. *Am J Surg*. 2022 Jul;224(1 Pt B):412-417. doi:10.1016/j.amjsurg.2022.01.033. PMID: 35123768. PMCID: PMC9232901.

**9.** Loderer T, Bonati E, Donato V, Viani L, Cozzani F, Del Rio P. Malignancy risk in Bethesda class IV thyroid nodules in an iodine deficient region. *Gland Surg*. 2023 Jul;12(7):884-893. doi:10.21037/gs-22-491. PMID: 37727346. PMCID: PMC10506119.

**10.** Hao Q, Segel JE, Vanness DJ, Shen C, Hao J, Hollenbeak CS. Hemithyroidectomy versus total thyroidectomy for patients with differentiated thyroid cancer: a systematic review and meta-analysis. *Gland Surg*. 2025 Nov;14(11):2271-2287. doi:10.21037/gs-2025-364. PMID: 41377887. PMCID: PMC12685788.

Manuscript ref **[8]** is **Sutton** (*Am J Surg* 2022), not Conroy (*Surgery* 2022); Conroy is listed under **Removed** in `references_working_20260326.md`.

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

1. Institution, IRB, funding, COI per existing `AUTHOR_*` docs.
2. Optional **Figure 1** relabel for production.

## Readiness

Refs **7–10** are **complete** in `manuscript_submission_v1.md` and `references_working_20260326.md` (2026-03-26 verification). Suitable for **figure / package / DOCX** assembly where complete bibliographies are required; internal numeric story unchanged and **READY** per `CLAIM_SOURCE_LEDGER.md`.
