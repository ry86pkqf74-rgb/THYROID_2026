# DICOM exact-accession linkage hardening (2026-04-13)

## Prior behavior gap

For a single `research_id` and exact normalized accession match, when multiple candidate rows included **specimen** spine rows with **different** `specimen_id` values, `resolve_exact_links` could attach **`specimen_id` from `spec_sub.iloc[0]`** (first matching row) on an `exact_accession` link. That silently picked one of several valid specimen keys.

Distinct **`imaging_exam_id`** values already routed to **`AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM`** without auto-link.

## New rule

After resolving to exactly one `research_id` and **not** triggering multi–imaging-exam or date-discordant review:

1. Compute distinct non-blank **`imaging_exam_id`** values from **imaging** candidate rows only (blanks ignored).
2. Compute distinct non-blank **`specimen_id`** values from **all** candidate rows for that accession (blanks ignored).
3. If there are **more than one** distinct `specimen_id` values and **at most one** distinct imaging exam ID, **do not auto-link**. Emit review reason **`AMBIGUOUS_ACCESSION_MULTI_SPECIMEN`** with `candidate_specimen_ids_json` (and `candidate_imaging_exam_ids_json`) populated per existing conventions.

## Precedence

1. **`AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM`** — Triggered when **>1** distinct imaging exam ID. If multiple distinct specimen IDs are also present, the reason code stays **multi-imaging-exam**; **`candidate_specimen_ids_json`** still lists specimen IDs, and **`conflict_note`** notes specimen multiplicity when applicable.
2. **`AMBIGUOUS_ACCESSION_MULTI_SPECIMEN`** — Only when multi-imaging-exam is **not** triggered and specimen multiplicity is.
3. **`DATE_DISCORDANT_ACCESSION_MATCH`** — Unchanged ordering (runs before the multi-specimen branch when dates are comparable).

## Exam-date discordance (follow-up)

Earlier logic compared DICOM study date only to the **first** distinct `exam_date_yyyymmdd` in the candidate group, which could miss discordance when multiple exam dates were present. The resolver now evaluates **all distinct parsed 8-digit exam dates** and uses the **maximum** absolute skew vs study date against `date_skew_days_max`.

## Non-regression expectations

- Explicit **`research_id`** in the file still wins without candidate spine joins.
- **MRN + date** alone still never links.
- **Exact accession** remains the only non-explicit auto-link path.
- **Repeated** candidate rows with the **same** single `specimen_id` still produce **one** link with that `specimen_id`.
- **Malformed** raw DICOM: provenance/QC path unchanged; see `docs/dicom_header_ingest_runbook.md` (malformed subsection).
