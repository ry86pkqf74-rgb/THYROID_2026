# Cursor Prompt — mig_197 data dictionary refresh with CF annotations + per-canonical methods footnote

**Date:** 2026-04-30
**Lane:** mig_197 / data_dictionary_refresh_with_cf_annotations
**Batch (proposed):** `mig_197_data_dictionary_refresh_with_cf_annotations_20260430`
**Posture:** **READ-ONLY scoping + authoring.** No execute against MotherDuck.
**Target DB:** `thyroid_canonical_publication_v1_0`
**Touches:** none (read-only); produces CSV + Markdown

---

## Background

Manuscript supplementary appendix needs:
1. **Data dictionary** — column-by-column description of every verified canonical, with type, nullability, CF annotation, verification method, last-verified date
2. **Per-canonical methods footnote** — short Markdown blurb per canonical table describing what it represents, how it was built, known limitations

mig_197 produces both as live-MD-grounded artifacts. Logan refines for manuscript voice.

---

## Conditional pre-flight gate

If mig_188b/186b/185b/187 NOT yet applied, the data dictionary will reflect pre-apply state. **Note this caveat in the README** but proceed (the dictionary structure is independent of apply state — only specific col annotations would change).

---

## Required scope

### §1 Data dictionary CSV

Author `qc_framework_v1/manuscript/data_dictionary.sql` that produces:

```
schema_name | table_name | column_name | data_type | is_nullable | verification_status |
verification_method | batch_id | verified_ts | cf_tags (semicolon-separated) | notes_excerpt
```

Source query: JOIN `information_schema.columns` × `canonical_column_verification_registry_v1` × `canonical_table_signoff_registry_v1`. Filter to `table_status='verified'` canonicals. Extract CF tags from `notes` via regex `regexp_extract_all(notes, 'CF-[A-Za-z0-9_-]+')`.

Output: `qc_framework_v1/manuscript/data_dictionary.csv` with header + ~3,300 rows (one per verified col + na col).

### §2 Per-canonical methods footnote

For each `table_status='verified'` canonical, author a short Markdown block at `qc_framework_v1/manuscript/canonical_methods_footnotes/<table_name>.md`:

Template per file (~10-30 lines each):
```markdown
# `<table_name>`

**Grain:** [event / patient / etc.]
**Total rows:** [N]
**Distinct patients:** [N]
**Verification status:** verified
**Signoff migration:** [signoff_migration value]

## Purpose
[1-3 sentences from registry note context describing what this table represents]

## Build pipeline
[1-3 sentences pulled from registry's verification_method + scripts/<N>_*.py reference]

## Key columns
[bulleted list of the 5-10 most analytically important cols with one-line descriptions]

## Known limitations
[bulleted list pulled from CF tags on the col registry, with verbatim note excerpts]

## Verification methods used
[from registry verification_method distinct values for this table]
```

Use registry note excerpts verbatim where possible — they already contain the canonical operational language.

For the ~62 Tier-2 canonicals + canonical_patient_master, this produces ~63 footnote files.

### §3 README + manuscript-appendix integration

Author `qc_framework_v1/manuscript/canonical_methods_footnotes/README.md`:
- How to use the footnotes (cite by table_name in manuscript supplement)
- How to regenerate (re-run mig_197 against current MD; one Cursor invocation)
- Update cadence (after each registry-mutating round)

Also author `qc_framework_v1/manuscript/supplementary_appendix_starter.md` integrating:
- mig_190's manuscript appendix candidates table
- This data dictionary
- Per-canonical methods footnotes (transcluded or referenced)
- Cleanup CFs (from mig_186b indeterminate-events landing, mig_185b source-distinct duplicates flag, mig_188b T0 cohort)

### §4 Audit/report

Author `qc_framework_v1/reports/mig_197_data_dictionary_refresh_with_cf_annotations_20260430.md`:
- §1 deliverables list
- §2 row counts produced (rows in dictionary CSV; n footnote files generated)
- §3 caveats: state on which apply lanes have/have not landed at generation time
- §4 regeneration recipe

### §5 Mark deliverables READY

Header per artifact: `<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->`

---

## Governance reminders

- Read-only investigation only. No `query_rw`.
- Author = `Logan Glosser <logan.glosser@gmail.com>`.
- Footnote text is STARTER — Logan refines voice and selects which limitations to surface in the appendix vs main methods.

---

## Deliverables

1. `qc_framework_v1/manuscript/data_dictionary.sql`
2. `qc_framework_v1/manuscript/data_dictionary.csv` (placeholder; populated post-Path-C run of the SQL)
3. `qc_framework_v1/manuscript/canonical_methods_footnotes/<table_name>.md` × ~63 files
4. `qc_framework_v1/manuscript/canonical_methods_footnotes/README.md`
5. `qc_framework_v1/manuscript/supplementary_appendix_starter.md`
6. `qc_framework_v1/reports/mig_197_data_dictionary_refresh_with_cf_annotations_20260430.md`

Commit message: `qc: mig_197 data dictionary refresh with CF annotations + per-canonical methods footnotes (~63 footnote files; supplementary appendix starter; data dictionary SQL + CSV placeholder)`

---

End of prompt.
