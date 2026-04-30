<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->

# Canonical methods footnotes — supplementary appendix starters

These Markdown files (**one per `table_status = verified`** canonical table) are scaffolded for citation in Supplementary Methods.

## How to cite

Manuscript Supplement: cite as `canonical_methods_footnotes/<table_name>.md` keyed to the analytic table referenced in prose (e.g. `canonical_survival_followup_v1`).

Do **not** treat machine-generated stubs as immutable — Logan performs final clinical voice edits.

## How to regenerate (Path-C friendly)

Run read-only exporter (MotherDuck token via `motherduck_client.get_token()` / `motherduck.local.toml`; see `_md_connect.connect_locked()`):

```
.venv/bin/python scripts/render_mig197_data_dictionary_readonly.py
```

This refreshes:

- `qc_framework_v1/manuscript/data_dictionary.csv`
- Every `canonical_methods_footnotes/<verified_table>.md`

Companion SQL-only pull (no CSV massaging):

- `qc_framework_v1/manuscript/data_dictionary.sql` — augment with spreadsheet CF tagging if scripting changes.

## Update cadence

Regenerate whenever `canonical_column_verification_registry_v1` or `canonical_table_signoff_registry_v1` changes after a Lane close-out (verification batch).

## Preconditions / caveats

If upstream apply lanes (**mig_185b / mig_186b / mig_187 / mig_188b / …**) lag the registry snapshot, placeholders still reflect whichever rows are CURRENTLY `verified`; check `signoff_migration` per footnote footer for batch provenance.

---

_Author: Logan Glosser <logan.glosser@gmail.com> — starter scaffolding for mig_197._
