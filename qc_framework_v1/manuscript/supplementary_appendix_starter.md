<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->

# Supplementary appendix — starter scaffolding (mig_197)

**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Batch:** `mig_197_data_dictionary_refresh_with_cf_annotations_20260430`  
**Target DB:** `thyroid_canonical_publication_v1_0`

This file wires together manuscript supplement pieces produced in the qc_framework lane:

1. **mig_190 appendix candidates — open-CF disposition table** — see `qc_framework_v1/reports/mig_190_smaller_cf_triage_sweep_20260430.md` §1 inventory (Disposition A/B/C, methods tags, excerpts). Fold the disposition table or a synopsis into Supplementary Methods traceability alongside limitations you choose to elevate to main text versus supplement-only.

2. **Column-level data dictionary (verified canonical columns)** — `qc_framework_v1/manuscript/data_dictionary.csv` (generated jointly with registry state). Companion SQL audit pull: `qc_framework_v1/manuscript/data_dictionary.sql`. Columns include verification status/method/batch timestamps plus semicolon-separated `cf_tags` scraped from registry notes and truncated `notes_excerpt`.

3. **Per-table methods footnotes** — one Markdown stub per **`canonical_table_signoff_registry_v1.table_status = verified`** row:  
   `qc_framework_v1/manuscript/canonical_methods_footnotes/<table_name>.md`  
   See `canonical_methods_footnotes/README.md` for citation/regeneration.

4. **Cross-lane limitation carry-forwards mentioned in playbook** (retrieve exact wording from mig_186b / mig_185b / mig_188b close-outs when drafting final supplement — this starter does **not** re-duplicate ratified DDL):
   - **mig_186b** — indeterminate / imaging-event adjudication overlays (consult ratified lane report before citing patient counts).
   - **mig_185b** — source-distinct duplicate flags on multi-source rollups where applicable.
   - **mig_188b** — T0 / nodal cohort edge rules when LN-only staging strata appear in oncology supplement tables.

---

## Regeneration (Path-C friendly)

```
.venv/bin/python scripts/render_mig197_data_dictionary_readonly.py
```

This is **SELECT-only** on MotherDuck via `scripts/_md_connect.connect_locked()`; it does **not** mutate registry or analytic tables.

---

## Machine vs voice

Starter blocks are intentionally mechanical (registry excerpts, METHOD lists). **Final supplement prose is Logan-authored** — delete hedging scaffolding, reconcile voice with main Methods, and drop footnotes that are purely operational.
