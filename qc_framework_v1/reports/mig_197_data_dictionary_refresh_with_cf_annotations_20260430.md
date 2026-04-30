<!-- READY FOR LOGAN MANUSCRIPT REFINEMENT -->

# mig_197 — data dictionary refresh with CF annotations + per-canonical methods footnotes

**Lane:** mig_197 / `data_dictionary_refresh_with_cf_annotations`  
**Batch:** `mig_197_data_dictionary_refresh_with_cf_annotations_20260430`  
**Prompt:** `cursor_prompts/CURSOR_PROMPT_mig197_data_dictionary_refresh_with_cf_annotations_20260430.md`  
**Author:** Logan Glosser <logan.glosser@gmail.com>  
**Posture:** Read-only SELECT against MotherDuck through `scripts/render_mig197_data_dictionary_readonly.py`; no registry or production DDL/DML  
**Generation:** Local run `2026-04-29` (Cursor agent; machine clock)

---

## §1 Deliverables

| Artifact | Purpose |
|---------|---------|
| `qc_framework_v1/manuscript/data_dictionary.sql` | Path-C repeatable SELECT for registry × `information_schema` join |
| `qc_framework_v1/manuscript/data_dictionary.csv` | Exported column dictionary + `cf_tags` + `notes_excerpt` (Python scrape of `CF-*` tokens) |
| `qc_framework_v1/manuscript/canonical_methods_footnotes/<table>.md` | Per verified-table supplementary stub (~10–25 lines scaffold) |
| `qc_framework_v1/manuscript/canonical_methods_footnotes/README.md` | Usage + regeneration |
| `qc_framework_v1/manuscript/supplementary_appendix_starter.md` | Wires mig_190 + dictionary + footnotes + cross-lane CF pointers |
| `scripts/render_mig197_data_dictionary_readonly.py` | Read-only exporter (also carries NaN-safe CF scraping) |

---

## §2 Counts observed (live MotherDuck at generation)

| Metric | Value |
|--------|------:|
| Rows in `data_dictionary.csv` | **4,577** |
| `table_status = verified` canonicals (main) — footnote stubs | **104** |
| Footnote Markdown files (excluding `README`) | **104** |

**Note vs prompt heuristic (~63):** Publication registry now carries **104** verified `main` canonicals/sign-off rows; Tier-2-only estimate in the originating prompt understates breadth (includes SOURCE tier note_entities_\*, pathology imaging feeds, specimens, VIEW sign-offs, governance registries, etc.). All receive footnote stubs regardless of analytic tier — Logan may prune unneeded tables from Supplement.

---

## §3 Caveats

1. **Apply-lane staleness:** If **mig_185b / mig_186b / mig_187 / mig_188b** (or successors) lag this export, annotations reflect **whatever** was live in `canonical_*_registry_v*` at SELECT time — not hypothetical post-apply wording. Confirm `batch_id` / `signoff_migration` on hot columns before citation-lock.

2. **VIEW SCAN failures:** **3** verified objects surfaced DuckDB binder errors (“Contents of view were altered”) on naive `COUNT(*)` during generation:
   - `canonical_us_patient_master_VIEW_v2`
   - `molecular_variants_unnested_VIEW_v2`
   - `molecular_fusions_unnested_VIEW_v2`  
   Corresponding stubs document the trimmed binder excerpt; reconcile underlying VIEW DDL on MotherDuck before citing row cardinality from those stubs.

3. **CF tagging:** Regex `CF-[A-Za-z0-9_-]+` extracts tags from **`canonical_column_verification_registry_v1.notes`** only; inline prose limitations without CF prefixes will not populate `cf_tags`.

---

## §4 Regeneration recipe

```bash
cd /path/to/THyroid 2026
.venv/bin/python -m py_compile scripts/render_mig197_data_dictionary_readonly.py
.venv/bin/python scripts/render_mig197_data_dictionary_readonly.py
```

Prereqs:

- MotherDuckRW token resolving through `motherduck_client.get_token()` (see `.cursor/skills/motherduck-credentials`)

Optional SQL-only pull:

```bash
# After connecting with USE thyroid_canonical_publication_v1_0 ...
duckdb ":memory:" < qc_framework_v1/manuscript/data_dictionary.sql
# (adapt client — MotherDuck CLI / scripted connection)
```

Manual CF column if not using exporter: regexp-extract CF tokens from `registry_notes` in spreadsheet toolchain.
