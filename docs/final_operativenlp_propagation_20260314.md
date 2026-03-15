# Operative NLP Propagation — Final Engineering Pass
> Updated: 2026-03-14 | Scripts: 71, 22, 81

---

## 1. Root-Cause Diagnosis

`operative_episode_detail_v2` is built by **script 22** using a JOIN of structured
tables (`path_synoptics`, `operative_details`) from raw sources.  That build sets
all NLP-derived flag columns to their default values (`FALSE` for booleans, `NULL`
for text fields) because NLP inference had not been applied at construction time.

**Script 26** (MotherDuck materialization) runs after script 22.  Historically it
materialized `operative_episode_detail_v2` immediately — *before* **script 71**
had a chance to execute the NLP UPDATE pass.  The consequence: MotherDuck contained
the un-enriched base table and the NLP fields remained at default values.

**Script 71** (`71_operative_nlp_to_motherduck.py`) is the correct fix path.  It:
1. Connects to MotherDuck RW (`thyroid_research_2026`)
2. Loads `clinical_notes_long` (operative note sub-set)
3. Runs `OperativeDetailExtractor` (in `notes_extraction/extract_operative_v2.py`)
4. Stages results into a temp table `_v2_operative_enrichment`
5. Issues `UPDATE operative_episode_detail_v2 SET ... FROM _v2_operative_enrichment`
6. Recreates the `md_oper_episode_detail_v2` mirror via script 26 re-run

The correct **deployment order** is:
```
22 → 23 → 24 → 25 → 71 → 26 --md
```
Script 71 must run **before** script 26 materializes the mirror; running it after
requires a targeted re-materialize (`scripts/26_motherduck_materialize_v2.py --md --table md_oper_episode_detail_v2`).

---

## 2. NLP Field Catalog

### Category A — Structured / Reliable (script 22, always populated)
| Field | Source |
|-------|--------|
| `procedure_normalized` | `path_synoptics.thyroid_procedure` |
| `central_neck_dissected_flag` | Central LND composite (level 6 logic) |
| `lateral_neck_dissected_flag` | Phase 10 Phase A NLP + structured |
| `rln_injury_documented_flag` | `complications.vocal_cord_status` |

### Category B — NLP-Populated via Script 71 (requires script 71 to run)
| Field | OperativeDetailExtractor entity |
|-------|--------------------------------|
| `rln_monitoring_flag` | `rln_monitoring` |
| `rln_finding_raw` | `rln_finding` (free text) |
| `parathyroid_autograft_flag` | `parathyroid_autograft` |
| `gross_ete_flag` | `gross_ete` |
| `local_invasion_flag` | `local_invasion` |
| `tracheal_involvement_flag` | `tracheal_involvement` |
| `esophageal_involvement_flag` | `esophageal_involvement` |
| `strap_muscle_involvement_flag` | `strap_muscle_involvement` |
| `reoperative_field_flag` | `reoperative_field` |
| `drain_flag` | `drain_placed` |
| `operative_findings_raw` | aggregated finding snippets |

> **Semantic note**: When Category B fields are `FALSE`, they mean **UNKNOWN /
> NOT PARSED**, not confirmed negative.  False negative is possible because the
> NLP entity type may not have appeared in the note.  Always treat FALSE as
> "absent or not detected" unless the note has been confirmed negative.

### Category C — Source Absent / Vocabulary Gap
| Field | Reason |
|-------|--------|
| `berry_ligament_flag` | Entity type absent from NLP vocabulary |
| `frozen_section_flag` | Entity type absent from NLP vocabulary |
| `ebl_ml_nlp` | EBL numeric entity absent from vocabulary |
| `parathyroid_identified_count` | Count extraction not implemented |
| `parathyroid_resection_flag` | Separate entity from autograft, not yet added |

Category C fields cannot be improved without adding entities to
`notes_extraction/vocab.py` and re-running script 71.

---

## 3. Validation Table

`val_operative_nlp_propagation_v1` is created by **script 81**.

| Column | Description |
|--------|-------------|
| `field_name` | NLP field being audited |
| `category` | A / B / C as above |
| `total_rows` | Rows in `operative_episode_detail_v2` |
| `populated_count` | Non-NULL / non-FALSE rows |
| `populated_pct` | Coverage percentage |
| `null_or_false_count` | Rows still at default |
| `source_note` | Human-readable interpretation |

---

## 4. Dashboard Implications

- **Complications tab**: RLN injury section uses `extracted_rln_injury_refined_v2`,
  NOT `operative_episode_detail_v2.rln_monitoring_flag` — this section is unaffected
  by the propagation gap.
- **Operative Dashboard tab**: Procedure flags, EBL structured, and neck dissection
  are Category A (reliable).  Category B fields should be shown with a data-quality
  caveat until script 71 has been verified to have run.

---

## 5. Run Instructions

```bash
# 1. Run operative NLP enrichment (takes 15-30 min for full note corpus)
MOTHERDUCK_TOKEN=$(python -c "import toml; print(toml.load('.streamlit/secrets.toml')['MOTHERDUCK_TOKEN'])")
export MOTHERDUCK_TOKEN

.venv/bin/python scripts/71_operative_nlp_to_motherduck.py --md

# 2. Validate propagation coverage
.venv/bin/python scripts/81_operative_nlp_propagation_validate.py --md

# 3. Re-materialize updated mirror
.venv/bin/python scripts/26_motherduck_materialize_v2.py --md
```

Exports written to `exports/final_md_optimization_20260314/operative_nlp_*.csv`.
