# LLM Extraction — To-Do List (QC framework v1)

Tracking structured fields that cannot be derived from current canonical columns
and require a new LLM extraction pass over narrative notes (op note, gross
description, microscopic, addenda).

Format: one item per structured column we need; each includes (a) the rule
that needs it, (b) source notes to parse, (c) expected vocabulary, (d) priority.

---

## 1. T4b — invasion of prevertebral fascia, carotid, or mediastinal vessels

- **Rule it unblocks**: AJCC8 thyroid T4b = "any size, gross ETE into
  prevertebral fascia or encases carotid artery or mediastinal vessels"
  (all tumor types: DTC, MTC, ATC).
- **Why it's not derivable now**: `canonical_path_malignant_events_v1` carries
  `extrathyroidal_extension` (35 free-text variants) and `gross_ete` (BIGINT
  flag) but no structured column for prevertebral-fascia or carotid-encasement
  involvement. T4a (strap muscle / larynx / trachea / esophagus / RLN) vs T4b
  (prevertebral fascia / carotid / mediastinal) are clinically and
  prognostically distinct but collapsed in the current ETE free text.
- **Source notes to parse**:
  - Operative notes — gross dissection descriptions, resectability language
  - Pathology reports — gross description section, microscopic, final comment
  - CT/MRI radiology reports — may describe carotid encasement pre-op
- **Expected output fields**:
  - `t4b_prevertebral_fascia_invasion` ∈ {`present`, `absent`, `unknown`}
  - `t4b_carotid_encasement`           ∈ {`present`, `absent`, `unknown`}
  - `t4b_mediastinal_vessel_invasion`  ∈ {`present`, `absent`, `unknown`}
  - `t4b_evidence_span`                (verbatim quote + source_table, row id)
  - `t4b_confidence`                   ∈ {`high`, `medium`, `low`}
- **Priority**: HIGH — blocks exact AJCC8 T-stage derivation for all ATC rows
  and the long tail of DTC/MTC with gross ETE (~1,571 path_malignant rows
  with `gross_ete=1`, subset of which are T4b candidates).
- **Interim handling in migration 04**: rows with `reported t_stage='T4b'` or
  `reported t_stage='T4a' with extensive/macroscopic ETE` are flagged
  `derived_t_stage='indeterminate_t4b_requires_llm'` rather than forced to T4a.
  The T-stage discordance flag stays NULL on those rows (not TRUE / not
  FALSE — honestly missing).
- **Owner / follow-up**: add to the next Tier-2 LLM extraction batch; align
  prompt with the round-2 `esophageal_invasion` / `vascular_invasion` /
  `airway_invasion` extractor pattern (same 9-domain v5 architecture).

---

<!-- Add further items below as downstream prompts surface new LLM needs. -->
