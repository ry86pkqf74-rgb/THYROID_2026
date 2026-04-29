# Cursor Prompt — mig_174 cnln_img_laterality + lateral_levels_v10 + ene_levels_v9 Multi-Label Parser

**Lane:** 63 / mig_174
**Batch_id:** `mig_174_multilabel_laterality_parser_20260429`
**Generated:** 2026-04-29
**Type:** Multi-label parser design + apply. Logan ratifies representation before any apply.

---

## §0 Why this lane exists

`cnln_img_laterality` carries semicolon-delimited multi-label values (`'left; bilateral'`, `'null; bilateral'` with literal `'null'` token, `'right; bilateral; left'`). It also has 31 distinct values across 272 non-null rows (Cowork live 2026-04-29). mig_168 audit flagged this column for token-level normalization.

Two related cols share the same multi-label structure:
- `lateral_levels_v10` — semicolon-delimited level codes (`'level II'`, `'II; III'`, `'II; III; IV'`, etc.)
- `ene_levels_v9` — semicolon-delimited extranodal-extension level annotations

This lane:
1. Profiles the value spread + token grammar across all 3 cols.
2. Designs a canonical lateralization enum + level-code enum + multi-label representation.
3. Surfaces a Logan-decision package: representation choice (delimited array vs per-side BOOLEANs) BEFORE any apply.
4. Authors the apply SQL once Logan ratifies the representation.

Cowork live 2026-04-29:

| Col | n_distinct | n_nonnull | n_sentinel |
|---|---:|---:|---:|
| `cnln_img_laterality` | 31 | 272 | 8 |

(`lateral_levels_v10` and `ene_levels_v9` numbers must be probed live in §2.)

## §1 Governance posture

- Two-phase lane:
  - **Phase 1 (this lane, mig_174a)**: read-only profile + design doc + Logan-ratification package. No `query_rw`.
  - **Phase 2 (mig_174b later)**: apply SQL after Logan ratifies representation.
- AGENTS-governance binding: agent ships profile + design + decision package only. Cowork applies after Logan ratifies.

## §2 Required pre-flight probes

```sql
-- §2a Cardinality + sample for each col
SELECT cnln_img_laterality, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE cnln_img_laterality IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;

SELECT lateral_levels_v10, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE lateral_levels_v10 IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;

SELECT ene_levels_v9, COUNT(*) AS n
FROM main.canonical_patient_master
WHERE ene_levels_v9 IS NOT NULL
GROUP BY 1 ORDER BY 2 DESC LIMIT 50;

-- §2b Token-level enumeration (split by ';')
WITH tokens AS (
  SELECT TRIM(unnest(string_split(cnln_img_laterality, ';'))) AS token
  FROM main.canonical_patient_master
  WHERE cnln_img_laterality IS NOT NULL
)
SELECT LOWER(token) AS norm_token, COUNT(*) AS n
FROM tokens
GROUP BY 1 ORDER BY 2 DESC LIMIT 30;
-- Repeat for lateral_levels_v10 + ene_levels_v9.

-- §2c Literal 'null' token quantification
SELECT
  SUM(CASE WHEN cnln_img_laterality ILIKE '%null%' THEN 1 ELSE 0 END) AS n_null_token_lat,
  SUM(CASE WHEN lateral_levels_v10 ILIKE '%null%' THEN 1 ELSE 0 END) AS n_null_token_lvl,
  SUM(CASE WHEN ene_levels_v9 ILIKE '%null%' THEN 1 ELSE 0 END) AS n_null_token_ene
FROM main.canonical_patient_master;
```

## §3 Canonical enums (proposed)

### §3.1 Lateralization enum

| canonical_token | accepts (case-insensitive, post-trim) |
|---|---|
| `left` | left, lt, l |
| `right` | right, rt, r |
| `bilateral` | bilateral, bilat, both |
| `central` | central, ctr |
| `lateral_neck` | lateral neck, lateral_neck, lat neck, lateral |

**Rejected tokens** (map to NULL or drop): `null` (literal string), `n/a`, `unspecified`, `''`, `-`.

Multi-label preservation: a row with `'left; bilateral'` becomes `{'left','bilateral'}`. Decision needed: serialize as ARRAY<VARCHAR> or as 5 BOOLEAN cols.

### §3.2 Level-code enum

`I, II, IIa, IIb, III, IV, V, Va, Vb, VI, VII, central, lateral_unspec`

(Roman-numeral cervical lymph node levels per AJCC.) Same multi-label question: ARRAY vs per-level BOOLEANs.

### §3.3 ENE level enum

Token grammar likely `'<level>:ene_<status>'` or similar — agent profiles in §2 and proposes.

## §4 Logan-decision package (must be in design doc)

For Logan to ratify before mig_174b apply:

| Question | Option A | Option B | Recommendation |
|---|---|---|---|
| Multi-label representation for laterality | `cnln_img_laterality_canonical` `ARRAY<VARCHAR>` | 5 new BOOLEAN cols (`cnln_lat_left`, `cnln_lat_right`, `cnln_lat_bilateral`, `cnln_lat_central`, `cnln_lat_lateral_neck`) | Recommend B for analytic ergonomics |
| Multi-label representation for levels | `lateral_levels_v10_canonical` `ARRAY<VARCHAR>` | 11 new BOOLEAN cols (`level_i`, `level_iia`, etc.) | Recommend B |
| Literal `'null'` tokens | drop silently | preserve as `cnln_img_laterality_has_null_token` BOOLEAN flag | Recommend flag preserved for QC |
| Sentinel-only rows | NULL all canonical cols | Mark with `parse_status='sentinel'` | Recommend status tracking |
| Original VARCHAR fate | DROP | RENAME to `_legacy_raw` | Recommend rename per mig_173 pattern |

Agent profiles real data and decides whether to recommend A or B per col; recommendation must be evidence-based (e.g., "ARRAY chosen because the per-level BOOLEAN approach would inflate PM by 17 cols and the multi-label cardinality is low").

## §5 Phase 2 apply skeleton (mig_174b — author later, NOT in this lane)

For reference only, so Cowork can review the future apply structure:

```sql
-- Phase 2 (mig_174b) skeleton:
-- A. Pre-snapshot the 3 cols
-- B. ALTER ADD COLUMN per chosen representation (5 BOOLEANs + parse_status per col, or ARRAY<VARCHAR> per col)
-- C. Parser UPDATE (single transaction)
-- D. RENAME originals to *_legacy_raw
-- E. Registry resync
-- F. Post-state verify (token-level coverage, sentinel handling, multi-label distinct count)
```

DO NOT author this in mig_174a — wait for Logan's representation choice.

## §6 Required CFs

- `CF-mig168-VOCAB-DRIFT-CNLN-LATERALITY-MULTILABEL` → CLOSED via mig_174b apply
- `CF-mig174a-DESIGN-RATIFICATION-PENDING` (opens at mig_174a; closes at mig_174b)
- `CF-mig174a-LITERAL-NULL-TOKEN-PRESENT` (informational; quantifies `'null'` token rows)
- `CF-mig174a-LATERAL-LEVELS-V10-MULTILABEL` (informational, scope flag)
- `CF-mig174a-ENE-LEVELS-V9-MULTILABEL` (informational, scope flag)

## §7 Files + Git workflow

- `qc_framework_v1/reports/mig_174a_multilabel_laterality_design_20260429.md` — profile + design + Logan-decision package
- `qc_framework_v1/migrations/174a_multilabel_laterality_probes_20260429.sql` — commented probe SQL Logan can replay
- Commit: `qc: mig_174a cnln/lateral/ene multi-label parser design (read-only)`
- Push.

## §8 Out of scope

- Do NOT apply any UPDATE in this lane. mig_174b is the apply.
- Do NOT touch other multi-label cols on PM (e.g., `histologic_types_all`, `histologic_variants_all`) — those are mig_172 territory.
- Do NOT propose level-code reforms outside the 3 cols listed.
- Do NOT touch the underlying source (`canonical_us_lymph_node_v2` will rebuild from sources later in mig_171).
- Do NOT use cross-DB sourcing.

## §9 Apply governance

This lane is **profile + design + Logan-decision package only**. No data writes from agent. Cowork applies mig_174b after Logan ratifies the representation choice in the design doc.

Per AGENTS governance: agent ships profile only. **No `query_rw` from agent.**
