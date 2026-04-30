# mig_174b token enumeration — cnln_img_laterality Option A

**Date:** 2026-04-29  
**Lane:** mig_174b  
**Batch:** `mig_174b_cnln_img_laterality_per_side_boolean_20260429`  
**Target DB:** `thyroid_canonical_publication_v1_0`  
**Target table:** `main.canonical_patient_master`  
**Posture:** read-only probe + SQL-only Path-C apply artifact; no MotherDuck DDL/DML executed by this authoring session.

## Executive summary

Logan-ratified **Option A** is ready for Path-C dispatch for `cnln_img_laterality`: preserve the legacy raw VARCHAR and add 5 analyst-facing per-side BOOLEAN columns.

The live read-only probe found only handled `cnln_img_laterality` tokens:

- `left`
- `right`
- `central`
- `bilateral`
- `lateral`
- `lateral neck`
- sentinel `null`

Unhandled token count: **0**.

The sister columns `lateral_levels_v10` and `ene_levels_v9` are not structurally equivalent to laterality side labels. They primarily store nodal level/location strings (`level 2`, `level v`, `paratracheal`, etc.), so they should not be mixed into mig_174b. A later lane should parse them as nodal level/location features rather than per-side flags.

## Read-only token enumeration SQL

```sql
WITH tokens AS (
  SELECT
    column_name,
    research_id,
    TRIM(LOWER(t)) AS token_norm
  FROM (
    SELECT 'cnln_img_laterality' AS column_name, research_id, UNNEST(string_split(cnln_img_laterality, ';')) AS t
    FROM main.canonical_patient_master WHERE cnln_img_laterality IS NOT NULL
    UNION ALL
    SELECT 'lateral_levels_v10', research_id, UNNEST(string_split(lateral_levels_v10, ';')) AS t
    FROM main.canonical_patient_master WHERE lateral_levels_v10 IS NOT NULL
    UNION ALL
    SELECT 'ene_levels_v9', research_id, UNNEST(string_split(ene_levels_v9, ';')) AS t
    FROM main.canonical_patient_master WHERE ene_levels_v9 IS NOT NULL
  )
)
SELECT column_name, token_norm, COUNT(*) AS n_appearances, COUNT(DISTINCT research_id) AS n_pts
FROM tokens GROUP BY 1, 2 ORDER BY 1, 3 DESC, 2;
```

## Live token enumeration result

| column_name | token_norm | n_appearances | n_pts |
|---|---:|---:|---:|
| cnln_img_laterality | bilateral | 116 | 116 |
| cnln_img_laterality | right | 87 | 87 |
| cnln_img_laterality | left | 85 | 85 |
| cnln_img_laterality | central | 32 | 32 |
| cnln_img_laterality | null | 12 | 12 |
| cnln_img_laterality | lateral | 4 | 4 |
| cnln_img_laterality | lateral neck | 3 | 3 |
| ene_levels_v9 | level 3 | 12 | 12 |
| ene_levels_v9 | level 4 | 11 | 11 |
| ene_levels_v9 | level 6 | 6 | 6 |
| ene_levels_v9 | central | 3 | 3 |
| ene_levels_v9 | level 2 | 3 | 3 |
| ene_levels_v9 | paratracheal | 3 | 3 |
| ene_levels_v9 | ij | 1 | 1 |
| ene_levels_v9 | level 2-5 | 1 | 1 |
| ene_levels_v9 | level 2a | 1 | 1 |
| ene_levels_v9 | level ii | 1 | 1 |
| ene_levels_v9 | pretracheal | 1 | 1 |
| ene_levels_v9 | supraclavicular | 1 | 1 |
| lateral_levels_v10 | *(empty string)* | 21 | 21 |
| lateral_levels_v10 | level v | 16 | 16 |
| lateral_levels_v10 | ii | 10 | 10 |
| lateral_levels_v10 | levels ii | 6 | 6 |
| lateral_levels_v10 | level ii | 5 | 5 |
| lateral_levels_v10 | level iv | 5 | 5 |
| lateral_levels_v10 | level 2 | 4 | 4 |
| lateral_levels_v10 | level 3 | 4 | 4 |
| lateral_levels_v10 | levels 2, 3, 4 | 4 | 4 |
| lateral_levels_v10 | level 2,3, 4, 5 | 2 | 2 |
| lateral_levels_v10 | level 4 | 2 | 2 |
| lateral_levels_v10 | levels 2 | 2 | 2 |
| lateral_levels_v10 | levels ii-v | 2 | 2 |
| lateral_levels_v10 | level ii-iv | 1 | 1 |
| lateral_levels_v10 | levels 2 - 4 | 1 | 1 |
| lateral_levels_v10 | levels 2-4 | 1 | 1 |
| lateral_levels_v10 | levels iv, v | 1 | 1 |
| lateral_levels_v10 | levels v | 1 | 1 |

## `cnln_img_laterality` preconditions

| check | value |
|---|---:|
| CPM rows | 10,871 |
| CPM distinct `research_id` | 10,871 |
| `cnln_img_laterality` non-NULL rows | 272 |
| `cnln_img_laterality` NULL rows | 10,599 |
| new per-side columns already present | 0 |
| unhandled `cnln_img_laterality` tokens | 0 |

## Expected per-side BOOLEAN counts after Path-C apply

| derived column | TRUE | FALSE | NULL |
|---|---:|---:|---:|
| `cnln_img_left_present` | 85 | 187 | 10,599 |
| `cnln_img_right_present` | 87 | 185 | 10,599 |
| `cnln_img_central_present` | 32 | 240 | 10,599 |
| `cnln_img_bilateral_present` | 116 | 156 | 10,599 |
| `cnln_img_lateral_neck_present` | 7 | 265 | 10,599 |

Interpretation:

- `TRUE`: token is present in legacy `cnln_img_laterality` after trim/lower normalization.
- `FALSE`: raw laterality is present but this side/category token is absent.
- `NULL`: raw laterality is NULL; no source laterality data.

## Mapping used in the apply artifact

| normalized token | derived column |
|---|---|
| `left` | `cnln_img_left_present` |
| `right` | `cnln_img_right_present` |
| `central` | `cnln_img_central_present` |
| `bilateral` | `cnln_img_bilateral_present` |
| `lateral`, `lateral neck`, `lateral_neck` | `cnln_img_lateral_neck_present` |
| `null`, `nan`, `none`, `n/a`, `unknown`, empty string | sentinel/drop; no flag set |

## Sister-column assessment

`lateral_levels_v10` and `ene_levels_v9` are **not** semantically equivalent to `cnln_img_laterality`:

- `lateral_levels_v10` contains level labels and ranges, including `level v`, `levels ii-v`, `levels 2, 3, 4`, and empty strings.
- `ene_levels_v9` contains nodal level/location labels, including `level 2`, `level 3`, `level 4`, `level 6`, `paratracheal`, `pretracheal`, `supraclavicular`, and `ij`.

Recommendation: do **not** create mig_174c as a simple per-side BOOLEAN extension. If needed, open a separate nodal-level parsing lane (e.g., mig_174d) with level-specific canonicalization rules and review of range expressions such as `levels ii-v` and `level 2-5`.

## Deliverable

Path-C apply artifact authored at:

- `qc_framework_v1/migrations/174b_cnln_img_laterality_per_side_boolean_20260429.sql`
