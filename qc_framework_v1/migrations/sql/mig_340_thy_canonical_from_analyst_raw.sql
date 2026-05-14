-- -----------------------------------------------------------------------------
-- mig_340 — BQ rebuild: pub_canonical.canonical_labs_thyroglobulin_v1 FROM
--   pub_raw.thyroglobulin_analyst_ehr_20251120 (all-STRING ingest).
--
-- Run via:
--   qc_framework_v1/migrations/mig_340_thyroglobulin_analyst_bq_rebuild.py --csv <path.csv> --apply
-- Snapshot of the prior canonical runs in Python immediately before this script executes.
--
-- Combo interim rule (until analyst confirms row order / unit column):
--   1) JS heuristic (aligned to Script 113 pair sentinel rules)
--   2) SQL cross-ref vs same-patient literal sets from NON-combo Tg/TgAb rows in
--      this analyst load
--   3) Fallback: inferred_combo_pair_fallback_row_order (lexicographically
--      larger TRIM(result) mapped to Tg; smaller → TgAb)
--
-- Value normalization TEMP JS UDF aligns with scripts/_lab_value_normalizer.py
-- (thyroglobulin + anti_thyroglobulin only).
-- -----------------------------------------------------------------------------
BEGIN

DECLARE run_ingestion_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

CREATE TEMP FUNCTION thy_normalize_lab_value(raw STRING, canon_key STRING)
RETURNS STRUCT<value_numeric FLOAT64, is_censored BOOL, value_correction_note STRING>
LANGUAGE js AS R"""
function stripSuffixLoop(s0) {
  var s = String(s0);
  var suf = [" miu/ml"," iu/ml"," ng/ml"," ku/l"];
  var chg = true;
  while (chg) {
    chg = false;
    var lo = s.toLowerCase();
    for (var i = 0; i < suf.length; i++) {
      var u = suf[i];
      if (lo.endsWith(u)) {
        s = s.substring(0, s.length - u.length).trim();
        chg = true;
        break;
      }
    }
  }
  return s;
}

function cleanup(raw) {
  if (raw == null) return {s: "", notes: ["unparseable_string"]};
  var s = String(raw).trim();
  if (!s.length) return {s: "", notes: ["unparseable_string"]};
  var notes = [];
  while (/\s*\((LL|HH|L|H)\)\s*$/i.test(s)) {
    notes.push("suffix_stripped_flag");
    s = s.replace(/\s*\((LL|HH|L|H)\)\s*$/i, "").trim();
  }
  var mPar = /^(.+[0-9.])\s+([LH])\s*$/i.exec(s);
  if (mPar && mPar[1]) {
    s = mPar[1].trim();
    notes.push("suffix_stripped_bare_hl");
  }
  s = stripSuffixLoop(s);
  s = s.replace(/\s+/g, " ").trim();
  return {s: s, notes: notes};
}

function detectCensor(s) {
  var res = [
    [/^\s*goal\s+less\s+than\s+(-?\d+\.?\d*)/i, 1],
    [/^\s*goal\s*<\s*(-?\d+\.?\d*)/i, 1],
    [/^\s*less\s+than\s+(-?\d+\.?\d*)/i, 1],
    [/^\s*greater\s+than\s+(-?\d+\.?\d*)/i, 1],
    [/^\s*<\s*(-?\d+\.?\d*)/, 1],
    [/^\s*>\s*(-?\d+\.?\d*)/, 1]
  ];
  for (var i = 0; i < res.length; i++) {
    var m = res[i][0].exec(s);
    if (m) return {c: true, v: parseFloat(m[1])};
  }
  return {c: false, v: null};
}

function parseTiter(s) {
  var m = /^\s*1\s*:\s*(\d+)\s*$/.exec(s);
  return m ? parseFloat(m[1]) : null;
}

function firstNum(s) {
  var m = /(-?\d+\.?\d*(?:[eE][-+]?\d+)?)/.exec(s);
  return m ? parseFloat(m[1]) : null;
}

function plausible(name, v) {
  var pmin = 0.0, pmax, oor;
  if (name === "thyroglobulin") {
    pmax = 10000.0;
    oor = 1000000.0;
  } else if (name === "anti_thyroglobulin") {
    pmax = 40000.0;
    oor = 1000000.0;
  } else {
    return {ok: true, v: v, n: []};
  }
  if (v < 0) return {ok: false, v: null, n: ["nulled_negative"]};
  if (v === 0) return {ok: true, v: 0.0, n: []};
  if (v <= pmax && v >= pmin) return {ok: true, v: v, n: []};
  if (v > pmax && v <= oor) {
    var d10 = v / 10, d100 = v / 100;
    if (d10 <= pmax && d10 >= pmin) return {ok: true, v: d10, n: ["divided_by_10"]};
    if (d100 <= pmax && d100 >= pmin) return {ok: true, v: d100, n: ["divided_by_100"]};
  }
  return {ok: false, v: null, n: ["nulled_unrecoverable_implausible"]};
}

function main(raw, canonKey) {
  if (raw == null)
    return {value_numeric: null, is_censored: false, value_correction_note: "unparseable_string"};
  var cl = cleanup(raw);
  if (!cl.s.length) {
    return {
      value_numeric: null,
      is_censored: false,
      value_correction_note: cl.notes.join(",") || "unparseable_string"
    };
  }
  var notes = cl.notes;
  var dc = detectCensor(cl.s);
  var cens = dc.c;
  var thr = dc.v;
  var titerVal = (!cens && canonKey === "anti_thyroglobulin") ? parseTiter(cl.s) : null;
  var parsed = null;
  if (cens) parsed = thr;
  else if (titerVal != null) {
    parsed = titerVal;
    notes.push("titer_denominator_extracted");
  } else {
    parsed = firstNum(cl.s);
    if (parsed == null || isNaN(parsed)) {
      notes.push("unparseable_string");
      return {
        value_numeric: null,
        is_censored: cens,
        value_correction_note: notes.join(",") || null
      };
    }
  }
  if (parsed == null || isNaN(parsed)) {
    notes.push("unparseable_string");
    return {
      value_numeric: null,
      is_censored: cens,
      value_correction_note: notes.join(",") || null
    };
  }
  if (cens)
    return {
      value_numeric: parsed,
      is_censored: true,
      value_correction_note: notes.join(",") || null
    };
  if (!canonKey)
    return {
      value_numeric: parsed,
      is_censored: false,
      value_correction_note: notes.join(",") || null
    };
  var pl = plausible(canonKey, parsed);
  if (pl.n && pl.n.length) notes = notes.concat(pl.n);
  if (!pl.ok)
    return {
      value_numeric: null,
      is_censored: false,
      value_correction_note: notes.join(",") || null
    };
  return {
    value_numeric: pl.v,
    is_censored: false,
    value_correction_note: notes.join(",") || null
  };
}
return main(raw, canon_key);
"""
;

CREATE TEMP FUNCTION thy_combo_heuristic_pair(res_a STRING, res_b STRING)
RETURNS STRUCT<tg STRING, tgab STRING, assignment_method STRING>
LANGUAGE js AS R"""
function heur(x, y) {
  var tgabS = {"<0.9": 1};
  var tgS = {"<0.1": 1, "<0.2": 1};
  var tgHi = {"<2": 1, "<2.0": 1, "<20": 1};
  if (tgabS[x] && !tgabS[y]) return {tg: y, tgab: x, assignment_method: "inferred_combo_pair_heuristic"};
  if (tgabS[y] && !tgabS[x]) return {tg: x, tgab: y, assignment_method: "inferred_combo_pair_heuristic"};
  if (tgS[x] && !tgS[y]) return {tg: x, tgab: y, assignment_method: "inferred_combo_pair_heuristic"};
  if (tgS[y] && !tgS[x]) return {tg: y, tgab: x, assignment_method: "inferred_combo_pair_heuristic"};
  if (tgHi[x] && /^<0\./.test(y)) return {tg: y, tgab: x, assignment_method: "inferred_combo_pair_heuristic"};
  if (tgHi[y] && /^<0\./.test(x)) return {tg: x, tgab: y, assignment_method: "inferred_combo_pair_heuristic"};
  return null;
}
function main(a, b) {
  if (a == null || b == null) return {tg: null, tgab: null, assignment_method: null};
  var x = String(a).trim();
  var y = String(b).trim();
  var h = heur(x, y);
  if (h) return h;
  h = heur(y, x);
  if (h) return h;
  return {tg: null, tgab: null, assignment_method: null};
}
return main(res_a, res_b);
"""
;

CREATE TEMP FUNCTION thy_singleton_combo_analyte(result_raw STRING)
RETURNS STRUCT<analyte STRING, assignment_method STRING>
LANGUAGE js AS R"""
function main(sr) {
  if (sr == null) return {analyte: null, assignment_method: "inferred_combo_singleton_missing"};
  var s = String(sr).trim();
  var slug = s.toLowerCase().replace(/\s+/g, " ");
  if (/^\s*1\s*:\s*\d+\s*$/i.test(slug)) return {analyte: "TgAb", assignment_method: "inferred_value_pattern_singleton"};
  if (/ titer /.test(slug)) return {analyte: "TgAb", assignment_method: "inferred_value_pattern_singleton"};
  var tgabWide = {};
  "<0.9 <0.91 <0.92 <0.93 <0.94 <1.0".split(/\s+/).forEach(function (x) { tgabWide[x]=1; });
  if (tgabWide[slug]) return {analyte: "TgAb", assignment_method: "inferred_value_pattern_singleton"};
  if (["<0.1","<0.2","<0.15"].indexOf(slug)>=0)return {analyte:"Tg",assignment_method:"inferred_value_pattern_singleton"};
  var edge = /<([0-9.]+)/.exec(slug);
  if (slug.startsWith("<") && edge) {
    var fv = parseFloat(edge[1]);
    if (fv <= 2.5 && fv >= 0.8) return {analyte: "TgAb", assignment_method: "inferred_value_pattern_singleton"};
    if (fv <= 0.3) return {analyte: "Tg", assignment_method: "inferred_value_pattern_singleton"};
  }
  var stripped = slug.replace(/^[<>≤≥]/,'');
  var nm = /^(-?\d+\.?\d*)/.exec(stripped);
  var anchor = nm ? parseFloat(nm[1]) : -1;
  if (anchor >= 80000) return {analyte: "TgAb", assignment_method: "inferred_value_pattern_singleton"};
  if (anchor >= 10 && anchor < 80000) return {analyte: "Tg", assignment_method: "inferred_value_pattern_singleton"};
  return {analyte: "Tg", assignment_method: "inferred_value_pattern_singleton_default_tg_unclassified"};
}
return main(result_raw);
"""
;

CREATE OR REPLACE TABLE `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`
OPTIONS(
  description=(
    'mig_340 2026-05-14: row universe analyst CSV pub_raw.thyroglobulin_analyst_ehr_20251120; '
    'source=analyst_ehr_tg; combo heuristic+cross-ref+lexicographic fallback; '
    'normalization aligns with scripts/_lab_value_normalizer.py; '
    'dedup (research_id, analyte, lab_datetime, COALESCE(CAST(value_numeric AS STRING), value_raw)); '
    'analyte_assignment_method is audit-only'
  ))
AS

WITH raw AS (
  SELECT
    SAFE_CAST(NULLIF(TRIM(research_id_number), "") AS INT64) AS research_id,
    TRIM(test_name) AS test_name,
    TRIM(IFNULL(result, "")) AS value_raw,
    TRIM(IFNULL(specimen_collect_dt, "")) AS specimen_collect_dt_raw
  FROM `thyroid-canonical-pub-2026.pub_raw.thyroglobulin_analyst_ehr_20251120`
  WHERE NULLIF(TRIM(research_id_number), "") IS NOT NULL
),

raw_ts AS (
  SELECT
    *,
    COALESCE(
      SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S UTC", specimen_collect_dt_raw),
      SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S%Ez", specimen_collect_dt_raw),
      SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%E*S", specimen_collect_dt_raw),
      SAFE.PARSE_TIMESTAMP("%Y-%m-%d %H:%M:%S", specimen_collect_dt_raw),
      SAFE.PARSE_TIMESTAMP("%m/%d/%Y %H:%M:%S", specimen_collect_dt_raw),
      SAFE.PARSE_TIMESTAMP("%m/%d/%Y %I:%M:%S %p", specimen_collect_dt_raw),
      TIMESTAMP(SAFE.PARSE_DATETIME("%Y-%m-%d %H:%M:%S", specimen_collect_dt_raw)),
      TIMESTAMP(SAFE.PARSE_DATE("%Y-%m-%d", specimen_collect_dt_raw)),
      TIMESTAMP(SAFE.PARSE_DATE("%m/%d/%Y", specimen_collect_dt_raw)),
      TIMESTAMP(SAFE.PARSE_DATE("%m-%d-%Y", specimen_collect_dt_raw))
    ) AS lab_datetime
  FROM raw
),

eligible AS (
  SELECT * FROM raw_ts
  WHERE research_id IS NOT NULL AND lab_datetime IS NOT NULL AND value_raw IS NOT NULL
),

tagged AS (
  SELECT
    *,
    LOWER(TRIM(REGEXP_REPLACE(test_name, r'\s+', ' '))) AS tn_key
  FROM eligible
),

classified AS (
  SELECT
    *,
    CASE tn_key
      WHEN 'thyroglobulinlevel' THEN STRUCT("explicit" AS analyte_assignment_method, "Tg" AS analyte, "immunoassay" AS assay_method)
      WHEN 'thyroglobulin' THEN STRUCT("explicit", "Tg", "immunoassay")
      WHEN 'thyroglobulin by ima' THEN STRUCT("explicit", "Tg", "IMA")
      WHEN 'comprehensive thyroglobulin' THEN STRUCT("explicit", "Tg", "comprehensive")
      WHEN 'thyroglobulin by lc-ms/ms' THEN STRUCT("explicit", "Tg", "LC-MS/MS")
      WHEN 'thyroglobulin by lcms' THEN STRUCT("explicit", "Tg", "LC-MS/MS")
      WHEN 'thyroglobulin by reflex lc-ms/ms or cia' THEN STRUCT("explicit", "Tg", "LC-MS/MS")
      WHEN 'thyroglobulin by ria' THEN STRUCT("explicit", "Tg", "RIA")
      WHEN 'thyroglobulin, ria' THEN STRUCT("explicit", "Tg", "RIA")
      WHEN 'thyroglobulinantibody' THEN STRUCT("explicit", "TgAb", "immunoassay")
      WHEN 'thyroglobulin antibody' THEN STRUCT("explicit", "TgAb", "immunoassay")
      WHEN 'antithyroglobulin' THEN STRUCT("explicit", "TgAb", "immunoassay")
      WHEN 'antithyroglobulinantibody' THEN STRUCT("explicit", "TgAb", "immunoassay")
      WHEN 'anti thyroglobulin antibody' THEN STRUCT("explicit", "TgAb", "immunoassay")
      WHEN 'antithyroglobulinigg' THEN STRUCT("explicit", "TgAb", "IgG")
      WHEN 'thyroglobulin antibody and thyroglobulin, ima or lc/ms-ms' THEN STRUCT("explicit", "TgAb", "reflex")
      WHEN 'thyroglobulin and thyroglobulin antibody' THEN STRUCT("combo_panel_placeholder" AS analyte_assignment_method, CAST(NULL AS STRING) AS analyte, CAST(NULL AS STRING) AS assay_method)
      WHEN 'thyroid peroxidase and thyroglobulin antibodies' THEN STRUCT("combo_panel_placeholder" AS analyte_assignment_method, CAST(NULL AS STRING) AS analyte, CAST(NULL AS STRING) AS assay_method)
      ELSE STRUCT(
        "explicit_infer_name_token_fallback" AS analyte_assignment_method,
        CASE
          WHEN REGEXP_CONTAINS(
            tn_key,
            r'(antithyro|^anti thyroglobulin|thyroglobulin antibody|thyroglobulinantibody)'
          ) THEN CAST("TgAb" AS STRING)
          WHEN REGEXP_CONTAINS(tn_key, r'thyroglobulin') THEN CAST("Tg" AS STRING)
          ELSE CAST(NULL AS STRING)
        END AS analyte,
        "immunoassay" AS assay_method
      )
    END AS slot
  FROM tagged
),

rows_marked AS (
  SELECT
    *,
    tn_key IN (
      'thyroglobulin and thyroglobulin antibody',
      'thyroid peroxidase and thyroglobulin antibodies'
    ) AS is_combo_panel,
    IF(
      tn_key NOT IN (
        'thyroglobulin and thyroglobulin antibody',
        'thyroid peroxidase and thyroglobulin antibodies'
      ),
      slot.analyte,
      CAST(NULL AS STRING)
    ) AS explicit_analyte
  FROM classified
),

explicit_literals AS (
  SELECT
    research_id,
    COALESCE(
      ARRAY_AGG(DISTINCT CASE WHEN explicit_analyte = 'Tg' THEN TRIM(value_raw) END),
      ARRAY<STRING>[]
    ) AS tg_lits_r,
    COALESCE(
      ARRAY_AGG(DISTINCT CASE WHEN explicit_analyte = 'TgAb' THEN TRIM(value_raw) END),
      ARRAY<STRING>[]
    ) AS tgab_lits_r
  FROM rows_marked
  WHERE explicit_analyte IS NOT NULL
  GROUP BY 1
),

combo_numbered AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY research_id, lab_datetime, tn_key ORDER BY TRIM(value_raw) ASC, value_raw ASC
    ) AS combo_rn,
    COUNT(*) OVER (PARTITION BY research_id, lab_datetime, tn_key) AS combo_cnt
  FROM rows_marked
  WHERE is_combo_panel
),

combo_pairs AS (
  SELECT
    research_id,
    lab_datetime,
    test_name AS source_test_name,
    tn_key,
    MAX(IF(combo_rn = 1, value_raw, NULL)) AS raw_row1,
    MAX(IF(combo_rn = 2, value_raw, NULL)) AS raw_row2,
    MAX(combo_cnt) AS combo_cnt
  FROM combo_numbered
  GROUP BY research_id, lab_datetime, test_name, tn_key
),

paired_logic AS (
  SELECT
    p.*,
    IFNULL(el.tg_lits_r, ARRAY<STRING>[]) AS tg_lits_r,
    IFNULL(el.tgab_lits_r, ARRAY<STRING>[]) AS tgab_lits_r,
    thy_combo_heuristic_pair(IFNULL(TRIM(raw_row1), ""), IFNULL(TRIM(raw_row2), "")) AS heur
  FROM combo_pairs p
    LEFT JOIN explicit_literals el USING (research_id)
),

paired_hp AS (
  SELECT
    *,
    IF(
      combo_cnt = 2
        AND heur.tg IS NOT NULL
        AND heur.tgab IS NOT NULL,
      STRUCT(
        heur.tg AS tg_v,
        heur.tgab AS tgab_v,
        heur.assignment_method AS meth
      ),
      CAST(NULL AS STRUCT<tg_v STRING, tgab_v STRING, meth STRING>)
    ) AS hp
  FROM paired_logic
),

cross_layer AS (
  SELECT
    pr.* EXCEPT(heur),
    COALESCE(
      pr.hp,
      IF(
        pr.combo_cnt = 2 AND pr.raw_row1 IS NOT NULL AND pr.raw_row2 IS NOT NULL
          AND ARRAY_LENGTH(pr.tg_lits_r) > 0
          AND ARRAY_LENGTH(pr.tgab_lits_r) > 0
          AND EXISTS (SELECT 1 FROM UNNEST(pr.tg_lits_r) lit WHERE lit = TRIM(pr.raw_row1))
          AND EXISTS (SELECT 1 FROM UNNEST(pr.tgab_lits_r) lit WHERE lit = TRIM(pr.raw_row2))
          AND NOT EXISTS (SELECT 1 FROM UNNEST(pr.tgab_lits_r) lit WHERE lit = TRIM(pr.raw_row1))
          AND NOT EXISTS (SELECT 1 FROM UNNEST(pr.tg_lits_r) lit WHERE lit = TRIM(pr.raw_row2)),
        STRUCT(
          TRIM(pr.raw_row1) AS tg_v,
          TRIM(pr.raw_row2) AS tgab_v,
          "inferred_combo_crossref_patient_literals" AS meth
        ),
        CAST(NULL AS STRUCT<tg_v STRING, tgab_v STRING, meth STRING>)
      ),
      IF(
        pr.combo_cnt = 2 AND pr.raw_row1 IS NOT NULL AND pr.raw_row2 IS NOT NULL
          AND ARRAY_LENGTH(pr.tg_lits_r) > 0
          AND ARRAY_LENGTH(pr.tgab_lits_r) > 0
          AND EXISTS (SELECT 1 FROM UNNEST(pr.tg_lits_r) lit WHERE lit = TRIM(pr.raw_row2))
          AND EXISTS (SELECT 1 FROM UNNEST(pr.tgab_lits_r) lit WHERE lit = TRIM(pr.raw_row1))
          AND NOT EXISTS (SELECT 1 FROM UNNEST(pr.tgab_lits_r) lit WHERE lit = TRIM(pr.raw_row2))
          AND NOT EXISTS (SELECT 1 FROM UNNEST(pr.tg_lits_r) lit WHERE lit = TRIM(pr.raw_row1)),
        STRUCT(
          TRIM(pr.raw_row2) AS tg_v,
          TRIM(pr.raw_row1) AS tgab_v,
          "inferred_combo_crossref_patient_literals" AS meth
        ),
        CAST(NULL AS STRUCT<tg_v STRING, tgab_v STRING, meth STRING>)
      ),
      IF(
        pr.combo_cnt = 2 AND pr.raw_row1 IS NOT NULL AND pr.raw_row2 IS NOT NULL,
        STRUCT(
          IF(
            TRIM(pr.raw_row2) >= TRIM(pr.raw_row1),
            TRIM(pr.raw_row2),
            TRIM(pr.raw_row1)
          ) AS tg_v,
          IF(
            TRIM(pr.raw_row2) >= TRIM(pr.raw_row1),
            TRIM(pr.raw_row1),
            TRIM(pr.raw_row2)
          ) AS tgab_v,
          "inferred_combo_pair_fallback_row_order" AS meth
        ),
        CAST(NULL AS STRUCT<tg_v STRING, tgab_v STRING, meth STRING>)
      )
    ) AS resolved
  FROM paired_hp pr
),

combo_two AS (
  SELECT
    research_id,
    lab_datetime,
    source_test_name,
    "Tg" AS analyte,
    "immunoassay" AS assay_method,
    resolved.tg_v AS value_raw,
    resolved.meth AS analyte_assignment_method
  FROM cross_layer
  WHERE combo_cnt = 2 AND resolved.tg_v IS NOT NULL
  UNION ALL
  SELECT
    research_id,
    lab_datetime,
    source_test_name,
    "TgAb",
    "immunoassay",
    resolved.tgab_v,
    resolved.meth
  FROM cross_layer
  WHERE combo_cnt = 2 AND resolved.tgab_v IS NOT NULL
),

combo_one AS (
  SELECT
    research_id,
    lab_datetime,
    source_test_name,
    singleton.analyte,
    "immunoassay" AS assay_method,
    TRIM(IFNULL(cp.raw_row1, cp.raw_row2)) AS value_raw,
    singleton.assignment_method AS analyte_assignment_method
  FROM combo_pairs cp,
    UNNEST([thy_singleton_combo_analyte(TRIM(IFNULL(cp.raw_row1, cp.raw_row2)))]) AS singleton
  WHERE combo_cnt = 1
),

combo_multi AS (
  SELECT
    cn.research_id,
    cn.lab_datetime,
    cn.test_name AS source_test_name,
    s.analyte,
    "immunoassay" AS assay_method,
    cn.value_raw,
    CAST("inferred_combo_multirow_singleton" AS STRING) AS analyte_assignment_method
  FROM combo_numbered cn,
    UNNEST([thy_singleton_combo_analyte(TRIM(cn.value_raw))]) AS s
  WHERE combo_cnt > 2
),

combo_exploded AS (
  SELECT * FROM combo_two
  UNION ALL
  SELECT * FROM combo_one WHERE analyte IS NOT NULL AND value_raw IS NOT NULL AND value_raw != ""
  UNION ALL
  SELECT * FROM combo_multi WHERE analyte IS NOT NULL
),

explicit_rows AS (
  SELECT
    r.research_id,
    r.lab_datetime,
    r.test_name AS source_test_name,
    r.explicit_analyte AS analyte,
    r.slot.assay_method AS assay_method,
    r.value_raw,
    r.slot.analyte_assignment_method AS analyte_assignment_method
  FROM rows_marked r
  WHERE r.explicit_analyte IS NOT NULL
),

unioned AS (
  SELECT * FROM explicit_rows
  UNION ALL
  SELECT * FROM combo_exploded
),

normalized_once AS (
  SELECT
    u.*,
    thy_normalize_lab_value(
      u.value_raw,
      IF(u.analyte = "Tg", "thyroglobulin", "anti_thyroglobulin")
    ) AS norm
  FROM unioned u
  WHERE u.analyte IS NOT NULL
),

with_cohort AS (
  SELECT
    n.research_id,
    n.analyte,
    n.assay_method,
    n.lab_datetime,
    n.value_raw,
    n.norm.value_numeric AS value_numeric,
    n.norm.is_censored AS is_censored,
    n.norm.value_correction_note AS value_correction_note,
    IF(n.analyte = "Tg", "ng/mL", "IU/mL") AS unit_standardized,
    CAST("analyst_ehr_tg" AS STRING) AS source,
    n.analyte_assignment_method AS analyte_assignment_method,
    IF(cp.research_id IS NOT NULL, TRUE, FALSE) AS is_in_canonical_cancer_cohort,
    run_ingestion_ts AS ingestion_date
  FROM normalized_once n
    LEFT JOIN `thyroid-canonical-pub-2026.pub_canonical.canonical_patient_master` cp
      ON SAFE_CAST(cp.research_id AS INT64) = n.research_id
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        research_id,
        analyte,
        lab_datetime,
        COALESCE(CAST(value_numeric AS STRING), value_raw)
      ORDER BY ingestion_date DESC, source ASC, value_raw ASC, analyte_assignment_method ASC
    ) AS drn
  FROM with_cohort
)

SELECT
  research_id,
  analyte,
  assay_method,
  lab_datetime,
  value_raw,
  value_numeric,
  is_censored,
  value_correction_note,
  unit_standardized,
  source,
  is_in_canonical_cancer_cohort,
  ingestion_date,
  analyte_assignment_method
FROM ranked
WHERE drn = 1;

CREATE OR REPLACE VIEW `thyroid-canonical-pub-2026.pub_canonical.thyroglobulin_lab_VIEW_v1`
AS SELECT
  research_id,
  analyte,
  assay_method,
  lab_datetime AS specimen_collect_dt,
  value_raw AS result_raw,
  value_numeric AS result_numeric,
  is_censored,
  value_correction_note,
  unit_standardized,
  source AS ingestion_script,
  is_in_canonical_cancer_cohort,
  ingestion_date,
  analyte_assignment_method
FROM `thyroid-canonical-pub-2026.pub_canonical.canonical_labs_thyroglobulin_v1`;

END;
