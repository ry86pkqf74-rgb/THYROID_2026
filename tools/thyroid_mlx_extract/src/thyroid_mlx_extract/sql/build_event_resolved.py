"""Phase 5: build canonical_<feature>_event_resolved_v1 tables.

Mirrors the column shape of canonical_ete_event_resolved_v1. Given a feature
name and the candidate source tables/columns, emits a CREATE OR REPLACE TABLE
in pub_workspace that fuses sources, computes a resolved value per (research_id,
tumor_ordinal), and surfaces disagreement flags.

Feature definitions live in FEATURES below. To add a new feature:
1. Add an entry to FEATURES
2. Define how each source contributes its candidate value (synoptic column,
   LLM result_json path, regex extract, etc.)
3. Define the resolution priority (which source wins on disagreement)
"""
from __future__ import annotations

from dataclasses import dataclass

from google.cloud import bigquery

from ..config import BQ_CANONICAL, BQ_WORKSPACE


@dataclass(frozen=True)
class FeatureSpec:
    """How to build canonical_<key>_event_resolved_v1."""
    key: str                              # e.g. 'capsular_invasion'
    description: str
    synoptic_column: str                  # path_synoptics.<col>
    llm_table: str                        # note_entities_llm_*.* (result_json.<path>)
    llm_json_path: str                    # e.g. "$.tumors[0].capsular_invasion"
    resolution_priority: tuple[str, ...]  # ordered preference, e.g. ('synoptic', 'llm', 'pm')


FEATURES: dict[str, FeatureSpec] = {
    "capsular_invasion": FeatureSpec(
        key="capsular_invasion",
        description="Capsular invasion status per tumor (WHO 2022 + Turin criteria)",
        synoptic_column="tumor_1_capsular_invasion",
        llm_table="note_entities_llm_synoptic_pathology_enrichment",
        llm_json_path="$.tumors[0].capsular_invasion",
        resolution_priority=("synoptic", "llm"),
    ),
    "perineural_invasion": FeatureSpec(
        key="perineural_invasion",
        description="Perineural invasion present/absent per tumor",
        synoptic_column="tumor_1_perineural_invasion",
        llm_table="note_entities_llm_synoptic_pathology_enrichment",
        llm_json_path="$.tumors[0].perineural_invasion",
        resolution_priority=("synoptic", "llm"),
    ),
    "angioinvasion": FeatureSpec(
        key="angioinvasion",
        description="Angioinvasion / vascular invasion per tumor, with vessel count",
        synoptic_column="tumor_1_angioinvasion",
        llm_table="note_entities_llm_vascular_invasion_v2",
        llm_json_path="$.angioinvasion_present",
        resolution_priority=("synoptic", "llm"),
    ),
    "extranodal_extension": FeatureSpec(
        key="extranodal_extension",
        description="Extranodal extension when LN positive",
        synoptic_column="tumor_1_extranodal_extension",
        llm_table="note_entities_llm_synoptic_pathology_enrichment",
        llm_json_path="$.tumors[0].extranodal_extension_present",
        resolution_priority=("synoptic", "llm"),
    ),
}


def build_sql(feature: FeatureSpec) -> str:
    """Emit the CREATE OR REPLACE TABLE statement for this feature."""
    target = f"{BQ_WORKSPACE}.canonical_{feature.key}_event_resolved_v1"
    return f"""
-- Build canonical_{feature.key}_event_resolved_v1
-- Source priority: {' > '.join(feature.resolution_priority)}
CREATE OR REPLACE TABLE `{target}`
AS
WITH synoptic_candidate AS (
  SELECT
    research_id,
    1 AS tumor_ordinal,
    surg_date,
    {feature.synoptic_column} AS synoptic_value,
    'synoptic' AS synoptic_source_label
  FROM `{BQ_CANONICAL}.path_synoptics`
  WHERE {feature.synoptic_column} IS NOT NULL
),
llm_candidate AS (
  SELECT
    research_id,
    1 AS tumor_ordinal,
    note_date AS surg_date,
    JSON_VALUE(result_json, '{feature.llm_json_path}') AS llm_value,
    'llm' AS llm_source_label
  FROM `{BQ_CANONICAL}.{feature.llm_table}`
  WHERE result_json IS NOT NULL
    AND JSON_VALUE(result_json, '{feature.llm_json_path}') IS NOT NULL
),
joined AS (
  SELECT
    COALESCE(s.research_id, l.research_id) AS research_id,
    COALESCE(s.tumor_ordinal, l.tumor_ordinal) AS tumor_ordinal,
    COALESCE(s.surg_date, l.surg_date) AS event_date,
    s.synoptic_value,
    l.llm_value,
    -- Resolution priority: {' > '.join(feature.resolution_priority)}
    COALESCE(s.synoptic_value, l.llm_value) AS resolved_value,
    CASE
      WHEN s.synoptic_value IS NOT NULL THEN 'synoptic'
      WHEN l.llm_value IS NOT NULL THEN 'llm'
      ELSE 'none'
    END AS resolved_source,
    -- Disagreement: both present and different (case-insensitive trim)
    CASE
      WHEN s.synoptic_value IS NOT NULL
       AND l.llm_value IS NOT NULL
       AND LOWER(TRIM(s.synoptic_value)) != LOWER(TRIM(l.llm_value))
      THEN TRUE ELSE FALSE
    END AS pm_disagreement_flag,
    CASE
      WHEN s.synoptic_value IS NULL AND l.llm_value IS NULL THEN TRUE ELSE FALSE
    END AS is_unresolved
  FROM synoptic_candidate s
  FULL OUTER JOIN llm_candidate l
    USING (research_id, tumor_ordinal)
)
SELECT
  research_id,
  tumor_ordinal,
  event_date,
  resolved_value,
  resolved_source,
  synoptic_value,
  llm_value,
  pm_disagreement_flag,
  is_unresolved,
  CURRENT_TIMESTAMP() AS built_ts,
  '{feature.key}_event_resolved_v1' AS source_table_name
FROM joined
WHERE NOT is_unresolved
"""


def run(feature_key: str, *, project: str | None = None) -> str:
    """Execute the event-resolved build for a feature. Returns the target table."""
    if feature_key not in FEATURES:
        raise KeyError(f"Unknown feature '{feature_key}'. Choose: {sorted(FEATURES)}")
    feature = FEATURES[feature_key]
    sql = build_sql(feature)
    client = bigquery.Client(project=project)
    client.query(sql).result()
    return f"{BQ_WORKSPACE}.canonical_{feature_key}_event_resolved_v1"


def main():
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("feature", choices=sorted(FEATURES) + ["all"])
    p.add_argument("--print-sql", action="store_true", help="Print SQL only, don't run")
    args = p.parse_args()

    keys = list(FEATURES) if args.feature == "all" else [args.feature]
    for k in keys:
        feature = FEATURES[k]
        if args.print_sql:
            print(f"-- ===== {k} =====")
            print(build_sql(feature))
        else:
            target = run(k)
            print(f"Built: {target}")


if __name__ == "__main__":
    main()
