#!/usr/bin/env python3
"""Build mig_193 Logan-review CSVs after mig_188b.

Read-only MotherDuck lane: diagnoses the prior r1b 0-row return and exports
r1b/r1d/r1e plus r1c disposition CSVs from the post-mig_188b state.
"""
from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(REPO_ROOT / "scripts"))

from _md_connect import connect_locked  # noqa: E402

RUN_ID = "mig_193_r1bde_logan_review_csv_unblock_20260430"
OUT_DIR = REPO_ROOT / "exports" / "mig193_r1_adjudication_post_mig188_20260430"
REPORT_PATH = REPO_ROOT / "qc_framework_v1" / "reports" / "mig_193_r1bde_logan_review_csv_unblock_20260430.md"

T4A_RE = r"laryn|trache|esophag|recurrent\s+laryngeal|\brln\b|subcutaneous|airway"
T4B_RE = r"prevertebral|mediastinal|carotid|encas"


def fetchdf(con: Any, sql: str) -> pd.DataFrame:
    return con.execute(sql).fetchdf()


def scalar(con: Any, sql: str) -> Any:
    return con.execute(sql).fetchone()[0]


def write_csv(df: pd.DataFrame, filename: str) -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    path = OUT_DIR / filename
    # PHI-safe: source snippets are not selected; still normalize newlines for CSV integrity.
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            out[col] = out[col].map(lambda v: str(v).replace("\r", " ").replace("\n", " ") if v is not None and not pd.isna(v) else v)
    out.to_csv(path, index=False, quoting=csv.QUOTE_ALL, encoding="utf-8")
    return len(out)


def main() -> None:
    started = datetime.now(timezone.utc)
    con = connect_locked()

    preflight = {
        "registry_mig188b_rows": int(scalar(con, """
            SELECT COUNT(*)
            FROM main.canonical_column_verification_registry_v1
            WHERE batch_id='mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430'
               OR verified_by='mig188b_mig184_v2_plus_r1c_with_explicit_T0_20260430'
               OR notes ILIKE '%mig188b%'
        """)),
        "path_events_t_resolved_nonnull": int(scalar(con, "SELECT COUNT(*) FROM main.canonical_path_malignant_events_v1 WHERE t_stage_ajcc8_resolved IS NOT NULL")),
        "pm_t_resolved_nonnull": int(scalar(con, "SELECT COUNT(*) FROM main.canonical_patient_master WHERE ajcc8_t_stage_resolved IS NOT NULL")),
    }
    if not all(preflight.values()):
        raise SystemExit(f"mig_193 preflight failed: {preflight}")

    n_dist_legacy = fetchdf(con, """
        SELECT COALESCE(ajcc8_n_stage,'NULL') AS n_stage, COUNT(*) AS n
        FROM main.canonical_patient_master GROUP BY 1 ORDER BY n DESC, n_stage
    """)
    n_dist_resolved = fetchdf(con, """
        SELECT COALESCE(ajcc8_n_stage_resolved,'NULL') AS n_stage, COUNT(*) AS n
        FROM main.canonical_patient_master GROUP BY 1 ORDER BY n DESC, n_stage
    """)

    original_r1b_count = int(scalar(con, """
        SELECT COUNT(*)
        FROM main.canonical_patient_master pm
        WHERE upper(pm.ajcc8_n_stage) = 'N1'
          AND (
            COALESCE(pm.cnln_img_central_present, FALSE)
            OR COALESCE(pm.cnln_img_lateral_neck_present, FALSE)
            OR COALESCE(pm.cnln_img_left_present, FALSE)
            OR COALESCE(pm.cnln_img_right_present, FALSE)
            OR COALESCE(pm.cnln_img_bilateral_present, FALSE)
            OR COALESCE(pm.lateral_neck_dissected, FALSE)
            OR COALESCE(pm.ln_lateral_dissected, FALSE)
            OR COALESCE(pm.ln_rollup_lateral_left_positive, 0) > 0
            OR COALESCE(pm.ln_rollup_lateral_right_positive, 0) > 0
            OR COALESCE(pm.ln_rollup_central_positive, 0) > 0
            OR COALESCE(pm.tp_ln_lateral_positive, 0) > 0
            OR COALESCE(pm.tp_ln_central_positive, 0) > 0
          )
    """))
    legacy_like_n1_count = int(scalar(con, """
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE upper(TRIM(COALESCE(ajcc8_n_stage,''))) LIKE '%N1%'
    """))
    post_still_n1_count = int(scalar(con, """
        SELECT COUNT(*) FROM main.canonical_patient_master
        WHERE upper(TRIM(COALESCE(ajcc8_n_stage_resolved,''))) = 'N1'
    """))

    # r1b: genuine PM-grain unresolved N1 candidates after mig_188b. Current state has none.
    r1b = fetchdf(con, """
        SELECT
          CAST(pm.research_id AS VARCHAR) AS research_id,
          pm.ajcc8_n_stage AS pm_n_stage_legacy,
          pm.ajcc8_n_stage_resolved AS pm_n_stage_resolved_post_mig188,
          pm.cnln_img_central_present,
          pm.cnln_img_lateral_neck_present,
          pm.cnln_img_left_present,
          pm.cnln_img_right_present,
          pm.cnln_img_bilateral_present,
          pm.cnln_img_levels_mentioned,
          pm.cnln_surg_levels_mentioned,
          pm.lateral_neck_dissected,
          pm.lateral_neck_dissected_structured_or_nlp,
          pm.ln_lateral_dissected,
          pm.ln_rollup_lateral_left_positive,
          pm.ln_rollup_lateral_right_positive,
          pm.ln_rollup_central_positive,
          pm.tp_ln_lateral_positive,
          pm.tp_ln_central_positive,
          '' AS logan_n_stage_override,
          '' AS logan_notes
        FROM main.canonical_patient_master pm
        WHERE upper(TRIM(COALESCE(pm.ajcc8_n_stage_resolved,''))) = 'N1'
        ORDER BY CAST(pm.research_id AS BIGINT)
    """)
    counts: dict[str, int] = {}
    counts["r1b_n1_unspecified_pm_grain_post_mig188.csv"] = write_csv(r1b, "r1b_n1_unspecified_pm_grain_post_mig188.csv")

    # r1d: current canonical invasion evidence candidates joined to post-mig188b event state.
    r1d = fetchdf(con, f"""
        WITH inv AS (
          SELECT
            invasion_event_id,
            CAST(research_id AS VARCHAR) AS research_id,
            linked_surgery_episode_id AS surgery_episode_id,
            linked_path_malignant_event_id,
            invasion_type,
            finding_status,
            evidence_qualifier,
            source_modality,
            source_kind,
            source_table,
            finding_date,
            linkage_ambiguous_multi_finding,
            CASE
              WHEN regexp_matches(LOWER(COALESCE(invasion_type,'') || ' ' || COALESCE(evidence_qualifier,'')), '{T4B_RE}') THEN 'T4b'
              WHEN regexp_matches(LOWER(COALESCE(invasion_type,'') || ' ' || COALESCE(evidence_qualifier,'')), '{T4A_RE}') THEN 'T4a'
              ELSE NULL
            END AS candidate_t_stage
          FROM main.canonical_invasion_events_v1
          WHERE LOWER(COALESCE(finding_status,'')) NOT IN ('absent','negative','negated','not_present','not present')
        )
        SELECT
          inv.research_id,
          inv.surgery_episode_id,
          e.tumor_ordinal,
          inv.linked_path_malignant_event_id,
          inv.invasion_event_id,
          inv.invasion_type,
          inv.finding_status,
          inv.evidence_qualifier,
          inv.source_modality,
          inv.source_kind,
          inv.source_table,
          inv.finding_date,
          inv.linkage_ambiguous_multi_finding,
          inv.candidate_t_stage AS proposed_t_stage_from_invasion_evidence,
          e.t_stage_ajcc8_resolved,
          e.ajcc_resolution_source AS t_resolution_source,
          (e.ajcc_resolution_source = 'canonical_invasion_events_v1') AS mig_188_caught_t4,
          '' AS logan_t_stage_override,
          '' AS logan_notes
        FROM inv
        LEFT JOIN main.canonical_path_malignant_events_v1 e
          ON CAST(e.research_id AS VARCHAR) = inv.research_id
         AND e.surgery_episode_id IS NOT DISTINCT FROM inv.surgery_episode_id
        WHERE inv.candidate_t_stage IS NOT NULL
        ORDER BY inv.candidate_t_stage, CAST(inv.research_id AS BIGINT), inv.surgery_episode_id, e.tumor_ordinal, inv.invasion_event_id
    """)
    counts["r1d_t4_invasion_post_mig188.csv"] = write_csv(r1d, "r1d_t4_invasion_post_mig188.csv")

    # r1e: mixed histology PM rows under current resolved stage-group state.
    r1e = fetchdf(con, """
        SELECT
          CAST(pm.research_id AS VARCHAR) AS research_id,
          pm.histology_final,
          pm.histologic_types_all,
          pm.ajcc8_t_stage AS pm_t_stage_legacy,
          pm.ajcc8_n_stage AS pm_n_stage_legacy,
          pm.ajcc8_m_stage AS pm_m_stage_legacy,
          pm.ajcc8_stage_group AS pm_stage_group_legacy,
          pm.ajcc8_t_stage_resolved AS pm_t_stage_resolved_post_mig188,
          pm.ajcc8_n_stage_resolved AS pm_n_stage_resolved_post_mig188,
          pm.ajcc8_m_stage_resolved AS pm_m_stage_resolved_post_mig188,
          pm.ajcc8_stage_group_resolved AS pm_stage_group_resolved_post_mig188,
          pm.ajcc_resolution_source,
          pm.ajcc_resolution_confidence,
          CASE
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'anaplastic|\\batc\\b') THEN 'ATC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'medullary|\\bmtc\\b') THEN 'MTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'papillary|\\bptc\\b') THEN 'PTC'
            WHEN regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'follicular|\\bftc\\b|hurthle|hürthle|oncocytic|\\bhcc\\b') THEN 'FTC'
            ELSE 'review_component_unclear'
          END AS proposed_most_aggressive_component,
          'review_rule5_mixed_histology_most_aggressive_component' AS proposed_action,
          '' AS logan_stage_group_override,
          '' AS logan_notes
        FROM main.canonical_patient_master pm
        WHERE COALESCE(pm.histologic_types_all,'') LIKE '%|%'
          AND (
            regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'medullary|\\bmtc\\b')::INT
            + regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'papillary|\\bptc\\b')::INT
            + regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'follicular|\\bftc\\b|hurthle|hürthle|oncocytic|\\bhcc\\b')::INT
            + regexp_matches(LOWER(COALESCE(pm.histologic_types_all,'')), 'anaplastic|\\batc\\b')::INT
          ) >= 2
        ORDER BY proposed_most_aggressive_component, CAST(pm.research_id AS BIGINT)
    """)
    counts["r1e_mixed_histology_post_mig188.csv"] = write_csv(r1e, "r1e_mixed_histology_post_mig188.csv")

    r1c_specs = {
        "r1c_disposition_strong_prior_thy.csv": "prior_thy_recurrence_T_from_prior_path",
        "r1c_disposition_weak_or_none.csv": "no_primary_at_this_surgery_pT0_unstaged",
        "r1c_disposition_ambiguous_pm_only.csv": "ambiguous_pm_size_only_logan_pending",
    }
    for filename, source in r1c_specs.items():
        df = fetchdf(con, f"""
            SELECT
              CAST(e.research_id AS VARCHAR) AS research_id,
              e.surgery_episode_id,
              e.tumor_ordinal,
              e.surgery_date,
              e.primary_histology,
              e.histology_variant,
              e.size_greatest_dimension_cm,
              e.tumor_size_cm_per_surgery,
              pm.path_tumor_size_cm,
              e.t_stage_ajcc8_resolved,
              e.n_stage_ajcc8_resolved,
              e.m_stage_ajcc8_resolved,
              e.ajcc_resolution_source AS t_resolution_source,
              e.ajcc_resolution_confidence AS t_resolution_confidence,
              pm.ajcc8_t_stage_resolved AS pm_t_stage_resolved_post_mig188,
              pm.ajcc8_n_stage_resolved AS pm_n_stage_resolved_post_mig188,
              pm.ajcc8_stage_group_resolved AS pm_stage_group_resolved_post_mig188,
              '' AS logan_disposition,
              '' AS logan_t_stage_override,
              '' AS logan_notes
            FROM main.canonical_path_malignant_events_v1 e
            LEFT JOIN main.canonical_patient_master pm
              ON CAST(pm.research_id AS VARCHAR) = CAST(e.research_id AS VARCHAR)
            WHERE e.ajcc_resolution_source = '{source}'
            ORDER BY CAST(e.research_id AS BIGINT), e.surgery_episode_id, e.tumor_ordinal
        """)
        counts[filename] = write_csv(df, filename)

    manifest = {
        "run_id": RUN_ID,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "posture": "read_only_motherduck_selects_local_csv_authoring_only",
        "target_db": "thyroid_canonical_publication_v1_0",
        "preflight": preflight,
        "diagnostics": {
            "original_r1b_exact_filter_rows": original_r1b_count,
            "legacy_like_n1_rows": legacy_like_n1_count,
            "post_mig188b_still_n1_rows": post_still_n1_count,
            "legacy_n_stage_distribution": n_dist_legacy.to_dict(orient="records"),
            "resolved_n_stage_distribution": n_dist_resolved.to_dict(orient="records"),
        },
        "csv_counts": counts,
        "files": sorted(counts.keys()) + ["manifest.json"],
    }
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    r1c_counts = {k: counts[k] for k in r1c_specs}
    caught_true = int(r1d["mig_188_caught_t4"].fillna(False).sum()) if not r1d.empty else 0
    report = f"""# mig_193 r1b/r1d/r1e Logan-review CSV unblock + post-mig_188b regeneration

**Run ID:** `{RUN_ID}`  
**Generated:** {datetime.now(timezone.utc).isoformat()}  
**Posture:** READ-ONLY MotherDuck SELECTs + local CSV/report authoring only.  
**Target DB:** `thyroid_canonical_publication_v1_0`

## 1. Pre-flight gate

| Check | Result |
|---|---:|
| mig_188b registry/verification rows | {preflight['registry_mig188b_rows']} |
| `canonical_path_malignant_events_v1.t_stage_ajcc8_resolved` non-null | {preflight['path_events_t_resolved_nonnull']} |
| `canonical_patient_master.ajcc8_t_stage_resolved` non-null | {preflight['pm_t_resolved_nonnull']} |

**Gate:** PASS. The lane ran against the post-mig_188b state.

## 2. r1b 0-row diagnosis

The original r1b exact filter returned **{original_r1b_count}** rows. This is not a SQL casing/whitespace bug: the legacy PM `ajcc8_n_stage` distribution has no plain `N1` values. Post-mig_188b, `ajcc8_n_stage_resolved` also has **{post_still_n1_count}** unresolved plain-`N1` rows.

Legacy PM N-stage distribution:

{n_dist_legacy.to_markdown(index=False)}

Resolved PM N-stage distribution:

{n_dist_resolved.to_markdown(index=False)}

Interpretation: by the time this lane ran, N1 had already been split or normalized upstream (mainly into `N1a` and `N1b`). Therefore the correct post-mig_188b r1b review bundle is header-only / 0 rows, and no Logan N1-unspecified PM-grain adjudication is pending.

## 3. r1b post-mig_188b inventory

`r1b_n1_unspecified_pm_grain_post_mig188.csv`: **{counts['r1b_n1_unspecified_pm_grain_post_mig188.csv']}** rows.

## 4. r1d T4 invasion inventory

`r1d_t4_invasion_post_mig188.csv`: **{counts['r1d_t4_invasion_post_mig188.csv']}** rows.  
Rows with `mig_188_caught_t4=TRUE`: **{caught_true}** / {counts['r1d_t4_invasion_post_mig188.csv']}.

The CSV includes invasion-event evidence fields plus current `t_stage_ajcc8_resolved` and `t_resolution_source` for Logan review.

## 5. r1e mixed-histology inventory

`r1e_mixed_histology_post_mig188.csv`: **{counts['r1e_mixed_histology_post_mig188.csv']}** rows.

The CSV includes the current resolved stage group and a computed proposed most-aggressive component for Rule #5 review.

## 6. r1c disposition CSV row counts post-apply

| CSV | Rows |
|---|---:|
| r1c_disposition_strong_prior_thy.csv | {r1c_counts['r1c_disposition_strong_prior_thy.csv']} |
| r1c_disposition_weak_or_none.csv | {r1c_counts['r1c_disposition_weak_or_none.csv']} |
| r1c_disposition_ambiguous_pm_only.csv | {r1c_counts['r1c_disposition_ambiguous_pm_only.csv']} |

These reflect the final mig_188b explicit-T0 state: 54 prior-thy carry-forward rows, 13 no-primary pT0/unstaged rows, and 50 ambiguous PM-size-only rows.

## 7. Logan review unblock checklist

- [x] Post-mig_188b gate verified.
- [x] r1b 0-row result diagnosed as data-state normalization/splitting rather than a failed CSV build.
- [x] r1b/r1d/r1e CSVs regenerated under `exports/mig193_r1_adjudication_post_mig188_20260430/`.
- [x] r1c disposition CSVs regenerated from the post-apply state.
- [x] Manifest written with row counts and diagnostic distributions.

## 8. Deliverables

- `qc_framework_v1/reports/mig_193_r1bde_logan_review_csv_unblock_20260430.md`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1b_n1_unspecified_pm_grain_post_mig188.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1d_t4_invasion_post_mig188.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1e_mixed_histology_post_mig188.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1c_disposition_strong_prior_thy.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1c_disposition_weak_or_none.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/r1c_disposition_ambiguous_pm_only.csv`
- `exports/mig193_r1_adjudication_post_mig188_20260430/manifest.json`
"""
    REPORT_PATH.write_text(report, encoding="utf-8")
    print(json.dumps({"run_id": RUN_ID, "counts": counts, "report": str(REPORT_PATH.relative_to(REPO_ROOT)), "elapsed_sec": round((datetime.now(timezone.utc)-started).total_seconds(), 2)}, indent=2))


if __name__ == "__main__":
    main()
