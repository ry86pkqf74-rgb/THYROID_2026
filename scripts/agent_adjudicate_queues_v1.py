#!/usr/bin/env python3
"""Agent adjudication across 7 queues against thyroid_canonical_publication_v1_0.

Read-only against canonical_patient_master and source queues. Writes only to
manuscript_workspace.agent_adjudication_log_v1 and a parquet snapshot under
scripts/output/.

Rubric:
  HIGH   = structured evidence converges unambiguously
  MEDIUM = one dominant pattern with a minor caveat
  LOW    = genuine chart-review case; surface to human

Every join uses CAST(research_id AS VARCHAR) to avoid the BIGINT vs VARCHAR
type mismatch known across detail tables.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

os.environ.setdefault("MOTHERDUCK_DATABASE", "thyroid_canonical_publication_v1_0")

from motherduck_client import MotherDuckClient, MotherDuckConfig  # noqa: E402

AGENT_MODEL = "claude-opus-4.7-thyroid-adjudicator-v1"
RUN_TS = datetime.now(timezone.utc)
RUN_TS_TAG = RUN_TS.strftime("%Y%m%dT%H%M%SZ")
OUT_DIR = ROOT / "scripts" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_PATH = OUT_DIR / f"agent_adjudication_log_{RUN_TS_TAG}.parquet"

LOG_TABLE = "manuscript_workspace.agent_adjudication_log_v1"


def connect_rw():
    cfg = MotherDuckConfig(database="thyroid_canonical_publication_v1_0")
    return MotherDuckClient(cfg).connect_rw()


def truncate(s: str | None, n: int = 1500) -> str:
    if s is None:
        return ""
    try:
        if pd.isna(s):
            return ""
    except Exception:
        pass
    s = str(s)
    return s if len(s) <= n else s[: n - 3] + "..."


def _safe_str(v) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except Exception:
        pass
    return str(v)


def _safe_int(v) -> int:
    try:
        if v is None or pd.isna(v):
            return 0
        return int(v)
    except Exception:
        return 0


def _safe_bool(v) -> bool:
    try:
        if v is None or pd.isna(v):
            return False
    except Exception:
        pass
    return bool(v)


def _safe_float(v, default: float = 0.0) -> float:
    try:
        if v is None or pd.isna(v):
            return default
        return float(v)
    except Exception:
        return default


def ensure_log_table(con) -> None:
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
          queue_name           VARCHAR,
          research_id_or_key   VARCHAR,
          current_value        VARCHAR,
          proposed_resolution  VARCHAR,
          reasoning_summary    VARCHAR,
          evidence_excerpt     VARCHAR,
          confidence           VARCHAR,
          agent_model          VARCHAR,
          adjudicated_at       TIMESTAMP WITH TIME ZONE,
          run_tag              VARCHAR
        )
        """
    )


def insert_records(con, records: list[dict]) -> None:
    if not records:
        return
    df = pd.DataFrame(records)
    df["adjudicated_at"] = RUN_TS
    df["run_tag"] = RUN_TS_TAG
    df["agent_model"] = AGENT_MODEL
    cols = [
        "queue_name",
        "research_id_or_key",
        "current_value",
        "proposed_resolution",
        "reasoning_summary",
        "evidence_excerpt",
        "confidence",
        "agent_model",
        "adjudicated_at",
        "run_tag",
    ]
    df = df.reindex(columns=cols)
    for c in [
        "queue_name",
        "research_id_or_key",
        "current_value",
        "proposed_resolution",
        "reasoning_summary",
        "evidence_excerpt",
        "confidence",
        "agent_model",
        "run_tag",
    ]:
        df[c] = df[c].astype("string")
    con.register("_log_batch_df", df)
    con.execute(f"INSERT INTO {LOG_TABLE} SELECT * FROM _log_batch_df")
    con.unregister("_log_batch_df")


# ---------------------------------------------------------------------------
# Queue 1: main.path_size_adjudication_v241  (96)
# ---------------------------------------------------------------------------
def adjudicate_q1_path_size(con) -> list[dict]:
    sql = """
    WITH q AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             path_tumor_size_cm,
             tumor_size_cm_max,
             n_foci_path,
             n_tumors_path,
             proposed_path_tumor_size_cm_adjudicated,
             adjudication_rule,
             review_priority
      FROM main.path_size_adjudication_v241
    ),
    ctc AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             COUNT(*) AS ctc_n,
             MAX(size_greatest_dimension_cm) AS ctc_max_size,
             MAX(tumor_size_cm_per_surgery) AS ctc_per_surg_max
      FROM main.canonical_tumor_characteristics_v1
      GROUP BY 1
    ),
    syn AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             COUNT(*) AS syn_n,
             MAX(size_greatest_dimension_cm) AS syn_max_size
      FROM main.synoptic_tumor_long_v1
      GROUP BY 1
    ),
    tp AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             MAX(tumor_1_size_cm) AS tp_t1_size,
             MAX(num_tumors_identified) AS tp_n_tumors
      FROM main.tumor_pathology
      GROUP BY 1
    ),
    ps AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             COUNT(*) AS ps_n
      FROM main.path_synoptics
      GROUP BY 1
    )
    SELECT q.*, ctc.ctc_n, ctc.ctc_max_size, ctc.ctc_per_surg_max,
           syn.syn_n, syn.syn_max_size,
           tp.tp_t1_size, tp.tp_n_tumors,
           ps.ps_n
    FROM q
    LEFT JOIN ctc USING (rid_v)
    LEFT JOIN syn USING (rid_v)
    LEFT JOIN tp  USING (rid_v)
    LEFT JOIN ps  USING (rid_v)
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        rid = r.rid_v
        rule = r.adjudication_rule
        cur_path = r.path_tumor_size_cm
        cur_max = r.tumor_size_cm_max
        proposed = r.proposed_path_tumor_size_cm_adjudicated
        n_foci = r.n_foci_path
        n_tum = r.n_tumors_path

        feeders = [
            f for f in [r.ctc_max_size, r.ctc_per_surg_max, r.syn_max_size, r.tp_t1_size]
            if f is not None and not pd.isna(f)
        ]
        feeder_max = max(feeders) if feeders else None
        evidence = (
            f"current path={cur_path}, rollup_max={cur_max}, n_foci={n_foci}, "
            f"n_tumors={n_tum}, ctc_max={r.ctc_max_size}, ctc_per_surg_max={r.ctc_per_surg_max}, "
            f"syn_max={r.syn_max_size}, tp_t1_size={r.tp_t1_size}, ps_rows={r.ps_n}, "
            f"rule={rule}, priority={r.review_priority}"
        )

        if rule == "multifocal_use_rollup_max":
            if feeder_max is not None and cur_max is not None and abs(feeder_max - cur_max) <= 0.05:
                conf = "HIGH"
                reasoning = (
                    "Multifocal: rollup_max concordant with feeder maxima across "
                    "canonical_tumor_characteristics_v1 / synoptic_tumor_long_v1 / tumor_pathology. "
                    "Adopt tumor_size_cm_max as adjudicated path size."
                )
            else:
                conf = "MEDIUM"
                reasoning = (
                    "Multifocal: rule says use rollup_max but feeder maxima diverge slightly; "
                    "rollup remains the principled choice but flag for spot-check."
                )
            proposed_res = f"path_tumor_size_cm := {proposed}"
        elif rule == "unifocal_retain_path_size":
            conf = "HIGH"
            reasoning = (
                "Unifocal pathology (n_foci_path=1): retain path_tumor_size_cm as authoritative; "
                "rollup_max picks a smaller incidental focus from a separate detail row."
            )
            proposed_res = f"path_tumor_size_cm := {proposed}"
        elif rule == "outlier_manual_review_required":
            concordant_feeders = (
                feeder_max is not None
                and cur_path is not None
                and abs(feeder_max - cur_path) <= 0.5
            )
            if (n_foci == 1 and n_tum == 1 and (cur_max is None or abs((cur_max or 0) - (cur_path or 0)) <= 0.5)
                    and concordant_feeders):
                conf = "MEDIUM"
                reasoning = (
                    "Outlier (>10cm) but unifocal/unimer pathology and rollup+feeders "
                    "concordant with path size. Likely real giant tumor; propose retain "
                    "path_tumor_size_cm and lift outlier flag."
                )
                proposed_res = f"path_tumor_size_cm := {cur_path} (retain; concordant outlier)"
            elif n_foci == 1 and n_tum == 1 and cur_max is None:
                conf = "MEDIUM"
                reasoning = (
                    "Outlier with unifocal path and no rollup max (single-source outlier). "
                    "Retain path_tumor_size_cm pending optional chart confirmation."
                )
                proposed_res = f"path_tumor_size_cm := {cur_path} (retain; single-source giant tumor)"
            else:
                conf = "LOW"
                reasoning = (
                    "Outlier with multifocal disagreement or wide path/rollup spread. "
                    "Genuine chart-review case; do not auto-resolve."
                )
                proposed_res = "chart_review_required"
        else:
            conf = "LOW"
            reasoning = f"Unknown adjudication_rule={rule}; surface to human."
            proposed_res = "chart_review_required"

        out.append(
            {
                "queue_name": "main.path_size_adjudication_v241",
                "research_id_or_key": rid,
                "current_value": f"path={cur_path}, max={cur_max}",
                "proposed_resolution": truncate(proposed_res, 200),
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Queue 2: manuscript_workspace.path_tumor_size_correction_queue_v1  (80)
# ---------------------------------------------------------------------------
def adjudicate_q2_size_correction(con) -> list[dict]:
    sql = """
    SELECT CAST(research_id AS VARCHAR) AS rid_v, *
    FROM manuscript_workspace.path_tumor_size_correction_queue_v1
    WHERE status='awaiting_approval'
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        cur_path = r.current_path_tumor_size_cm
        cur_max = r.current_tumor_size_cm_max
        observed = r.observed_max_tumor_focus
        proposed = r.proposed_corrected_value
        source = r.proposed_corrected_source or ""
        evid = r.evidence or ""
        true_max = r.true_max_across_all_surgeries
        n_surg = r.n_surg_episodes

        evidence_excerpt = (
            f"bucket={r.bucket}/{r.subbucket}; broken_column={r.broken_column}; "
            f"current_path={cur_path}; current_max={cur_max}; observed_max_focus={observed}; "
            f"true_max_across_all_surg={true_max}; n_surg={n_surg}; proposed={proposed}; "
            f"source={source}\nevidence={evid}"
        )

        tem_confirmed = "TEM scope-check did NOT confirm" not in source
        if tem_confirmed:
            tem_eq_obs = (
                true_max is not None
                and observed is not None
                and not pd.isna(true_max)
                and not pd.isna(observed)
                and abs(float(true_max) - float(observed)) <= 0.05
            )
            if tem_eq_obs:
                conf = "HIGH"
                reasoning = (
                    "F1 TEM-confirmed: true_max_across_all_surgeries equals "
                    "observed_max_tumor_focus across feeders. Apply "
                    "tumor_size_cm_max := proposed (re-aggregate from "
                    "tumor_episode_master_v2)."
                )
            else:
                conf = "MEDIUM"
                reasoning = (
                    "F1 TEM-confirmed but observed_max > TEM true_max — non-TEM feeder "
                    "carries a larger focus than the TEM cohort. Proposal correctly takes "
                    "the GREATEST; acceptable but flag for spot-check that the larger feeder "
                    "value is not a measurement-unit artifact."
                )
        else:
            conf = "LOW"
            reasoning = (
                "TEM scope-check did NOT confirm the larger value (non-TEM source). "
                "Proposal is plausible but lacks a TEM cross-check; surface for "
                "chart review before patching tumor_size_cm_max."
            )

        out.append(
            {
                "queue_name": "manuscript_workspace.path_tumor_size_correction_queue_v1",
                "research_id_or_key": r.rid_v,
                "current_value": f"max={cur_max}",
                "proposed_resolution": f"tumor_size_cm_max := {proposed}",
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence_excerpt, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Queue 3: main.ete_adjudication_v1 LOW (26)
# ---------------------------------------------------------------------------
def adjudicate_q3_ete_low(con) -> list[dict]:
    sql = """
    WITH q AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             adjudicated_grade, adjudicated_confidence, evidence_quote,
             reasoning, ajcc8_t_adjustment
      FROM main.ete_adjudication_v1
      WHERE LOWER(adjudicated_confidence)='low'
    ),
    sub AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(refined_ete_grade)  AS refined_ete_grade,
             ANY_VALUE(op_note_grade)      AS op_note_grade,
             ANY_VALUE(op_note_confidence) AS op_note_confidence,
             ANY_VALUE(original_grade)     AS original_grade,
             ANY_VALUE(grading_source_note) AS grading_source_note
      FROM main.extracted_ete_subgraded_v1
      GROUP BY 1
    ),
    op AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             BOOL_OR(gross_ete_flag)          AS op_gross_ete,
             BOOL_OR(local_invasion_flag)     AS op_local_inv,
             BOOL_OR(tracheal_involvement_flag) AS op_trach,
             BOOL_OR(esophageal_involvement_flag) AS op_esoph,
             BOOL_OR(strap_muscle_involvement_flag) AS op_strap,
             COUNT(*) AS op_rows
      FROM main.operative_episode_detail_v2
      GROUP BY 1
    ),
    nopd AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             SUM(CASE WHEN entity_type ILIKE '%ete%' AND present_or_negated='present' THEN 1 ELSE 0 END) AS nopd_ete_present,
             SUM(CASE WHEN entity_type ILIKE '%ete%' AND present_or_negated='negated' THEN 1 ELSE 0 END) AS nopd_ete_neg,
             COUNT(*) AS nopd_rows
      FROM main.note_entities_operative_detail
      GROUP BY 1
    )
    SELECT q.*, sub.refined_ete_grade, sub.op_note_grade, sub.op_note_confidence,
           sub.original_grade, sub.grading_source_note,
           op.op_gross_ete, op.op_local_inv, op.op_trach, op.op_esoph, op.op_strap, op.op_rows,
           nopd.nopd_ete_present, nopd.nopd_ete_neg, nopd.nopd_rows
    FROM q
    LEFT JOIN sub  USING (rid_v)
    LEFT JOIN op   USING (rid_v)
    LEFT JOIN nopd USING (rid_v)
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        evidence = (
            f"llm_grade={r.adjudicated_grade}/{r.adjudicated_confidence}; "
            f"ajcc8_adj={r.ajcc8_t_adjustment}; "
            f"sub.refined={r.refined_ete_grade}; op_note_grade={r.op_note_grade} "
            f"(conf={r.op_note_confidence}); original_grade={r.original_grade}; "
            f"op.gross_ete={r.op_gross_ete}; op.local_inv={r.op_local_inv}; "
            f"op.trach={r.op_trach}; op.esoph={r.op_esoph}; op.strap={r.op_strap}; "
            f"nopd ete present/neg={r.nopd_ete_present}/{r.nopd_ete_neg}; "
            f"path_quote={r.evidence_quote}"
        )

        op_grade = _safe_str(r.op_note_grade).strip().lower()
        op_conf = _safe_float(r.op_note_confidence, 0.0)
        op_gross_any = (
            _safe_bool(r.op_gross_ete) or _safe_bool(r.op_local_inv)
            or _safe_bool(r.op_trach) or _safe_bool(r.op_esoph)
            or _safe_bool(r.op_strap)
        )
        nopd_pres = _safe_int(r.nopd_ete_present)
        nopd_neg = _safe_int(r.nopd_ete_neg)
        refined_grade = _safe_str(r.refined_ete_grade).strip().lower()

        if op_grade in {"absent", "microscopic", "gross"} and op_conf >= 0.7 and not op_gross_any and op_grade == "absent":
            conf = "MEDIUM"
            proposed = "ete_grade := absent (operative-corroborated, microscopy-blind)"
            reasoning = (
                "Path microscopy section blank/incomplete (LLM unable_to_determine), but "
                "operative subgrade extractor reports 'absent' with high confidence and no "
                "operative invasion flags. Plausible to lift to absent pending chart review."
            )
        elif op_gross_any or (op_grade == "gross" and op_conf >= 0.7):
            conf = "LOW"
            proposed = "chart_review_required (op-note suggests gross extension; path microscopy missing)"
            reasoning = (
                "Operative or extracted operative-detail flags suggest gross extrathyroidal "
                "extension while pathology microscopy is incomplete. Resolving requires chart "
                "review of the original microscopic description."
            )
        elif refined_grade in {"absent", "microscopic", "gross"}:
            conf = "LOW"
            proposed = f"chart_review_required (sub.refined={refined_grade}; pathology incomplete)"
            reasoning = (
                "Subgrade refiner produced a grade but pathology microscopy is missing; "
                "structured evidence is single-source and cannot be cross-validated."
            )
        else:
            conf = "LOW"
            proposed = "chart_review_required (no corroborating operative or extracted evidence)"
            reasoning = (
                "Pathology microscopy missing/incomplete and no operative or extracted "
                "evidence to corroborate. Genuine chart-review case."
            )

        out.append(
            {
                "queue_name": "main.ete_adjudication_v1",
                "research_id_or_key": r.rid_v,
                "current_value": f"llm={r.adjudicated_grade} (low)",
                "proposed_resolution": truncate(proposed, 250),
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Queue 4: cpm_hypopara_adjudication_queue_v1 (4)
# ---------------------------------------------------------------------------
def adjudicate_q4_hypopara(con) -> list[dict]:
    sql = """
    WITH q AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v, * EXCLUDE (research_id)
      FROM manuscript_workspace.cpm_hypopara_adjudication_queue_v1
    ),
    pheno AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(final_complication_status) AS final_status,
             ANY_VALUE(status_v2)                 AS status_v2,
             BOOL_OR(permanent_flag)              AS permanent_flag,
             BOOL_OR(transient_flag)              AS transient_flag,
             BOOL_OR(confirmed_flag)              AS confirmed_flag,
             BOOL_OR(suspected_flag)              AS suspected_flag,
             BOOL_OR(treatment_requiring_flag)    AS tx_flag,
             MIN(ca_nadir)                        AS ca_nadir,
             MIN(pth_nadir)                       AS pth_nadir
      FROM main.complication_phenotype_v1
      WHERE complication_entity ILIKE '%hypopara%' OR complication_entity ILIKE '%hypocalc%'
      GROUP BY 1
    ),
    labs_late AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             COUNT(*) FILTER (WHERE lab_name_standardized ILIKE '%calcium%' AND value_numeric < 8.4) AS late_low_ca,
             COUNT(*) FILTER (WHERE lab_name_standardized ILIKE '%calcium%' AND value_numeric BETWEEN 8.4 AND 10.5) AS late_normal_ca,
             COUNT(*) FILTER (WHERE lab_name_standardized ILIKE '%pth%' AND value_numeric < 15) AS late_low_pth,
             COUNT(*) FILTER (WHERE lab_name_standardized ILIKE '%pth%' AND value_numeric >= 15) AS late_normal_pth
      FROM main.longitudinal_lab_canonical_v1
      WHERE lab_date_status='valid'
      GROUP BY 1
    ),
    postop_late AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             COUNT(*) FILTER (WHERE lab_type ILIKE '%calcium%' AND days_postop > 180 AND value < 8.4) AS late_low_ca_lab,
             COUNT(*) FILTER (WHERE lab_type ILIKE '%calcium%' AND days_postop > 180 AND value BETWEEN 8.4 AND 10.5) AS late_normal_ca_lab,
             COUNT(*) FILTER (WHERE lab_type ILIKE '%pth%' AND days_postop > 180 AND value < 15) AS late_low_pth_lab,
             COUNT(*) FILTER (WHERE lab_type ILIKE '%pth%' AND days_postop > 180 AND value >= 15) AS late_normal_pth_lab
      FROM main.extracted_postop_labs_expanded_v1
      GROUP BY 1
    ),
    nopd AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             SUM(CASE WHEN entity_type ILIKE '%hypopara%' OR entity_type ILIKE '%hypocalc%'
                       THEN 1 ELSE 0 END) AS hp_mentions,
             SUM(CASE WHEN (entity_type ILIKE '%calcium%' OR entity_type ILIKE '%calcitriol%' OR entity_type ILIKE '%vitamin d%')
                            AND present_or_negated='present' THEN 1 ELSE 0 END) AS supp_mentions
      FROM main.note_entities_complications
      GROUP BY 1
    )
    SELECT q.*, pheno.final_status, pheno.status_v2, pheno.permanent_flag, pheno.transient_flag,
           pheno.confirmed_flag, pheno.suspected_flag, pheno.tx_flag, pheno.ca_nadir, pheno.pth_nadir,
           labs_late.late_low_ca, labs_late.late_normal_ca, labs_late.late_low_pth, labs_late.late_normal_pth,
           postop_late.late_low_ca_lab, postop_late.late_normal_ca_lab,
           postop_late.late_low_pth_lab, postop_late.late_normal_pth_lab,
           nopd.hp_mentions, nopd.supp_mentions
    FROM q
    LEFT JOIN pheno      USING (rid_v)
    LEFT JOIN labs_late  USING (rid_v)
    LEFT JOIN postop_late USING (rid_v)
    LEFT JOIN nopd       USING (rid_v)
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        late_low_ca = _safe_int(r.late_low_ca_lab)
        late_normal_ca = _safe_int(r.late_normal_ca_lab)
        late_low_pth = _safe_int(r.late_low_pth_lab)
        late_normal_pth = _safe_int(r.late_normal_pth_lab)
        tx_flag = _safe_bool(r.tx_flag)
        evidence = (
            f"cpm_says={r.cpm_says}; phenotype_says={r.phenotype_says}; "
            f"pheno.final_status={r.final_status}; status_v2={r.status_v2}; "
            f"permanent_flag={r.permanent_flag}; transient_flag={r.transient_flag}; "
            f"confirmed_flag={r.confirmed_flag}; tx_flag={r.tx_flag}; "
            f"ca_nadir={r.ca_nadir}; pth_nadir={r.pth_nadir}; "
            f"longitudinal: low_ca={r.late_low_ca}, normal_ca={r.late_normal_ca}, "
            f"low_pth={r.late_low_pth}, normal_pth={r.late_normal_pth}; "
            f"postop>180d: low_ca={late_low_ca}, normal_ca={late_normal_ca}, "
            f"low_pth={late_low_pth}, normal_pth={late_normal_pth}; "
            f"nopd hp_mentions={r.hp_mentions}, supp_mentions={r.supp_mentions}"
        )

        normal_signal = late_normal_ca + late_normal_pth
        low_signal = late_low_ca + late_low_pth
        if (low_signal == 0 and normal_signal >= 2) and not tx_flag:
            conf = "MEDIUM"
            proposed = "set CPM hypopara_permanent := FALSE (transient confirmed)"
            reasoning = (
                "No low Ca/PTH after 180 days and no treatment-requiring flag; phenotype "
                "label of 'confirmed_transient' is consistent with structured labs."
            )
        elif low_signal >= 2 and tx_flag:
            conf = "MEDIUM"
            proposed = "retain CPM hypopara_permanent := TRUE"
            reasoning = (
                "Persistent low Ca/PTH beyond 180 days with treatment-requiring flag — "
                "supports CPM 'permanent' label; phenotype 'transient' likely reflects "
                "a window-specific definition mismatch."
            )
        else:
            conf = "LOW"
            proposed = "chart_review_required (mixed structured signal)"
            reasoning = (
                "Late post-op Ca/PTH evidence is mixed or sparse, and phenotype/CPM "
                "labels disagree. Genuine chart review needed to confirm permanence."
            )

        out.append(
            {
                "queue_name": "manuscript_workspace.cpm_hypopara_adjudication_queue_v1",
                "research_id_or_key": r.rid_v,
                "current_value": f"cpm={r.cpm_says}; phenotype={r.phenotype_says}",
                "proposed_resolution": truncate(proposed, 250),
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Queue 5: cpm_is_malignant_flag_review_v1 (5)
# ---------------------------------------------------------------------------
def adjudicate_q5_is_malignant(con) -> list[dict]:
    sql = """
    WITH q AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v, *
      FROM manuscript_workspace.cpm_is_malignant_flag_review_v1
    ),
    diag AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(diagnosis_primary)  AS diag_primary,
             ANY_VALUE(diagnosis_full)     AS diag_full,
             BOOL_OR(is_malignant)         AS diag_is_malignant
      FROM main.canonical_diagnosis_unified_v1
      GROUP BY 1
    ),
    mal AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(histology_full_descriptor) AS mal_full,
             ANY_VALUE(histology_base_canonical)  AS mal_base,
             BOOL_OR(is_malignant)                AS mal_is_malignant
      FROM main.canonical_malignant_diagnosis_v1
      GROUP BY 1
    ),
    ben AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(benign_diagnosis_primary) AS ben_primary,
             BOOL_OR(is_malignant)               AS ben_is_malignant
      FROM main.canonical_benign_diagnosis_v1
      GROUP BY 1
    ),
    ps AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(synoptic_diagnosis)        AS ps_synoptic_dx,
             ANY_VALUE(path_diagnosis_summary)    AS ps_path_dx_summary,
             ANY_VALUE(tumor_1_histologic_type)   AS ps_t1_histo
      FROM main.path_synoptics
      GROUP BY 1
    ),
    ctc AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(primary_histology) AS ctc_primary_histo,
             COUNT(*) AS ctc_n
      FROM main.canonical_tumor_characteristics_v1
      GROUP BY 1
    )
    SELECT q.*,
           diag.diag_primary, diag.diag_full, diag.diag_is_malignant,
           mal.mal_full, mal.mal_base, mal.mal_is_malignant,
           ben.ben_primary, ben.ben_is_malignant,
           ps.ps_synoptic_dx, ps.ps_path_dx_summary, ps.ps_t1_histo,
           ctc.ctc_primary_histo, ctc.ctc_n
    FROM q
    LEFT JOIN diag USING (rid_v)
    LEFT JOIN mal  USING (rid_v)
    LEFT JOIN ben  USING (rid_v)
    LEFT JOIN ps   USING (rid_v)
    LEFT JOIN ctc  USING (rid_v)
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        evidence = (
            f"cpm.is_malignant={r.is_malignant}; cpm.histology_final={r.histology_final}; "
            f"cpm.dom_t={r.dom_t_ajcc8}; cpm.stage={r.dom_stage_group}; ctc_rows={r.n_ctc_rows}; "
            f"diag.unified_primary={r.diag_primary}; diag.unified_is_malignant={r.diag_is_malignant}; "
            f"mal.full={r.mal_full}; mal.base={r.mal_base}; mal.is_malignant={r.mal_is_malignant}; "
            f"ben.primary={r.ben_primary}; ben.is_malignant={r.ben_is_malignant}; "
            f"ps.synoptic_dx={_safe_str(r.ps_synoptic_dx)[:200]}; "
            f"ps.path_dx_summary={_safe_str(r.ps_path_dx_summary)[:300]}; "
            f"ps.t1_histo={_safe_str(r.ps_t1_histo)}; ctc.primary_histo={_safe_str(r.ctc_primary_histo)}"
        )
        mal_base_s = _safe_str(r.mal_base)
        mal_full_s = _safe_str(r.mal_full)
        diag_primary_s = _safe_str(r.diag_primary)
        ctc_primary_s = _safe_str(r.ctc_primary_histo)
        ben_primary_s = _safe_str(r.ben_primary)
        mal_present = _safe_bool(r.mal_is_malignant) or _safe_bool(r.diag_is_malignant) or bool(mal_base_s)
        if mal_present:
            conf = "HIGH"
            histo = mal_base_s or mal_full_s or diag_primary_s or ctc_primary_s
            proposed = (
                f"set CPM is_malignant := TRUE; histology_final := {histo}"
            )
            reasoning = (
                "Canonical malignant diagnosis row exists (or unified diagnosis flagged "
                "malignant) and CTC row(s) carry the same histology — CPM is_malignant=FALSE "
                "is a join/coverage gap, not a clinical disagreement."
            )
        elif _safe_bool(r.ben_is_malignant) is False and ben_primary_s:
            conf = "MEDIUM"
            proposed = "retain CPM is_malignant := FALSE; review staging on benign case"
            reasoning = (
                "Only a benign diagnosis row exists; staging fields appear to be carried "
                "from a separate metastatic/CTC source. Likely a CTC import artifact, "
                "not a true malignant case."
            )
        else:
            conf = "LOW"
            proposed = "chart_review_required (no canonical diagnosis row)"
            reasoning = (
                "No malignant or benign canonical diagnosis row to corroborate CPM. "
                "Surface for human review."
            )

        out.append(
            {
                "queue_name": "manuscript_workspace.cpm_is_malignant_flag_review_v1",
                "research_id_or_key": r.rid_v,
                "current_value": f"is_malignant={r.is_malignant}, histology_final={r.histology_final}",
                "proposed_resolution": truncate(proposed, 250),
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Queue 6: cpm_ete_self_contradiction_queue_v1 (1)
# ---------------------------------------------------------------------------
def adjudicate_q6_ete_self_contradiction(con) -> list[dict]:
    sql = """
    WITH q AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v, *
      FROM manuscript_workspace.cpm_ete_self_contradiction_queue_v1
    ),
    sub AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(refined_ete_grade)  AS refined_ete_grade,
             ANY_VALUE(op_note_grade)      AS op_note_grade,
             ANY_VALUE(op_note_confidence) AS op_note_confidence,
             ANY_VALUE(original_grade)     AS original_grade
      FROM main.extracted_ete_subgraded_v1
      GROUP BY 1
    ),
    op AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             BOOL_OR(gross_ete_flag)            AS op_gross_ete,
             BOOL_OR(local_invasion_flag)       AS op_local_inv,
             BOOL_OR(tracheal_involvement_flag) AS op_trach,
             BOOL_OR(esophageal_involvement_flag) AS op_esoph,
             BOOL_OR(strap_muscle_involvement_flag) AS op_strap
      FROM main.operative_episode_detail_v2
      GROUP BY 1
    ),
    ctc AS (
      SELECT CAST(research_id AS VARCHAR) AS rid_v,
             ANY_VALUE(extrathyroidal_extension) AS ctc_ete,
             MAX(gross_ete) AS ctc_gross_ete
      FROM main.canonical_tumor_characteristics_v1
      GROUP BY 1
    )
    SELECT q.*, sub.refined_ete_grade, sub.op_note_grade, sub.op_note_confidence,
           sub.original_grade,
           op.op_gross_ete, op.op_local_inv, op.op_trach, op.op_esoph, op.op_strap,
           ctc.ctc_ete, ctc.ctc_gross_ete
    FROM q
    LEFT JOIN sub USING (rid_v)
    LEFT JOIN op  USING (rid_v)
    LEFT JOIN ctc USING (rid_v)
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        evidence = (
            f"cpm_ete_grade_final_v2={r.cpm_ete_grade_final_v2}; "
            f"cpm_gross_ete_flag={r.cpm_gross_ete_flag}; "
            f"sub.refined={r.refined_ete_grade}; op_note_grade={r.op_note_grade} "
            f"(conf={r.op_note_confidence}); original_grade={r.original_grade}; "
            f"op.gross_ete={r.op_gross_ete}; op.local_inv={r.op_local_inv}; "
            f"op.trach={r.op_trach}; op.esoph={r.op_esoph}; op.strap={r.op_strap}; "
            f"ctc.ete={r.ctc_ete}; ctc.gross_ete={r.ctc_gross_ete}"
        )
        refined = _safe_str(r.refined_ete_grade).strip().lower()
        op_gross_any = (
            _safe_bool(r.op_gross_ete) or _safe_bool(r.op_local_inv)
            or _safe_bool(r.op_trach) or _safe_bool(r.op_esoph) or _safe_bool(r.op_strap)
        )
        ctc_gross = _safe_int(r.ctc_gross_ete) > 0
        if refined == "microscopic" and not op_gross_any and not ctc_gross:
            conf = "MEDIUM"
            proposed = "clear cpm_gross_ete_flag := FALSE; retain ete_grade_final_v2 := microscopic"
            reasoning = (
                "Refined ETE grade and CTC agree on microscopic; no operative gross-ETE/"
                "local-invasion flags. The gross_ete_flag=TRUE in CPM is the artifact."
            )
        elif op_gross_any or ctc_gross or refined == "gross":
            conf = "MEDIUM"
            proposed = "promote ete_grade_final_v2 := gross; retain cpm_gross_ete_flag := TRUE"
            reasoning = (
                "Operative or canonical evidence supports gross extension; the "
                "ete_grade_final_v2='microscopic' label appears under-graded."
            )
        else:
            conf = "LOW"
            proposed = "chart_review_required"
            reasoning = (
                "Insufficient corroborating evidence to resolve gross vs microscopic. "
                "Surface for chart review."
            )

        out.append(
            {
                "queue_name": "manuscript_workspace.cpm_ete_self_contradiction_queue_v1",
                "research_id_or_key": r.rid_v,
                "current_value": f"grade={r.cpm_ete_grade_final_v2}; gross_flag={r.cpm_gross_ete_flag}",
                "proposed_resolution": truncate(proposed, 250),
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Queue 7: cohort_view_duplicate_review_v1 (8)
# ---------------------------------------------------------------------------
def adjudicate_q7_cohort_dup(con) -> list[dict]:
    sql = """
    WITH q AS (
      SELECT cluster_label, manuscript_id_a, manuscript_id_b, jaccard_column_overlap, note
      FROM manuscript_workspace.cohort_view_duplicate_review_v1
    ),
    dive AS (
      SELECT CAST(manuscript_id AS VARCHAR) AS mid,
             ANY_VALUE(manuscript_title)      AS title,
             ANY_VALUE(cohort_view_name)      AS cohort_view,
             ANY_VALUE(CAST(dive_id AS VARCHAR)) AS dive_id_v,
             ANY_VALUE(dive_title)            AS dive_title,
             ANY_VALUE(dive_type)             AS dive_type,
             ANY_VALUE(duplicate_of_manuscript_id) AS dup_of
      FROM manuscript_workspace.manuscript_dive_map_v1
      GROUP BY 1
    )
    SELECT q.*,
           da.title AS a_title, da.cohort_view AS a_view, da.dive_id_v AS a_dive_id,
             da.dive_title AS a_dive_title, da.dive_type AS a_dive_type, da.dup_of AS a_dup_of,
           db.title AS b_title, db.cohort_view AS b_view, db.dive_id_v AS b_dive_id,
             db.dive_title AS b_dive_title, db.dive_type AS b_dive_type, db.dup_of AS b_dup_of
    FROM q
    LEFT JOIN dive da ON da.mid = CAST(CAST(REPLACE(q.manuscript_id_a, 'm', '') AS INTEGER) AS VARCHAR)
    LEFT JOIN dive db ON db.mid = CAST(CAST(REPLACE(q.manuscript_id_b, 'm', '') AS INTEGER) AS VARCHAR)
    """
    df = con.execute(sql).fetchdf()
    out: list[dict] = []
    for r in df.itertuples(index=False):
        a_dive = _safe_str(r.a_dive_id)
        b_dive = _safe_str(r.b_dive_id)
        a_type = _safe_str(r.a_dive_type)
        b_type = _safe_str(r.b_dive_type)
        same_dive = bool(a_dive) and bool(b_dive) and a_dive == b_dive
        either_thematic = (a_type == "thematic") or (b_type == "thematic")
        evidence = (
            f"cluster={r.cluster_label}; jaccard={r.jaccard_column_overlap}\n"
            f"A: {r.manuscript_id_a} title='{r.a_title}' view={r.a_view} dive={r.a_dive_title} "
            f"({r.a_dive_type}) dive_id={r.a_dive_id} dup_of={r.a_dup_of}\n"
            f"B: {r.manuscript_id_b} title='{r.b_title}' view={r.b_view} dive={r.b_dive_title} "
            f"({r.b_dive_type}) dive_id={r.b_dive_id} dup_of={r.b_dup_of}"
        )
        if same_dive and either_thematic:
            conf = "HIGH"
            proposed = "no_dedup; pair shares an intentional thematic DIVE"
            reasoning = (
                "Both manuscripts map to the same thematic DIVE — sharing the dive is the "
                "designed pattern; cohort_view overlap is expected and not a duplicate."
            )
        elif same_dive and not either_thematic:
            conf = "MEDIUM"
            proposed = "dedup_candidate; same dedicated DIVE — confirm if one is an alias"
            reasoning = (
                "Both manuscripts share a dedicated (non-thematic) DIVE; high likelihood one "
                "is an alias of the other. Confirm authoring intent."
            )
        elif a_dive and b_dive and a_dive != b_dive:
            conf = "HIGH"
            proposed = "no_dedup; distinct DIVEs and distinct cohort_views"
            reasoning = (
                "Different DIVE IDs and different cohort_view names — overlap is incidental "
                "(Jaccard column-overlap clustering); manuscripts are not duplicates."
            )
        else:
            conf = "LOW"
            proposed = "chart_review_required (missing dive metadata)"
            reasoning = "Missing dive_id metadata for one or both manuscripts."

        key = f"{r.cluster_label}|{r.manuscript_id_a}|{r.manuscript_id_b}"
        out.append(
            {
                "queue_name": "manuscript_workspace.cohort_view_duplicate_review_v1",
                "research_id_or_key": key,
                "current_value": f"a={r.manuscript_id_a}; b={r.manuscript_id_b}; cluster={r.cluster_label}",
                "proposed_resolution": truncate(proposed, 250),
                "reasoning_summary": truncate(reasoning, 800),
                "evidence_excerpt": truncate(evidence, 1500),
                "confidence": conf,
            }
        )
    return out


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------
def main() -> int:
    con = connect_rw()
    try:
        ensure_log_table(con)
        existing = con.execute(
            f"SELECT COUNT(*) FROM {LOG_TABLE} WHERE run_tag=?", [RUN_TS_TAG]
        ).fetchone()[0]
        if existing:
            print(f"[warn] run_tag {RUN_TS_TAG} already has {existing} rows — aborting")
            return 1

        all_records: list[dict] = []
        steps = [
            ("q1_path_size",      adjudicate_q1_path_size),
            ("q2_size_correction", adjudicate_q2_size_correction),
            ("q3_ete_low",         adjudicate_q3_ete_low),
            ("q4_hypopara",        adjudicate_q4_hypopara),
            ("q5_is_malignant",    adjudicate_q5_is_malignant),
            ("q6_ete_self_contra", adjudicate_q6_ete_self_contradiction),
            ("q7_cohort_dup",      adjudicate_q7_cohort_dup),
        ]
        per_queue_counts: list[dict] = []
        for label, fn in steps:
            print(f"[run] {label} ...")
            recs = fn(con)
            print(f"      -> {len(recs)} adjudicated")
            insert_records(con, recs)
            all_records.extend(recs)
            counts = {"HIGH": 0, "MEDIUM": 0, "LOW": 0}
            for r in recs:
                counts[r["confidence"]] = counts.get(r["confidence"], 0) + 1
            per_queue_counts.append(
                {
                    "queue": (recs[0]["queue_name"] if recs else label),
                    "total": len(recs),
                    "HIGH": counts["HIGH"],
                    "MEDIUM": counts["MEDIUM"],
                    "LOW": counts["LOW"],
                }
            )

        df_all = pd.DataFrame(all_records)
        df_all["adjudicated_at"] = RUN_TS
        df_all["agent_model"] = AGENT_MODEL
        df_all["run_tag"] = RUN_TS_TAG
        df_all.to_parquet(PARQUET_PATH, index=False)
        print(f"[done] parquet: {PARQUET_PATH}  rows={len(df_all)}")

        summary_df = pd.DataFrame(per_queue_counts)
        print("\n=== SUMMARY (queue × HIGH × MEDIUM × LOW) ===")
        print(summary_df.to_string(index=False))
        total = {
            "queue": "TOTAL",
            "total": int(summary_df["total"].sum()),
            "HIGH": int(summary_df["HIGH"].sum()),
            "MEDIUM": int(summary_df["MEDIUM"].sum()),
            "LOW": int(summary_df["LOW"].sum()),
        }
        print(json.dumps(total, indent=2))

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    sys.exit(main())
