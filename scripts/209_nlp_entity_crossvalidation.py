"""
Script 209: Comprehensive NLP Entity Cross-Validation
=======================================================
Database : thyroid_ete_fix_20260413 (MotherDuck)
Purpose  : Cross-validate ALL NLP-extracted entity tables against structured
           canonical data. DOES NOT modify canonical_patient_master_v1.
Output   : scripts/output/nlp_crossval_report.md

Domains covered (6 highest cross-validation value):
  1. RECURRENCE   — note_entities_llm_recurrence      (Set A, qwen3:32b)
  2. PATHOLOGY    — note_entities_llm_pathology        (Set B fleet)
  3. TIRADS       — note_entities_llm_tirads_granular  (Set B fleet)
  4. CERVICAL LN  — note_entities_llm_cervical_ln_detail (Set B fleet)
  5. VASCULAR INV — note_entities_llm_vascular_invasion (Set A, qwen3:32b)
  6. TG KINETICS  — note_entities_llm_tg_kinetics      (Set A, qwen3:32b)
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

# ---------------------------------------------------------------------------
# Token / connection
# ---------------------------------------------------------------------------
def _get_token() -> str:
    """Load MD token from env → motherduck.local.toml → .streamlit/secrets.toml."""
    for env_key in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
        val = os.environ.get(env_key, "")
        if val.strip():
            return val.strip()
    try:
        import toml
        toml_path = Path(__file__).parent.parent / "motherduck.local.toml"
        if toml_path.exists():
            cfg = toml.load(str(toml_path))
            for k in ("MD_SA_TOKEN", "MOTHERDUCK_TOKEN", "motherduck_token"):
                if cfg.get(k, "").strip():
                    return cfg[k].strip()
    except Exception:
        pass
    return ""


DB_NAME = "thyroid_ete_fix_20260413"
MIN_CONFIDENCE = 0.5   # entity confidence threshold
HIGH_CONFIDENCE = 0.7  # threshold for "novel finding" NLP-only flags

REPO_ROOT = Path(__file__).parent.parent
OUTPUT_DIR = REPO_ROOT / "scripts" / "output"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_PATH = OUTPUT_DIR / "nlp_crossval_report.md"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def connect_md() -> duckdb.DuckDBPyConnection:
    tok = _get_token()
    if not tok:
        raise RuntimeError("MotherDuck token not found")
    print(f"[MD] token SET, len={len(tok)}")
    con = duckdb.connect(f"md:{DB_NAME}?motherduck_token={tok}")
    return con


def flatten_entities(con: duckdb.DuckDBPyConnection, table: str,
                     extra_filter: str = "") -> pd.DataFrame:
    """
    Parse result_json → flat DataFrame of individual entities.
    Filters to confidence >= MIN_CONFIDENCE and valid JSON rows.
    """
    filter_clause = "AND result_json LIKE '%entity_type%'"
    if extra_filter:
        filter_clause += f" AND {extra_filter}"

    sql = f"""
    WITH raw AS (
        SELECT DISTINCT research_id, note_row_id, note_date, note_type, llm_model,
               unnest(
                   CASE
                       WHEN TRY_CAST(result_json AS JSON) IS NOT NULL
                        AND json_extract(result_json, '$.entities') IS NOT NULL
                       THEN CAST(json_extract(result_json, '$.entities') AS JSON[])
                       ELSE ARRAY[]::JSON[]
                   END
               ) AS ent
        FROM {table}
        WHERE result_json IS NOT NULL {filter_clause}
    )
    SELECT
        research_id,
        note_row_id,
        note_date,
        note_type,
        llm_model,
        json_extract_string(ent, '$.entity_type')       AS entity_type,
        json_extract_string(ent, '$.entity_value')      AS entity_value,
        json_extract_string(ent, '$.entity_date')       AS entity_date,
        json_extract_string(ent, '$.present_or_negated') AS present_or_negated,
        TRY_CAST(json_extract(ent, '$.confidence') AS DOUBLE)  AS confidence,
        json_extract_string(ent, '$.evidence_text')     AS evidence_text
    FROM raw
    WHERE TRY_CAST(json_extract(ent, '$.confidence') AS DOUBLE) >= {MIN_CONFIDENCE}
      AND json_extract_string(ent, '$.entity_type') IS NOT NULL
    """
    df = con.execute(sql).fetchdf()
    print(f"  [{table}] flattened {len(df):,} entities from "
          f"{df['research_id'].nunique():,} patients")
    return df


def flatten_from_parquet(con: duckdb.DuckDBPyConnection,
                         parquet_path: str) -> pd.DataFrame:
    """
    Flatten already-validated flat parquet (fleet_*_new_rows.parquet).
    These have result_json still, so re-parse.
    """
    abs_path = str(REPO_ROOT / parquet_path)
    if not Path(abs_path).exists():
        print(f"  [parquet] NOT FOUND: {abs_path}")
        return pd.DataFrame()

    sql = f"""
    WITH raw AS (
        SELECT DISTINCT research_id, note_row_id, note_date, note_type, llm_model,
               unnest(
                   CASE
                       WHEN TRY_CAST(result_json AS JSON) IS NOT NULL
                        AND json_extract(result_json, '$.entities') IS NOT NULL
                       THEN CAST(json_extract(result_json, '$.entities') AS JSON[])
                       ELSE ARRAY[]::JSON[]
                   END
               ) AS ent
        FROM read_parquet('{abs_path}')
        WHERE result_json LIKE '%entity_type%'
    )
    SELECT
        research_id,
        note_row_id,
        note_date,
        note_type,
        llm_model,
        json_extract_string(ent, '$.entity_type')        AS entity_type,
        json_extract_string(ent, '$.entity_value')       AS entity_value,
        json_extract_string(ent, '$.entity_date')        AS entity_date,
        json_extract_string(ent, '$.present_or_negated') AS present_or_negated,
        TRY_CAST(json_extract(ent, '$.confidence') AS DOUBLE) AS confidence,
        json_extract_string(ent, '$.evidence_text')      AS evidence_text
    FROM raw
    WHERE TRY_CAST(json_extract(ent, '$.confidence') AS DOUBLE) >= {MIN_CONFIDENCE}
      AND json_extract_string(ent, '$.entity_type') IS NOT NULL
    """
    df = con.execute(sql).fetchdf()
    print(f"  [parquet {Path(parquet_path).name}] {len(df):,} entities, "
          f"{df['research_id'].nunique():,} patients")
    return df


def combined_fleet_entities(con: duckdb.DuckDBPyConnection,
                             table: str, parquet: str) -> pd.DataFrame:
    """
    Union fleet MD table + new_rows parquet, deduplicate on note_row_id + entity_type + entity_value.
    """
    df_md  = flatten_entities(con, table)
    df_par = flatten_from_parquet(con, parquet)
    if df_par.empty:
        return df_md
    df_all = pd.concat([df_md, df_par], ignore_index=True)
    before = len(df_all)
    df_all = df_all.drop_duplicates(
        subset=["research_id", "note_row_id", "entity_type", "entity_value"],
        keep="first"
    )
    print(f"  [combined] {before:,} → {len(df_all):,} after dedup, "
          f"{df_all['research_id'].nunique():,} patients")
    return df_all


# ---------------------------------------------------------------------------
# Domain 1: RECURRENCE
# ---------------------------------------------------------------------------
RECURRENCE_POSITIVE_TYPES = {
    "local_recurrence", "regional_recurrence", "distant_recurrence",
    "biochemical_recurrence", "structural_recurrence",
}
RECURRENCE_NEGATIVE_TYPES = {"disease_free", "no_evidence_of_disease"}


def crossval_recurrence(con: duckdb.DuckDBPyConnection,
                         master: pd.DataFrame) -> dict:
    """
    Cross-validate recurrence NLP entities against canonical_patient_master_v1.
    """
    print("\n[RECURRENCE] Parsing entities…")
    ents = flatten_entities(con, "note_entities_llm_recurrence")

    # Determine per-patient NLP signal
    present_mask = (
        (ents["entity_type"].isin(RECURRENCE_POSITIVE_TYPES)) &
        (ents["present_or_negated"] == "present")
    )
    neg_mask = (
        (ents["entity_type"].isin(RECURRENCE_POSITIVE_TYPES)) &
        (ents["present_or_negated"] == "negated")
    ) | (
        ents["entity_type"].isin(RECURRENCE_NEGATIVE_TYPES)
    )

    nlp_pos = set(ents.loc[present_mask, "research_id"].unique())
    nlp_neg = set(ents.loc[neg_mask & ~ents["research_id"].isin(nlp_pos), "research_id"].unique())

    # Canonical: recurrence_confirmed
    can = master[["research_id", "recurrence_confirmed", "recurrence_type",
                   "recurrence_site", "any_recurrence_flag"]].copy()
    can["can_recurrence"] = can["recurrence_confirmed"].fillna(False).astype(bool) | \
                             can["any_recurrence_flag"].fillna(False).astype(bool)

    # Concordance
    concordant = discordant = novel_pos = 0
    discordant_rows = []
    novel_rows = []

    for rid in nlp_pos:
        row = can[can["research_id"] == rid]
        if row.empty:
            novel_pos += 1
            continue
        can_val = bool(row["can_recurrence"].values[0])
        if can_val:
            concordant += 1
        else:
            discordant += 1
            can_null = pd.isna(row["recurrence_confirmed"].values[0])
            if can_null:
                # NLP-only: canonical is NULL
                novel_pos += 1
                best_ent = ents[ents["research_id"] == rid].nlargest(1, "confidence").iloc[0]
                novel_rows.append({
                    "research_id": rid, "nlp_says": "recurrence_present",
                    "canonical_says": "NULL",
                    "confidence": best_ent["confidence"],
                    "entity_type": best_ent["entity_type"],
                    "evidence": str(best_ent["evidence_text"])[:120],
                })
            else:
                discordant_rows.append({
                    "research_id": rid, "nlp_says": "recurrence_present",
                    "canonical_says": "recurrence_confirmed=False",
                    "confidence": ents[ents["research_id"] == rid]["confidence"].max(),
                })

    for rid in nlp_neg:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        can_val = bool(row["can_recurrence"].values[0])
        if not can_val:
            concordant += 1
        else:
            discordant += 1
            discordant_rows.append({
                "research_id": rid, "nlp_says": "disease_free",
                "canonical_says": "recurrence_confirmed=True",
                "confidence": ents[ents["research_id"] == rid]["confidence"].max(),
            })

    total_assessed = concordant + discordant
    concordance_rate = concordant / total_assessed if total_assessed else 0.0

    # Entity type breakdown
    type_counts = ents.groupby("entity_type").size().sort_values(ascending=False).to_dict()

    # NLP-only novel findings (high confidence NLP with NULL canonical)
    nlp_only_flags = []
    for rid in nlp_pos:
        row = can[can["research_id"] == rid]
        if not row.empty and pd.isna(row["recurrence_confirmed"].values[0]):
            best = ents[
                (ents["research_id"] == rid) & ents["entity_type"].isin(RECURRENCE_POSITIVE_TYPES)
            ]
            if not best.empty:
                best_row = best.nlargest(1, "confidence").iloc[0]
                if best_row["confidence"] >= HIGH_CONFIDENCE:
                    nlp_only_flags.append({
                        "research_id": rid,
                        "nlp_entity_type": best_row["entity_type"],
                        "nlp_entity_value": str(best_row["entity_value"])[:80],
                        "confidence": best_row["confidence"],
                        "canonical_field": "recurrence_confirmed",
                        "canonical_value": "NULL",
                    })

    return {
        "domain": "RECURRENCE",
        "source_table": "note_entities_llm_recurrence",
        "model": "qwen3:32b",
        "n_rows": len(ents) + ents.duplicated(subset=["note_row_id", "entity_type"]).sum(),
        "n_notes_with_entities": ents["note_row_id"].nunique(),
        "n_patients": ents["research_id"].nunique(),
        "n_entities_total": len(ents),
        "mean_confidence": float(ents["confidence"].mean()),
        "concordance_rate": concordance_rate,
        "n_concordant": concordant,
        "n_discordant": discordant,
        "n_novel_findings": novel_pos,
        "entity_type_counts": type_counts,
        "discordant_rows": discordant_rows[:20],
        "novel_rows": novel_rows[:20],
        "nlp_only_flags": nlp_only_flags,
    }


# ---------------------------------------------------------------------------
# Domain 2: PATHOLOGY (fleet)
# ---------------------------------------------------------------------------
PATHOLOGY_MALIGNANT_TYPES = {
    "surgical_pathology", "histology_type", "ptc_variant", "malignant_histology"
}
PATHOLOGY_BENIGN_TYPES = {"benign_pathology"}
ETE_POSITIVE_TYPES = {"ete", "extrathyroidal_extension"}
VASC_POSITIVE_TYPES = {"vascular_invasion", "angioinvasion"}
LN_POSITIVE_TYPES = {"lymph_node_positive", "positive_ln"}


def crossval_pathology(con: duckdb.DuckDBPyConnection,
                        master: pd.DataFrame,
                        diag: pd.DataFrame) -> dict:
    print("\n[PATHOLOGY] Parsing fleet entities…")
    ents = combined_fleet_entities(
        con, "note_entities_llm_pathology",
        "scripts/output/fleet_pathology_new_rows.parquet"
    )

    can = master[["research_id", "ete_grade", "vascular_invasion_grade",
                   "ln_positive_flag", "ln_rollup_any_positive"]].copy()
    can_diag = diag[["research_id", "diagnosis_primary", "is_malignant"]].drop_duplicates("research_id")
    can = can.merge(can_diag, on="research_id", how="left")

    type_counts = ents.groupby("entity_type").size().sort_values(ascending=False).to_dict()

    concordant = discordant = 0
    discordant_rows = []
    novel_rows = []

    # Sub-check 1: malignant histology presence
    mal_pos_rids = set(ents[
        ents["entity_type"].isin(PATHOLOGY_MALIGNANT_TYPES) &
        (ents["present_or_negated"] == "present")
    ]["research_id"].unique())

    for rid in mal_pos_rids:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        is_mal = row["is_malignant"].values[0]
        if pd.isna(is_mal):
            novel_rows.append({
                "research_id": rid, "nlp_says": "malignant_histology",
                "canonical_says": "NULL", "sub_domain": "histology/malignancy"
            })
            continue
        if bool(is_mal):
            concordant += 1
        else:
            discordant += 1
            discordant_rows.append({
                "research_id": rid, "nlp_says": "malignant_histology",
                "canonical_says": f"is_malignant={is_mal}",
                "confidence": ents[ents["research_id"] == rid]["confidence"].max(),
            })

    # Sub-check 2: ETE
    ete_pos = ents[
        ents["entity_type"].isin(ETE_POSITIVE_TYPES) &
        (ents["present_or_negated"] == "present")
    ]["research_id"].unique()
    for rid in ete_pos:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        ete_grade = row["ete_grade"].values[0]
        if pd.isna(ete_grade) or ete_grade in ("", "none", "None"):
            novel_rows.append({
                "research_id": rid, "nlp_says": "ETE_present",
                "canonical_says": f"ete_grade={ete_grade}", "sub_domain": "ETE"
            })
        else:
            concordant += 1

    # Sub-check 3: Vascular invasion
    vasc_pos = ents[
        ents["entity_type"].isin(VASC_POSITIVE_TYPES) &
        (ents["present_or_negated"] == "present") &
        (~ents["entity_value"].fillna("").str.lower().isin(["absent", "none", "no", "negative"]))
    ]["research_id"].unique()
    for rid in vasc_pos:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        grade = row["vascular_invasion_grade"].values[0]
        if pd.isna(grade) or str(grade).lower() in ("none", "", "absent"):
            novel_rows.append({
                "research_id": rid, "nlp_says": "vascular_invasion_present",
                "canonical_says": f"grade={grade}", "sub_domain": "vascular_invasion"
            })
        else:
            concordant += 1

    total_assessed = concordant + discordant
    concordance_rate = concordant / total_assessed if total_assessed else 0.0

    # NLP-only high-confidence flags
    nlp_only_flags = []
    for item in novel_rows:
        best = ents[ents["research_id"] == item["research_id"]]
        if not best.empty and best["confidence"].max() >= HIGH_CONFIDENCE:
            brow = best.nlargest(1, "confidence").iloc[0]
            nlp_only_flags.append({
                "research_id": item["research_id"],
                "nlp_entity_type": brow["entity_type"],
                "nlp_entity_value": str(brow["entity_value"])[:80],
                "confidence": brow["confidence"],
                "canonical_field": item["sub_domain"],
                "canonical_value": item["canonical_says"],
            })

    return {
        "domain": "PATHOLOGY",
        "source_table": "note_entities_llm_pathology (fleet)",
        "model": "qwen3:32b + qwen3:14b",
        "n_notes_with_entities": ents["note_row_id"].nunique(),
        "n_patients": ents["research_id"].nunique(),
        "n_entities_total": len(ents),
        "mean_confidence": float(ents["confidence"].mean()),
        "concordance_rate": concordance_rate,
        "n_concordant": concordant,
        "n_discordant": discordant,
        "n_novel_findings": len(novel_rows),
        "entity_type_counts": type_counts,
        "discordant_rows": discordant_rows[:20],
        "novel_rows": novel_rows[:20],
        "nlp_only_flags": nlp_only_flags[:50],
    }


# ---------------------------------------------------------------------------
# Domain 3: TIRADS (fleet)
# ---------------------------------------------------------------------------
TIRADS_CAT_MAP = {
    "TR1": 1, "TR2": 2, "TR3": 3, "TR4": 4, "TR5": 5,
    "tr1": 1, "tr2": 2, "tr3": 3, "tr4": 4, "tr5": 5,
}


def _parse_tirads_category(val: str) -> int | None:
    if pd.isna(val):
        return None
    v = str(val).strip().upper()
    for k, n in TIRADS_CAT_MAP.items():
        if k.upper() in v:
            return n
    # Numeric score
    try:
        return int(float(v))
    except Exception:
        return None


def crossval_tirads(con: duckdb.DuckDBPyConnection,
                     master: pd.DataFrame) -> dict:
    print("\n[TIRADS] Parsing fleet entities…")
    ents = combined_fleet_entities(
        con, "note_entities_llm_tirads_granular",
        "scripts/output/fleet_tirads_granular_new_rows.parquet"
    )

    tirads_ents = ents[ents["entity_type"].isin(
        {"tirads_category", "tirads_score", "ti_rads"}
    )].copy()

    can = master[["research_id", "tirads_best_category_v12", "tirads_best_score_v12",
                   "tirads_worst_category_v12", "max_tirads_ever",
                   "preop_tirads_best", "preop_tirads_category"]].copy()

    type_counts = ents.groupby("entity_type").size().sort_values(ascending=False).to_dict()

    concordant = discordant = 0
    discordant_rows = []
    novel_rows = []

    # Per patient: NLP best TIRADS category (max score)
    nlp_per_patient = {}
    for rid, grp in tirads_ents.groupby("research_id"):
        cats = [_parse_tirads_category(v) for v in grp["entity_value"] if not pd.isna(v)]
        cats = [c for c in cats if c is not None]
        if cats:
            nlp_per_patient[rid] = max(cats)

    # Confusion matrix
    confusion = {}
    for rid, nlp_cat in nlp_per_patient.items():
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        can_cat = row["tirads_best_category_v12"].values[0]
        if pd.isna(can_cat) or str(can_cat).strip() == "":
            novel_rows.append({
                "research_id": rid, "nlp_says": f"TR{nlp_cat}",
                "canonical_says": "NULL", "sub_domain": "tirads"
            })
            continue
        # Normalize canonical
        can_num = _parse_tirads_category(str(can_cat))
        key = (nlp_cat, can_num)
        confusion[key] = confusion.get(key, 0) + 1
        if can_num is not None:
            if abs(nlp_cat - can_num) <= 1:
                concordant += 1
            else:
                discordant += 1
                discordant_rows.append({
                    "research_id": rid,
                    "nlp_says": f"TR{nlp_cat}",
                    "canonical_says": f"TR{can_num} (best_category_v12={can_cat})",
                    "confidence": tirads_ents[tirads_ents["research_id"] == rid]["confidence"].max(),
                })

    total_assessed = concordant + discordant
    concordance_rate = concordant / total_assessed if total_assessed else 0.0

    nlp_only_flags = [
        {
            "research_id": r["research_id"],
            "nlp_entity_type": "tirads_category",
            "nlp_entity_value": f"TR{nlp_per_patient.get(r['research_id'], '?')}",
            "confidence": tirads_ents[
                tirads_ents["research_id"] == r["research_id"]
            ]["confidence"].max() if r["research_id"] in nlp_per_patient else 0.0,
            "canonical_field": "tirads_best_category_v12",
            "canonical_value": "NULL",
        }
        for r in novel_rows
        if nlp_per_patient.get(r["research_id"], 0) >= 4
    ][:50]

    return {
        "domain": "TIRADS",
        "source_table": "note_entities_llm_tirads_granular (fleet)",
        "model": "qwen3:32b + qwen3:14b",
        "n_notes_with_entities": ents["note_row_id"].nunique(),
        "n_patients": ents["research_id"].nunique(),
        "n_entities_total": len(ents),
        "mean_confidence": float(ents["confidence"].mean()),
        "concordance_rate": concordance_rate,
        "n_concordant": concordant,
        "n_discordant": discordant,
        "n_novel_findings": len(novel_rows),
        "entity_type_counts": type_counts,
        "discordant_rows": discordant_rows[:20],
        "novel_rows": novel_rows[:20],
        "nlp_only_flags": nlp_only_flags,
        "confusion_matrix": {str(k): v for k, v in sorted(confusion.items())},
    }


# ---------------------------------------------------------------------------
# Domain 4: CERVICAL LN (fleet)
# ---------------------------------------------------------------------------
def crossval_cervical_ln(con: duckdb.DuckDBPyConnection,
                          master: pd.DataFrame) -> dict:
    """
    Actual entity types in note_entities_llm_cervical_ln_detail:
      ln_level (present=LN identified/dissected, negated=LN not found)
      ln_number_per_level, ln_size, fna_of_ln, etc.
    Cross-validation strategy:
      - Patients with ANY `ln_level` present entity → NLP found LN involvement
        (dissection or suspicious node). Compare to ln_rollup_any_positive.
      - Patients with ALL `ln_level` entities negated → NLP says no LN involvement.
        Compare to canonical ln_positive_flag.
      - ln_number_per_level / ln_size present → per-level data; compare to
        canonical ln_rollup_has_per_level_data.
    """
    print("\n[CERVICAL LN] Parsing fleet entities…")
    ents = combined_fleet_entities(
        con, "note_entities_llm_cervical_ln_detail",
        "scripts/output/fleet_cervical_ln_detail_new_rows.parquet"
    )

    can = master[[
        "research_id", "ln_positive_flag", "ln_rollup_any_positive",
        "ln_rollup_total_positive", "ln_total_positive", "tp_ln_positive",
        "ln_rollup_has_per_level_data",
    ]].copy()

    type_counts = ents.groupby("entity_type").size().sort_values(ascending=False).to_dict()

    # Per patient: any ln_level present = NLP found LN involvement
    ln_present_rids = set(ents[
        (ents["entity_type"] == "ln_level") &
        (ents["present_or_negated"] == "present")
    ]["research_id"].unique())

    # All ln_level entities negated for a patient → NLP says no LN involvement
    ln_all_negated_rids = set(ents[
        ents["entity_type"] == "ln_level"
    ]["research_id"].unique()) - ln_present_rids

    # Patients with per-level detail (ln_number_per_level)
    per_level_rids = set(ents[
        ents["entity_type"] == "ln_number_per_level"
    ]["research_id"].unique())

    concordant = discordant = 0
    discordant_rows = []
    novel_rows = []
    tp = tn = fp = fn = 0

    # Cross-validate: NLP found LN level involvement → canonical LN status
    for rid in ln_present_rids:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        # Canonical: any LN positive data?
        can_null = (
            pd.isna(row["ln_rollup_any_positive"].values[0]) and
            pd.isna(row["ln_positive_flag"].values[0]) and
            pd.isna(row["ln_rollup_total_positive"].values[0]) and
            pd.isna(row["tp_ln_positive"].values[0])
        )
        if can_null:
            novel_rows.append({
                "research_id": rid, "nlp_says": "ln_level_identified",
                "canonical_says": "all_ln_fields=NULL", "sub_domain": "cervical_ln"
            })
        else:
            concordant += 1
            tp += 1

    # Cross-validate: NLP says no LN → canonical should show no data or negative
    for rid in ln_all_negated_rids:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        can_pos = bool(row["ln_rollup_any_positive"].values[0]) \
            if pd.notna(row["ln_rollup_any_positive"].values[0]) else None
        if can_pos is None:
            tn += 1
            concordant += 1
        elif not can_pos:
            tn += 1
            concordant += 1
        else:
            fn += 1
            discordant += 1
            discordant_rows.append({
                "research_id": rid, "nlp_says": "no_ln_involvement",
                "canonical_says": "ln_rollup_any_positive=True",
                "confidence": ents[ents["research_id"] == rid]["confidence"].max(),
            })

    # Cross-validate per-level data
    for rid in per_level_rids:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        has_per_level = row["ln_rollup_has_per_level_data"].values[0]
        if pd.isna(has_per_level) or not bool(has_per_level):
            novel_rows.append({
                "research_id": rid, "nlp_says": "ln_number_per_level_data",
                "canonical_says": "ln_rollup_has_per_level_data=NULL/False",
                "sub_domain": "ln_per_level"
            })
        else:
            concordant += 1

    total_assessed = concordant + discordant
    concordance_rate = concordant / total_assessed if total_assessed else 0.0

    # Sensitivity / PPV (NLP detected LN vs structured confirmed)
    sensitivity = tp / (tp + fn) if (tp + fn) > 0 else None
    ppv = tp / (tp + 0) if tp > 0 else None  # No FP counted for presence check

    nlp_only_flags = []
    for r in novel_rows:
        if r["sub_domain"] != "cervical_ln":
            continue
        rid = r["research_id"]
        rid_ents = ents[ents["research_id"] == rid]
        if rid_ents.empty or rid_ents["confidence"].max() < HIGH_CONFIDENCE:
            continue
        ln_ents = rid_ents[rid_ents["entity_type"] == "ln_level"]
        ev = str(ln_ents.iloc[0]["entity_value"])[:80] if not ln_ents.empty else ""
        nlp_only_flags.append({
            "research_id": rid,
            "nlp_entity_type": "ln_level",
            "nlp_entity_value": ev,
            "confidence": rid_ents["confidence"].max(),
            "canonical_field": "ln_rollup_any_positive / ln_positive_flag",
            "canonical_value": "NULL",
        })
    nlp_only_flags = nlp_only_flags[:50]

    return {
        "domain": "CERVICAL LN",
        "source_table": "note_entities_llm_cervical_ln_detail (fleet)",
        "model": "qwen3:32b + qwen3:14b",
        "n_notes_with_entities": ents["note_row_id"].nunique(),
        "n_patients": ents["research_id"].nunique(),
        "n_entities_total": len(ents),
        "mean_confidence": float(ents["confidence"].mean()),
        "concordance_rate": concordance_rate,
        "n_concordant": concordant,
        "n_discordant": discordant,
        "n_novel_findings": len(novel_rows),
        "entity_type_counts": type_counts,
        "discordant_rows": discordant_rows[:20],
        "novel_rows": novel_rows[:20],
        "nlp_only_flags": nlp_only_flags,
        "sensitivity_nlp_vs_structured": sensitivity,
        "ppv_nlp_vs_structured": ppv,
        "tp": tp, "tn": tn, "fp": fp, "fn": fn,
    }


# ---------------------------------------------------------------------------
# Domain 5: VASCULAR INVASION
# ---------------------------------------------------------------------------
VASC_POSITIVE_VALUES = {"present", "focal", "extensive", "angioinvasion", "vascular invasion"}
VASC_NEGATIVE_VALUES = {"absent", "no", "none", "negative", "not identified"}


def crossval_vascular_invasion(con: duckdb.DuckDBPyConnection,
                                master: pd.DataFrame) -> dict:
    print("\n[VASCULAR INVASION] Parsing entities…")
    ents = flatten_entities(con, "note_entities_llm_vascular_invasion")

    can = master[["research_id", "vascular_invasion_grade",
                   "vascular_who_2022_grade"]].copy()

    type_counts = ents.groupby("entity_type").size().sort_values(ascending=False).to_dict()

    nlp_pos = set(ents[
        (ents["entity_type"] == "vascular_invasion") &
        (ents["present_or_negated"] == "present") &
        (~ents["entity_value"].fillna("").str.lower().isin(VASC_NEGATIVE_VALUES))
    ]["research_id"].unique())

    nlp_neg = set(ents[
        (ents["entity_type"] == "vascular_invasion") &
        (
            (ents["present_or_negated"] == "negated") |
            ents["entity_value"].fillna("").str.lower().isin(VASC_NEGATIVE_VALUES)
        )
    ]["research_id"].unique()) - nlp_pos

    concordant = discordant = 0
    discordant_rows = []
    novel_rows = []

    for rid in nlp_pos:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        grade = str(row["vascular_invasion_grade"].values[0]).lower()
        if grade in ("nan", "none", "", "absent"):
            novel_rows.append({
                "research_id": rid, "nlp_says": "vascular_invasion_present",
                "canonical_says": f"grade={grade}", "sub_domain": "vascular_invasion"
            })
        else:
            concordant += 1

    for rid in nlp_neg:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        grade = str(row["vascular_invasion_grade"].values[0]).lower()
        if grade in ("nan", "none", "", "absent"):
            concordant += 1
        else:
            discordant += 1
            discordant_rows.append({
                "research_id": rid, "nlp_says": "vascular_invasion_absent",
                "canonical_says": f"vascular_invasion_grade={grade}",
                "confidence": ents[ents["research_id"] == rid]["confidence"].max(),
            })

    total_assessed = concordant + discordant
    concordance_rate = concordant / total_assessed if total_assessed else 0.0

    nlp_only_flags = [
        {
            "research_id": r["research_id"],
            "nlp_entity_type": "vascular_invasion",
            "nlp_entity_value": str(ents[ents["research_id"] == r["research_id"]].iloc[0]["entity_value"])[:80],
            "confidence": ents[ents["research_id"] == r["research_id"]]["confidence"].max(),
            "canonical_field": "vascular_invasion_grade",
            "canonical_value": "NULL/absent",
        }
        for r in novel_rows
        if ents[ents["research_id"] == r["research_id"]]["confidence"].max() >= HIGH_CONFIDENCE
    ][:50]

    return {
        "domain": "VASCULAR INVASION",
        "source_table": "note_entities_llm_vascular_invasion",
        "model": "qwen3:32b",
        "n_notes_with_entities": ents["note_row_id"].nunique(),
        "n_patients": ents["research_id"].nunique(),
        "n_entities_total": len(ents),
        "mean_confidence": float(ents["confidence"].mean()),
        "concordance_rate": concordance_rate,
        "n_concordant": concordant,
        "n_discordant": discordant,
        "n_novel_findings": len(novel_rows),
        "entity_type_counts": type_counts,
        "discordant_rows": discordant_rows[:20],
        "novel_rows": novel_rows[:20],
        "nlp_only_flags": nlp_only_flags,
    }


# ---------------------------------------------------------------------------
# Domain 6: TG KINETICS
# ---------------------------------------------------------------------------
def crossval_tg_kinetics(con: duckdb.DuckDBPyConnection,
                          master: pd.DataFrame) -> dict:
    """
    Actual entity types in note_entities_llm_tg_kinetics (sparse: 173 entities, 61 patients):
      tg_value      — numeric Tg measurement (present=measurable, negated=undetectable)
      anti_tg_value — anti-Tg measurement
      tg_trend      — explicit trend statement (rare: 1 entity total)
      paired_tsh    — TSH at time of Tg
    Cross-validation strategy:
      - tg_value present with numeric > 2.0 → elevated, compare to canonical tg_peak / tg_rising_flag
      - tg_value negated (<0.1, undetectable) → suppressed, compare to tg_nadir / tg_trajectory_class
      - Any tg_value present → NLP found Tg data; compare to tg_n_measurements (structured)
      - Note: very sparse table (61 patients) — mainly coverage assessment.
    """
    print("\n[TG KINETICS] Parsing entities…")
    ents = flatten_entities(con, "note_entities_llm_tg_kinetics")

    can = master[["research_id", "tg_trajectory_class", "tg_rising_flag",
                   "tg_nadir", "tg_peak", "tg_n_measurements",
                   "tg_last_value", "tg_mean"]].copy()

    type_counts = ents.groupby("entity_type").size().sort_values(ascending=False).to_dict()

    # Parse numeric tg_values
    tg_val_ents = ents[ents["entity_type"] == "tg_value"].copy()

    def _parse_tg(v: str) -> float | None:
        if pd.isna(v):
            return None
        v = str(v).strip().lstrip("<").lstrip(">").split()[0].replace(",", "")
        try:
            return float(v)
        except Exception:
            return None

    tg_val_ents = tg_val_ents.copy()
    tg_val_ents["tg_numeric"] = tg_val_ents["entity_value"].apply(_parse_tg)

    # Per patient: max NLP Tg value
    nlp_tg_max = tg_val_ents[tg_val_ents["tg_numeric"].notna()].groupby("research_id")["tg_numeric"].max()
    nlp_tg_undetectable = set(tg_val_ents[
        (tg_val_ents["present_or_negated"] == "negated") &
        (tg_val_ents["tg_numeric"].fillna(999) < 0.5)
    ]["research_id"].unique())

    concordant = discordant = 0
    discordant_rows = []
    novel_rows = []

    # Check: NLP found Tg values → canonical should have Tg measurements
    all_tg_rids = set(tg_val_ents["research_id"].unique())
    for rid in all_tg_rids:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        n_meas = row["tg_n_measurements"].values[0]
        if pd.isna(n_meas) or n_meas == 0:
            novel_rows.append({
                "research_id": rid,
                "nlp_says": f"tg_value={nlp_tg_max.get(rid, '?')}",
                "canonical_says": "tg_n_measurements=NULL/0",
                "sub_domain": "tg_kinetics"
            })
        else:
            concordant += 1

    # Check: NLP high Tg (>10) → canonical tg_rising_flag or tg_peak > 10
    for rid, nlp_val in nlp_tg_max.items():
        if nlp_val < 10:
            continue
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        can_peak = row["tg_peak"].values[0]
        can_rising = row["tg_rising_flag"].values[0]
        if pd.isna(can_peak) and pd.isna(can_rising):
            # Already counted in novel_rows above
            pass
        elif (pd.notna(can_peak) and can_peak >= 10) or can_rising is True:
            concordant += 1
        elif pd.notna(can_peak) and can_peak < 2.0:
            discordant += 1
            discordant_rows.append({
                "research_id": rid,
                "nlp_says": f"tg_value={nlp_val:.1f} (elevated)",
                "canonical_says": f"tg_peak={can_peak:.2f}",
                "confidence": tg_val_ents[tg_val_ents["research_id"] == rid]["confidence"].max(),
            })

    # Check: NLP undetectable Tg → canonical tg_nadir should be low
    for rid in nlp_tg_undetectable:
        row = can[can["research_id"] == rid]
        if row.empty:
            continue
        traj = str(row["tg_trajectory_class"].values[0]).lower()
        can_nadir = row["tg_nadir"].values[0]
        if pd.isna(row["tg_trajectory_class"].values[0]) and pd.isna(can_nadir):
            pass  # Already in novel_rows
        elif "suppress" in traj or "undetect" in traj or "remis" in traj:
            concordant += 1
        elif pd.notna(can_nadir) and can_nadir < 1.0:
            concordant += 1

    total_assessed = concordant + discordant
    concordance_rate = concordant / total_assessed if total_assessed else 0.0

    nlp_only_flags = [
        {
            "research_id": r["research_id"],
            "nlp_entity_type": "tg_value",
            "nlp_entity_value": r["nlp_says"],
            "confidence": tg_val_ents[tg_val_ents["research_id"] == r["research_id"]]["confidence"].max()
                if r["research_id"] in tg_val_ents["research_id"].values else 0.0,
            "canonical_field": "tg_n_measurements",
            "canonical_value": "NULL/0",
        }
        for r in novel_rows
        if tg_val_ents[tg_val_ents["research_id"] == r["research_id"]]["confidence"].max() >= HIGH_CONFIDENCE
        if len(tg_val_ents[tg_val_ents["research_id"] == r["research_id"]]) > 0
    ][:50]

    return {
        "domain": "TG KINETICS",
        "source_table": "note_entities_llm_tg_kinetics",
        "model": "qwen3:32b",
        "n_notes_with_entities": ents["note_row_id"].nunique(),
        "n_patients": ents["research_id"].nunique(),
        "n_entities_total": len(ents),
        "mean_confidence": float(ents["confidence"].mean()),
        "concordance_rate": concordance_rate,
        "n_concordant": concordant,
        "n_discordant": discordant,
        "n_novel_findings": len(novel_rows),
        "entity_type_counts": type_counts,
        "discordant_rows": discordant_rows[:20],
        "novel_rows": novel_rows[:20],
        "nlp_only_flags": nlp_only_flags,
    }


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------
def _fmt_confidence(c: float | None) -> str:
    return f"{c:.3f}" if c is not None else "N/A"


def _concordance_bar(rate: float) -> str:
    filled = int(rate * 20)
    return "█" * filled + "░" * (20 - filled) + f"  {rate:.1%}"


def write_report(results: list[dict], run_ts: str) -> None:
    lines = []
    w = lines.append

    w("# NLP Entity Cross-Validation Report")
    w(f"> Generated: {run_ts}  ")
    w("> Database: `thyroid_ete_fix_20260413`  ")
    w(f"> Confidence threshold: {MIN_CONFIDENCE} (entity filter), {HIGH_CONFIDENCE} (novel-finding flag)")
    w("> **CANONICAL DATA IS READ-ONLY — no modifications made.**")
    w("")

    # ---- Summary table ----
    w("## 1. Summary Table")
    w("")
    header = ("| Domain | Source table | Notes w/ entities | Patients | "
               "Entities | Mean conf | Concordance | Discordant | Novel findings |")
    w(header)
    w("|" + "|".join(["---"] * 8) + "|")
    for r in results:
        w(
            f"| {r['domain']} | {r['source_table']} | "
            f"{r['n_notes_with_entities']:,} | {r['n_patients']:,} | "
            f"{r['n_entities_total']:,} | {_fmt_confidence(r['mean_confidence'])} | "
            f"{r['concordance_rate']:.1%} | {r['n_discordant']:,} | "
            f"{r['n_novel_findings']:,} |"
        )
    w("")

    # ---- Concordance bar chart ----
    w("### Concordance Overview")
    w("```")
    for r in results:
        name = r["domain"].ljust(20)
        w(f"{name} {_concordance_bar(r['concordance_rate'])}")
    w("```")
    w("")

    # ---- Per-domain detail ----
    w("## 2. Per-Domain Detail")
    w("")

    for r in results:
        domain = r["domain"]
        w(f"### 2.{results.index(r)+1}  {domain}")
        w(f"**Source:** `{r['source_table']}` | **Model:** `{r['model']}`")
        w("")
        w(f"- Notes with entities: **{r['n_notes_with_entities']:,}**")
        w(f"- Patients with entities: **{r['n_patients']:,}**")
        w(f"- Total entities (conf ≥ {MIN_CONFIDENCE}): **{r['n_entities_total']:,}**")
        w(f"- Mean confidence: **{_fmt_confidence(r['mean_confidence'])}**")
        w(f"- Concordance vs canonical: **{r['concordance_rate']:.1%}** "
           f"({r['n_concordant']:,} concordant, {r['n_discordant']:,} discordant)")
        w(f"- Novel findings (NLP only, no structured counterpart): **{r['n_novel_findings']:,}**")
        w("")

        # Sensitivity/PPV for LN
        if "sensitivity_nlp_vs_structured" in r and r["sensitivity_nlp_vs_structured"] is not None:
            w(f"- NLP sensitivity vs structured LN positive: **{r['sensitivity_nlp_vs_structured']:.1%}**")
            w(f"- NLP PPV vs structured LN positive: **{r['ppv_nlp_vs_structured']:.1%}**")
            w(f"- TP/TN/FP/FN: {r['tp']}/{r['tn']}/{r['fp']}/{r['fn']}")
            w("")

        # Confusion matrix for TIRADS
        if "confusion_matrix" in r and r["confusion_matrix"]:
            w("#### TIRADS Category Confusion (NLP score, canonical score) → count")
            w("```")
            for k, v in sorted(r["confusion_matrix"].items()):
                w(f"  NLP={k[1:3]} canonical={k[-2:]}: {v}")
            w("```")
            w("")

        # Entity type breakdown
        w("#### Entity Type Breakdown")
        w("| entity_type | count |")
        w("|---|---|")
        for et, cnt in list(r["entity_type_counts"].items())[:15]:
            w(f"| `{et}` | {cnt:,} |")
        w("")

        # Discordant patients
        if r["discordant_rows"]:
            w("#### Top Discordant Patients (NLP ≠ Canonical)")
            w("| research_id | NLP says | Canonical says | Confidence |")
            w("|---|---|---|---|")
            for row in r["discordant_rows"][:20]:
                conf = _fmt_confidence(row.get("confidence"))
                w(f"| {row['research_id']} | {row['nlp_says']} | "
                   f"{row['canonical_says']} | {conf} |")
            w("")

        # Novel findings
        if r["novel_rows"]:
            w("#### Novel Findings (NLP present, canonical NULL)")
            w("| research_id | NLP says | Canonical says | Sub-domain |")
            w("|---|---|---|---|")
            for row in r["novel_rows"][:20]:
                sub = row.get("sub_domain", "")
                w(f"| {row['research_id']} | {row['nlp_says']} | "
                   f"{row['canonical_says']} | {sub} |")
            w("")

    # ---- NLP-only flags section ----
    w("## 3. NLP-Only Flags (No Structured Data Counterpart, conf ≥ 0.7)")
    w("")
    w("These patients have **high-confidence NLP entities** but NULL in the corresponding "
       "canonical column. Highest priority for manual review / future gap-filling.")
    w("")

    total_novel_high = 0
    for r in results:
        flags = r.get("nlp_only_flags", [])
        total_novel_high += len(flags)
        if flags:
            w(f"### {r['domain']} — {len(flags)} NLP-only patients (conf ≥ 0.7, canonical NULL)")
            w("")
            w("| research_id | entity_type | entity_value | confidence | canonical_field | canonical_value |")
            w("|---|---|---|---|---|---|")
            for f in flags[:30]:
                w(f"| {f['research_id']} | `{f['nlp_entity_type']}` | "
                   f"{str(f['nlp_entity_value'])[:60]} | {_fmt_confidence(f['confidence'])} | "
                   f"`{f['canonical_field']}` | {f['canonical_value']} |")
            w("")

    w(f"**Total NLP-only high-confidence flags across all domains: {total_novel_high:,}**")
    w("")

    # ---- Recommendations ----
    w("## 4. Recommendations")
    w("")
    w("### Which NLP entities are trustworthy enough to backfill canonical gaps?")
    w("")

    for r in results:
        rate = r["concordance_rate"]
        verdict = "✅ TRUSTWORTHY" if rate >= 0.80 else ("⚠️ CONDITIONAL" if rate >= 0.60 else "❌ UNRELIABLE")
        w(f"- **{r['domain']}** ({rate:.1%} concordance): {verdict}")
        if rate >= 0.80:
            w(f"  → Candidates for NLP-assisted backfill. Novel findings ({r['n_novel_findings']:,}) "
               "can be routed to structured gap-fill (future script).")
        elif rate >= 0.60:
            w("  → Use with caution. Manual review of discordant cases recommended before backfill.")
        else:
            w("  → Do NOT backfill without manual adjudication. High discordance suggests "
               "entity type mismatch or boilerplate contamination.")
    w("")

    w("### Domains with too many discordances to trust without review")
    w("")
    for r in results:
        if r["n_discordant"] > 50:
            w(f"- **{r['domain']}**: {r['n_discordant']:,} discordant cases — root-cause "
               "investigation recommended before any backfill.")
    w("")

    w("### Novel findings warranting manual review")
    w("")
    for r in results:
        if r["n_novel_findings"] > 10:
            w(f"- **{r['domain']}**: {r['n_novel_findings']:,} patients with NLP evidence "
               "but no structured data — highest value for data enrichment. "
               "Cross-reference with source notes before promotion.")
    w("")

    w("---")
    w("*Report generated by `scripts/209_nlp_entity_crossvalidation.py`*  ")
    w(f"*Database: `thyroid_ete_fix_20260413` | Run: {run_ts}*")

    text = "\n".join(lines)
    REPORT_PATH.write_text(text, encoding="utf-8")
    print(f"\n[REPORT] Written to {REPORT_PATH}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    run_ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"=== Script 209: NLP Entity Cross-Validation | {run_ts} ===\n")

    con = connect_md()

    # Load canonical master once (read-only)
    print("[CANONICAL] Loading canonical_patient_master_v1…")
    master = con.execute("SELECT * FROM canonical_patient_master_v1").fetchdf()
    print(f"  master rows={len(master):,}, patients={master['research_id'].nunique():,}")

    print("[CANONICAL] Loading canonical_diagnosis_unified_v1…")
    diag = con.execute("SELECT * FROM canonical_diagnosis_unified_v1").fetchdf()
    print(f"  diag rows={len(diag):,}, patients={diag['research_id'].nunique():,}")

    results = []

    results.append(crossval_recurrence(con, master))
    results.append(crossval_pathology(con, master, diag))
    results.append(crossval_tirads(con, master))
    results.append(crossval_cervical_ln(con, master))
    results.append(crossval_vascular_invasion(con, master))
    results.append(crossval_tg_kinetics(con, master))

    # Summary print
    print("\n" + "=" * 70)
    print("CROSS-VALIDATION SUMMARY")
    print("=" * 70)
    print(f"{'Domain':<22} {'Concordance':>12} {'Discordant':>12} {'Novel':>8}")
    print("-" * 60)
    for r in results:
        print(f"{r['domain']:<22} {r['concordance_rate']:>11.1%} "
              f"{r['n_discordant']:>12,} {r['n_novel_findings']:>8,}")

    write_report(results, run_ts)

    print("\n✅ Cross-validation complete. Report written to:")
    print(f"   {REPORT_PATH}")


if __name__ == "__main__":
    main()
