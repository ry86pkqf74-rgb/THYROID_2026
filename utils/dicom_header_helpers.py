"""DICOM flattened-header ingest helpers (deterministic normalization + linkage prep).

No PHI in logs; callers must not print raw payloads at scale in production pipelines.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pandas as pd
import yaml

try:
    import pydicom
except ImportError:  # pragma: no cover
    pydicom = None  # type: ignore[misc, assignment]

_UID_RE = re.compile(r"^[0-9.]+$")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")


def repo_root_from_here() -> Path:
    return Path(__file__).resolve().parent.parent


def default_alias_path() -> Path:
    return repo_root_from_here() / "config" / "dicom_header_aliases.yml"


def load_alias_config(path: Path | None = None) -> dict[str, Any]:
    p = path or default_alias_path()
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or "canonical_fields" not in raw:
        raise ValueError("dicom_header_aliases.yml must contain a canonical_fields mapping")
    return raw


def build_column_lookup(
    canonical_fields: Mapping[str, Sequence[str]],
) -> dict[str, str]:
    """Map lowercased stripped input column → canonical field key."""
    out: dict[str, str] = {}
    for canon, aliases in canonical_fields.items():
        for a in aliases:
            key = str(a).strip().lower()
            if key in out and out[key] != canon:
                raise ValueError(f"Alias collision: {key!r} maps to {out[key]} and {canon}")
            out[key] = str(canon)
        c = str(canon).strip().lower()
        out[c] = str(canon)
    return out


def normalize_column_name(name: str) -> str:
    return str(name).strip().lower()


# CSV / flattened-export column names (must match config/dicom_header_aliases.yml DICOM tags).
_DICOM_FLAT_HEADER_COLUMNS: tuple[str, ...] = (
    "StudyInstanceUID",
    "SeriesInstanceUID",
    "AccessionNumber",
    "StudyDate",
    "SeriesDate",
    "Modality",
    "BodyPartExamined",
    "StudyDescription",
    "SeriesDescription",
    "PatientID",
    "InstitutionName",
)


def require_pydicom() -> None:
    if pydicom is None:
        raise ImportError(
            "Reading .dcm files requires pydicom; install with `pip install pydicom`.",
        )


def _format_dicom_scalar(v: Any) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _format_dicom_element(elem: Any) -> str:
    """Stringify a pydicom DataElement without decoding pixel or other bulk data."""
    from pydicom.dataelem import DataElement as _DE

    if isinstance(elem, _DE):
        if getattr(elem, "keyword", None) == "PixelData":
            return ""
        v = elem.value
    else:
        v = elem
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode(errors="replace").strip()
    if isinstance(v, (list, tuple)):
        return "\\".join(_format_dicom_scalar(x) for x in v)
    return _format_dicom_scalar(v)


def read_dicom_metadata_flat_row(path: Path) -> dict[str, str]:
    """Read DICOM metadata only (no pixel decode). Keys match flattened CSV export columns.

    On read failure or missing tags, returns empty strings for affected fields so downstream
    QC/review routing matches the flattened path.
    """
    require_pydicom()
    empty = {c: "" for c in _DICOM_FLAT_HEADER_COLUMNS}
    try:
        ds = pydicom.dcmread(  # type: ignore[union-attr]
            str(path),
            stop_before_pixels=True,
            force=True,
        )
    except Exception:
        return dict(empty)
    out = dict(empty)
    for name in _DICOM_FLAT_HEADER_COLUMNS:
        elem = ds.get(name)
        if elem is None:
            continue
        try:
            out[name] = _format_dicom_element(elem)
        except Exception:
            out[name] = ""
    return out


def read_dicom_files(paths: Sequence[Path]) -> pd.DataFrame:
    """Load one row per file, same logical columns as flattened CSV/XLSX (all string dtype)."""
    rows: list[dict[str, str]] = []
    for p in paths:
        row = read_dicom_metadata_flat_row(p)
        row["_source_file"] = str(p.resolve())
        rows.append(row)
    return pd.DataFrame(rows)


def normalize_accession_key(val: Any) -> str | None:
    """Align with imaging_fna_linkage_mm_v1 SQL: lower + strip non-alphanumeric."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip().lower()
    if not s:
        return None
    s = _NON_ALNUM.sub("", s)
    return s or None


def normalize_uid(val: Any) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    return s or None


def is_plausible_dicom_uid(uid: str | None) -> bool:
    if not uid or len(uid) < 5:
        return False
    if not _UID_RE.match(uid):
        return False
    return uid.replace(".", "").isdigit()


def normalize_dicom_date(val: Any) -> tuple[str | None, str | None]:
    """Return (raw_string, yyyymmdd_or_none)."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None, None
    if isinstance(val, datetime):
        return val.strftime("%Y%m%d"), val.strftime("%Y%m%d")
    if isinstance(val, date):
        return val.strftime("%Y%m%d"), val.strftime("%Y%m%d")
    s = str(val).strip()
    if not s:
        return None, None
    digits = re.sub(r"\D", "", s)
    if len(digits) >= 8:
        ymd = digits[:8]
        return s, ymd
    if len(digits) == 6:
        return s, digits + "01"
    return s, None


def stable_json_dict(row: Mapping[str, Any]) -> dict[str, Any]:
    def _norm(v: Any) -> Any:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, (datetime, date)):
            return v.isoformat()
        return v

    return {k: _norm(row[k]) for k in sorted(row.keys(), key=lambda x: str(x).lower())}


def row_fingerprint_sha256(row: Mapping[str, Any]) -> str:
    payload = stable_json_dict(row)
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def canonicalize_row(
    row: Mapping[str, Any],
    col_lookup: Mapping[str, str],
    *extra_raw_pairs: tuple[str, Any],
) -> dict[str, Any]:
    """Project a row onto canonical keys; unknown columns preserved only via caller's raw payload."""
    out: dict[str, Any] = {k: None for k in sorted(set(col_lookup.values()))}
    out["_extra_columns"] = {}
    for k_raw, v in row.items():
        kn = normalize_column_name(str(k_raw))
        if kn in col_lookup:
            out[col_lookup[kn]] = v
        else:
            out["_extra_columns"][str(k_raw)] = v
    for ek, ev in extra_raw_pairs:
        out["_extra_columns"][ek] = ev
    return out


def read_input_files(
    paths: Sequence[Path],
    fmt: str,
) -> pd.DataFrame:
    """Load one or more files into a single frame with original column names."""
    frames: list[pd.DataFrame] = []
    for p in paths:
        f = fmt
        if f == "auto":
            suf = p.suffix.lower()
            if suf == ".csv":
                f = "csv"
            elif suf in (".xlsx", ".xls"):
                f = "xlsx"
            elif suf == ".json":
                f = "json"
            elif suf == ".parquet":
                f = "parquet"
            elif suf == ".dcm":
                f = "dcm"
            else:
                raise ValueError(f"Cannot infer format for {p}")
        if f == "dcm" and p.suffix.lower() != ".dcm":
            raise ValueError(f"--format dcm expects .dcm files; got {p}")
        if f != "dcm" and p.suffix.lower() == ".dcm":
            raise ValueError(
                f"{p} is a DICOM file; use --format auto or dcm (not {f!r}).",
            )
        if f == "dcm":
            frames.append(read_dicom_files([p]))
            continue
        if f == "csv":
            df = pd.read_csv(p, dtype=str, keep_default_na=False)
        elif f == "xlsx":
            df = pd.read_excel(p, dtype=str)
        elif f == "json":
            blob = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(blob, dict) and "rows" in blob:
                blob = blob["rows"]
            if not isinstance(blob, list):
                raise ValueError("JSON must be array or {rows: [...]}")
            df = pd.DataFrame(blob)
        elif f == "parquet":
            df = pd.read_parquet(p)
        else:
            raise ValueError(f"Unknown format {f}")
        df["_source_file"] = str(p.resolve())
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def build_enriched_rows(df: pd.DataFrame, col_lookup: Mapping[str, str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pos, (_, rec) in enumerate(df.iterrows(), start=1):
        raw: dict[str, Any] = {str(k): v for k, v in rec.to_dict().items()}
        src = raw.pop("_source_file", None)
        c = canonicalize_row(raw, col_lookup)
        extra = c.pop("_extra_columns", {})
        c["source_file"] = src
        c["source_row_number"] = pos
        c["raw_payload_json"] = json.dumps(raw, sort_keys=True, default=str)
        c["row_fingerprint_sha256"] = row_fingerprint_sha256(raw)
        if extra:
            c["raw_payload_json"] = json.dumps(
                {"row": raw, "unmapped_columns": extra},
                sort_keys=True,
                default=str,
            )
            c["row_fingerprint_sha256"] = row_fingerprint_sha256(
                {"row": raw, "unmapped": extra},
            )
        rows.append(c)
    return rows


def _qc_list(*flags: str | None) -> list[str]:
    return [f for f in flags if f]


def rows_to_study_series(
    enriched: Sequence[Mapping[str, Any]],
    *,
    ingestion_run_id: str,
    ingestion_ts: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Returns (provenance_df, study_df, series_df).
    """
    ts = ingestion_ts or datetime.now(timezone.utc)
    prov_rows: list[dict[str, Any]] = []
    study_acc: dict[str, dict[str, Any]] = {}
    series_acc: dict[str, dict[str, Any]] = {}

    for er in enriched:
        su = normalize_uid(er.get("study_instance_uid"))
        seu = normalize_uid(er.get("series_instance_uid"))
        sou = normalize_uid(er.get("sop_instance_uid"))
        acc_raw = er.get("accession_number")
        acc_n = normalize_accession_key(acc_raw)
        sd_raw, sd_n = normalize_dicom_date(er.get("study_date"))
        serd_raw, serd_n = normalize_dicom_date(er.get("series_date"))

        qc: list[str] = []
        parse_status = "ok"
        if su and not is_plausible_dicom_uid(su):
            qc.append("MALFORMED_STUDY_INSTANCE_UID")
            parse_status = "warn"
        if seu and not is_plausible_dicom_uid(seu):
            qc.append("MALFORMED_SERIES_INSTANCE_UID")
            parse_status = "warn"
        if not su:
            qc.append("MISSING_STUDY_INSTANCE_UID")
            parse_status = "error"

        prov_rows.append(
            {
                "source_file": er.get("source_file"),
                "source_row_number": int(er.get("source_row_number") or 0),
                "raw_payload_json": er.get("raw_payload_json"),
                "row_fingerprint_sha256": er.get("row_fingerprint_sha256"),
                "ingestion_ts": ts,
                "ingestion_run_id": ingestion_run_id,
                "parse_status": parse_status,
                "qc_flags_json": json.dumps(_qc_list(*qc)),
                "study_instance_uid": su,
                "series_instance_uid": seu,
                "sop_instance_uid": sou,
            },
        )

        if not su:
            continue

        if su not in study_acc:
            study_acc[su] = {
                "study_instance_uid": su,
                "study_date_raw": sd_raw,
                "study_date_normalized": sd_n,
                "accession_number_raw": str(acc_raw) if acc_raw is not None and str(acc_raw) != "" else None,
                "accession_norm": acc_n,
                "study_description_raw": er.get("study_description"),
                "institution_name_raw": er.get("institution_name"),
                "patient_id_raw": er.get("patient_id"),
                "research_id_explicit": None,
                "modalities_seen": set(),
                "body_parts_seen": set(),
                "n_source_rows": 0,
                "qc_flags": set(qc),
                "accession_norm_distinct": set(),
            }
        st = study_acc[su]
        st["n_source_rows"] += 1
        st["qc_flags"].update(qc)
        if acc_n:
            st["accession_norm_distinct"].add(acc_n)
        m = er.get("modality")
        if m is not None and str(m).strip():
            st["modalities_seen"].add(str(m).strip())
        bp = er.get("body_part_examined")
        if bp is not None and str(bp).strip():
            st["body_parts_seen"].add(str(bp).strip())
        rid = er.get("research_id")
        if rid is not None and str(rid).strip() != "":
            try:
                rv = int(float(str(rid).strip()))
                if st["research_id_explicit"] is None:
                    st["research_id_explicit"] = rv
                elif st["research_id_explicit"] != rv:
                    st["qc_flags"].add("DISCORDANT_EXPLICIT_RESEARCH_ID")
            except (TypeError, ValueError):
                st["qc_flags"].add("INVALID_EXPLICIT_RESEARCH_ID")
        if sd_raw and not st.get("study_date_raw"):
            st["study_date_raw"] = sd_raw
            st["study_date_normalized"] = sd_n
        if st["accession_norm"] is None and acc_n:
            st["accession_norm"] = acc_n
            st["accession_number_raw"] = (
                str(acc_raw) if acc_raw is not None and str(acc_raw) != "" else None
            )
        elif acc_n and st["accession_norm"] and acc_n != st["accession_norm"]:
            st["qc_flags"].add("DISCORDANT_ACCESSION_WITHIN_STUDY")

        if seu and is_plausible_dicom_uid(seu):
            if seu not in series_acc:
                series_acc[seu] = {
                    "series_instance_uid": seu,
                    "study_instance_uid": su,
                    "series_date_raw": serd_raw,
                    "series_date_normalized": serd_n,
                    "modality_raw": er.get("modality"),
                    "body_part_examined_raw": er.get("body_part_examined"),
                    "series_description_raw": er.get("series_description"),
                    "institution_name_raw": er.get("institution_name"),
                    "patient_id_raw": er.get("patient_id"),
                    "n_source_rows": 0,
                    "qc_flags": set(qc),
                    "study_instance_uid_distinct": set(),
                }
            se = series_acc[seu]
            se["n_source_rows"] += 1
            se["qc_flags"].update(qc)
            se["study_instance_uid_distinct"].add(su)
            if len(se["study_instance_uid_distinct"]) > 1:
                se["qc_flags"].add("SERIES_UID_MULTI_STUDY_CONFLICT")

    study_rows_out: list[dict[str, Any]] = []
    for su, st in study_acc.items():
        flags = sorted(st["qc_flags"])
        acc_set = st["accession_norm_distinct"]
        if len(acc_set) > 1:
            flags.append("DISCORDANT_ACCESSION_WITHIN_STUDY")
        study_rows_out.append(
            {
                "study_instance_uid": su,
                "study_date_raw": st["study_date_raw"],
                "study_date_normalized": st["study_date_normalized"],
                "accession_number_raw": st["accession_number_raw"],
                "accession_norm": st["accession_norm"],
                "study_description_raw": st["study_description_raw"],
                "institution_name_raw": st["institution_name_raw"],
                "patient_id_raw": st["patient_id_raw"],
                "research_id_explicit": st["research_id_explicit"],
                "modality_summary": "|".join(sorted(st["modalities_seen"])),
                "body_part_examined_summary": "|".join(sorted(st["body_parts_seen"])),
                "n_source_rows": st["n_source_rows"],
                "qc_flags_json": json.dumps(flags),
                "ingestion_run_id": ingestion_run_id,
                "ingestion_ts": ts,
            },
        )

    series_rows_out: list[dict[str, Any]] = []
    for seu, se in series_acc.items():
        flags = sorted(se["qc_flags"])
        series_rows_out.append(
            {
                "series_instance_uid": seu,
                "study_instance_uid": se["study_instance_uid"],
                "series_date_raw": se["series_date_raw"],
                "series_date_normalized": se["series_date_normalized"],
                "modality_raw": se["modality_raw"],
                "body_part_examined_raw": se["body_part_examined_raw"],
                "series_description_raw": se["series_description_raw"],
                "institution_name_raw": se["institution_name_raw"],
                "patient_id_raw": se["patient_id_raw"],
                "n_source_rows": se["n_source_rows"],
                "qc_flags_json": json.dumps(flags),
                "ingestion_run_id": ingestion_run_id,
                "ingestion_ts": ts,
            },
        )

    prov_df = pd.DataFrame(prov_rows)
    study_df = pd.DataFrame(study_rows_out)
    series_df = pd.DataFrame(series_rows_out)
    return prov_df, study_df, series_df


def _exams_for_candidate_group(g: pd.DataFrame) -> list[str]:
    if "imaging_exam_id" not in g.columns:
        return []
    ex = g["imaging_exam_id"].dropna().astype(str).unique().tolist()
    return sorted(ex)


def _link_id_hash(parts: Sequence[str]) -> str:
    h = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()
    return f"dicom_link_{h[:40]}"


def _review_id_hash(parts: Sequence[str]) -> str:
    h = hashlib.sha256(("|R|".join(parts)).encode("utf-8")).hexdigest()
    return f"dicom_rq_{h[:36]}"


def _ymd_delta_days(a: str, b: str) -> int:
    da = date(int(a[0:4]), int(a[4:6]), int(a[6:8]))
    db = date(int(b[0:4]), int(b[4:6]), int(b[6:8]))
    return abs((da - db).days)


def _qc_json_series(val: Any) -> list[Any]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []
    if isinstance(val, str) and not val.strip():
        return []
    try:
        out = json.loads(val)
    except json.JSONDecodeError:
        return []
    return out if isinstance(out, list) else []


def resolve_exact_links(
    study_df: pd.DataFrame,
    candidates: pd.DataFrame | None,
    *,
    ingestion_run_id: str,
    date_skew_days_max: int = 14,
    ingestion_ts: datetime | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Deterministic linkage: explicit research_id > exact accession against candidates.
    Never invent links from MRN/date alone. candidates columns expected:
    research_id, accession_norm, imaging_exam_id (nullable), imaging_nodule_id (nullable),
    specimen_id (nullable), source_table (imaging|specimen), exam_date_yyyymmdd (optional).
    """
    ts = ingestion_ts or datetime.now(timezone.utc)
    links: list[dict[str, Any]] = []
    reviews: list[dict[str, Any]] = []
    if candidates is None or candidates.empty:
        cand = pd.DataFrame(
            columns=[
                "research_id",
                "accession_norm",
                "imaging_exam_id",
                "imaging_nodule_id",
                "specimen_id",
                "source_table",
                "exam_date_yyyymmdd",
            ],
        )
    else:
        cand = candidates.copy()
    if "source_table" not in cand.columns:
        cand["source_table"] = "imaging"
    else:
        cand["source_table"] = cand["source_table"].fillna("imaging")

    for _, st in study_df.iterrows():
        su = st.get("study_instance_uid")
        accn = st.get("accession_norm")
        sd_n = st.get("study_date_normalized")
        rid_ex = st.get("research_id_explicit")
        qcf = _qc_json_series(st.get("qc_flags_json"))
        conflict_note_parts: list[str] = []
        blocked = (
            "MISSING_STUDY_INSTANCE_UID",
            "MALFORMED_STUDY_INSTANCE_UID",
            "DISCORDANT_EXPLICIT_RESEARCH_ID",
            "INVALID_EXPLICIT_RESEARCH_ID",
        )
        if any(x in qcf for x in blocked):
            reviews.append(
                {
                    "review_id": _review_id_hash([ingestion_run_id, str(su), "HEADER_QC"]),
                    "reason_code": "STUDY_HEADER_QC_BLOCKER",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": accn,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": None,
                    "candidate_imaging_exam_ids_json": None,
                    "candidate_specimen_ids_json": None,
                    "conflict_note": "Study row failed QC: " + ",".join(qcf),
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        if rid_ex is not None and not (isinstance(rid_ex, float) and pd.isna(rid_ex)):
            rid_i = int(rid_ex)
            link_id = _link_id_hash(
                [ingestion_run_id, str(su), "", "explicit_research_id", str(rid_i), str(accn or "")],
            )
            links.append(
                {
                    "link_id": link_id,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "linkage_tier": "explicit_research_id",
                    "research_id": rid_i,
                    "imaging_exam_id": None,
                    "imaging_nodule_id": None,
                    "specimen_id": None,
                    "accession_norm": accn,
                    "date_concordance_flag": None,
                    "candidate_digest": None,
                    "ingestion_run_id": ingestion_run_id,
                    "ingestion_ts": ts,
                },
            )
            continue

        if accn is None or (isinstance(accn, float) and pd.isna(accn)):
            reviews.append(
                {
                    "review_id": _review_id_hash([ingestion_run_id, str(su), "NO_KEYS"]),
                    "reason_code": "MISSING_ACCESSION_NO_RESEARCH_ID",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": None,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": None,
                    "candidate_imaging_exam_ids_json": None,
                    "candidate_specimen_ids_json": None,
                    "conflict_note": "No explicit research_id and no normalized accession.",
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        if cand.empty:
            reviews.append(
                {
                    "review_id": _review_id_hash([ingestion_run_id, str(su), str(accn), "NO_DB"]),
                    "reason_code": "ACCESSION_NO_DB_MATCH",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": accn,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": json.dumps([]),
                    "candidate_imaging_exam_ids_json": None,
                    "candidate_specimen_ids_json": None,
                    "conflict_note": "No candidate table rows to match (empty catalog).",
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        sub = cand[cand["accession_norm"].astype(str) == str(accn)]
        if sub.empty:
            reviews.append(
                {
                    "review_id": _review_id_hash([ingestion_run_id, str(su), str(accn), "NOMATCH"]),
                    "reason_code": "ACCESSION_NO_DB_MATCH",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": accn,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": json.dumps([]),
                    "candidate_imaging_exam_ids_json": None,
                    "candidate_specimen_ids_json": None,
                    "conflict_note": "Exact accession not present in candidate spine.",
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        rids = sorted({int(x) for x in sub["research_id"].dropna().unique().tolist()})
        if len(rids) > 1:
            exams = _exams_for_candidate_group(sub)
            specs = sorted(
                {str(x) for x in sub["specimen_id"].dropna().astype(str).unique().tolist()},
            )
            reviews.append(
                {
                    "review_id": _review_id_hash(
                        [ingestion_run_id, str(su), str(accn), "MULTI_RID"],
                    ),
                    "reason_code": "AMBIGUOUS_ACCESSION_MULTI_RESEARCH_ID",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": accn,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": json.dumps(rids),
                    "candidate_imaging_exam_ids_json": json.dumps(exams),
                    "candidate_specimen_ids_json": json.dumps(specs),
                    "conflict_note": "Multiple research_id for same normalized accession in spine.",
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        rid = rids[0]
        img_sub = sub[sub["source_table"].astype(str).eq("imaging")]
        exams = _exams_for_candidate_group(img_sub)
        if len(exams) > 1:
            reviews.append(
                {
                    "review_id": _review_id_hash(
                        [ingestion_run_id, str(su), str(accn), "MULTI_EXAM"],
                    ),
                    "reason_code": "AMBIGUOUS_ACCESSION_MULTI_IMAGING_EXAM",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": accn,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": json.dumps([rid]),
                    "candidate_imaging_exam_ids_json": json.dumps(exams),
                    "candidate_specimen_ids_json": None,
                    "conflict_note": "One research_id but multiple imaging exam_ids for accession.",
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        date_ok: bool | None = True
        if sd_n and isinstance(sd_n, str) and len(sd_n) == 8 and "exam_date_yyyymmdd" in sub.columns:
            ex_dates = sub["exam_date_yyyymmdd"].dropna().astype(str).unique().tolist()
            if ex_dates:
                ex0 = str(ex_dates[0])
                if len(ex0) == 8:
                    try:
                        skew = _ymd_delta_days(sd_n, ex0)
                        if skew > int(date_skew_days_max):
                            date_ok = False
                            conflict_note_parts.append(
                                f"study_date {sd_n} vs exam_date {ex0} delta {skew}d "
                                f"> {date_skew_days_max}d",
                            )
                    except (ValueError, TypeError):
                        date_ok = None

        if date_ok is False:
            reviews.append(
                {
                    "review_id": _review_id_hash(
                        [ingestion_run_id, str(su), str(accn), "DATEBAD"],
                    ),
                    "reason_code": "DATE_DISCORDANT_ACCESSION_MATCH",
                    "source_file": None,
                    "study_instance_uid": su,
                    "series_instance_uid": None,
                    "accession_raw": st.get("accession_number_raw"),
                    "accession_norm": accn,
                    "study_date_normalized": sd_n,
                    "series_date_normalized": None,
                    "modality_raw": st.get("modality_summary"),
                    "candidate_research_ids_json": json.dumps([rid]),
                    "candidate_imaging_exam_ids_json": json.dumps(exams),
                    "candidate_specimen_ids_json": json.dumps(
                        sorted(
                            {
                                str(x)
                                for x in sub["specimen_id"].dropna().astype(str).unique().tolist()
                            },
                        ),
                    ),
                    "conflict_note": "; ".join(conflict_note_parts),
                    "ingestion_run_id": ingestion_run_id,
                    "created_ts": ts,
                },
            )
            continue

        img_row = img_sub.head(1)
        exam_id = None
        nodule_id = None
        if not img_row.empty and "imaging_exam_id" in img_row.columns:
            exam_id = img_row.iloc[0].get("imaging_exam_id")
            nodule_id = img_row.iloc[0].get("imaging_nodule_id")
        spec_sub = sub[sub["source_table"].astype(str).eq("specimen")]
        specimen_id = None
        if not spec_sub.empty and "specimen_id" in spec_sub.columns:
            specimen_id = spec_sub.iloc[0].get("specimen_id")

        cand_blob = json.dumps(
            sorted([tuple(str(x) for x in r) for r in sub.astype(str).values.tolist()]),
            default=str,
        )
        digest = hashlib.sha256(cand_blob.encode("utf-8")).hexdigest()[:32]

        def _cell_str(v: Any) -> str | None:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
            return str(v)

        links.append(
            {
                "link_id": _link_id_hash(
                    [ingestion_run_id, str(su), "", "exact_accession", str(rid), str(accn)],
                ),
                "study_instance_uid": su,
                "series_instance_uid": None,
                "linkage_tier": "exact_accession",
                "research_id": rid,
                "imaging_exam_id": _cell_str(exam_id),
                "imaging_nodule_id": _cell_str(nodule_id),
                "specimen_id": _cell_str(specimen_id),
                "accession_norm": str(accn) if accn is not None else None,
                "date_concordance_flag": (bool(date_ok) if date_ok is not None else None),
                "candidate_digest": digest,
                "ingestion_run_id": ingestion_run_id,
                "ingestion_ts": ts,
            },
        )

    return pd.DataFrame(links), pd.DataFrame(reviews)


def fetch_linkage_candidates_union(con: Any) -> pd.DataFrame:
    """Read-only UNION of accession-keyed candidates from imaging, FNA history, and specimen tables."""
    frames: list[pd.DataFrame] = []
    empty_cols = [
        "research_id",
        "accession_norm",
        "imaging_exam_id",
        "imaging_nodule_id",
        "specimen_id",
        "source_table",
        "exam_date_yyyymmdd",
    ]

    def _cols(table: str) -> dict[str, str]:
        try:
            rows = con.execute(
                "SELECT DISTINCT column_name FROM information_schema.columns "
                "WHERE table_schema = 'main' AND table_name = ?",
                [table],
            ).fetchall()
            return {str(r[0]).lower(): str(r[0]) for r in rows}
        except Exception:
            return {}

    def _pick(cmap: dict[str, str], cands: list[str]) -> str | None:
        for c in cands:
            if c.lower() in cmap:
                return cmap[c.lower()]
        return None

    def _norm_expr(colq: str) -> str:
        return (
            "NULLIF(TRIM(REGEXP_REPLACE(LOWER(CAST("
            + colq
            + " AS VARCHAR)), '[^a-z0-9]', '', 'g')), '')"
        )

    img_cols = _cols("imaging_nodule_master_v1")
    if img_cols:
        acc = _pick(
            img_cols,
            [
                "accession_norm",
                "accession_number",
                "accession",
                "specimen_id",
                "specimen_received",
                "accession_no",
            ],
        )
        rid = _pick(img_cols, ["research_id"])
        exam = _pick(img_cols, ["exam_id", "imaging_exam_id"])
        nod = _pick(img_cols, ["nodule_id"])
        dtcol = _pick(img_cols, ["exam_date", "imaging_exam_date", "study_date"])
        if acc and rid:
            acc_sql = _norm_expr(f'"{acc}"')
            ex_sql = f'CAST("{exam}" AS VARCHAR)' if exam else "NULL::VARCHAR"
            nod_sql = f'CAST("{nod}" AS VARCHAR)' if nod else "NULL::VARCHAR"
            if dtcol:
                dt_sql = (
                    f"CASE WHEN length(regexp_replace(CAST(\"{dtcol}\" AS VARCHAR), "
                    f"'[^0-9]', '', 'g')) >= 8 THEN substr(regexp_replace("
                    f"CAST(\"{dtcol}\" AS VARCHAR), '[^0-9]', '', 'g'), 1, 8) "
                    f"ELSE NULL END"
                )
            else:
                dt_sql = "NULL::VARCHAR"
            sql_img = f"""
            SELECT DISTINCT CAST("{rid}" AS BIGINT) AS research_id,
                   {acc_sql} AS accession_norm,
                   {ex_sql} AS imaging_exam_id,
                   {nod_sql} AS imaging_nodule_id,
                   CAST(NULL AS VARCHAR) AS specimen_id,
                   CAST('imaging' AS VARCHAR) AS source_table,
                   {dt_sql} AS exam_date_yyyymmdd
            FROM imaging_nodule_master_v1
            WHERE {acc_sql} IS NOT NULL
            """
            try:
                frames.append(con.execute(sql_img).df())
            except Exception:
                pass

    fh_cols = _cols("fna_history")
    if fh_cols:
        rid = _pick(fh_cols, ["research_id"])
        sp = _pick(
            fh_cols,
            ["specimen_received", "specimen", "accession", "accession_number"],
        )
        if rid and sp:
            sp_sql = _norm_expr(f"h.{sp}")
            sql_fh = f"""
            SELECT DISTINCT CAST(h.{rid} AS BIGINT) AS research_id,
                   {sp_sql} AS accession_norm,
                   CAST(NULL AS VARCHAR) AS imaging_exam_id,
                   CAST(NULL AS VARCHAR) AS imaging_nodule_id,
                   CAST(NULL AS VARCHAR) AS specimen_id,
                   CAST('fna_history' AS VARCHAR) AS source_table,
                   CAST(NULL AS VARCHAR) AS exam_date_yyyymmdd
            FROM fna_history h
            WHERE {sp_sql} IS NOT NULL
            """
            try:
                frames.append(con.execute(sql_fh).df())
            except Exception:
                pass

    if _cols("specimen_master_v1"):
        sql_sp = """
        SELECT DISTINCT CAST(research_id AS BIGINT) AS research_id,
               NULLIF(TRIM(REGEXP_REPLACE(
                   LOWER(CAST(accession_or_source_id AS VARCHAR)),
                   '[^a-z0-9]', '', 'g'
               )), '') AS accession_norm,
               CAST(NULL AS VARCHAR) AS imaging_exam_id,
               CAST(NULL AS VARCHAR) AS imaging_nodule_id,
               CAST(specimen_id AS VARCHAR) AS specimen_id,
               CAST('specimen' AS VARCHAR) AS source_table,
               CAST(NULL AS VARCHAR) AS exam_date_yyyymmdd
        FROM specimen_master_v1
        WHERE accession_or_source_id IS NOT NULL
          AND TRIM(CAST(accession_or_source_id AS VARCHAR)) <> ''
        """
        try:
            frames.append(con.execute(sql_sp).df())
        except Exception:
            pass

    if not frames:
        return pd.DataFrame(columns=empty_cols)
    out = pd.concat(frames, ignore_index=True)
    return out


def optional_attach_dicom_to_imaging_nodule_frame(
    imaging_df: pd.DataFrame,
    duckdb_con: Any,
    *,
    study_table: str = "dicom_study_header_v1",
) -> pd.DataFrame:
    """No-op enricher: returns *imaging_df* unchanged if *study_table* is missing.

    Future: left-join study-level accession/UAD hints onto imaging rows by exact accession only.
    Scripts 128/129 remain unchanged; callers may use this helper explicitly when DICOM tables exist.
    """
    try:
        duckdb_con.execute(f"SELECT 1 FROM {study_table} LIMIT 1")
    except Exception:
        return imaging_df
    return imaging_df

