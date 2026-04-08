#!/usr/bin/env python3
"""Generic data-contract gate — thin safety net (YAML contracts, deterministic checks).

Complements (does not replace) scripts 112, 29, and 119 — see docs/data_contract_gate.md.

Default: offline / dry-run — reads parquet/CSV or DuckDB with read_only; emits artifacts only.
Optional --md / --md-sa: read tables from MotherDuck (still no writes unless --write-qa surfaces).

Never logs raw clinical note text; violation detail uses column names, rule ids, and redacted locators.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
import uuid
import dataclasses
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import duckdb
import pandas as pd
import yaml

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from utils.md_connect import connect_md_or_file  # noqa: E402

SAFE_TABLE_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)?$")
GENESIS_HASH = "0" * 64


@dataclass
class Violation:
    row_index: int
    column_name: str
    rule_id: str
    severity: str
    observed_value: str
    expected_constraint: str
    suggested_fix: str
    row_locator: dict[str, Any]
    source_file_id: str | None
    extraction_run_id: str | None
    action: str = "flag"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def row_fingerprint_sha256(row: Mapping[str, Any]) -> str:
    payload = json.dumps(row, sort_keys=True, default=_json_default, ensure_ascii=True)
    return _sha256_hex(payload.encode("utf-8"))


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, float) and pd.isna(o):
        return None
    if pd.isna(o):
        return None
    return str(o)


def load_contract(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"Contract must be a mapping: {path}")
    return data


def resolve_contract_path(contract: str | None, contract_name: str | None, contract_dir: Path) -> Path:
    if contract:
        p = Path(contract)
        return p if p.is_absolute() else (ROOT / p)
    if not contract_name:
        raise ValueError("Provide --contract or --contract-name")
    stem = contract_name if contract_name.endswith(".yaml") else f"{contract_name}.yaml"
    return contract_dir / stem


def load_input_frame(
    *,
    input_path: Path | None,
    table: str | None,
    db_path: Path,
    md: bool,
    md_sa: bool,
) -> tuple[pd.DataFrame, duckdb.DuckDBPyConnection | None]:
    if input_path and table:
        raise ValueError("Use only one of --input-path or --table")
    if input_path:
        p = input_path if input_path.is_absolute() else (ROOT / input_path)
        if not p.exists():
            raise FileNotFoundError(p)
        suf = p.suffix.lower()
        if suf == ".parquet":
            return pd.read_parquet(p), None
        if suf == ".csv":
            return pd.read_csv(p), None
        raise ValueError(f"Unsupported input format: {suf}")

    if not table:
        raise ValueError("Provide --input-path or --table")
    if not SAFE_TABLE_RE.match(table):
        raise ValueError(f"Unsafe table name: {table}")
    con = connect_md_or_file(
        db_path,
        md=md,
        fail_closed=md,
        prefer_service_account=md_sa,
    )
    try:
        df = con.execute(f"SELECT * FROM {table}").df()
    except Exception:
        con.close()
        raise
    return df, con


def _parse_date_value(val: Any) -> date | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if isinstance(val, date) and not isinstance(val, datetime):
        return val
    if isinstance(val, datetime):
        return val.date()
    s = str(val).strip()
    if not s:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return date.fromisoformat(s)
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def _is_integer_like(v: Any) -> bool:
    if v is None or pd.isna(v):
        return False
    if isinstance(v, bool):
        return False
    if isinstance(v, int):
        return True
    if isinstance(v, float):
        return float(v).is_integer()
    try:
        f = float(v)
        return f.is_integer()
    except (TypeError, ValueError):
        return False


def _check_dtype(series: pd.Series, dtype: str, col: str) -> list[Violation]:
    out: list[Violation] = []
    dtype_l = dtype.lower()
    if dtype_l == "int":
        for i, v in enumerate(series):
            if pd.isna(v) or v is None:
                continue
            if _is_integer_like(v):
                continue
            out.append(
                Violation(
                    row_index=i,
                    column_name=col,
                    rule_id="dtype_int",
                    severity="error",
                    observed_value=repr(v)[:200],
                    expected_constraint="integer",
                    suggested_fix="coerce_or_reject_non_integer",
                    row_locator={},
                    source_file_id=None,
                    extraction_run_id=None,
                )
            )
    elif dtype_l == "float":
        for i, v in enumerate(series):
            if pd.isna(v) or v is None:
                continue
            try:
                float(v)  # type: ignore[arg-type]
            except (TypeError, ValueError):
                out.append(
                    Violation(
                        row_index=i,
                        column_name=col,
                        rule_id="dtype_float",
                        severity="error",
                        observed_value=repr(v)[:200],
                        expected_constraint="float",
                        suggested_fix="parse_numeric",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )
    elif dtype_l in ("string", "str", "varchar"):
        pass
    elif dtype_l == "date":
        for i, v in enumerate(series):
            if pd.isna(v) or v is None:
                continue
            if _parse_date_value(v) is None:
                out.append(
                    Violation(
                        row_index=i,
                        column_name=col,
                        rule_id="dtype_date",
                        severity="error",
                        observed_value=repr(v)[:200],
                        expected_constraint="parseable_date",
                        suggested_fix="normalize_date_string",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )
    return out


def _col_series(df: pd.DataFrame, name: str) -> pd.Series:
    if name not in df.columns:
        return pd.Series([None] * len(df))
    return df[name]


def validate_schema_and_columns(df: pd.DataFrame, contract: Mapping[str, Any]) -> list[Violation]:
    violations: list[Violation] = []
    cols_spec: list[Mapping[str, Any]] = list(contract.get("columns") or [])
    required_names = [c["name"] for c in cols_spec if c.get("name")]
    for name in required_names:
        if name not in df.columns:
            violations.append(
                Violation(
                    row_index=-1,
                    column_name=name,
                    rule_id="missing_column",
                    severity="error",
                    observed_value="",
                    expected_constraint="column_present",
                    suggested_fix="align_schema_with_contract",
                    row_locator={},
                    source_file_id=None,
                    extraction_run_id=None,
                )
            )

    for c in cols_spec:
        name = c.get("name")
        if not name or name not in df.columns:
            continue
        dtype = str(c.get("dtype", "string"))
        nullable = bool(c.get("nullable", True))
        series = df[name]
        violations.extend(_check_dtype(series, dtype, name))
        if not nullable:
            is_null = series.isna()
            if series.dtype == object or str(series.dtype) == "string":
                is_null = is_null | (series.astype(str).str.strip() == "")
            for pos in is_null.to_numpy().nonzero()[0].tolist():
                violations.append(
                    Violation(
                        row_index=int(pos),
                        column_name=name,
                        rule_id="non_null_violation",
                        severity="error",
                        observed_value="NULL_OR_EMPTY",
                        expected_constraint="NOT NULL",
                        suggested_fix="populate_required_field",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )

        allowed = c.get("allowed_values")
        if allowed:
            allowed_set = {str(x) for x in allowed}
            for i, v in enumerate(series):
                if pd.isna(v) or v is None:
                    continue
                vs = str(v).strip().lower() if isinstance(v, str) else str(v)
                # case-insensitive enum match for strings
                low_set = {a.lower() for a in allowed_set}
                if vs.lower() not in low_set:
                    violations.append(
                        Violation(
                            row_index=i,
                            column_name=name,
                            rule_id="enum_violation",
                            severity="error",
                            observed_value=vs[:120],
                            expected_constraint=f"in {sorted(allowed_set)}",
                            suggested_fix="map_to_allowed_enum",
                            row_locator={},
                            source_file_id=None,
                            extraction_run_id=None,
                        )
                    )

        if c.get("no_future_date"):
            today = date.today()
            for i, v in enumerate(series):
                if pd.isna(v) or v is None:
                    continue
                d = _parse_date_value(v)
                if d and d > today:
                    violations.append(
                        Violation(
                            row_index=i,
                            column_name=name,
                            rule_id="future_date",
                            severity="error",
                            observed_value=str(d),
                            expected_constraint=f"date<={today.isoformat()}",
                            suggested_fix="correct_date_or_timezone",
                            row_locator={},
                            source_file_id=None,
                            extraction_run_id=None,
                        )
                    )

        if c.get("provenance_required"):
            for i, v in enumerate(series):
                if pd.isna(v) or v is None:
                    empty = True
                else:
                    empty = isinstance(v, str) and v.strip() == ""
                if empty:
                    violations.append(
                        Violation(
                            row_index=i,
                            column_name=name,
                            rule_id="provenance_required",
                            severity="warning",
                            observed_value="MISSING",
                            expected_constraint="non_empty_provenance",
                            suggested_fix="backfill_from_note_extraction_runs",
                            row_locator={},
                            source_file_id=None,
                            extraction_run_id=None,
                        )
                    )

    return violations


def validate_conditional_bounds(df: pd.DataFrame, contract: Mapping[str, Any]) -> list[Violation]:
    violations: list[Violation] = []
    for spec in contract.get("conditional_numeric_bounds") or []:
        when_col = spec["when_column"]
        when_val = spec["when_value"]
        val_col = spec["column"]
        lo = spec.get("min")
        hi = spec.get("max")
        if when_col not in df.columns or val_col not in df.columns:
            continue
        mask = df[when_col].astype(str).str.lower() == str(when_val).lower()
        mask = mask.fillna(False).to_numpy()
        for pos in mask.nonzero()[0].tolist():
            v = df.iloc[pos][val_col]
            if pd.isna(v) or v is None:
                continue
            try:
                x = float(v)
            except (TypeError, ValueError):
                continue
            if lo is not None and x < float(lo):
                violations.append(
                    Violation(
                        row_index=int(pos),
                        column_name=val_col,
                        rule_id="numeric_bound_min",
                        severity="error",
                        observed_value=str(x),
                        expected_constraint=f">={lo} when {when_col}={when_val}",
                        suggested_fix="verify_lab_value_or_unit",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )
            if hi is not None and x > float(hi):
                violations.append(
                    Violation(
                        row_index=int(pos),
                        column_name=val_col,
                        rule_id="numeric_bound_max",
                        severity="error",
                        observed_value=str(x),
                        expected_constraint=f"<={hi} when {when_col}={when_val}",
                        suggested_fix="verify_lab_value_or_unit",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )
    return violations


def validate_foreign_keys(df: pd.DataFrame, contract: Mapping[str, Any]) -> list[Violation]:
    violations: list[Violation] = []
    for fk in contract.get("foreign_keys") or []:
        name = str(fk.get("name", "fk"))
        lcols: list[str] = list(fk.get("local_columns") or [])
        rcols: list[str] = list(fk.get("ref_columns") or [])
        ref_rel = fk.get("ref_csv") or fk.get("ref_parquet")
        if not ref_rel or not lcols or not rcols:
            continue
        path = ROOT / str(ref_rel)
        if not path.exists():
            violations.append(
                Violation(
                    row_index=-1,
                    column_name=",".join(lcols),
                    rule_id=f"fk_ref_missing:{name}",
                    severity="error",
                    observed_value="",
                    expected_constraint=f"ref_file_exists:{ref_rel}",
                    suggested_fix="add_ref_lookup_file",
                    row_locator={},
                    source_file_id=None,
                    extraction_run_id=None,
                )
            )
            continue
        suf = path.suffix.lower()
        if suf == ".csv":
            ref = pd.read_csv(path)
        elif suf == ".parquet":
            ref = pd.read_parquet(path)
        else:
            continue
        if any(c not in df.columns for c in lcols) or any(c not in ref.columns for c in rcols):
            continue
        keys = pd.to_numeric(ref[rcols[0]], errors="coerce").dropna()
        keyset = {int(x) for x in keys}
        for pos in range(len(df)):
            v0 = df.iloc[pos][lcols[0]]
            if pd.isna(v0):
                continue
            try:
                vid = int(v0)
            except (TypeError, ValueError):
                violations.append(
                    Violation(
                        row_index=pos,
                        column_name=lcols[0],
                        rule_id=f"fk_lookup_invalid_key:{name}",
                        severity="error",
                        observed_value=str(v0)[:120],
                        expected_constraint=f"integer_key_in_{ref_rel}",
                        suggested_fix="fix_research_id_type",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )
                continue
            if vid not in keyset:
                violations.append(
                    Violation(
                        row_index=pos,
                        column_name=",".join(lcols),
                        rule_id=f"fk_not_found:{name}",
                        severity="error",
                        observed_value=str(vid),
                        expected_constraint=f"exists_in_{ref_rel}",
                        suggested_fix="reconcile_patient_spine",
                        row_locator={},
                        source_file_id=None,
                        extraction_run_id=None,
                    )
                )
    return violations


def validate_composite_unique(df: pd.DataFrame, contract: Mapping[str, Any]) -> list[Violation]:
    violations: list[Violation] = []
    for grp in contract.get("composite_unique") or []:
        cols = grp["columns"]
        name = grp.get("name", "|".join(cols))
        only_nn = bool(grp.get("only_when_value_numeric_not_null"))
        d = df
        if only_nn and "value_numeric" in d.columns:
            d = d[d["value_numeric"].notna()]
        if any(c not in d.columns for c in cols):
            continue
        dup_mask = d.duplicated(cols, keep=False)
        for pos in dup_mask.to_numpy().nonzero()[0].tolist():
            violations.append(
                Violation(
                    row_index=int(pos),
                    column_name=",".join(cols),
                    rule_id=f"duplicate_composite:{name}",
                    severity="error",
                    observed_value="duplicate_key",
                    expected_constraint=f"unique {cols}",
                    suggested_fix="deduplicate_or_adjust_keys",
                    row_locator={},
                    source_file_id=None,
                    extraction_run_id=None,
                )
            )
    return violations


def _late_norm(s: Any) -> str | None:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return None
    return str(s).strip().lower() or None


def run_builtin_linkage_discordant_laterality(
    df: pd.DataFrame, sever: str
) -> list[Violation]:
    violations: list[Violation] = []
    if "imaging_laterality" not in df.columns or "fna_laterality" not in df.columns:
        return violations
    for pos in range(len(df)):
        row = df.iloc[pos]
        a = _late_norm(row.get("imaging_laterality"))
        b = _late_norm(row.get("fna_laterality"))
        if a and b and a != b:
            violations.append(
                Violation(
                    row_index=pos,
                    column_name="imaging_laterality,fna_laterality",
                    rule_id="linkage_discordant_laterality",
                    severity=sever,
                    observed_value=f"{a}|{b}",
                    expected_constraint="laterality_equal_when_both_present",
                    suggested_fix="manual_linkage_review",
                    row_locator={},
                    source_file_id=None,
                    extraction_run_id=None,
                )
            )
    return violations


def run_builtin_linkage_fna_after_surgery(
    df: pd.DataFrame, sever: str
) -> list[Violation]:
    violations: list[Violation] = []
    if "fna_date" not in df.columns or "surgery_date" not in df.columns:
        return violations
    for pos in range(len(df)):
        row = df.iloc[pos]
        surg = _parse_date_value(row.get("surgery_date"))
        fna = _parse_date_value(row.get("fna_date"))
        if surg and fna and fna > surg:
            violations.append(
                Violation(
                    row_index=pos,
                    column_name="fna_date,surgery_date",
                    rule_id="linkage_fna_after_surgery",
                    severity=sever,
                    observed_value=f"fna={fna.isoformat()},surg={surg.isoformat()}",
                    expected_constraint="fna_date<=surgery_date_for_preop_prep",
                    suggested_fix="verify_episode_dates_or_completion_case",
                    row_locator={},
                    source_file_id=None,
                    extraction_run_id=None,
                )
            )
    return violations


def enrich_locators_and_src(
    violations: list[Violation],
    df: pd.DataFrame,
    contract: Mapping[str, Any],
) -> None:
    loc_cols: list[str] = list(contract.get("row_locator_columns") or [])
    has_sf = "source_file_id" in df.columns
    has_er = "extraction_run_id" in df.columns
    for v in violations:
        ri = v.row_index
        try:
            if 0 <= ri < len(df):
                row = df.iloc[ri]
            else:
                row = None
        except Exception:
            row = None
        if row is not None:
            loc: dict[str, Any] = {}
            for c in loc_cols:
                if c in df.columns:
                    val = row.get(c)
                    loc[c] = None if pd.isna(val) else val
            v.row_locator = loc
            if has_sf:
                x = row.get("source_file_id")
                v.source_file_id = None if pd.isna(x) else str(x)
            if has_er:
                x = row.get("extraction_run_id")
                v.extraction_run_id = None if pd.isna(x) else str(x)


def build_audit_events(
    run_id: str,
    dataset_name: str,
    violations: list[Violation],
    df: pd.DataFrame,
    ts_utc: str,
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    prev = GENESIS_HASH
    for v in sorted(violations, key=lambda x: (x.row_index, x.rule_id, x.column_name)):
        row_payload: dict[str, Any] = {}
        if 0 <= v.row_index < len(df):
            raw = df.iloc[v.row_index].to_dict()
            row_payload = {str(k): val for k, val in raw.items()}
        rf = row_fingerprint_sha256(row_payload)
        body = {
            "run_id": run_id,
            "ts_utc": ts_utc,
            "dataset_name": dataset_name,
            "row_locator": json.dumps(v.row_locator, sort_keys=True, default=str),
            "column_name": v.column_name,
            "rule_id": v.rule_id,
            "severity": v.severity,
            "action": v.action,
            "observed_value": v.observed_value[:500],
            "expected_constraint": v.expected_constraint[:500],
            "suggested_fix": v.suggested_fix[:500],
            "source_file_id": v.source_file_id,
            "extraction_run_id": v.extraction_run_id,
            "row_fingerprint_sha256": rf,
            "prev_event_hash": prev,
        }
        canon = json.dumps(body, sort_keys=True, default=str, ensure_ascii=True)
        ev_hash = _sha256_hex(canon.encode("utf-8"))
        full = {**body, "event_hash": ev_hash}
        events.append(full)
        prev = ev_hash
    return events


def validate_all(
    df: pd.DataFrame,
    contract: Mapping[str, Any],
    *,
    rules_run_counter: list[int],
) -> list[Violation]:
    violations: list[Violation] = []
    violations.extend(validate_schema_and_columns(df, contract))
    rules_run_counter[0] += len(contract.get("columns") or []) + 4

    violations.extend(validate_conditional_bounds(df, contract))
    rules_run_counter[0] += len(contract.get("conditional_numeric_bounds") or [])

    violations.extend(validate_composite_unique(df, contract))
    rules_run_counter[0] += len(contract.get("composite_unique") or [])

    violations.extend(validate_foreign_keys(df, contract))
    rules_run_counter[0] += len(contract.get("foreign_keys") or [])

    builtin = contract.get("builtin_rules") or []
    sever_by_id = {str(b.get("id")): str(b.get("severity", "error")) for b in builtin}
    for b in builtin:
        bid = str(b.get("id"))
        sev = sever_by_id.get(bid, "error")
        if bid == "linkage_discordant_laterality":
            violations.extend(run_builtin_linkage_discordant_laterality(df, sev))
            rules_run_counter[0] += 1
        elif bid == "linkage_fna_after_surgery":
            violations.extend(run_builtin_linkage_fna_after_surgery(df, sev))
            rules_run_counter[0] += 1

    enrich_locators_and_src(violations, df, contract)
    return violations


def compute_metrics(
    rows_scanned: int,
    violations: list[Violation],
    rules_run: int,
    tables_checked: int,
    audit_events: list[dict[str, Any]],
) -> dict[str, Any]:
    n_flag = len(violations)
    schema_v = sum(1 for v in violations if v.rule_id == "missing_column" or v.rule_id.startswith("dtype_"))
    schema_rate = (schema_v / max(n_flag, 1)) if n_flag else 0.0
    with_run = sum(1 for e in audit_events if e.get("extraction_run_id"))
    audit_complete = (with_run / max(len(audit_events), 1)) if audit_events else 1.0
    return {
        "rows_scanned": rows_scanned,
        "rows_flagged": n_flag,
        "schema_violation_rate": round(schema_rate, 6),
        "audit_trail_completeness": round(audit_complete, 6),
        "rules_run": rules_run,
        "tables_checked": tables_checked,
    }


def write_outputs(
    output_dir: Path,
    violations: list[Violation],
    audit_events: list[dict[str, Any]],
    metrics: dict[str, Any],
    contract_path: Path,
    strict: bool,
) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    vcols = [f.name for f in dataclasses.fields(Violation)]

    def _vio_dict(v: Violation) -> dict[str, Any]:
        d = v.__dict__.copy()
        d["row_locator"] = json.dumps(v.row_locator, sort_keys=True, default=str)
        return d

    vdf = pd.DataFrame([_vio_dict(v) for v in violations]) if violations else pd.DataFrame(columns=vcols)
    vdf.to_parquet(output_dir / "violations.parquet")

    adf = pd.DataFrame(audit_events) if audit_events else pd.DataFrame()
    if not adf.empty:
        adf.to_parquet(output_dir / "audit_events.parquet")
    with (output_dir / "audit_events.jsonl").open("w", encoding="utf-8") as f:
        for e in audit_events:
            f.write(json.dumps(e, default=str, ensure_ascii=True) + "\n")

    (output_dir / "run_metrics.json").write_text(
        json.dumps(metrics, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    status = "FAIL" if strict and any(v.severity == "error" for v in violations) else "OK"
    summary = f"""# Data contract gate summary

- contract: `{contract_path.relative_to(ROOT)}`
- status: **{status}**
- rows_scanned: {metrics["rows_scanned"]}
- rows_flagged: {metrics["rows_flagged"]}
- schema_violation_rate: {metrics["schema_violation_rate"]}
- audit_trail_completeness: {metrics["audit_trail_completeness"]}
- rules_run: {metrics["rules_run"]}
- tables_checked: {metrics["tables_checked"]}

Artifacts: `violations.parquet`, `audit_events.jsonl`, `run_metrics.json`.
"""
    (output_dir / "summary.md").write_text(summary, encoding="utf-8")
    return 1 if (strict and any(v.severity == "error" for v in violations)) else 0


def write_qa_surface(output_dir: Path, metrics: dict[str, Any], violations: list[Violation]) -> None:
    qa = output_dir / "qa_surface"
    qa.mkdir(parents=True, exist_ok=True)
    (qa / "data_contract_run_summary.json").write_text(
        json.dumps(
            {
                "metrics": metrics,
                "n_violations": len(violations),
                "severity_counts": {
                    sev: sum(1 for v in violations if v.severity == sev)
                    for sev in ("error", "warning", "info")
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    if violations:

        def _qd(v: Violation) -> dict[str, Any]:
            d = v.__dict__.copy()
            d["row_locator"] = json.dumps(v.row_locator, sort_keys=True, default=str)
            return d

        pd.DataFrame([_qd(v) for v in violations]).to_parquet(qa / "violations_for_review.parquet")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Data contract gate (YAML-driven, offline by default).")
    p.add_argument("--contract-dir", type=Path, default=ROOT / "config" / "data_contracts")
    p.add_argument("--contract", type=str, default=None, help="Path to contract YAML")
    p.add_argument("--contract-name", type=str, default=None)
    p.add_argument("--input-path", type=Path, default=None)
    p.add_argument("--table", type=str, default=None)
    p.add_argument("--db-path", type=Path, default=ROOT / "thyroid_master.duckdb")
    p.add_argument("--md", action="store_true", help="Read --table via MotherDuck (no default).")
    p.add_argument("--md-sa", action="store_true", help="Prefer MD_SA_TOKEN (with --md).")
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--strict", action="store_true", help="Exit 1 on any error-severity violation.")
    p.add_argument("--write-qa", action="store_true", help="Emit qa_surface/ artifacts under output-dir.")
    p.add_argument("--run-id", type=str, default=None, help="UUID for run_id (default: random UUID4).")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    cpath = resolve_contract_path(args.contract, args.contract_name, args.contract_dir)
    if not cpath.exists():
        print(f"Contract not found: {cpath}", file=sys.stderr)
        return 2

    contract = load_contract(cpath)
    dataset_name = str(contract.get("dataset_name", cpath.stem))

    df, con = load_input_frame(
        input_path=args.input_path,
        table=args.table,
        db_path=args.db_path,
        md=args.md,
        md_sa=args.md_sa,
    )
    if con is not None:
        con.close()

    df = df.reset_index(drop=True)

    run_id = args.run_id or str(uuid.uuid4())
    ts = _utc_now_iso()
    rules_run_counter = [0]
    violations = validate_all(df, contract, rules_run_counter=rules_run_counter)
    audit_events = build_audit_events(run_id, dataset_name, violations, df, ts)
    n_fk = len(contract.get("foreign_keys") or [])
    metrics = compute_metrics(
        rows_scanned=len(df),
        violations=violations,
        rules_run=rules_run_counter[0],
        tables_checked=1 + n_fk,
        audit_events=audit_events,
    )
    metrics["contract_file"] = str(cpath.relative_to(ROOT))
    metrics["run_id"] = run_id

    rc = write_outputs(
        args.output_dir.resolve(),
        violations,
        audit_events,
        metrics,
        cpath,
        args.strict,
    )
    if args.write_qa:
        write_qa_surface(args.output_dir.resolve(), metrics, violations)

    print(
        json.dumps(
            {"dataset": dataset_name, "rows": len(df), "violations": len(violations), "exit": rc},
            sort_keys=True,
        )
    )
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
