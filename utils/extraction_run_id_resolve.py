"""Resolve extraction_run_id for canonical facts from note_extraction_runs.

Timeline rule (fail-closed provenance):
1. If the fact row already has a non-blank extraction_run_id, keep it.
2. Else, among runs with success = true, pick the latest run whose started_at <= extracted_at.
3. Else (extracted before first successful run, or missing extracted_at), use the
   chronologically first successful run_id — documented attribution for pre-telemetry
   rows processed before run registry row zero.

If no successful runs exist, fall back to any run rows (last resort for dev fixtures).
"""

from __future__ import annotations

import pandas as pd


def _nonblank_run_id(val: object) -> str | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    s = str(val).strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    return s


def prepare_runs_for_timeline(runs_df: pd.DataFrame) -> pd.DataFrame | None:
    """Return successful runs sorted by started_at, or None if unusable."""
    if runs_df is None or runs_df.empty or "run_id" not in runs_df.columns:
        return None
    ok = runs_df
    if "success" in runs_df.columns:
        ok = runs_df[runs_df["success"] == True].copy()  # noqa: E712
    if ok.empty:
        ok = runs_df.copy()
    if "started_at" not in ok.columns:
        return None
    ok = ok.copy()
    ok["_run_ts"] = pd.to_datetime(ok["started_at"], utc=True, errors="coerce")
    ok = ok.dropna(subset=["_run_ts", "run_id"]).sort_values("_run_ts")
    if ok.empty:
        return None
    return ok


def resolve_extraction_run_id_series(
    df: pd.DataFrame,
    runs_df: pd.DataFrame | None,
    *,
    extracted_at_col: str = "extracted_at",
    existing_col: str = "extraction_run_id",
) -> pd.Series:
    """Return a Series of resolved run_id strings aligned to df.index."""
    prepared = prepare_runs_for_timeline(runs_df) if runs_df is not None else None
    if prepared is None:
        return df[existing_col] if existing_col in df.columns else pd.Series([None] * len(df), index=df.index)

    first_id = str(prepared.iloc[0]["run_id"]).strip()
    ext = (
        pd.to_datetime(df[extracted_at_col], utc=True, errors="coerce")
        if extracted_at_col in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )

    resolved: list[str | None] = []
    for i in range(len(df)):
        existing = _nonblank_run_id(df[existing_col].iloc[i]) if existing_col in df.columns else None
        if existing is not None:
            resolved.append(existing)
            continue
        ts = ext.iloc[i]
        if pd.isna(ts):
            resolved.append(first_id)
            continue
        sub = prepared[prepared["_run_ts"] <= ts]
        if len(sub):
            resolved.append(str(sub.iloc[-1]["run_id"]).strip())
        else:
            resolved.append(first_id)

    return pd.Series(resolved, index=df.index, dtype="object")


def backfill_extraction_run_id_column(
    df: pd.DataFrame,
    runs_df: pd.DataFrame | None,
    *,
    extracted_at_col: str = "extracted_at",
    existing_col: str = "extraction_run_id",
) -> pd.DataFrame:
    """Copy of df with extraction_run_id filled where blank; no-op if no runs table."""
    if runs_df is None or runs_df.empty:
        return df
    out = df.copy()
    if existing_col not in out.columns:
        out[existing_col] = None
    resolved = resolve_extraction_run_id_series(
        out, runs_df, extracted_at_col=extracted_at_col, existing_col=existing_col
    )
    out[existing_col] = resolved
    return out
