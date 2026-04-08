"""Lightweight invariants for ``126_final_master_release`` orchestration (no MotherDuck)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load126():
    path = ROOT / "scripts" / "126_final_master_release.py"
    spec = importlib.util.spec_from_file_location("final_master_126", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod, path


def test_126_parse_args_specimen_fhir_flags() -> None:
    mod_, _ = _load126()
    prev = sys.argv
    try:
        sys.argv = ["126_final_master_release.py", "--md"]
        args = mod_.parse_args()
    finally:
        sys.argv = prev
    assert args.materialize_specimen_fhir is False
    assert args.skip_specimen_fhir_gate is False
    assert args.release_mode is True


def test_126_main_orders_specimen_gate_before_115_final_master() -> None:
    """Release-mode path must materialize/gate specimen surface before immutable snapshot exports."""
    _, path = _load126()
    text = path.read_text(encoding="utf-8")
    gate_anchor = "run_specimen_fhir_release_gate"
    snap_anchor = 'str(SCRIPTS / "115_release_snapshot.py")'
    assert gate_anchor in text, "expected shared gate helper in 126"
    assert snap_anchor in text, "expected 115 invocation in 126"
    gate_ix = text.index(gate_anchor)
    # First occurrence of gate should precede 115 snapshot block in main()
    snap_ix = text.index(snap_anchor)
    assert gate_ix < snap_ix, "specimen/FHIR gate must run before 115 --final-master"


def test_126_dry_run_plan_string_lists_specimen_and_snapshot_steps() -> None:
    _, path = _load126()
    # Printed-only contract: operators rely on dry-run for rehearsal
    src = path.read_text(encoding="utf-8")
    assert "[specimen gate]" in src and "115/118" in src
