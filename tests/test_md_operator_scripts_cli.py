"""Argparse smoke for MotherDuck operator scripts (no live connection)."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def _load(name: str, rel: str):
    path = ROOT / rel
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    # Required for Python 3.14+ dataclass string annotations when loading via importlib.
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_124_parse_args_default_flags() -> None:
    mod = _load("md124", "scripts/124_md_live_release_audit.py")
    prev = sys.argv
    try:
        sys.argv = ["124_md_live_release_audit.py"]
        args = mod.parse_args()
    finally:
        sys.argv = prev
    assert args.md is False
    assert args.dry_run is False
    assert args.final_release is False
    assert args.materialize_specimen_fhir is False
    assert args.skip_specimen_fhir_gate is False


def test_124_parse_args_md_final_and_specimen_flags() -> None:
    mod = _load("md124", "scripts/124_md_live_release_audit.py")
    prev = sys.argv
    try:
        sys.argv = [
            "124_md_live_release_audit.py",
            "--md",
            "--final-release",
            "--materialize-specimen-fhir",
            "--md-env",
            "prod",
            "--tag",
            "20260409",
        ]
        args = mod.parse_args()
    finally:
        sys.argv = prev
    assert args.md is True
    assert args.final_release is True
    assert args.materialize_specimen_fhir is True
    assert args.md_env == "prod"
    assert args.tag == "20260409"


def test_130_build_parser_print_env() -> None:
    mod = _load("md130", "scripts/130_md_env_bootstrap.py")
    prev = sys.argv
    try:
        # Global flags (--date-tag) must precede the subcommand (see 130 epilog).
        sys.argv = ["130_md_env_bootstrap.py", "--date-tag", "20260407", "print-env"]
        args = mod.build_parser().parse_args()
    finally:
        sys.argv = prev
    assert args.command == "print-env"
    assert args.date_tag == "20260407"
    assert args.execute is False


def test_130_build_parser_prepromote_backup_flags() -> None:
    mod = _load("md130", "scripts/130_md_env_bootstrap.py")
    prev = sys.argv
    try:
        sys.argv = [
            "130_md_env_bootstrap.py",
            "--execute",
            "--md-sa",
            "prepromote-backup",
            "--label",
            "smoke_test_01",
        ]
        args = mod.build_parser().parse_args()
    finally:
        sys.argv = prev
    assert args.command == "prepromote-backup"
    assert args.execute is True
    assert args.md_sa is True
    assert args.label == "smoke_test_01"


def test_137_build_parser_promote_subcommand() -> None:
    mod = _load("md137", "scripts/137_md_molecular_release_workflow.py")
    prev = sys.argv
    try:
        sys.argv = [
            "137_md_molecular_release_workflow.py",
            "--execute",
            "--md-sa",
            "--tag",
            "20260409",
            "promote",
            "--tag",
            "20260410",
            "--label",
            "lb1",
        ]
        args = mod.build_parser().parse_args()
    finally:
        sys.argv = prev
    assert args.command == "promote"
    assert args.execute is True
    assert args.md_sa is True
    # Subcommand --tag shadows global when provided last in argv
    assert args.tag == "20260410"
    assert args.label == "lb1"


def test_137_qa_validate_parser() -> None:
    mod = _load("md137b", "scripts/137_md_molecular_release_workflow.py")
    prev = sys.argv
    try:
        sys.argv = [
            "137_md_molecular_release_workflow.py",
            "qa-validate",
            "--validation-output-dir",
            "studies/_tmp_qa",
        ]
        args = mod.build_parser().parse_args()
    finally:
        sys.argv = prev
    assert args.command == "qa-validate"
    assert args.validation_output_dir == "studies/_tmp_qa"


def test_smoke_script_catalog_probe_requires_md() -> None:
    mod = _load("smoke_md", "scripts/smoke_test_md_connection.py")
    prev = sys.argv
    try:
        sys.argv = ["smoke_test_md_connection.py", "--catalog-probe"]
        assert mod.main() == 1
    finally:
        sys.argv = prev
