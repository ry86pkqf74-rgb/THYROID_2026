"""Shared assessment for specimen + FHIR release gates (119 Check 13 vs orchestration).

Used by ``124_md_live_release_audit.py`` and ``126_final_master_release.py`` to fail early
with operator-facing remediation when ``main.synoptic_tumor_long_v1`` exists but the
specimen/FHIR surface expected in release-mode validation is absent or only partially deployed.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb

# Kept aligned with scripts/119_md_formalization_validate.py (Check 13).
SPECIMEN_FHIR_ANCHOR = "synoptic_tumor_long_v1"

SPECIMEN_FHIR_TABLES: tuple[str, ...] = (
    "specimen_master_v1",
    "specimen_tumor_focus_v1",
    "specimen_genomic_assay_v1",
    "specimen_source_xref_v1",
    "fhir_patient_deid_map_v1",
    "fhir_specimen_v1",
    "fhir_procedure_collection_v1",
    "fhir_encounter_v1",
    "fhir_episode_of_care_v1",
    "fhir_bundle_specimen_export_v1",
)

# Kept aligned with scripts/119_md_formalization_validate.py SPECIMEN_FHIR_DIAG_* .
SPECIMEN_FHIR_DIAG_VIEWS: tuple[str, ...] = (
    "v_diag_specimen_duplicate_master_fp_v1",
    "v_diag_specimen_duplicate_focus_fp_v1",
    "v_diag_specimen_orphan_focus_master_v1",
    "v_diag_specimen_orphan_genomic_focus_v1",
    "v_diag_specimen_orphan_genomic_master_v1",
    "v_diag_specimen_fhir_broken_refs_v1",
    "v_diag_specimen_fhir_bundle_entry_drift_v1",
    "v_diag_specimen_genomics_dupe_thyroseq_slice_v1",
    "v_diag_specimen_genomics_tier_enum_v1",
    "v_diag_specimen_genomics_A_tier_requires_specimen_v1",
    "v_diag_specimen_genomics_thyroseq_ordinality_v1",
    "v_diag_specimen_provenance_master_v1",
    "v_diag_specimen_provenance_focus_v1",
    "v_diag_specimen_provenance_focus_gaps_v1",
    "v_diag_specimen_provenance_genomic_v1",
    "v_diag_specimen_review_burden_v1",
)

SPECIMEN_FHIR_DIAG_TABLES: tuple[str, ...] = (
    "t_diag_specimen_focus_qa_metrics_v1",
)


def _main_rel_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        ident = name.replace('"', '""')
        con.execute(f'SELECT 1 FROM main."{ident}" LIMIT 1')
        return True
    except Exception:
        return False


def _qa_rel_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        ident = name.replace('"', '""')
        con.execute(f'SELECT 1 FROM qa."{ident}" LIMIT 1')
        return True
    except Exception:
        return False


@dataclass(frozen=True)
class SpecimenFhirGateAssessment:
    """Result of ``assess_specimen_fhir_gate``."""

    anchor_present: bool
    missing_tables: tuple[str, ...]
    missing_diag_views: tuple[str, ...]
    missing_diag_tables: tuple[str, ...]

    @property
    def gate_applies(self) -> bool:
        """True when Check 13 is applicable (anchor table exists)."""
        return self.anchor_present

    @property
    def is_satisfied(self) -> bool:
        """Layer + 142 views + focus metrics table present (119 can run authoritative focus checks)."""
        return (
            self.anchor_present
            and not self.missing_tables
            and not self.missing_diag_views
            and not self.missing_diag_tables
        )

    @property
    def needs_full_materialization(self) -> bool:
        return self.anchor_present and bool(self.missing_tables)

    @property
    def needs_diagnostics_deploy_only(self) -> bool:
        return (
            self.anchor_present
            and not self.missing_tables
            and (bool(self.missing_diag_views) or bool(self.missing_diag_tables))
        )


def assess_specimen_fhir_gate(con: duckdb.DuckDBPyConnection) -> SpecimenFhirGateAssessment:
    """Classify specimen/FHIR readiness for release-mode validation."""
    if not _main_rel_exists(con, SPECIMEN_FHIR_ANCHOR):
        return SpecimenFhirGateAssessment(
            anchor_present=False,
            missing_tables=(),
            missing_diag_views=(),
            missing_diag_tables=(),
        )
    missing_tables = tuple(t for t in SPECIMEN_FHIR_TABLES if not _main_rel_exists(con, t))
    missing_diag = tuple(v for v in SPECIMEN_FHIR_DIAG_VIEWS if not _qa_rel_exists(con, v))
    missing_dtbl = tuple(t for t in SPECIMEN_FHIR_DIAG_TABLES if not _qa_rel_exists(con, t))
    return SpecimenFhirGateAssessment(
        anchor_present=True,
        missing_tables=missing_tables,
        missing_diag_views=missing_diag,
        missing_diag_tables=missing_dtbl,
    )


def remediation_message(*, py: str, scripts_dir: str) -> str:
    """Static operator hints (no secrets)."""
    return (
        "Specimen/FHIR release gate:\n"
        f"  Full layer (identity + FHIR + genomics binding + QA deploy):   {py} {scripts_dir}/138_md_specimen_fhir_layer.py --md\n"
        f"  QA diagnostics only (142 views, when main tables already exist): {py} {scripts_dir}/143_md_specimen_fhir_qa_diagnostics_deploy.py --md\n"
        "Overrides:\n"
        "  --materialize-specimen-fhir     run 138 / 143 automatically before validation (124 / 126)\n"
        "  --skip-specimen-fhir-gate       skip this preflight (119 --release-mode may still FAIL)\n"
    )


def format_gate_failure_detail(assessment: SpecimenFhirGateAssessment) -> str:
    parts: list[str] = []
    if assessment.missing_tables:
        parts.append(f"missing main tables ({len(assessment.missing_tables)}): {', '.join(assessment.missing_tables)}")
    if assessment.missing_diag_views:
        parts.append(
            f"missing qa diagnostic views ({len(assessment.missing_diag_views)}): "
            f"{', '.join(assessment.missing_diag_views)}"
        )
    if assessment.missing_diag_tables:
        parts.append(
            f"missing qa diagnostic tables ({len(assessment.missing_diag_tables)}): "
            f"{', '.join(assessment.missing_diag_tables)}"
        )
    return "; ".join(parts) if parts else "unknown"


class SpecimenFhirGateDecision:
    """Outcome for orchestrators (see ``decide_specimen_fhir_gate``)."""

    PASS = "pass"
    WARN_CONTINUE = "warn_continue"
    FAIL_EARLY = "fail_early"
    RUN_138 = "run_138"
    RUN_143 = "run_143"


def decide_specimen_fhir_gate(
    assessment: SpecimenFhirGateAssessment,
    *,
    enforce: bool,
    materialize: bool,
    skip_gate: bool,
) -> str:
    """Classify what the release orchestrator should do before ``119 --release-mode``."""
    if not assessment.gate_applies or assessment.is_satisfied:
        return SpecimenFhirGateDecision.PASS
    if skip_gate:
        return SpecimenFhirGateDecision.WARN_CONTINUE
    if not enforce:
        return SpecimenFhirGateDecision.WARN_CONTINUE
    if not materialize:
        return SpecimenFhirGateDecision.FAIL_EARLY
    if assessment.needs_full_materialization:
        return SpecimenFhirGateDecision.RUN_138
    if assessment.needs_diagnostics_deploy_only:
        return SpecimenFhirGateDecision.RUN_143
    return SpecimenFhirGateDecision.PASS


def run_specimen_fhir_release_gate(
    con: duckdb.DuckDBPyConnection,
    *,
    enforce: bool,
    materialize: bool,
    skip_gate: bool,
    dry_run: bool,
    materialize_target_md: bool,
    audit_dir: Path,
    step_results: list[dict],
    py: str,
    scripts_dir: Path,
    runner: Callable[[str, list[str], Path], bool],
    now_iso: Callable[[], str],
) -> bool:
    """Orchestrate preflight/maybe materialize for Check 13. ``runner`` logs like 124 ``_run``."""
    assessment = assess_specimen_fhir_gate(con)
    decision = decide_specimen_fhir_gate(
        assessment,
        enforce=enforce,
        materialize=materialize,
        skip_gate=skip_gate,
    )

    if decision == SpecimenFhirGateDecision.PASS:
        if assessment.gate_applies:
            print("  [PASS] Specimen/FHIR gate: Check 13 prerequisites present")
        return True

    detail = format_gate_failure_detail(assessment)
    if decision == SpecimenFhirGateDecision.WARN_CONTINUE:
        print(
            f"  [WARN] Specimen/FHIR incomplete ({detail}); continuing — "
            f"{'119 is not in release-mode' if not enforce else '119 may still FAIL in release-mode'}"
        )
        return True

    if decision == SpecimenFhirGateDecision.FAIL_EARLY:
        print("\n  FATAL: Specimen/FHIR gate — strict validation requires this surface when "
              "main.synoptic_tumor_long_v1 exists.")
        print(f"  Detail: {detail}")
        print(remediation_message(py=py, scripts_dir=str(scripts_dir)))
        step_results.append({
            "step": "Specimen/FHIR gate (precheck)",
            "started": now_iso(),
            "finished": now_iso(),
            "returncode": 1,
            "success": False,
            "log": "specimen_fhir_gate.txt",
        })
        (audit_dir / "specimen_fhir_gate.txt").write_text(
            f"FAIL_EARLY\n{detail}\n", encoding="utf-8"
        )
        return False

    if dry_run:
        print(f"  [dry-run] Would run specimen/FHIR materialization ({detail})")
        return True

    if decision == SpecimenFhirGateDecision.RUN_138:
        cmd = [py, str(scripts_dir / "138_md_specimen_fhir_layer.py")]
        if materialize_target_md:
            cmd.append("--md")
        return runner("Specimen/FHIR layer (138)", cmd, audit_dir / "specimen_fhir_138_output.log")

    if decision == SpecimenFhirGateDecision.RUN_143:
        cmd = [py, str(scripts_dir / "143_md_specimen_fhir_qa_diagnostics_deploy.py")]
        if materialize_target_md:
            cmd.append("--md")
        return runner(
            "Specimen/FHIR QA diagnostics (143)",
            cmd,
            audit_dir / "specimen_fhir_143_output.log",
        )

    return True


specimen_fhir_release_gate = run_specimen_fhir_release_gate
