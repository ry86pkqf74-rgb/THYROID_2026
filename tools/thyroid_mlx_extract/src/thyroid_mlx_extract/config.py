"""Task definitions, model registry, BQ configuration.

The single source of truth for what tasks exist, which model is recommended,
which BQ tables feed each task, and where results land.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# BigQuery
# ---------------------------------------------------------------------------
BQ_PROJECT = os.environ.get("BQ_PROJECT", "thyroid-canonical-pub-2026")
BQ_CANONICAL = f"{BQ_PROJECT}.pub_canonical"
BQ_WORKSPACE = f"{BQ_PROJECT}.pub_workspace"

# ---------------------------------------------------------------------------
# Model registry
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class ModelSpec:
    """A single MLX-runnable model."""

    key: str                       # short name used in CLI flags
    hf_repo: str                   # HuggingFace repo (mlx-community/<...>)
    quant: str                     # '4bit', '8bit', 'fp16'
    memory_gb: float               # approx VRAM at this quant
    strengths: tuple[str, ...]     # task fits
    is_reasoning: bool = False     # use for adjudication / hard cases
    is_medical: bool = False       # medical-domain fine-tune


MODELS: dict[str, ModelSpec] = {
    # Small medical
    "medgemma4b": ModelSpec(
        key="medgemma4b",
        hf_repo="mlx-community/MedGemma-1.5-4B-IT-4bit",
        quant="4bit",
        memory_gb=2.5,
        strengths=("short_medical", "high_volume", "templated"),
        is_medical=True,
    ),
    # Best small reasoning
    "phi4": ModelSpec(
        key="phi4",
        hf_repo="mlx-community/Phi-4-4bit",
        quant="4bit",
        memory_gb=8.0,
        strengths=("small_reasoning", "math", "code"),
    ),
    # Best open-weight medical
    "medgemma27b": ModelSpec(
        key="medgemma27b",
        hf_repo="mlx-community/MedGemma-1.5-27B-IT-4bit",
        quant="4bit",
        memory_gb=14.0,
        strengths=("templated_medical", "lab_reports", "fna", "complications", "ihc"),
        is_medical=True,
    ),
    "gemma3-27b": ModelSpec(
        key="gemma3-27b",
        hf_repo="mlx-community/Gemma-3-27B-IT-4bit",
        quant="4bit",
        memory_gb=14.0,
        strengths=("general", "tool_use", "multilingual"),
    ),
    "qwen3-32b": ModelSpec(
        key="qwen3-32b",
        hf_repo="mlx-community/Qwen3-32B-Instruct-4bit",
        quant="4bit",
        memory_gb=18.0,
        strengths=("general_instruct", "sql"),
    ),
    # Best general extractor at this size
    "llama33-70b": ModelSpec(
        key="llama33-70b",
        hf_repo="mlx-community/Llama-3.3-70B-Instruct-4bit",
        quant="4bit",
        memory_gb=38.0,
        strengths=("hard_semantics", "long_narrative", "imaging", "extraction_general"),
    ),
    # Best local reasoning at 70B
    "r1-distill-70b": ModelSpec(
        key="r1-distill-70b",
        hf_repo="mlx-community/DeepSeek-R1-Distill-Llama-70B-4bit",
        quant="4bit",
        memory_gb=38.0,
        strengths=("adjudication", "reasoning", "ete_distinctions"),
        is_reasoning=True,
    ),
    "qwen3-72b": ModelSpec(
        key="qwen3-72b",
        hf_repo="mlx-community/Qwen3-72B-Instruct-4bit",
        quant="4bit",
        memory_gb=40.0,
        strengths=("general_instruct", "long_context"),
    ),
    # Top open-weight reasoner — M5 Ultra only
    "qwen3-235b-thinking": ModelSpec(
        key="qwen3-235b-thinking",
        hf_repo="mlx-community/Qwen3-235B-A22B-Thinking-4bit",
        quant="4bit",
        memory_gb=120.0,
        strengths=("hardest_adjudication", "synoptic", "reasoning"),
        is_reasoning=True,
    ),
}


# ---------------------------------------------------------------------------
# Task registry
# ---------------------------------------------------------------------------
@dataclass(frozen=True)
class TaskSpec:
    """A single extraction task."""

    task_id: str                      # used in CLI and table names
    source_tables: tuple[str, ...]    # BQ tables in pub_canonical the source text comes from
    source_columns: tuple[str, ...]   # column names containing the free text
    output_table: str                 # where structured output lands
    schema_module: str                # python path to Pydantic schema
    prompt_file: str                  # filename in prompts/
    primary_model: str                # default model key (must exist in MODELS)
    fallback_model: str               # alternate at similar memory
    adjudicator_model: Optional[str]  # second model for hard cases (None = no adjudication)
    domain: str                       # entity_domain value written to BQ
    notes: str                        # operational notes


TASKS: dict[str, TaskSpec] = {
    "molecular": TaskSpec(
        task_id="molecular",
        source_tables=("molecular_results",),
        source_columns=("raw_payload_json",),
        output_table="note_entities_llm_molecular_v1",
        schema_module="thyroid_mlx_extract.schemas.molecular",
        prompt_file="molecular.txt",
        primary_model="medgemma27b",
        fallback_model="llama33-70b",
        adjudicator_model="r1-distill-70b",
        domain="molecular",
        notes="Templated lab reports. raw_payload_json is BYTES — cast to STRING.",
    ),
    "synoptic": TaskSpec(
        task_id="synoptic",
        source_tables=("path_synoptics", "clinical_notes_long"),
        source_columns=("synoptic_diagnosis", "path_diagnosis_comment", "note_text"),
        output_table="note_entities_llm_synoptic_pathology_v2",
        schema_module="thyroid_mlx_extract.schemas.synoptic",
        prompt_file="synoptic.txt",
        primary_model="llama33-70b",
        fallback_model="qwen3-72b",
        adjudicator_model="r1-distill-70b",
        domain="pathology",
        notes="Hard semantics (capsule vs ETE vs gross ETE). Consider LoRA fine-tune.",
    ),
    "ultrasound": TaskSpec(
        task_id="ultrasound",
        source_tables=("ultrasound_reports",),
        source_columns=(
            "source_us_impression",
            "lymph_node_assessment",
            "clinical_impression",
            "nodule_1_source_description",
            "nodule_2_source_description",
            "nodule_3_source_description",
        ),
        output_table="note_entities_llm_us_nodule_features_v1",
        schema_module="thyroid_mlx_extract.schemas.ultrasound",
        prompt_file="ultrasound.txt",
        primary_model="medgemma4b",
        fallback_model="medgemma27b",
        adjudicator_model=None,
        domain="ultrasound",
        notes="Short text. Run small model fast; only escalate on confidence < 0.7.",
    ),
    "imaging_ct": TaskSpec(
        task_id="imaging_ct",
        source_tables=("ct_imaging",),
        source_columns=("original_report",),
        output_table="note_entities_llm_imaging_ct_v1",
        schema_module="thyroid_mlx_extract.schemas.imaging",
        prompt_file="imaging_ct.txt",
        primary_model="llama33-70b",
        fallback_model="medgemma27b",
        adjudicator_model="r1-distill-70b",
        domain="imaging_ct",
        notes="T4a/T4b detail: tracheal cartilage, esophageal layers, RLN groove, mediastinal LN.",
    ),
    "imaging_mri": TaskSpec(
        task_id="imaging_mri",
        source_tables=("mri_imaging",),
        source_columns=("original_report",),
        output_table="note_entities_llm_imaging_mri_v1",
        schema_module="thyroid_mlx_extract.schemas.imaging",
        prompt_file="imaging_mri.txt",
        primary_model="llama33-70b",
        fallback_model="medgemma27b",
        adjudicator_model=None,
        domain="imaging_mri",
        notes="Small corpus (~715). Single model + manual QC on full set is feasible.",
    ),
    "imaging_nm": TaskSpec(
        task_id="imaging_nm",
        source_tables=("nuclear_med",),
        source_columns=("findings_text", "impression_text"),
        output_table="note_entities_llm_imaging_nm_v1",
        schema_module="thyroid_mlx_extract.schemas.imaging",
        prompt_file="imaging_nm.txt",
        primary_model="medgemma27b",
        fallback_model="medgemma4b",
        adjudicator_model=None,
        domain="imaging_nm",
        notes="Short reports. Focal uptake patterns, mediastinal, retrosternal extension.",
    ),
    "fna": TaskSpec(
        task_id="fna",
        source_tables=("fna_cytology",),
        source_columns=("path_text",),
        output_table="note_entities_llm_fna_features_v1",
        schema_module="thyroid_mlx_extract.schemas.fna",
        prompt_file="fna.txt",
        primary_model="medgemma27b",
        fallback_model="medgemma4b",
        adjudicator_model=None,
        domain="fna_cytology",
        notes="Nuclear features (grooves, pseudoinclusions), architecture, adequacy.",
    ),
    "complications": TaskSpec(
        task_id="complications",
        source_tables=("clinical_notes_long",),
        source_columns=("note_text",),
        output_table="note_entities_llm_complications_v2",
        schema_module="thyroid_mlx_extract.schemas.complications",
        prompt_file="complications.txt",
        primary_model="medgemma27b",
        fallback_model="llama33-70b",
        adjudicator_model="r1-distill-70b",
        domain="complications",
        notes="Pre-filter with regex (hypocalc, RLN, hematoma, seroma, chyle) to drop volume.",
    ),
    "death": TaskSpec(
        task_id="death",
        source_tables=("clinical_notes_long",),
        source_columns=("note_text",),
        output_table="note_entities_llm_death_attribution_v1",
        schema_module="thyroid_mlx_extract.schemas.death",
        prompt_file="death.txt",
        primary_model="llama33-70b",
        fallback_model="r1-distill-70b",
        adjudicator_model="r1-distill-70b",
        domain="death_attribution",
        notes="Filter to note_type='DEATH' (n=153). Two-model agreement required.",
    ),
    "risk_factors": TaskSpec(
        task_id="risk_factors",
        source_tables=("clinical_notes_long",),
        source_columns=("note_text",),
        output_table="note_entities_llm_risk_factors_v1",
        schema_module="thyroid_mlx_extract.schemas.risk_factors",
        prompt_file="risk_factors.txt",
        primary_model="medgemma4b",
        fallback_model="medgemma27b",
        adjudicator_model=None,
        domain="risk_factors",
        notes="Filter to note_type='HP'. Childhood XRT, family hx, smoking, BMI.",
    ),

    "llm_dynamic_risk": TaskSpec(
        task_id="llm_dynamic_risk",
        source_tables=("clinical_notes_long",),
        source_columns=("note_text",),
        output_table="note_entities_llm_dynamic_risk_response_v2",
        schema_module="thyroid_mlx_extract.schemas.llm_dynamic_risk",
        prompt_file="llm_dynamic_risk.txt",
        primary_model="medgemma27b",
        fallback_model="llama33-70b",
        adjudicator_model=None,
        domain="dynamic_risk_response",
        notes="Phase 4 re-extraction. Filter to note_type IN ('HP', 'ENDOCRINE_FM').",
    ),
    "llm_recurrence": TaskSpec(
        task_id="llm_recurrence",
        source_tables=("clinical_notes_long",),
        source_columns=("note_text",),
        output_table="note_entities_llm_recurrence_v2",
        schema_module="thyroid_mlx_extract.schemas.llm_recurrence",
        prompt_file="llm_recurrence.txt",
        primary_model="medgemma27b",
        fallback_model="llama33-70b",
        adjudicator_model="r1-distill-70b",
        domain="recurrence",
        notes="Phase 4 re-extraction. Filter to follow-up note types.",
    ),
    "llm_us_dynamics": TaskSpec(
        task_id="llm_us_dynamics",
        source_tables=("ultrasound_reports",),
        source_columns=("source_us_impression", "clinical_impression"),
        output_table="note_entities_llm_us_nodule_dynamics_v2",
        schema_module="thyroid_mlx_extract.schemas.llm_us_dynamics",
        prompt_file="llm_us_dynamics.txt",
        primary_model="medgemma4b",
        fallback_model="medgemma27b",
        adjudicator_model=None,
        domain="us_dynamics",
        notes="Phase 4 re-extraction. Source is follow-up US comparisons.",
    ),
    "llm_synoptic_enrich": TaskSpec(
        task_id="llm_synoptic_enrich",
        source_tables=("path_synoptics", "clinical_notes_long"),
        source_columns=("synoptic_diagnosis", "path_diagnosis_comment", "microscopic_description"),
        output_table="note_entities_llm_synoptic_pathology_enrichment_v2",
        schema_module="thyroid_mlx_extract.schemas.synoptic",
        prompt_file="synoptic.txt",
        primary_model="llama33-70b",
        fallback_model="qwen3-72b",
        adjudicator_model="r1-distill-70b",
        domain="pathology",
        notes="Phase 4 re-extraction. Uses Phase 1 synoptic schema and prompt.",
    ),
}


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
def repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def prompts_dir() -> Path:
    return Path(__file__).resolve().parent / "prompts"


def gold_dir() -> Path:
    return repo_root() / "gold"


def results_dir() -> Path:
    p = repo_root() / "results"
    p.mkdir(parents=True, exist_ok=True)
    return p


def runs_dir() -> Path:
    p = repo_root() / "runs"
    p.mkdir(parents=True, exist_ok=True)
    return p
