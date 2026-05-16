.PHONY: install dev test lint fmt eval-molecular run-molecular help

help:
	@echo "thyroid-mlx-extract — common commands"
	@echo ""
	@echo "  install            Install package + deps"
	@echo "  dev                Install with dev deps"
	@echo "  test               Run pytest"
	@echo "  lint               ruff check"
	@echo "  fmt                ruff format"
	@echo ""
	@echo "  eval-<task>        Evaluate candidate models against gold for <task>"
	@echo "  run-<task>         Run winning model over full corpus for <task>"
	@echo "  push-<task>        Push extracted results to BQ note_entities_llm_<task>"
	@echo ""
	@echo "Tasks: molecular, synoptic, ultrasound, imaging_ct, imaging_mri,"
	@echo "       imaging_nm, fna, complications, death, risk_factors"

install:
	pip install -e .

dev:
	pip install -e ".[dev,vision,speech]"

test:
	pytest

lint:
	ruff check src tests

fmt:
	ruff format src tests

# Per-task shortcuts — pattern rules
eval-%:
	thyroid-mlx eval $* --gold gold/$*_gold.csv

run-%:
	thyroid-mlx run $*

push-%:
	thyroid-mlx push $*
