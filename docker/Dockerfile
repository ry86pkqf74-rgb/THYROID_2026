# THYROID_2026 LangChain Extraction Pipeline
# Docker Pro-optimized multi-stage build
FROM python:3.12-slim AS base

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps -- pinned for reproducibility
COPY docker/requirements.txt .
RUN pip install -r requirements.txt

# Copy extraction code
COPY scripts/langchain_extraction.py scripts/
COPY scripts/run_extraction_split.py scripts/
COPY llm_extraction/prompts/ llm_extraction/prompts/

# Data and output dirs (mounted at runtime)
RUN mkdir -p processed/remaining processed/output

# Default env vars
ENV OLLAMA_BASE_URL=http://host.docker.internal:11434/v1
ENV OLLAMA_MODEL=qwen3:14b
ENV LANGCHAIN_TRACING_V2=false
ENV LANGCHAIN_PROJECT=thyroid-2026-extraction

# Healthcheck -- verifies Ollama is reachable
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD curl -sf ${OLLAMA_BASE_URL}/models || exit 1

ENTRYPOINT ["python3", "scripts/langchain_extraction.py"]
CMD ["--domains", "imaging", "pathology", "labs"]
