-- ============================================================================
-- Migration 00 — Foundation: manual review queue + workspace schema
-- ============================================================================
-- Target DB:     thyroid_canonical_publication_v1_0 (MotherDuck)
-- Target schema: manuscript_workspace
-- Issue:         FOUNDATION (prerequisite for all QC cleanup prompts)
-- Author:        Logan Glosser
-- Date:          2026-04-22
-- ----------------------------------------------------------------------------
-- Purpose:
--   Create the workspace schema (no-op if present) and a queue table for rows
--   that QC rules flag as needing manual chart review. All downstream QC
--   prompts (01-59) write rejected/ambiguous rows here rather than silently
--   dropping them.
--
-- Idempotency:
--   Safe to re-run. Uses IF NOT EXISTS on both the sequence and the table.
--   No data is mutated if objects already exist.
--
-- Contract:
--   - Never mutate main.*
--   - All outputs land in manuscript_workspace.*
--   - queue_id auto-increments via manuscript_workspace.qc_queue_seq
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS manuscript_workspace;

CREATE SEQUENCE IF NOT EXISTS manuscript_workspace.qc_queue_seq;

CREATE TABLE IF NOT EXISTS manuscript_workspace.qc_manual_review_queue_v1 (
    queue_id        BIGINT PRIMARY KEY DEFAULT nextval('manuscript_workspace.qc_queue_seq'),
    issue_id        VARCHAR NOT NULL,
    research_id     INTEGER,
    source_table    VARCHAR,
    source_pk       VARCHAR,
    context_json    JSON,
    reason          VARCHAR,
    status          VARCHAR DEFAULT 'open',
    reviewer_notes  VARCHAR,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at     TIMESTAMP
);
