# Copilot Instructions — THYROID_2026

## Mandatory: Assess Tools Before Every Response

**Before answering any prompt**, you MUST silently run this checklist:

1. **Scan available MCP tools** — check which of the following are relevant to this request:
   - `docker-gateway` (build, run, compose, inspect, volumes, dry-run)
   - `composio-github` (PRs, issues, repos, code search)
   - `composio-slack` (send messages, read channels)
   - `composio-linear` / `composio-jira` (create/update issues)
   - `composio-notion` / `composio-confluence` (read/write docs)
   - `composio-sentry` (errors, releases)
   - `composio-vercel` (deployments)
   - `composio-figma` / `composio-airtable` / `composio-gmail` (design, data, email)
   - `playwright` (browser automation, UI testing)
   - `git` (local repo ops)
   - `context7` (up-to-date library docs)

2. **Check if a custom agent is better suited**:
   - `@CostOptimizer` — for any cost, architecture, or cloud decision
   - `@FullImplementer` — for end-to-end feature builds
   - Mention this if the user would benefit from switching

3. **Apply cheap-first routing** (from AGENTS.md):
   - Local/Docker first → Composio read-only → cloud (gated with cost estimate)

4. **State which tools you're using** at the start of your response when using MCP tools.

## Always-On Rules
- Never execute cloud writes, deployments, or destructive actions without explicit user confirmation.
- If a Composio tool would help but needs OAuth, say so and provide the connect prompt.
- Prefer read-only MCP calls first; escalate to write after confirming intent.
- For every production-impacting action: end with "Ready to execute — confirm?"

## Project Context
- Repo: THYROID_2026 — clinical thyroid cancer research pipeline
- Stack: Python, DuckDB, MotherDuck, Streamlit, Parquet
- Primary key: `research_id` (int) across all tables
- MCP tools, custom agents, and AGENTS.md policy apply to ALL tasks in this repo
