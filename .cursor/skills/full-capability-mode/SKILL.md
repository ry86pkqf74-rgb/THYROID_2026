---
name: full-capability-mode
description: Apply on every prompt and task. Ensures full use of superpowers (skill-first discipline), parallel processing via mcp_task and subagents, all relevant skills/tools/MCP, and continual learning when maintaining AGENTS.md or mining transcripts.
---

# Full Capability Mode

Use this skill on **every prompt**. It enforces maximum use of superpowers, parallel work, skills/tools/MCP, and continual learning.

## 1. Superpowers (skills-first)

- **Before any response or action:** Invoke the **using-superpowers** skill (or equivalent flow). If any skill might apply (even 1%), invoke it via the Skill tool.
- **Process skills before implementation:** e.g. brainstorming, debugging, TDD, writing-plans before touching code.
- Do not skip skill checks for "simple" questions or "quick" tasks.

## 2. Parallel processing

- **Independent tasks:** When 2+ tasks have no shared state or sequential dependency, run them in parallel (e.g. multiple tool calls at once, or `mcp_task` with subagent_type `explore`, `shell`, `generalPurpose`, etc.).
- **Subagents:** Use `mcp_task` for broad exploration, multi-step execution, or when the task benefits from a dedicated agent (e.g. `subagent_type="explore"` for codebase discovery, `subagent_type="shell"` for commands).
- Prefer parallel tool calls over sequential when operations are independent.

## 3. Skills, tools, and MCP

- **Skills:** Check available skills for every task; use domain skills (e.g. Convex, Figma, BrowserStack, Vercel, parallel-web-search, TDD, verification-before-completion) when they apply.
- **Tools:** Use the right tool (e.g. `Grep` for exact text, `SemanticSearch` for meaning, `Read` for known files; avoid redundant reads).
- **MCP:** When the task involves Convex, Linear, Figma, or BrowserStack, use MCP tools. List/read MCP tool descriptors before calling; use `call_mcp_tool` with correct server and tool name.

## 4. Continual learning

- When the user asks to mine chats, maintain AGENTS.md, or build a self-learning loop, invoke the **continual-learning** skill and follow its workflow (transcript root, incremental index, AGENTS.md output contract).
- When corrections or durable preferences emerge in conversation, consider updating AGENTS.md per project conventions (e.g. plain bullets under Learned User Preferences / Learned Workspace Facts).

## Checklist (every prompt)

- [ ] Relevant skills invoked (especially using-superpowers; process skills before implementation)
- [ ] Parallel work used where 2+ independent tasks exist (parallel tool calls or mcp_task)
- [ ] Appropriate skills/tools/MCP considered and used
- [ ] Continual learning applied when maintaining memory or mining transcripts
