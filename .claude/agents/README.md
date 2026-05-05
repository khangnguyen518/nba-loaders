# Sub-Agents — Usage Guide

Five specialized sub-agents for the sports_data dbt + loader workflow. Each runs in its own context window with restricted tools.

## How to invoke

**Auto-delegation** — Claude routes based on the agent's `description`. Just describe your task naturally:
- "Should the player_game_log be a fact or dimension?" → `dbt-design-consultant`
- "I need to scope a new model for shot charts" → `dbt-requirements-clarifier`

**Explicit invocation** — `@<agent-name> <task>` when you want a specific agent:
- `@data-loader-developer build a loader for the NBA play-by-play API`
- `@test-creator scaffold pytest cases for loaders/nba_box_score.py`
- `@documentation-writer write a runbook for the daily refresh`

After editing any agent file, restart Claude Code so changes load.

---

## The five agents

### 1. `dbt-design-consultant` — Pros/cons before code
**Read-only.** Use first when you don't yet know the right approach.

- Presents 2–3 options with trade-offs, then a recommendation
- Never writes SQL or files
- Tools: `Read, Glob, Grep`

**When to use**: SCD type choice, fact vs. dim, materialization (table/view/incremental), grain decisions, partition/cluster strategy.

**Example**:
> "I'm building a model for player season averages. Options for handling mid-season trades?"

---

### 2. `data-loader-developer` — Build new ingestion
**Writes code.** Use after you've decided on the approach.

- Reads existing loaders to match style before writing
- Defaults to incremental loading
- Includes retry, timeout, logging, type hints
- Tools: `Read, Write, Edit, Bash, Glob, Grep`

**When to use**: new API connector, refactoring an existing loader, adding a new data source.

**Example**:
> "@data-loader-developer add a loader for nba.com advanced box scores. Cursor on game_date."

---

### 3. `test-creator` — Test scaffolding
**Writes tests, runs them.** Use after a loader/model exists.

- pytest for Python, `dbt test` for SQL
- One assertion per test, descriptive names
- Covers happy path + edge cases (nulls, dupes, schema drift)
- Outputs the exact terminal command to run
- Tools: `Read, Write, Edit, Bash, Glob`

**Example**:
> "@test-creator add unit tests for loaders/nba_box_score.py covering empty response and malformed JSON"

---

### 4. `documentation-writer` — Markdown docs
**Writes docs only.** No bash, no code execution.

- Audience: data engineers (not novices)
- Code blocks over prose
- Sections: Purpose, Prerequisites, Steps, Examples, Troubleshooting
- Tools: `Read, Write, Edit, Glob`

**Example**:
> "@documentation-writer create a runbook for backfilling the player_game_log table"

---

### 5. `dbt-requirements-clarifier` — Ask first, design later
**Read-only.** Use at the start of vague requests.

- Asks structured questions across 7 dimensions (business logic, data quality, freshness, performance, downstream, testability, ownership)
- Outputs top 3–5 blocking questions + "what I learned so far" summary
- Does NOT propose solutions
- Tools: `Read, Glob, Grep`

**Example**:
> "Someone wants a 'team strength' metric. Help me figure out what they actually need."

---

## Recommended workflow for a new dbt model

```
1. @dbt-requirements-clarifier  → uncover assumptions, get blocking questions
2. (answer the questions)
3. @dbt-design-consultant       → pick an approach with pros/cons
4. (implement the model yourself or with main Claude)
5. @test-creator                → add dbt tests
6. @documentation-writer        → document the model
```

## Recommended workflow for a new loader

```
1. @dbt-requirements-clarifier  → scope the source
2. @data-loader-developer       → build the loader
3. @test-creator                → add pytest cases
4. @documentation-writer        → write setup/runbook docs
```

---

## Editing agents

Files live in `.claude/agents/`. Each is a markdown file with YAML frontmatter:

```markdown
---
name: agent-name
description: When Claude should delegate (this is the trigger)
tools: Read, Glob, Grep    # comma-separated; omit to inherit all
model: sonnet              # sonnet | haiku | opus
---

System prompt body.
```

**Tips**:
- The `description` is what triggers auto-delegation. Make it specific and keyword-rich.
- Use `tools:` to restrict (e.g., read-only consultants).
- Restart Claude Code after edits.

## Listing agents in a session

Run `/agents` in Claude Code to see all loaded agents and their descriptions.
