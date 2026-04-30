# Doc Consolidation Assignment (verbatim — 2026-04-30)

> Saved so future turns can reference the spec without paraphrasing. Owner provided this prompt + a guardrail to stop after inventory.

---

# Assignment: Consolidate Repository Documentation

You are reviewing the documentation for this repo. Your job is to consolidate the current scattered Markdown files into a smaller, cleaner documentation set with clear separation of concerns.

## Goal

Create documentation that serves two distinct audiences:

1. **Human operators**
   - People who need to run routine business/ops processes.
   - They are not necessarily developers.
   - They need clear, safe, step-by-step procedures.

2. **LLMs / developers**
   - People or agents working on the codebase.
   - They need architecture, workflow, system behavior, file locations, deployment notes, debugging notes, and implementation constraints.

## Current docs to review

Review every existing Markdown file in the repo, including but not limited to:

- GUIDE-ASSIGNMENT-SYSTEM.md
- GUIDE-CanonTools.md
- GUIDE-DAILY-COMMERCIALLOG.md
- GUIDE-MONTHLY-CLOSING.md
- GUIDE-OPERATIONS.md
- GUIDE-Railway.md
- GUIDE-RaspberryWorkflow.md
- GUIDE-TwentyNineColumns.md
- GUIDE-failover-failback.md
- GUIDE_Customer_Name_Normalization.md
- GUIDE_DEV_WORKFLOW.md
- GUIDE_GIT_WORKFLOW.md
- TAILSCALE_AUTH.md
- USER_MANAGEMENT_SETUP.md
- docker-setup.md
- ops.md
- planning-export-client-contract.md
- sheet-export-client-contract.md
- sheet-export-runbook.md
- workbook-ae-drift-tracker.md

## Required output structure

Create a new `docs/` structure like this:

```
docs/
  HUMAN_OPERATOR_GUIDE.md
  LLM_SYSTEM_GUIDE.md
  RUNBOOKS.md
  ARCHITECTURE.md
  DEV_WORKFLOW.md
  API_AND_EXPORT_CONTRACTS.md
  ARCHIVE/
```

## File purposes

### `HUMAN_OPERATOR_GUIDE.md`

This is the primary human-facing document.

Include only what a non-developer operator needs to safely operate the system.

It should cover:

* What the system is for
* Daily commercial log process
* Monthly manual close process
* Yearly recap / cash revenue update process, if documented
* Basic validation checks
* Where inputs come from
* Where outputs go
* What "good" looks like
* What to do when something fails
* What not to touch
* When to escalate to a developer

Use plain English. Avoid implementation detail unless needed for safe operation.

Every recurring process should have:

```
## Process Name

### When to do this

### Inputs needed

### Step-by-step procedure

### Validation checklist

### Common failures

### Escalate if
```

### `LLM_SYSTEM_GUIDE.md`

This is the primary LLM-facing system guide.

Include:

* System purpose
* Main app/service names
* Important directories
* Important scripts
* SQLite database role
* Import/update pipeline overview
* Deployment assumptions
* Environment variables
* Known invariants
* Known footguns
* Rules for modifying the system
* Where to look before changing code

This file should help a new coding LLM avoid hallucinating paths, commands, schema assumptions, or workflow behavior.

### `RUNBOOKS.md`

Include operational runbooks that are too technical for the human guide but still procedural.

Examples:

* Restarting services
* Checking logs
* Running imports manually
* Verifying timers
* Recovering from failed imports
* Backup/restore procedures
* Failover/failback procedures

Each runbook should be command-oriented and include expected output where possible.

### `ARCHITECTURE.md`

Consolidate system architecture material:

* App structure
* Data flow
* Import flow
* Database/storage layout
* Deployment topology
* Raspberry Pi / server / Docker / Railway notes, if still relevant
* Tailscale/networking model
* Authentication model
* Failover model

Separate current architecture from legacy/deprecated architecture.

### `DEV_WORKFLOW.md`

Consolidate developer workflow material:

* Branching model
* Git workflow
* Local dev setup
* Testing expectations
* PR expectations
* Deployment workflow
* Coding conventions
* LLM coding instructions
* How to safely make schema/import changes

### `API_AND_EXPORT_CONTRACTS.md`

Consolidate:

* sheet export client contract
* planning export client contract
* workbook AE drift tracker
* any API payload/field contracts
* TwentyNineColumns material if it defines schema/export expectations

This should be the canonical contract reference.

### `ARCHIVE/`

Move obsolete, duplicated, superseded, or historical docs here.

Do not delete information unless it is clearly wrong and replaced elsewhere.

## Method

Work in phases.

### Phase 1: Inventory

Read every Markdown file.

Create an inventory table:

```
| File | Main topic | Audience | Keep / Merge / Archive | Destination | Notes |
```

Audience should be one of:

* Human operator
* Developer
* LLM
* DevOps
* Historical
* Mixed

### Phase 2: Extract facts

Extract only durable facts.

Preserve exact:

* paths
* script names
* service names
* timer names
* environment variable names
* database paths
* command examples
* business process steps
* schema field names
* API field names
* known bugs / footguns

Flag conflicts instead of silently resolving them.

Use this format:

```
## Conflicts / items needing human confirmation

- Conflict:
  - Source A says:
  - Source B says:
  - Proposed resolution:
  - Needs confirmation:
```

### Phase 3: Consolidate

Create the new docs listed above.

Rules:

* Do not duplicate the same procedure across multiple files.
* Human guide links to technical runbooks instead of embedding deep technical detail.
* LLM guide links to architecture/dev/API docs instead of repeating them.
* Archive legacy docs after their useful content has been merged.
* Add "Last reviewed" date at the top of every new doc.
* Add "Audience" and "Purpose" at the top of every new doc.

### Phase 4: Produce migration plan

Create a final migration plan:

```
# Documentation Migration Plan

## New docs created

## Old docs merged

## Old docs archived

## Conflicts needing owner decision

## Suggested next review cadence
```

## Important style rules

* Prefer fewer, better docs.
* Be explicit about current vs legacy behavior.
* Never invent paths, commands, table names, or service names.
* If a fact is uncertain, mark it `NEEDS CONFIRMATION`.
* If two docs conflict, preserve both claims and flag the conflict.
* Keep human docs short and procedural.
* Keep LLM/developer docs precise and implementation-heavy.
* Use links between docs instead of repeating large sections.
* Remove motivational prose, stale planning notes, and duplicated explanations.
* Preserve operational warnings and footguns.

## Deliverables

1. Updated `docs/` folder with the new canonical docs.
2. Existing docs either merged or moved into `docs/ARCHIVE/`.
3. A `docs/DOCUMENTATION_MIGRATION_PLAN.md`.
4. A short summary of:

   * what changed
   * what conflicts remain
   * what needs human confirmation before old docs are deleted

---

## Owner-added guardrail

> Have the LLM do **inventory + proposed file map first**, then stop for owner approval before it rewrites anything.

So: Phase 1 lands first, owner approves, then Phases 2–4 proceed.
