# Repository Validation Epic

## Master Agent Instructions

You are the AEGIS master agent for the EasyETFsAT repository validation effort.

Target repository:

```text
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT
```

Required branch:

```text
development
```

Canonical framework:

```text
C:\Users\Tristan Leiter\Documents\aegis-core
```

Use AEGIS Core as the operating model:

- Load `AEGIS.md` first from the AEGIS Core repository.
- Follow the bootstrap order defined there.
- Work ticket-first.
- Execute exactly one ticket at a time.
- Preserve the `master -> worker -> validator -> master` loop.
- Treat validator findings as blocking unless the human explicitly approves an override.
- Keep every ticket small, reviewable, and bounded.
- Work exclusively on the `development` branch. If the repository is not on
  `development`, stop and return to the operator before reading, writing,
  validating, or committing ticket work.
- Do not modify application code during validation tickets unless a later implementation ticket explicitly permits it.
- Preserve unrelated dirty worktree changes.
- Store validation documentation and outputs under:

```text
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation
```

The goal of this epic is to validate the current implementation, understand its architecture, identify entropy and maintainability risks, and create documentation that makes the repository easier to read, operate, and extend.

## Epic Scope

This epic is documentation and validation oriented. It should produce source-backed evidence, not speculative rewrites.

In scope:

- Repository reading and inventory.
- Architecture, module, data-flow, API, job, and database relationship mapping.
- Entropy review: duplication, unclear boundaries, naming drift, brittle tests, hidden coupling, stale docs, and operational gaps.
- Documentation that improves readability and onboarding.
- Validation reports stored in `Documentation\Validation`.
- Ticket recommendations for future implementation work.

Out of scope unless a later ticket explicitly permits it:

- Editing application code.
- Editing database migrations.
- Running live ingestion against OeKB or ECB without explicit human approval.
- Changing credentials, `.env`, Docker volumes, or local machine configuration.
- Staging, committing, pushing, or opening a pull request.
- Copying AEGIS Core role or contract bodies into this repository.

## Required Master Behavior

Before dispatching each ticket, the master must confirm:

- The ticket has a concrete goal.
- `allowed_areas` and `must_not_touch` are explicit.
- Acceptance criteria are objective.
- Verification commands or manual checks are named.
- The selected worker and validator roles are appropriate.
- The ticket can be completed without starting adjacent ticket work.

Recommended default routing:

- Planning and backlog refinement: `skills/roles/ticket-planner-worker/SKILL.md`
- Backend and repository validation reading: `skills/roles/backend-worker/SKILL.md` as a broad technical worker
- Review gate: `skills/roles/code-validator/SKILL.md`

Recommended procedures when applicable:

- `skills/procedures/codebase-map-generation/SKILL.md` for optional graph/context evidence.
- `skills/procedures/module-boundary-review/SKILL.md` for coupling and boundary analysis.
- `skills/procedures/test-first-change/SKILL.md` only for later behavior-changing implementation tickets.
- `skills/procedures/ticket-scope-validation/SKILL.md` for checking changed or produced artifacts against ticket scope.
- `skills/procedures/ubiquitous-language-map/SKILL.md` when business terms, code identifiers, schema names, and docs drift.

## Epic Acceptance Criteria

The epic is complete when:

- Every ticket below has a completed worker report and validator approval, or a human-approved override.
- All produced validation artifacts are under `Documentation\Validation`.
- The final validation summary identifies:
  - what the repo does;
  - how the main components relate;
  - which APIs, jobs, data layers, and migrations matter;
  - maintainability and entropy risks;
  - recommended follow-up implementation tickets;
  - documentation artifacts created.
- No application code, migrations, credentials, local environment files, or unrelated docs are changed.
- Every validation artifact is produced and committed from the `development`
  branch.

## Ticket Backlog

### EV-001: Read The Repository

```yaml
ticket_id: EV-001
goal: Read the EasyETFsAT repository and produce a source-backed repository inventory for validation planning.
dependencies: []
business_context: The validation effort needs a factual baseline before architecture or entropy conclusions are drawn.
user_or_operator_outcome: The operator can see what exists in the repo, which files are authoritative, and where future validation should focus.
design_concept: Read first, summarize second, and avoid implementation work.
architecture_boundary: Repository-level discovery only.
success_signal: A concise inventory exists under Documentation\Validation and cites the key source files reviewed.
tradeoffs_or_constraints: Avoid broad generated artifacts; do not run live external ingestion; preserve existing untracked files.
allowed_areas:
  - Documentation\Validation\
  - README.md
  - pyproject.toml
  - docker-compose.yml
  - alembic.ini
  - alembic\
  - docs\
  - Documentation\
  - fondant\
  - tests\
  - scripts\
must_not_touch:
  - .env
  - .venv\
  - .git\
  - .pytest_cache\
  - .ruff_cache\
  - easyetfsat.egg-info\
  - application code outside read-only inspection
requirements:
  - Inspect repository structure, docs, package layout, tests, migrations, and operational instructions.
  - Produce Documentation\Validation\01_REPOSITORY_INVENTORY.md.
  - Mark source files as read evidence; do not claim behavior not supported by files.
non_goals:
  - Architecture critique.
  - Entropy critique.
  - Code edits.
  - Running network-dependent jobs.
acceptance_criteria:
  - Inventory lists major directories and their responsibilities.
  - Inventory identifies runtime stack, test stack, database stack, and operational entry points.
  - Inventory names existing documentation and agent instructions.
  - Inventory records current branch and dirty worktree summary.
manual_verification_required: true
manual_verification_steps:
  - Human reviews Documentation\Validation\01_REPOSITORY_INVENTORY.md for factual completeness.
verification_commands:
  - git status --short --branch
  - rg --files
completion_report_required: true
```

### EV-002: Identify Architecture, Relationships, And APIs

```yaml
ticket_id: EV-002
goal: Map the overall architecture, component relationships, data flows, APIs, jobs, and database relationships.
dependencies:
  - EV-001
business_context: The repository needs a shared architecture model before maintainability or improvement work can be planned.
user_or_operator_outcome: The operator can understand how OeKB ingestion, ECB FX ingestion, database tables, API endpoints, migrations, and tests connect.
design_concept: Produce an implementation-faithful map of the current system, not an aspirational redesign.
architecture_boundary: Cross-module architecture documentation only.
success_signal: A source-backed architecture map exists under Documentation\Validation.
tradeoffs_or_constraints: Favor diagrams and tables that are easy to review; avoid duplicating the entire technical architecture document.
allowed_areas:
  - Documentation\Validation\
  - fondant\
  - alembic\
  - tests\
  - docs\
  - Documentation\
  - scripts\
must_not_touch:
  - .env
  - .venv\
  - .git\
  - application code
  - database migrations
requirements:
  - Produce Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md.
  - Cover API routes, ingestion pipeline, OeKB client/parser, ECB FX pipeline, jobs, database models, migrations, and tests.
  - Include at least one Mermaid diagram for system relationships.
  - Identify public/internal interfaces and data handoff points.
non_goals:
  - Refactoring recommendations beyond factual relationship notes.
  - Live API or ingestion execution.
  - Schema changes.
acceptance_criteria:
  - Architecture map explains main modules and ownership boundaries.
  - API section names endpoints, inputs, outputs, and backing tables.
  - Data-flow section traces source data from OeKB to source tables to curated tables to API/query views.
  - FX section explains ECB data path and view dependency.
  - Test section maps test files to covered behavior.
manual_verification_required: true
manual_verification_steps:
  - Human reviews the architecture map for readability and source fidelity.
verification_commands:
  - rg -n "^(class|def|async def) " fondant tests
  - rg -n "@router|APIRouter|CREATE VIEW|revision" fondant alembic tests
completion_report_required: true
```

### EV-003: Address Software Entropy

```yaml
ticket_id: EV-003
goal: Identify software entropy, maintainability risks, unclear boundaries, and readability problems in the current implementation.
dependencies:
  - EV-001
  - EV-002
business_context: Entropy review helps prioritize changes that reduce future maintenance cost without prematurely rewriting the system.
user_or_operator_outcome: The operator receives a ranked, evidence-backed risk register and follow-up ticket candidates.
design_concept: Diagnose before changing; separate actual risks from style preferences.
architecture_boundary: Review only; no code changes.
success_signal: A ranked entropy and maintainability report exists under Documentation\Validation.
tradeoffs_or_constraints: Findings must cite files or observable behavior; avoid vague "clean up" recommendations.
allowed_areas:
  - Documentation\Validation\
  - fondant\
  - alembic\
  - tests\
  - docs\
  - Documentation\
  - scripts\
  - pyproject.toml
must_not_touch:
  - .env
  - .venv\
  - .git\
  - application code
  - database migrations
requirements:
  - Produce Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md.
  - Classify findings by severity and type: correctness risk, operational risk, readability risk, coupling risk, documentation drift, test gap, schema/API risk.
  - Identify quick wins separately from larger architectural work.
  - Convert material findings into proposed future AEGIS tickets.
non_goals:
  - Implementing fixes.
  - Formatting sweeps.
  - Dependency upgrades.
  - Renaming database columns.
acceptance_criteria:
  - Every finding has source evidence.
  - Report distinguishes confirmed issues, plausible risks, and open questions.
  - Follow-up tickets include allowed areas, non-goals, and verification ideas at a draft level.
manual_verification_required: true
manual_verification_steps:
  - Human reviews severity ranking and chooses which follow-up tickets matter.
verification_commands:
  - pytest --collect-only
  - ruff check .
completion_report_required: true
```

### EV-004: Prepare Streamlined Repository Documentation

```yaml
ticket_id: EV-004
goal: Prepare documentation that makes the repository easier to read, navigate, validate, and operate.
dependencies:
  - EV-001
  - EV-002
  - EV-003
business_context: The repository can be more valuable if future agents and humans can quickly understand where to look and how to validate changes.
user_or_operator_outcome: A new maintainer or agent can onboard faster and avoid touching the wrong files.
design_concept: Create a concise validation-oriented guide that complements existing docs instead of replacing them.
architecture_boundary: Documentation under Documentation\Validation only.
success_signal: A readable validation guide exists and links the inventory, architecture map, and entropy review.
tradeoffs_or_constraints: Do not rewrite existing README or technical docs unless a later ticket explicitly allows it.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - README.md
  - Documentation\README.md
  - Documentation\TECHNICAL_ARCHITECTURE.md
  - Documentation\DATA_AND_QUERY_GUIDE.md
  - Documentation\STAKEHOLDER_BRIEF.md
  - fondant\
  - alembic\
  - tests\
  - .env
requirements:
  - Produce Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md.
  - Include recommended reading order, authoritative docs, operational runbooks, validation commands, and common change areas.
  - Include "where to change what" guidance for API, ingestion, parser, FX, jobs, database schema, docs, and tests.
  - Include a short "do not touch casually" section.
non_goals:
  - Editing existing docs.
  - Adding implementation details not verified by previous tickets.
  - Creating a user-facing product guide.
acceptance_criteria:
  - Guide is concise enough to be used as an onboarding map.
  - Guide links or references the EV-001, EV-002, and EV-003 artifacts.
  - Guide names the safest validation commands for common work types.
manual_verification_required: true
manual_verification_steps:
  - Human reviews whether the guide improves navigation without duplicating existing docs.
verification_commands: []
completion_report_required: true
```

### EV-005: Validate Test And Verification Strategy

```yaml
ticket_id: EV-005
goal: Validate what the test suite covers, what it does not cover, and which commands should be trusted for future tickets.
dependencies:
  - EV-001
  - EV-002
business_context: Future changes need reliable verification commands and clear awareness of test gaps.
user_or_operator_outcome: The operator knows which tests protect parser, ingestion, jobs, API, FX, and migrations.
design_concept: Source-backed test coverage map, not coverage-percentage theater.
architecture_boundary: Test and verification documentation only.
success_signal: A test strategy report exists under Documentation\Validation.
tradeoffs_or_constraints: Docker-dependent PostgreSQL tests may skip when Docker is unavailable; record this explicitly.
allowed_areas:
  - Documentation\Validation\
  - tests\
  - pyproject.toml
  - README.md
  - Documentation\
must_not_touch:
  - .env
  - .venv\
  - .git\
  - application code
requirements:
  - Produce Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md.
  - Map each test file to behavior covered.
  - Identify missing validation for operational jobs, live integrations, auth/security, scheduler absence, API contract maturity, and FX fallback behavior where applicable.
  - Recommend ticket-specific verification commands for future work.
non_goals:
  - Writing tests.
  - Running live network calls.
  - Changing pytest or ruff configuration.
acceptance_criteria:
  - Report identifies fast local checks and slower/container checks.
  - Report states skipped or unavailable checks and residual risk.
  - Report proposes targeted future tests as tickets, not immediate edits.
manual_verification_required: false
manual_verification_steps: []
verification_commands:
  - pytest
  - ruff check .
completion_report_required: true
```

### EV-006: Final Validation Summary And Follow-Up Roadmap

```yaml
ticket_id: EV-006
goal: Consolidate all validation findings into a final summary and prioritized follow-up roadmap.
dependencies:
  - EV-001
  - EV-002
  - EV-003
  - EV-004
  - EV-005
business_context: The epic should end with a clear decision document, not scattered notes.
user_or_operator_outcome: The operator can choose next implementation tickets from a ranked roadmap.
design_concept: Summarize, prioritize, and sequence; do not implement.
architecture_boundary: Documentation\Validation only.
success_signal: A final validation summary and roadmap exists under Documentation\Validation.
tradeoffs_or_constraints: Roadmap tickets must preserve AEGIS boundaries and avoid broad rewrites.
allowed_areas:
  - Documentation\Validation\
must_not_touch:
  - fondant\
  - alembic\
  - tests\
  - README.md
  - Documentation\README.md
  - Documentation\TECHNICAL_ARCHITECTURE.md
  - Documentation\DATA_AND_QUERY_GUIDE.md
  - Documentation\STAKEHOLDER_BRIEF.md
  - .env
requirements:
  - Produce Documentation\Validation\06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md.
  - Summarize validation results from EV-001 through EV-005.
  - Rank follow-up work by risk reduction, stakeholder value, and implementation complexity.
  - Separate documentation-only, test-only, backend, database, operations, and product/API follow-ups.
non_goals:
  - Implementing roadmap items.
  - Rewriting previous validation artifacts.
  - Creating external tickets.
acceptance_criteria:
  - Final summary is readable without opening every previous artifact.
  - Roadmap items are small enough to become AEGIS tickets.
  - Residual risks and open questions are explicit.
manual_verification_required: true
manual_verification_steps:
  - Human reviews and selects the next implementation or cleanup ticket.
verification_commands: []
completion_report_required: true
```

## Suggested Artifact Index

The master should ensure the following files exist by the end of the epic:

- `Documentation\Validation\01_REPOSITORY_INVENTORY.md`
- `Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md`
- `Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md`
- `Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md`
- `Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md`
- `Documentation\Validation\06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md`

## First Master Handoff

Start with EV-001 only.

The first worker should read the repository, produce `01_REPOSITORY_INVENTORY.md`, and return a completion report. The validator should check that the report is source-backed, scoped to repository reading, and does not drift into architecture critique, entropy critique, or implementation recommendations beyond what EV-001 allows.
