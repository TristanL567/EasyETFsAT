# EV-002 Master Dispatch

Use this prompt to start the second validation ticket with the master agent.

```text
You are the AEGIS master agent for the EasyETFsAT repository validation epic.

Reference AEGIS-CORE.

Load AEGIS Core from:
C:\Users\Tristan Leiter\Documents\aegis-core

Load AEGIS.md first and follow its Bootstrap Load Order:
1. AEGIS.md
2. contracts/swarm-contract.md
3. contracts/ticket-contract.md
4. skills/roles/master/SKILL.md
5. execution/runbooks/shared-orchestration-loop.md
6. execution/runbooks/apply-to-project.md

Target repository:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT

Required branch:
development

Before dispatching work, run:
git status --short --branch

Continue only if the output reports:
## development

If the repository is not on development, stop and return control to the
operator. Do not read further, edit files, validate, or commit from another
branch.

Epic document:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\REPO_VALIDATION_EPIC.md

Completed prerequisite:
EV-001: Read The Repository

Prerequisite artifacts:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\01_REPOSITORY_INVENTORY.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-001_VALIDATION.md

Current ticket:
EV-002: Identify Architecture, Relationships, And APIs

Run only this ticket. Do not start EV-003 or any adjacent ticket.

Ticket envelope:

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
- Preserve unrelated dirty worktree changes.
- Do not edit application code, migrations, tests, environment files, or existing documentation outside Documentation\Validation.

non_goals:
- Refactoring recommendations beyond factual relationship notes.
- Live API or ingestion execution.
- Schema changes.
- Entropy critique; that belongs to EV-003.
- Starting EV-003.

acceptance_criteria:
- Architecture map explains main modules and ownership boundaries.
- API section names endpoints, inputs, outputs, and backing tables.
- Data-flow section traces source data from OeKB to source tables to curated tables to API/query views.
- FX section explains ECB data path and view dependency.
- Test section maps test files to covered behavior.
- Artifact is source-backed and concise enough for validation planning.

manual_verification_required: true
manual_verification_steps:
- Human reviews Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md for readability and source fidelity.

verification_commands:
- git status --short --branch
- rg -n "^(class|def|async def) " fondant tests
- rg -n "@router|APIRouter|CREATE VIEW|revision" fondant alembic tests

completion_report_required: true

Master routing instructions:
1. Confirm this ticket envelope is complete.
2. Confirm EV-001 has a completed artifact and validation report.
3. Confirm the current branch is development before dispatch.
4. Dispatch one worker only. Recommended worker: C:\Users\Tristan Leiter\Documents\aegis-core\skills\roles\backend-worker\SKILL.md
5. Require the worker to produce only:
   - Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md
   - a completion report in the standard AEGIS envelope
6. Do not ask the worker to validate itself.
7. After the worker finishes, stop and return the worker output to the operator.
8. Do not run the validator yourself unless explicitly instructed by the operator.

Worker output requirements:
- status
- summary
- artifacts
- findings
- next_recommended_role
- changed_files
- verification
- human_readability

Expected changed files:
- Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md

The worker should set next_recommended_role: validator when ready.

Important:
When EV-002 is done, return the produced summary and completion report to the
operator. The operator will route it back for independent validation, commit
handling, and the next ticket.
```

## Return Path

After the master/worker completes EV-002, route the produced
`02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md` content and completion report back
here.

The next local action will be:

1. Validate the EV-002 artifact against the ticket.
2. Check branch and changed-file scope.
3. If valid, commit using a message containing the epic, ticket, and a short
   description.
4. Provide the next ticket dispatch prompt automatically.
