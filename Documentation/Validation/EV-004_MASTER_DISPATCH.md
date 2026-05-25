# EV-004 Master Dispatch

Use this prompt to start the fourth validation ticket with the master agent.

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

Completed prerequisites:
EV-001: Read The Repository
EV-002: Identify Architecture, Relationships, And APIs
EV-003: Address Software Entropy

Prerequisite artifacts:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\01_REPOSITORY_INVENTORY.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-001_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-002_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-003_VALIDATION.md

Current ticket:
EV-004: Prepare Streamlined Repository Documentation

Run only this ticket. Do not start EV-005 or any adjacent ticket.

Ticket envelope:

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
- .venv\
- .git\

requirements:
- Produce Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md.
- Include recommended reading order, authoritative docs, operational runbooks, validation commands, and common change areas.
- Include "where to change what" guidance for API, ingestion, parser, FX, jobs, database schema, docs, and tests.
- Include a short "do not touch casually" section.
- Preserve unrelated dirty worktree changes.
- Do not edit application code, migrations, tests, environment files, or existing documentation outside Documentation\Validation.

non_goals:
- Editing existing docs.
- Adding implementation details not verified by previous tickets.
- Creating a user-facing product guide.
- Starting EV-005.

acceptance_criteria:
- Guide is concise enough to be used as an onboarding map.
- Guide links or references the EV-001, EV-002, and EV-003 artifacts.
- Guide names the safest validation commands for common work types.
- Guide complements existing docs instead of duplicating or replacing them.

manual_verification_required: true
manual_verification_steps:
- Human reviews whether the guide improves navigation without duplicating existing docs.

verification_commands:
- git status --short --branch
- rg --files Documentation\Validation

completion_report_required: true

Master routing instructions:
1. Confirm this ticket envelope is complete.
2. Confirm EV-001, EV-002, and EV-003 have completed artifacts and validation reports.
3. Confirm the current branch is development before dispatch.
4. Dispatch one worker only. Recommended worker: C:\Users\Tristan Leiter\Documents\aegis-core\skills\roles\backend-worker\SKILL.md
5. Require the worker to produce only:
   - Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md
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
- Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md

The worker should set next_recommended_role: validator when ready.

Important:
When EV-004 is done, return the produced summary and completion report to the
operator. The operator will route it back for independent validation, commit
handling, and the next ticket.
```

## Return Path

After the master/worker completes EV-004, route the produced
`04_REPOSITORY_READABILITY_GUIDE.md` content and completion report back here.

The next local action will be:

1. Validate the EV-004 artifact against the ticket.
2. Check branch and changed-file scope.
3. If valid, commit using a message containing the epic, ticket, and a short
   description.
4. Provide the next ticket dispatch prompt automatically.
