# EV-003 Master Dispatch

Use this prompt to start the third validation ticket with the master agent.

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

Prerequisite artifacts:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\01_REPOSITORY_INVENTORY.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-001_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-002_VALIDATION.md

Current ticket:
EV-003: Address Software Entropy

Run only this ticket. Do not start EV-004 or any adjacent ticket.

Ticket envelope:

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
- Preserve unrelated dirty worktree changes.
- Do not edit application code, migrations, tests, environment files, or existing documentation outside Documentation\Validation.

non_goals:
- Implementing fixes.
- Formatting sweeps.
- Dependency upgrades.
- Renaming database columns.
- Starting EV-004.

acceptance_criteria:
- Every finding has source evidence.
- Report distinguishes confirmed issues, plausible risks, and open questions.
- Follow-up tickets include allowed areas, non-goals, and verification ideas at a draft level.
- Report stays diagnostic and does not make code or schema changes.

manual_verification_required: true
manual_verification_steps:
- Human reviews severity ranking and chooses which follow-up tickets matter.

verification_commands:
- git status --short --branch
- pytest --collect-only
- ruff check .

completion_report_required: true

Master routing instructions:
1. Confirm this ticket envelope is complete.
2. Confirm EV-001 and EV-002 have completed artifacts and validation reports.
3. Confirm the current branch is development before dispatch.
4. Dispatch one worker only. Recommended worker: C:\Users\Tristan Leiter\Documents\aegis-core\skills\roles\backend-worker\SKILL.md
5. Require the worker to produce only:
   - Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md
   - a completion report in the standard AEGIS envelope
6. Do not ask the worker to validate itself.
7. After the worker finishes, stop and return the worker output to the operator.
8. Do not run the validator yourself unless explicitly instructed by the operator.

Recommended procedure composition:
- Use C:\Users\Tristan Leiter\Documents\aegis-core\skills\procedures\module-boundary-review\SKILL.md where coupling or ownership boundaries are part of a finding.
- Use C:\Users\Tristan Leiter\Documents\aegis-core\skills\procedures\ubiquitous-language-map\SKILL.md where business terms, code identifiers, schema names, and docs appear to drift.
- Use C:\Users\Tristan Leiter\Documents\aegis-core\skills\procedures\ticket-scope-validation\SKILL.md to confirm produced artifacts stay in scope.

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
- Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md

The worker should set next_recommended_role: validator when ready.

Important:
When EV-003 is done, return the produced summary and completion report to the
operator. The operator will route it back for independent validation, commit
handling, and the next ticket.
```

## Return Path

After the master/worker completes EV-003, route the produced
`03_SOFTWARE_ENTROPY_REVIEW.md` content and completion report back here.

The next local action will be:

1. Validate the EV-003 artifact against the ticket.
2. Check branch and changed-file scope.
3. If valid, commit using a message containing the epic, ticket, and a short
   description.
4. Provide the next ticket dispatch prompt automatically.
