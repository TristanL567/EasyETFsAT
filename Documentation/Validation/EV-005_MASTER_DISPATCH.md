# EV-005 Master Dispatch

Use this prompt to start the fifth validation ticket with the master agent.

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
EV-004: Prepare Streamlined Repository Documentation

Prerequisite artifacts:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\01_REPOSITORY_INVENTORY.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-001_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-002_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-003_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-004_VALIDATION.md

Current ticket:
EV-005: Validate Test And Verification Strategy

Run only this ticket. Do not start EV-006 or any adjacent ticket.

Ticket envelope:

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
- tests\
- pyproject.toml
- README.md
- Documentation\README.md
- Documentation\TECHNICAL_ARCHITECTURE.md
- Documentation\DATA_AND_QUERY_GUIDE.md
- Documentation\STAKEHOLDER_BRIEF.md

requirements:
- Produce Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md.
- Map each test file to behavior covered.
- Identify missing validation for operational jobs, live integrations, auth/security, scheduler absence, API contract maturity, and FX fallback behavior where applicable.
- Recommend ticket-specific verification commands for future work.
- Preserve unrelated dirty worktree changes.
- Do not edit application code, migrations, tests, environment files, or existing documentation outside Documentation\Validation.

non_goals:
- Writing tests.
- Running live network calls.
- Changing pytest or ruff configuration.
- Starting EV-006.

acceptance_criteria:
- Report identifies fast local checks and slower/container checks.
- Report states skipped or unavailable checks and residual risk.
- Report proposes targeted future tests as tickets, not immediate edits.
- Report maps every existing `tests\test_*.py` file to covered behavior.

manual_verification_required: false
manual_verification_steps: []

verification_commands:
- git status --short --branch
- py -3.10 -m pytest --collect-only
- py -3.10 -m ruff check .

completion_report_required: true

Master routing instructions:
1. Confirm this ticket envelope is complete.
2. Confirm EV-001 through EV-004 have completed artifacts and validation reports.
3. Confirm the current branch is development before dispatch.
4. Dispatch one worker only. Recommended worker: C:\Users\Tristan Leiter\Documents\aegis-core\skills\roles\backend-worker\SKILL.md
5. Require the worker to produce only:
   - Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md
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
- Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md

The worker should set next_recommended_role: validator when ready.

Important:
When EV-005 is done, return the produced summary and completion report to the
operator. The operator will route it back for independent validation, commit
handling, and the next ticket.
```

## Return Path

After the master/worker completes EV-005, route the produced
`05_TEST_AND_VERIFICATION_STRATEGY.md` content and completion report back here.

The next local action will be:

1. Validate the EV-005 artifact against the ticket.
2. Check branch and changed-file scope.
3. If valid, commit using a message containing the epic, ticket, and a short
   description.
4. Provide the next ticket dispatch prompt automatically.
