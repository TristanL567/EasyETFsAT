# EV-006 Master Dispatch

Use this prompt to start the final validation ticket with the master agent.

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
EV-005: Validate Test And Verification Strategy

Prerequisite artifacts:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\01_REPOSITORY_INVENTORY.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-001_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\02_ARCHITECTURE_RELATIONSHIPS_AND_APIS.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-002_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\03_SOFTWARE_ENTROPY_REVIEW.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-003_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\04_REPOSITORY_READABILITY_GUIDE.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-004_VALIDATION.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\05_TEST_AND_VERIFICATION_STRATEGY.md
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\EV-005_VALIDATION.md

Current ticket:
EV-006: Final Validation Summary And Follow-Up Roadmap

Run only this ticket. Do not start implementation work or any adjacent ticket.

Ticket envelope:

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
- .venv\
- .git\

requirements:
- Produce Documentation\Validation\06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md.
- Summarize validation results from EV-001 through EV-005.
- Rank follow-up work by risk reduction, stakeholder value, and implementation complexity.
- Separate documentation-only, test-only, backend, database, operations, and product/API follow-ups.
- Preserve unrelated dirty worktree changes.
- Do not edit application code, migrations, tests, environment files, or existing documentation outside Documentation\Validation.

non_goals:
- Implementing roadmap items.
- Rewriting previous validation artifacts.
- Creating external tickets.
- Editing existing project docs outside Documentation\Validation.

acceptance_criteria:
- Final summary is readable without opening every previous artifact.
- Roadmap items are small enough to become AEGIS tickets.
- Residual risks and open questions are explicit.
- Roadmap separates documentation-only, test-only, backend, database, operations, and product/API follow-ups.

manual_verification_required: true
manual_verification_steps:
- Human reviews and selects the next implementation or cleanup ticket.

verification_commands:
- git status --short --branch
- rg --files Documentation\Validation

completion_report_required: true

Master routing instructions:
1. Confirm this ticket envelope is complete.
2. Confirm EV-001 through EV-005 have completed artifacts and validation reports.
3. Confirm the current branch is development before dispatch.
4. Dispatch one worker only. Recommended worker: C:\Users\Tristan Leiter\Documents\aegis-core\skills\roles\ticket-planner-worker\SKILL.md
5. Require the worker to produce only:
   - Documentation\Validation\06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md
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
- Documentation\Validation\06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md

The worker should set next_recommended_role: validator when ready.

Important:
When EV-006 is done, return the produced summary and completion report to the
operator. The operator will route it back for independent validation and commit
handling.
```

## Return Path

After the master/worker completes EV-006, route the produced
`06_FINAL_VALIDATION_SUMMARY_AND_ROADMAP.md` content and completion report back
here.

The next local action will be:

1. Validate the EV-006 artifact against the ticket.
2. Check branch and changed-file scope.
3. If valid, commit using a message containing the epic, ticket, and a short
   description.
4. Report the epic completion state and remaining unrelated worktree files.
