# EV-001 Master Dispatch

Use this prompt to start the first validation ticket with the master agent.

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

Epic document:
C:\Users\Tristan Leiter\Documents\Reporting\EasyETFsAT\Documentation\Validation\REPO_VALIDATION_EPIC.md

Current ticket:
EV-001: Read The Repository

Run only this ticket. Do not start EV-002 or any adjacent ticket.
Work exclusively on the development branch. Before dispatching the worker, run
`git status --short --branch` and confirm it reports `## development`. If it
does not, stop and return control to the operator.

Ticket envelope:

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
- Preserve unrelated dirty worktree changes.
- Work only on the development branch.
- Do not edit application code, migrations, tests, environment files, or existing documentation outside Documentation\Validation.

non_goals:
- Architecture critique.
- Entropy critique.
- Code edits.
- Running network-dependent jobs.
- Running live OeKB or ECB ingestion.
- Starting EV-002.

acceptance_criteria:
- Inventory lists major directories and their responsibilities.
- Inventory identifies runtime stack, test stack, database stack, and operational entry points.
- Inventory names existing documentation and agent instructions.
- Inventory records current branch and dirty worktree summary.
- Inventory is source-backed and concise enough for validation planning.

manual_verification_required: true
manual_verification_steps:
- Human reviews Documentation\Validation\01_REPOSITORY_INVENTORY.md for factual completeness.

verification_commands:
- git status --short --branch
- rg --files

completion_report_required: true

Master routing instructions:
1. Confirm this ticket envelope is complete.
2. Dispatch one worker only. Recommended worker: C:\Users\Tristan Leiter\Documents\aegis-core\skills\roles\backend-worker\SKILL.md
3. Require the worker to produce only:
   - Documentation\Validation\01_REPOSITORY_INVENTORY.md
   - a completion report in the standard AEGIS envelope
4. Do not ask the worker to validate itself.
5. After the worker finishes, stop and return the worker output to the operator.
6. Do not run the validator yourself unless explicitly instructed by the operator.

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
- Documentation\Validation\01_REPOSITORY_INVENTORY.md

The worker should set next_recommended_role: validator when ready.

Important:
When EV-001 is done, return the produced summary and completion report to the operator. The operator will route it back for independent validation, commit handling, and the next ticket.
```

## Return Path

After the master/worker completes EV-001, route the produced `01_REPOSITORY_INVENTORY.md` content and completion report back here.

The next local action will be:

1. Validate the EV-001 artifact against the ticket.
2. Check scope and dirty worktree impact.
3. If valid, commit using a message containing the epic, ticket, and a short description.
4. Provide the next ticket dispatch prompt automatically.
