<!-- aegis:begin -->
# AEGIS

Rules for agentic coding work a human can trust and review: every change is
scoped by a ticket, checked by an independent validator, and traceable to a
commit.

These rules apply to any task that edits project files. Reading, exploring, and
answering questions need no ticket. The human may waive the ticket for one
specific trivial change by saying so explicitly.

Skills live in `.claude/skills/<name>/SKILL.md`. If your tool doesn't load
skills on its own, read the file and follow it.

## The loop

1. **Ticket.** Draft it with the `write-ticket` skill, save it as
   `.aegis/tickets/<ID>.yaml`, and write `<ID>` to `.aegis/active-ticket`.
   Note any files already modified before you start. If `human_gates`
   includes `ticket`, show the ticket and wait for the human's approval before
   saving it and going on.
2. **Worktree.** Work in the ticket's own worktree and branch (see
   Isolation).
3. **Implement** inside the ticket's scope, following the rules below.
4. **Validate**, without asking. Start a fresh-context subagent that did not
   write the code and tell it to follow `.claude/skills/validate/SKILL.md`.
   Pass it the ticket ID, the worktree path, the base ref, the originating
   request (issue text or the human's words), and the already-modified files.
   Don't pass your own test results; the validator runs the checks itself. On
   `FIXES_REQUIRED`, fix the findings and validate again with a new subagent;
   after three failed rounds, stop and ask the human. On `BLOCKED`, stop and
   bring the finding to the human.
5. **Report** to the human in the format below.
6. **Commit** the ticket on its branch once the validator approves. Push the
   branch and open a pull request when the human or the ticket asks for it.
   Merge only as Human gates allow.

Each agent works on one ticket at a time. Finish or explicitly abandon the
active ticket before starting another. Work too large for one ticket becomes
an epic: an ordered list of tickets that share an epic branch, each run
through this loop on its own.

## Ticket format

```yaml
id: PROJ-012                  # uppercase letters, digits, hyphens; = file name
goal: The outcome, in one sentence.
context: >                    # why it matters, who it serves, and the design
  ...                         # intent or architecture boundary to keep
epic: null                    # epic ID if the ticket belongs to one
depends_on: []                # ticket IDs or conditions; [] means none
human_gates: []               # ticket, merge; [] means fully automatic
allowed_areas:                # the only paths you may change
  - .aegis/tickets/PROJ-012.yaml
  - src/billing/              # trailing / = directory prefix; no globs
  - tests/billing/test_invoice.py
must_not_touch:               # wins over allowed_areas
  - src/billing/migrations/
non_goals:                    # adjacent work that is explicitly out of scope
  - ...
acceptance_criteria:          # each one objectively checkable
  - ...
verify:                       # commands the validator runs
  - pytest tests/billing
smoke:                        # how to start the dev environment, then what
  - Start the app with npm run dev on a free port   # to check in it;
  - Open /invoices/42; every line shows its VAT     # [] if nothing runs
manual_checks: []             # ordered checks only a human can do
```

## Human gates

Validation is automatic. The human adds gates per ticket in `human_gates`:

- `ticket`: the human approves the ticket before implementation starts.
- `merge`: the human approves before the ticket's branch merges anywhere,
  including into an epic branch.

With `[]`, the default, the agent tickets, implements, validates, and commits
without stopping. Set gates from the human's words or the issue (for example,
a label that names the gate). Never remove a gate the human set.

These always stop for the human, whatever `human_gates` says:

- merging or pushing to the default branch;
- deploys and releases;
- migrations on shared data;
- handling secrets: creating, reading out, or changing them;
- deleting data or branches, or rewriting pushed history, including
  force-push.

## Isolation

Parallel agents must not see each other's unfinished work.

- One worktree and branch per ticket: `aegis/<ID>`. A ticket in an epic
  branches from the epic branch `aegis/<EPIC>`; any other ticket branches from
  the default branch. Where each run is already its own container, such as CI
  or a cloud session, that container is the worktree.
- A worktree separates files only. Ports, databases, local services, caches,
  `.env` files, and the git stash stay shared, so smoke tests run in an
  environment private to the ticket:
  - pick a free port, never a fixed one another agent may hold;
  - use a per-ticket database, schema, or throwaway instance, never shared
    data;
  - install dependencies inside the worktree;
  - never use `git stash`; set work aside with a WIP commit on the ticket
    branch, and squash it into the ticket's commit before pushing;
  - stop everything you started when you're done, including on failure.
- After an epic's tickets merge into the epic branch, validate the epic once
  more on that branch: a validator subagent runs every ticket's `verify` and
  `smoke` together.

## Rules

**Scope**
- Change only paths in `allowed_areas`. Never change `must_not_touch`.
- Treat `non_goals` as boundaries, not suggestions. Don't do another ticket's
  work.
- If the ticket can't be finished inside its scope, stop and tell the human
  which scope change or follow-up ticket would unblock it. Never widen scope
  yourself.
- When intent, a domain term, or a boundary is unclear, ask. Don't fill the gap
  with a plausible default.

**Changes**
- A behavior change starts with a check that fails before the change: a test, a
  reproduction, or recorded baseline output. If none is feasible, say why
  before changing code.
- Make the smallest diff that meets the acceptance criteria. No speculative
  code, unrelated refactors, formatting sweeps, or dependency changes unless
  the ticket owns them.
- Add an abstraction only when it removes real complexity now, and say why in
  the report.

**Validation**
- The validator is blocking. Never report a ticket done without `APPROVED` or
  an explicit human override.
- Only the human can override a validator finding. Report an override as an
  override, never as approval.
- Never say a check passed unless you ran it and saw it pass. Name every check
  you skipped and why.

**Git**
- Commit subject: `[ID] what changed`, with the description at 72 characters
  or fewer.
- One commit per ticket, containing the ticket's file and its work, nothing
  else. Leave unrelated changes alone: don't stage, revert, or stash them.

## Completion report

```text
Ticket:       <ID>: <goal>
Validator:    APPROVED | OVERRIDDEN (<finding>, approved by the human because <reason>)
Gates:        <gates that stopped for the human>; or "none"
Changed:      <path>: <one-line purpose>, one per line
Verified:     <command> -> <result>, one per line
Not verified: <check>: <why skipped>, <remaining risk>; or "none"
Manual:       <manual_checks left for the human>; or "none"
Notes:        <abstractions added and why, follow-up tickets, risks>
```

Keep it short enough that the human can judge the change without reading the
whole diff.
<!-- aegis:end -->

## Project workflow (EasyETFsAT)

Work is planned on the
[EasyETFsAT board](https://github.com/users/TristanL567/projects/2/views/1)
and delivered through the AEGIS loop above. This section maps GitHub onto it;
the rules above still apply in full.

**Issues**
- An epic is an issue labelled `epic`. Epics are the items on the board.
- Work under an epic is added as sub-issues of the epic. Each sub-issue has
  exactly one kind label: `bug`, `feature`, `user-story`, or `issue`.
- Use the issue forms in `.github/ISSUE_TEMPLATE/`. Small work may be a
  standalone bug, feature, user story, or issue without an epic.
- Work too big for one ticket is split before it starts: under an epic, into
  further sub-issues of that epic (siblings, not nested); a standalone issue
  becomes an epic with its own sub-issues. One issue is never several
  tickets.

**IDs**

| GitHub | AEGIS |
| --- | --- |
| Epic issue #N | epic `EPIC-N` |
| Sub-issue or standalone issue #M | ticket `EET-M` in `.aegis/tickets/EET-M.yaml` |

- One issue is one ticket and one validated commit `[EET-M] what changed`.
  A ticket of an epic sets `epic: EPIC-N`.
- The ticket's `context` names the issue (`GitHub issue #M, part of epic
  #N`); the originating request passed to the validator is the issue's title
  and body.

**Branches and pull requests**
- The open pull request is the source of truth, not a branch name. Before
  working on epic #N, find its open pull request into `main` (its body
  contains `Part of #N` or `Closes #N`); for a standalone issue #M, the one
  containing `Closes #M`. There is at most one.
- If it exists, continue on its head branch. If none is open, start a new
  branch from `main` (`aegis/EPIC-N` or `aegis/EET-M`) and open a new pull
  request. Never continue a branch whose pull request was merged or closed.
- Locally, each ticket of an epic runs on `aegis/EET-M` branched from the
  epic's branch and is merged into it once validated.
- A cloud session pushes only to its designated branch; the container is
  the worktree, and each ticket is one commit on that branch. If an open
  pull request already exists on another branch, merge its head into the
  designated branch (never reset or force-push), open the new pull request,
  and close the old one with a comment linking the new one. Committing a
  ticket onto the designated branch counts as merging it into the epic
  branch, so with a `merge` gate the agent stops before that commit.

**Fully automatic by default**
- `human_gates` is `[]` unless the issue (or its epic) carries
  `aegis:gate-ticket` or `aegis:gate-merge`, its form's "Human gates" field
  names a gate, or the human asks for one. A gate on the epic applies to all
  its sub-issues.
- This section is the human's standing request to push and open pull
  requests. Without gates the agent runs without stopping. For each
  sub-issue, in the epic's sub-issue order: ticket, implement, validate,
  commit onto the epic's branch. After the last one: validate the epic
  branch (epic mode), push, and open or update the pull request into `main`.
- The pull request body lists `Closes #M` for each issue it completes. It
  adds `Closes #N` only when it completes every open sub-issue of the epic;
  otherwise it says `Part of #N`.
- The agent comments the completion report on each sub-issue it finishes.
- When the run stops early (a gate, `BLOCKED`, or three failed validation
  rounds), the agent pushes the tickets already validated, opens or updates
  the pull request as above, comments the reason on the sub-issue where it
  stopped, and waits; later sub-issues wait too. The human answers on that
  issue or in the session.
- Everything in the "always stop" list under Human gates stops for the
  human. Here that means in particular: merging or pushing into `main`,
  deploys (Render) and releases, migrations on shared data, secrets, and
  deleting data or branches or rewriting pushed history.

**Checks**
- Python 3.11+, install with `pip install -e ".[dev]"`.
- Default `verify` commands: `pytest <the touched test files>` and
  `ruff check .`. Tests marked `postgres` need Docker and skip without it.
- Smoke: `alembic upgrade head`, then
  `uvicorn fondant.api.main:app --port <free port>`, with `DATABASE_URL`
  pointing at a per-ticket database, never the shared `easyetfsat` one.

**Legacy**
- `epics/` (envelopes, ledgers, ticket envelopes for BQ4 to BQ6) records the
  workflow used before AEGIS. It is historical: read it for context, never
  add to it.
