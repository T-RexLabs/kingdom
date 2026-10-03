# How Runs Work

Kingdom gives a request a clear path through the crew and keeps the decisions, attempts, and results together. You can start in Planning or create a Work Order right away.

## Choose where to start

### Shape a task in Planning

Use the Planning workspace to work through a task, add project context, and shape a plan. When it is ready, issue a Work Order from that plan. You can also keep a planning session open without starting a run.

### Create a Work Order

Create a Work Order directly when you already know what needs doing. Planning is optional. The default workflow still gives the request planning and approval steps before implementation.

Before each run, choose a global model. Kingdom uses your OpenCode setup to list and run models. You can choose separate models for individual roles when you need to.

## Follow the work

Open **Runs** to see current and finished work. A run keeps the workflow, execution activity, results, and approval state in one place.

Workspace preparation appears before provider work begins and remains visible after reopening a run. Agent Activity provides scrollable conversation history, captured instructions rendered as Markdown, and an attempt selector when earlier attempts exist. Review and validation outcomes that request another pass are labeled **Rework Requested**, while execution errors remain **Failed**.

The default workflow, `work-order-v1`, usually moves through these stages:

In this workflow, separate advisor roles consider the request before a planner role drafts the run’s plan. This is different from the Planning workspace above.

| Stage | What happens |
| --- | --- |
| Planning | Advisors look at the request from different angles. The planner turns their input into a plan. |
| Plan decision | Kingdom approves the plan automatically or waits for you, based on the approval setting. |
| Build | The builder works in the run’s project workspace. |
| Review | A reviewer checks the changes and can ask for another pass. |
| Validate | The quality and security validators check the result. |
| Final decision | Kingdom closes the run automatically or waits for your decision. |

Some steps record a decision or hand the work to the next role. They do not call an LLM. The workflow definition lists every role and step.

## Decide how much to oversee

With `governance.autonomous_approvals: true`, Kingdom approves the plan and final result automatically. Set it to `false` to pause at both gates so you can approve or ask for changes.

Plan rework returns the work to planning. Review and validation rework can send it back through another build and review cycle. The default workflow sets the usual cycle caps, and you can change them for a Work Order. The separate `run_until_all_approve` option keeps review and validation going until they pass.

The **Skip Reviews and Validations** workflow omits those checks by design. Choose it only when that is what you want.

## See what changed

The builder’s saved changes appear as artifacts and, depending on `publish.mode`, may also be written to the project or a managed worktree. Kingdom compares work with a saved baseline. If a file changed outside the run, Kingdom leaves that version in place and records a conflict.

Only successful builder work contributes the current deliverable set. Earlier and failed attempts stay available as evidence. Other roles leave response records rather than project file changes.

With `publish.mode: direct`, Kingdom works against the project and publishes changes into its configured root. With `publish.mode: git_worktree`, it creates a separate worktree for the run and publishes there. The exact snapshot rules are in the [architecture guide](system-architecture.md).

## When a run stops

Kingdom may retry some failures automatically, based on the canonical rules in `policies/retry_policy.yaml`. It does not retry every error, and observed work can make an automatic retry unsafe. The run activity shows the decision and its reason.

If a provider process may still be running, Kingdom holds the run in a blocked state until it can prove that the process stopped. **Retry Stop** authorizes one more bounded stop request for an eligible blocked attempt. It does not skip the safety checks.

You can use **Retry** or **Rerun** when a run is eligible to reopen. If Kingdom reports `idempotency_pending` after an interrupted request, continue with the same request key. A new key could repeat an action whose result is still unknown.

## Other ways to operate

Use the web app for the full workflow. Use `kd help` for the current CLI commands and `kd help <command>` for their options. The [OpenAPI contract](../contracts/openapi/openapi.yaml) defines the API.

For setup and access, see [GitHub Connections](github-connections-runbook.md). For local development, see [Local Setup and Verification](local-setup-and-verification.md).
