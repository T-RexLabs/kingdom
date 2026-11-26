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

Kingdom retries some failures on its own. [`policies/retry_policy.yaml`](../policies/retry_policy.yaml) lists which ones, and the run's activity shows each retry and why it happened. Kingdom doesn't retry a step that may already have changed something, because doing it again could repeat that work.

When a run fails or you cancel it, you choose what happens next:

| Action | Use it when |
| --- | --- |
| **Resume** | You want the stopped steps to carry on where they left off. |
| **Rerun** | You want to start over from a step with a clean copy of your project. |
| **Try Stopping Again** | A step is paused because it stopped responding and hasn't fully stopped yet. |

Resume and Rerun wait until everything from the earlier attempt has stopped before they start new work. From the command line, use `kd run resume` or `kd node rerun-from`.

### Resume

Resume picks up each stopped step from a checkpoint: its conversation with the model so far and the files it had changed. Kingdom saves the checkpoint when the step stops. Steps that had already finished, and their approvals, are kept.

The resumed step's files are treated like any new work. Kingdom compares them with your project's last trusted state, and they go through review and validation as usual. A shell command that was cut off is not run again.

Resume is unavailable, with the reason shown, when a stopped step has no checkpoint. That happens when the step never started, when Kingdom ran it itself rather than a model, when it was a review or validation step, or when the run is older than Resume. Use Rerun instead. Kingdom never switches to Rerun for you.

If you edited the project after the checkpoint was saved, Resume refuses to start so it doesn't overwrite your edits. Undo the edits and choose **Resume** again.

### Rerun

Rerun starts a step over from your project's last trusted state, and every step after it runs again too. Kingdom picks the earliest stopped step when there is one clear choice. If the run stopped in several parallel branches, you pick the step.

### When Kingdom restarts

Restarting Kingdom stops every step that was running. Once Kingdom has confirmed the old processes are gone, it marks those steps as failed, puts the project back to its last trusted state, and applies the usual retry rules. It never sends the same model request twice. After that, the run offers Resume and Rerun like any other failed run.

### Paused and blocked steps

Sometimes a step stops responding, for example when the model goes quiet or a shell command hangs. Kingdom then stops the step, but it won't start anything new until it knows the step has fully stopped. Otherwise the old step and the new one could both change your project at the same time.

While Kingdom waits, the step shows as **Step Paused**. Kingdom keeps checking on its own, and most steps clear without you doing anything. If one stays paused, select **Try Stopping Again** to have Kingdom send the stop request once more. It doesn't skip any checks; it just asks again.

If Kingdom can't tell what a step left running, the run shows **Recovery Blocked** with the reason. Kingdom never guesses what an unfinished change or command did, so it keeps the run blocked until it knows.

### Other messages

* **`provider_protocol_error`** means Kingdom couldn't verify a reply from OpenCode, so the step failed. It isn't a sign-in or usage problem, and Kingdom doesn't retry it on its own. The rejected reply is kept in the step's details so you can see what went wrong before you resume or rerun.
* **Retry Resume** and **Retry Rerun** appear when Kingdom couldn't confirm that your last request went through. They send the same request again, so the work can't start twice. On the command line, `kd` reports `idempotency_pending`; run the same command again with the `--idempotency-key` it printed.
* Losing the browser connection doesn't stop a run. Reconnect and check the run before you start anything new.
* Older runs may show **Retry** in their history. Resume and Rerun have replaced it.

## Other ways to operate

Use the web app for the full workflow. Use `kd help` for the current CLI commands and `kd help <command>` for their options. The [OpenAPI contract](../contracts/openapi/openapi.yaml) defines the API.

For setup and access, see [GitHub Connections](github-connections-runbook.md). For local development, see [Local Setup and Verification](local-setup-and-verification.md).
