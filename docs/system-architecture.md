# System Architecture

Kingdom runs planned work through LLM roles. One Go service serves the web app and API. The `kd` command initializes and starts Kingdom, then uses that same API for command line operations. SQLite keeps the durable work record. OpenCode runs model work in a prepared project workspace.

This guide is a map of the current system and the rules that shape it. Use the source map below when you need implementation detail.

## The Main Parts

| Part | What it does |
| --- | --- |
| Web app | Lets operators plan work, follow runs, review results, and manage connections and appearance. |
| `kd` command | Initializes a project, starts Kingdom, and gives operators command line access. |
| Go runtime | Owns workflow state, role authority, approvals, provider execution, and recovery. |
| SQLite and runtime files | SQLite records work state and audit history. Runtime files hold project changes and evidence needed to recover unfinished work. |
| OpenCode | Runs the model work assigned by Kingdom on the host. |

The web app, command line, and API all work with the same local runtime. They do not each maintain a separate view of a run.

## How Work Moves

Planning is a separate workspace where an operator can shape a task before making a Work Order. It is optional. A Work Order can also be created directly. A planner turn that fails stays in the conversation as a failed reply with its reason; the planner never sees it, and retrying it adds a new reply beside it.

The workflow definition controls which roles run and when decisions happen. In the default workflow, advisor roles help shape the plan, a plan decision comes before implementation, and the work then moves through Build, Review, validation, and a final decision. Some stages record a decision or handoff without starting a model session. An alternate workflow can skip Review and validation. The workflow files define the exact steps.

For provider work, Kingdom prepares the workspace, starts the assigned OpenCode session, and records its activity and result. It checks each result against the role and node rules. Approval settings decide whether Kingdom makes the plan and final decisions automatically or pauses for the operator. Review or validation can send the work back for another pass.

Kingdom runs one authenticated OpenCode V2 server on loopback for model discovery and for every planning and execution request. The process entrypoint owns that server and closes it after its consumers drain. Discovery reads location-specific models from the user's OpenCode provider configuration and account store without copying credentials. Discovery does not make a model request or certify provider access. Automatic formatting is disabled in the server; a higher-priority configuration that enables it is reported as incompatible. Each request gets its own immutable profile, whose tools are limited to the role's capabilities, and a session. A Kingdom plugin in the server reports every file or shell tool the request attempts, and Kingdom admits the call before it runs. Shell commands run under the process guardian rather than inside OpenCode. A request is prepared and bound to its invocation before any work is sent, and it settles only from the verified terminal record of its own session. Provider-generated context, retries, and compaction checkpoints are matched to their projected records within that owned request; their scheduling and prose do not grant authority. Read-capable roles can read, glob, and grep, and managed shell commands can select existing directories within their workspace.

Agent Communication Style and Agent Guidance settings are shared workspace state. The built-in `neutral-v1` style is the default. Operators choose a workspace style on the Agents page and can edit role working guidance or select a style override. A theme revision may define a communication preference, but choosing a visual theme in a browser never applies it to agents. Protected role instructions retain identity and minimum duties; editable defaults own working methods, while Kingdom Contracts retain authority, evidence, and workflow/output obligations. User guidance is inserted literally without an Agent Guidance Scope wrapper; runtime permissions, transitions, and response validation remain enforced.

Run creation captures each role's effective settings and the protected prompt catalog revision; planning-session creation captures the Plan Author's effective settings and that catalog revision. Both snapshots are immutable and written in the same transaction as the new resource, so later edits or catalog changes affect new work without changing existing runs or sessions. OpenCode receives the captured role, style, guidance, and Kingdom Contract as the system instruction of the request's own immutable profile.

Dynamic Run Context is streamed at its workflow stage, followed by the snapshot-resolved Final Checks as the last authored section. Each provider attempt atomically captures the exact system prompt and dynamic context under its queued lease; Agent Activity displays both immutable channels, after content verification. Provider-added environment and tool prompts are outside Kingdom visibility. Planning uses one Plan Author identity and the selected mode contract, with mode-specific Final Checks appended after the complete transcript and external context. See the [prompt authoring guide](prompt-authoring.md) for the source map and change rules.

## Authority and Evidence

Kingdom owns the rules for what each role may do, which workflow transitions are legal, and whether a result is complete. Prompts guide the model, but they do not grant authority.

The runtime applies these rules:

* Run and node state changes go through the transition service and must follow the workflow.
* A run provider result belongs to its active attempt and invocation. Kingdom validates the result before it can become canonical.
* A process exit, a piece of text, or a saved file alone does not mean the work succeeded.
* If Kingdom cannot prove that a managed provider process has stopped, it keeps that attempt blocked from replacement. The shared server counts too: its guardian records it before the server runs and leaves durable stop evidence, and a server is proven stopped from that evidence or from the absence of its exact process.
* Shared-provider requests have immutable invocation, lease, server-generation, location, session, profile, and request bindings. Dispatch and tool admission require that exact active owner. Cancellation closes admission; late receipts can resolve existing obligations without authorizing more work.
* A shared server's lifetime is separate from request completion. Settlement requires the request's terminal evidence and closed native-tool and command obligations. A command's own exit status is tool output the agent may react to; success settlement instead rejects forced termination, unproven process trees, and gaps in the command sequence. A read, glob, or grep owes nothing; the unreceipted writes of a server proven stopped and its command claims that never reached a guardian are closed as unknown, because they can no longer happen and the workspace is restored or discarded unless the attempt is a promoted success. Workspace release, replacement, and planning-session deletion cannot use process-guardian closure alone.
* A request's terminal evidence is a verified record of its outcome, response, tool calls, and captured provider events. The settlement that depends on the record stores its digest, only if every projected tool matches a durable admission, so a request has terminal evidence exactly when its outcome is canonical or it was abandoned.
* Kingdom cancellation is a runtime decision independent of the provider’s raw terminal outcome. Settlement preserves both facts and still requires closed admissions and resource obligations.
* Cancellation handoffs remain discoverable while awaiting disposal, including after a stop was dispatched before a runtime restart. Observation may finish a proven-disposed handoff; it never authorizes repeating the external stop.
* Rejected provider snapshots remain unverified diagnostics; they never certify a response or tool authority. A rejected request can settle by abandonment only after its admitted effects are closed.
* A failed completion handoff without a verified provider answer retains its original evidence and settles through abandonment in the same transaction as the failed attempt, after exact ownership and closed-resource checks. Missing evidence never authorizes success.
* An interrupted provider attempt saves a resume checkpoint once its request, tools, and commands are proven closed and before the trusted workspace is restored. The checkpoint pins the conversation boundary and the partial-file snapshot, and the source attempt keeps its trusted base. Resume starts a new attempt with a fresh invocation, lease, profile, and request. It may adopt the conversation only along a recovery edge that pins that exact checkpoint, for every interrupted frontier step together. Partial files seed the workspace but never become trusted, and a workspace changed after the checkpoint is a conflict, not something to overwrite. A checkpoint is consumed once a resumed attempt reaches the provider; a Resume attempt that ends before that hands its checkpoint back.
* Planning messages, planning-session role-configuration snapshots, run configuration snapshots, and run activity are persisted so they can be inspected and recovered after a restart.

SQLite is the durable record for workflow state and audit history. Files under Kingdom's runtime and workspace directories support project work and recovery. On startup, Kingdom reconciles unfinished work before it resumes dispatch.

Canonical request content stays immutable while referenced. Planning sessions own their title, message, draft, and planner-activity content, including its chunks and activity digests; deleting a session removes those owned records. A remaining reference from another resource blocks that deletion, preserving shared content.

## Trust Boundaries

Kingdom currently runs on one host, with state stored in SQLite. The installed server listens on loopback, and Kingdom does not provide scheduling across hosts or enterprise identity management.

OpenCode processes run on the host in the workspace Kingdom prepares. Kingdom supervises their lifecycle and checks actions against its configured policies. This does not provide an operating system sandbox for shell commands or protect against hostile software already running as the local user.

OpenCode is the supported provider engine. GitHub Issues can be imported as project context; Kingdom does not write changes back to GitHub.

## Where to Look in the Code

| If you are changing | Start here |
| --- | --- |
| Startup and service assembly | `internal/projectruntime/`, `internal/cli/install_mode.go`, and `cmd/kingdomd/main.go` |
| HTTP routes and request handling | `internal/api/` and `contracts/openapi/openapi.yaml` |
| Durable state, transitions, and audit | `internal/state/` |
| Workflow rules and bundled workflows | `internal/core/workflow/` and `internal/bootstrapassets/files/workflows/` |
| Scheduling, execution, workspaces, and recovery | `internal/orchestration/` |
| OpenCode sessions and process supervision | `internal/providers/opencode/` and `internal/processguardian/` |
| Provider request ownership, admission, and settlement | `internal/execution/providerrequest/` |
| Planning and its activity protocol | `internal/planning/` and `internal/acp/` |
| GitHub issue context | `internal/external/github/` and `internal/api/server_external.go` |
| Themes and appearance | `internal/theme/`, `web/src/theme/`, and `contracts/theme/` |

## More Specific Guides

The architecture guide should stay stable as implementation details change. Use the focused references for details that change more often:

* [Workflows, approvals, results, and recovery](how-runs-work.md)
* [Runtime configuration](config-reference.md)
* [GitHub connection setup](github-connections-runbook.md)
* [Theme behavior and authoring](themes.md)
* [Prompt source and authoring rules](prompt-authoring.md)
* The files in `policies/` for role authority, provider behavior, communication, and retries
* The JSON files in `internal/bootstrapassets/files/workflows/` for the exact built in workflow steps
