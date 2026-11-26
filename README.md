<p align="center">
  <picture>
    <source srcset="web/public/t%2Drex%2Dwhite.svg" media="(prefers&#45;color&#45;scheme: dark)">
    <source srcset="web/public/t%2Drex%2Dblack.svg" media="(prefers&#45;color&#45;scheme: light)">
    <img src="web/public/t%2Drex%2Dblack.svg" alt="Kingdom's crowned Technology Rex" width="220">
  </picture>
</p>

<h1 align="center">Kingdom</h1>

<p align="center">
  <strong>One mission. A whole AI crew. You’re still in command.</strong><br>
  Your own command center for AI work that needs to ship.
</p>

<p align="center">
  <a href="#firstmission">Start Your First Mission</a> ·
  <a href="#watch">See How It Works</a> ·
  <a href="#contribute">Build With Us</a>
</p>

Kingdom is a local web app and command-line tool that runs a crew of AI agents on your codebase. Through [OpenCode](https://opencode.ai/docs/), the crew plans the work, builds it, reviews it, and validates the result. You decide whether to approve the plan and the finished work yourself.

Kingdom is open source and runs on your own machine. You choose the models, including local ones, decide where your data lives, and keep a record of every plan, change, and decision.

<h2 id="watch">Watch the Work Unfold</h2>

Start in Planning to work through an idea, or create a Work Order when you know what needs doing. A Work Order follows the default path through planning, approval, Build, Review, validation, and final decision. Follow the crew, its decisions, and evidence as it goes.

<picture>
  <source srcset="docs/assets/kingdom%2Dworkflow%2Ddark.svg" media="(prefers&#45;color&#45;scheme: dark)">
  <img src="docs/assets/kingdom%2Dworkflow%2Dlight.svg" alt="Diagram: plan with advisors or create a Work Order directly, follow the work through Build, Review, and Validate, then inspect changes, decisions, and evidence." width="960">
</picture>

The current crew includes advisors, a planner, a builder, a reviewer, and validators.

## Why Kingdom?

* **Open source, so you can judge it for yourself.** Inspect the code, adapt Kingdom to your needs, and run it on hardware you control.
* **Make a plan.** Turn a rough idea into a plan the crew can follow.
* **Each role has a clear job. Kingdom enforces it.** Advisors help plan, builders do the work, and reviewers check the result.
* **See what is happening.** Follow the crew, inspect files and past attempts, and keep a record of decisions and results.
* **Choose your models.** Use the models in your OpenCode setup, including hosted models.
* **Choose how much to oversee.** Approve the plan and finished work yourself, or let Kingdom handle both approvals.
* **Make it your own.** Choose a personal theme or create one to share with your team.

<h2 id="firstmission">Start Your First Mission</h2>

You’ll need Go 1.27.0, Node 26.2.0 with Corepack, and [OpenCode](https://opencode.ai/docs/) V2 installed and authenticated for the providers you want to use. `make tool-versions` prints the exact versions Kingdom builds with.

Get Kingdom, then build and install the `kd` command:

```bash
git clone https://github.com/T-RexLabs/kingdom.git
cd kingdom
make install
export PATH="$PATH:$(go env GOPATH)/bin"
```

Then move to the project you want Kingdom to work in, initialize it, and start the local runtime:

```bash
cd /path/to/your/project
kd init
kd summon
```

> [!WARNING]
> Kingdom's agents run shell commands and change files in your project with your permissions. There is no sandbox. By default, Kingdom also approves the plan and the finished work for you.
>
> To approve both yourself, run `kd init --autonomous-approvals false`. If your project is a Git repository, add `--publish-mode git_worktree` to keep each run's changes in their own worktree. You can change both later in `.kingdom/config.yaml`; see the [Config Reference](docs/config%2Dreference.md).

Open the local URL printed by Kingdom. In Planning, work through the task and shape a plan, then issue a Work Order. Or create a Work Order directly if you already know what needs doing. Choose a model before each run, even if OpenCode already has a default model set.

If a run fails or you stop it, [When a Run Stops](docs/how%2Druns%2Dwork.md#when-a-run-stops) explains how to pick it back up.

## Make It Yours

Connect GitHub to bring issues into Planning or a new Work Order. Pick a personal theme or create a theme package to share with your team. Keep it calm or let Rexie steal the show.

* [Set Up GitHub Connections](docs/github%2Dconnections%2Drunbook.md)
* [Explore Themes and Appearance](docs/themes.md)

<h2 id="contribute">Build With Us</h2>

Kingdom is built with Go and TypeScript/React. From the repository root:

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
make verify
```

`make verify` runs the main Go and web checks, race tests, vet, coverage, and contract checks. Use `make help` to see the available targets, including the frontend development server.

## For Maintainers

The Go runtime serves the embedded web app and the `kd` command. SQLite stores workflow and audit data. OpenCode runs the model roles. Start here when changing the system:

* [System Architecture](docs/system%2Darchitecture.md): design boundaries and invariants
* [OpenAPI Contract](contracts/openapi/openapi.yaml): HTTP API source of truth
* [Runtime Policies](policies/): retry and provider behavior
* [The Crew](docs/how%2Druns%2Dwork.md#the-crew): what each role does
* [Prompt Authoring](docs/prompt-authoring.md): protected role duties and contracts, editable working guidance, and prompt snapshot rules
* [Seeded Workflows](internal/bootstrapassets/files/workflows/): workflow definitions
* [Local Setup and Verification](docs/local%2Dsetup%2Dand%2Dverification.md): development and verification steps

## Keep Exploring

* Learn how Planning, Work Orders, and Runs fit together: [How Runs Work](docs/how%2Druns%2Dwork.md)
* Configure your local runtime: [Config Reference](docs/config%2Dreference.md)
* Connect a GitHub App: [GitHub Connections](docs/github%2Dconnections%2Drunbook.md)
* Build or verify Kingdom locally: [Local Setup and Verification](docs/local%2Dsetup%2Dand%2Dverification.md)
* Find the rest of the docs: [Docs Guide](docs/README.md)

Use `kd help <command>` to see CLI commands and options. Ready to put the crew to work? [Start your first mission](#firstmission). Rexie’s ready when you are. 🦖
