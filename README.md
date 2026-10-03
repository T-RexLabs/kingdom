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
  <a href="#contribute">Contribute to the Kingdom</a>
</p>

In a world of rising token costs, raising your own Kingdom can help protect the treasury. Kingdom is open source and runs on your hardware by default. You choose where your data lives, and keep planning, execution, and audit under your control.

Your LLM crew plans the work, builds it, and checks the result.

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

You’ll need Go 1.27.0, Node 26.2.0, pnpm 12.3.4, Python 3.13.12, and [OpenCode](https://opencode.ai/docs/) installed and authenticated for the providers you want to use. Install pnpm separately. The required tool versions are recorded in `.tool-versions`; `make tools-check` checks the active tools. Python runs tool preflight and development checks.

From the Kingdom repository, build and install the `kd` command:

```bash
make install
export PATH="$PATH:$(go env GOPATH)/bin"
```

Then move to the project you want Kingdom to work in, initialize it, and start the local runtime:

```bash
cd /path/to/your/project
kd init
kd summon
```

Open the local URL printed by Kingdom. In Planning, work through the task and shape a plan, then issue a Work Order. Or create a Work Order directly if you already know what needs doing. Choose a model before each run, even if OpenCode already has a default model set.

Kingdom uses OpenCode to list models and run them. Your OpenCode setup can use local or hosted models.

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

`make verify` runs Go and web checks, race tests, vet, coverage, contracts, OpenAPI lint, script checks, and shipped asset checks. Use `make help` to see the available targets. The [local verification guide](docs/local%2Dsetup%2Dand%2Dverification.md) explains test selection and the explicit sandbox backend required by the frontend development server.

Test concurrency defaults to two CPUs, locally and in CI. Make runs verification targets in sequence, including with `make -j`. Go runs one package at a time; Go tests and frontend workers use `KINGDOM_TEST_CPUS`. Use `make verify KINGDOM_TEST_CPUS=1` to reduce concurrency, or set a higher positive integer when more CPU capacity is available. This setting controls test concurrency, not a machine-wide CPU limit.

## For Maintainers

The Go runtime serves the embedded web app and the `kd` command. SQLite stores workflow and audit data. OpenCode runs the model roles. Start here when changing the system:

* [System Architecture](docs/system%2Darchitecture.md): design boundaries and invariants
* [OpenAPI Contract](contracts/openapi/openapi.yaml): HTTP API source of truth
* [Runtime Policies](policies/): retry and provider behavior
* [Seeded Workflows](internal/bootstrapassets/files/workflows/): workflow definitions
* [Local Setup and Verification](docs/local%2Dsetup%2Dand%2Dverification.md): development and verification steps

## Keep Exploring

* Learn how Planning, Work Orders, and Runs fit together: [How Runs Work](docs/how%2Druns%2Dwork.md)
* Configure your local runtime: [Config Reference](docs/config%2Dreference.md)
* Connect a GitHub App: [GitHub Connections](docs/github%2Dconnections%2Drunbook.md)
* Build or verify Kingdom locally: [Local Setup and Verification](docs/local%2Dsetup%2Dand%2Dverification.md)
* Find the rest of the docs: [Docs Guide](docs/README.md)

Use `kd help <command>` to see CLI commands and options. Ready to put the crew to work? [Start your first mission](#firstmission). Rexie’s ready when you are. 🦖
