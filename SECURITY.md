# Security

## Reporting a Vulnerability

Please don't report security problems in a public issue.

Report them privately through GitHub instead: [open a private report](https://github.com/T-RexLabs/kingdom/security/advisories/new). Only the maintainers can see it.

It helps to include:

* what you found and what an attacker could do with it
* the steps to reproduce it
* the Kingdom commit you tested

We'll reply as soon as we can, usually within a week. We'll keep you updated while we fix it and credit you when the fix ships, unless you'd rather stay anonymous.

## Supported Versions

Kingdom has no releases yet. Security fixes go to the latest `main`.

## What Kingdom Does by Design

Kingdom runs AI agents on your machine, as your user, in the project you start it in. Please read this before reporting.

* **There is no sandbox.** Agents run shell commands and change files in your project with your permissions. Kingdom checks agent actions against its policies, but that is not operating-system isolation.
* **Approvals are off by default.** A new setup moves through the plan and final approval gates without waiting for you. You can turn manual approvals on, or keep each run in its own Git worktree; see the [Config Reference](docs/config-reference.md).
* **Your models see your code.** Prompts and project files go to the model providers you set up in OpenCode. What those providers do with your data is up to them.

If you work with code you don't trust, run Kingdom in a container or a virtual machine.

## What We Want to Hear About

* GitHub tokens or other secrets Kingdom stores leaking, for example into logs, run records, agent prompts, or the web app
* Another website, or another device on your network, being able to use Kingdom's local web app or API
* Kingdom itself reading or writing outside the project it was started in

## What Isn't a Kingdom Vulnerability

* An agent running a command or changing a file. That is what Kingdom is for.
* Problems in OpenCode or in a model provider. Report those to them.
* Attacks that need you to run Kingdom on a project or config you already know is malicious, or that need someone who can already run code as your user.
