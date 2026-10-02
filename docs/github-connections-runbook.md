# GitHub Connections

Kingdom can read repository details and issues from GitHub. It does not write to GitHub or subscribe to events. Kingdom supports GitHub.com only. Personal access tokens, GitHub Enterprise, background issue polling, and write back are not supported. This guide covers connecting an App, bringing issue context into planning, and managing the credentials Kingdom keeps on your machine.

For the full design and its security boundaries, see [System Architecture](system-architecture.md). For local settings, see the [Configuration Reference](config-reference.md).

## Choose a Setup Path

Open **Connections**, select **Add Connection**, then choose how to connect:

* **Connect GitHub** uses Kingdom's public GitHub App. Fresh `kd init` workspaces use this path by default. It asks you to authorize with GitHub, then choose which App installations Kingdom can use. The App grants only Metadata read and Issues read, and subscribes to no events. Kingdom needs no App secret or private key for this path.
* **Create a Private GitHub App** creates an App that you own, through GitHub. Choose a personal account or organization. Kingdom guides you through creating and installing the App, then checks that it has only Metadata read and Issues read, with no subscribed events.
* **Use Manual Setup** connects an App you already own. Save its display name, App ID, Client ID, and Installation ID, then provide its private key from the Kingdom host. The App must have only Metadata read and Issues read, with no subscribed events.

The shared App can be changed or disabled through `github.shared_app.slug` and `github.shared_app.client_id`. Set both values or leave both blank. See the [Configuration Reference](config-reference.md).

## Connect the Shared App

Select **Connect GitHub**, copy the code, and open GitHub. Authorizing lets Kingdom see installations that already exist for your GitHub account. It does not install the App for you. The rotating user token pair is stored locally under `.kingdom/secrets/github-user-tokens/`.

If needed, select **Choose Repositories** to install the App and choose which repositories it can see. Return to Kingdom and select **I Installed the App** or **Refresh Installations**. Tick each account Kingdom should cover, then select **Connect Selected Installations**. The connection can cover several accounts, and Kingdom can read all repositories granted by those installations.

Use **Change** beside **Accounts and Access** on a saved connection to add or remove covered accounts, then select **Save Installations** and **Check**. To change which repositories an installation grants, use **Manage Repositories on GitHub**. Kingdom reads that grant from GitHub, so repository changes appear on the next discovery without another check.

One connection should cover every installation authorized together. GitHub rotates one refresh token for the whole authorization, so splitting its installations between connections can leave one connection with an invalid token.

To restore authorization, open **Manage Connection** and select **Reconnect Connection**. Reconnect must include every account already saved on that connection, with the same account identity. It cannot add or remove accounts. Restore a missing App installation or your GitHub access, then refresh the list. Use **Change** if an account should be removed. GitHub may require reconnect after six months without refresh token use. The owner of Kingdom's public App can also revoke or delete that App.

If you leave setup unfinished, return to **Connections** and select **Resume Setup**. Select **Discard Setup** to clear the shared App flow. Setup data is held in memory for your browser session, not in browser storage. A Kingdom restart clears unfinished setup.

## Create a Private GitHub App

Select **Create a Private GitHub App**, choose a personal account or organization, and enter the **Organization Login** when needed. Select **Create Private App on GitHub**. GitHub opens a separate setup tab. Complete each GitHub step there, then return to Kingdom. The Kingdom page stays open and updates as the setup progresses.

Kingdom asks you to install the App only on accounts and repositories you want it to discover. After installation, Kingdom asks you to authorize the App and verifies the installation and its access before saving the connection. GitHub lets you edit the App name before creating it. The App's private key stays on the Kingdom host under `.kingdom/secrets/github-apps/`. Kingdom discards the temporary client secret, user token, OAuth code, and webhook secret when setup finishes.

Use Kingdom over HTTPS unless the browser address is localhost or a loopback address. Setup callbacks use the browser's exact origin. Forwarded headers do not change that origin.

If you leave partway through, return to **Connections** and select **Resume Setup**. If a step fails, use **Retry This Step** when Kingdom offers it, or select **Start Again** when the flow cannot continue. Select **Abandon Setup** to clear a private App flow. Setup data is held in memory for your browser session, not in browser storage. A Kingdom restart clears unfinished setup. A private App setup also expires after an hour or when the browser session expires.

## Use Manual Setup

Select **Use Manual Setup** and enter the existing App and installation details. The form does not accept a private key or a key file path. After saving the details, run the command shown in Kingdom on the host where Kingdom is installed:

```bash
kd connections github key provision --connection-id <id> --from ./private-key.pem
```

The key must be RSA in PKCS#1 or PKCS#8 PEM format. Kingdom stores it under `.kingdom/secrets/github-apps/` with mode `0600`, inside a secret directory owned by the current user with mode `0700`. The database stores only a credential reference. Run **Check** after provisioning. To rotate a key, first create a replacement key in GitHub and save its PEM file on the Kingdom host. Then use:

```bash
kd connections github key rotate --connection-id <id> --from ./replacement.pem
```

Run **Check** again after rotation. App ID, Client ID, Installation ID, and numeric account identity cannot be edited. Only the display name can be changed. A revoked App or installation, missing permission, suspended account, or temporary GitHub failure will prevent a successful check. Kingdom reports a safe error class without exposing GitHub response text.

## Bring Issues Into Planning

In a planning session, select **Add GitHub Issue**. After adding a source, use **Add Source** to attach another. Select the source count to open **Linked Sources**. Kingdom lists issues from repositories granted to the connection and filters out pull requests.

Issue comments are off by default. Turn on **Include Comments** before choosing an issue if you want to include them. When enabled, Kingdom keeps the complete comment history it reads. When issue context is used for planning or a Work Order, its text and any selected comments may be sent to the configured LLM provider. Treat text from a GitHub issue as untrusted: it was written by someone outside Kingdom and can contain instructions or links.

Kingdom marks issue text as untrusted evidence. The planner can inspect the project and research the issue, but it has no shell, write, or subagent tools. Review the issue and repository before attaching it, especially if the text may be hostile. For direct Work Orders, review the issue text before issuing the work. Once issued, the Work Order keeps the text that you approved.

Refresh a linked issue when you want to check for changes. If an update appears, **Review Update** lets you inspect and adopt it for future planning. Refreshing or removing a source does not change work already done or inputs already used by an operation. If a source change has an unresolved outcome, use **Recover Source Request** to resolve it. **Refresh Source State** only rereads the current state.

## Disable, Remove, and Restore

**Disable** stops Kingdom from using a connection but keeps its local credential and history. **Remove Connection** removes the local credential and hides the connection from the Connections list. Kingdom keeps its history for audit. If removal does not finish, the connection remains listed; open **Manage Connection** and select **Retry Removal**. Kingdom also reconciles unfinished removal after a restart. Removal does not uninstall the GitHub App or revoke its authorization on GitHub. To revoke access fully, also uninstall the App or remove its authorization in GitHub.

An ordinary SQLite backup does not include `.kingdom/secrets`. After restoring a workspace, reconnect shared App connections and provide private App keys again, then run **Check**. `kd clean` removes `.kingdom`, including local credentials. Keep separate copies of private keys or be ready to rotate them, and be ready to reconnect the shared App.

## Maintainer Smoke Check

The optional `make test-github-smoke` command checks a controlled App and issue on GitHub.com. It needs a transferred issue and these environment values: `KINGDOM_GITHUB_SMOKE_APP_ID`, `KINGDOM_GITHUB_SMOKE_CLIENT_ID`, `KINGDOM_GITHUB_SMOKE_INSTALLATION_ID`, `KINGDOM_GITHUB_SMOKE_REPOSITORY_ID`, `KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER`, `KINGDOM_GITHUB_SMOKE_TRANSFERRED_REPOSITORY_ID`, `KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER`, and `KINGDOM_GITHUB_SMOKE_PRIVATE_KEY_FILE`. Point the last value at a protected PEM file. Never put key contents in an environment value.

**The final smoke phase uninstalls the controlled App installation.** Set `KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL=1` only after confirming that the test fixture can be installed again. This command is not part of `make verify`.
