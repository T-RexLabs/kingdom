# GitHub Connections Runbook

## Recommended Shared App Setup

Fresh `kd init` configuration uses Kingdom's official public App automatically: `kingdom-issue-importer` with client ID `Iv23li1tLBTzhr70LA1J`. Operators can override both `github.shared_app.slug` and `github.shared_app.client_id`, or blank both to disable the shared path. The App is public/installable, uses device flow and expiring user tokens, grants exactly Metadata read-only and Issues read-only, and subscribes to no events. Kingdom needs no App secret, private key, or broker service.

Select **Connect GitHub**, copy the short code, and open GitHub. Authorizing only lets Kingdom see installations that already exist; it does not install the App. Poll cadence is enforced by the local server. Kingdom paginates `/user/installations`, filters exact client ID/slug/permissions/events/suspension, and returns only safe candidates. Installing on a further account stays reachable throughout: with no candidates, **Choose Repositories** opens `https://github.com/apps/{slug}/installations/new`; with candidates listed, **Install on Another Account** opens the same page beside **Refresh Installations**. Refreshing re-lists candidates without repeating authorization. Each candidate reports whether the installation grants **All Repositories** or **Selected Repositories**; an absent or unrecognized scope is shown as no label and never blocks an otherwise valid installation.

Tick every account the connection should cover. One connection is created for all of them, because a device authorization returns a single rotating token pair for the whole App: splitting those installations across separate connections would have each holding a copy of a refresh token that the first rotation invalidates. Kingdom reads every repository every covered installation grants. **Change** on a saved connection reopens the same picker against that connection's own credential, so adding an organization later needs no fresh authorization, and **Reconnect** stays single-choice because it reauthorizes one existing connection rather than choosing what to cover. An installation already held by a live connection is refused; removing a connection releases its installations.

An issue transferred between repositories in two installations of the same connection keeps refreshing. One transferred outside the connection's grant is still refused.

Finalization re-fetches candidates and atomically stores one healthy connection plus `.kingdom/secrets/github-user-tokens/<connection>/tokens.json`. Access is the intersection of the App and authorizing user. Refresh rotates both tokens without a client secret and without changing generation; a GitHub `401` gets one safe refresh/retry independent of the normal safe-GET transport retry budget. Reauthorization through **Reconnect** advances generation. Six months of refresh-token disuse can require reconnect. The App owner can revoke or delete the shared App centrally.

## Private Manifest Setup

Open **Connections**, select **Create a private GitHub App**, and choose a personal or organization owner. Kingdom opens a dedicated GitHub setup tab and submits a browser POST to GitHub there with a manifest that creates a private user-owned App with:

- Metadata: read-only
- Issues: read-only

The manifest declares no events and no webhook at all (GitHub validates a manifest hook URL for public reachability even when the hook is inactive, which would reject every localhost origin), plus `request_oauth_on_install=false`, and dynamic callback URLs derived from the exact initiating browser origin. GitHub allows the App name to be edited before creation. Install the App only on repositories Kingdom should discover.

Every later hop — installing the App, then authorizing it — is another button that reuses the same setup tab. The Kingdom tab never navigates away: it keeps driving the flow and polls for the stage change GitHub produces, so an interrupted or closed setup tab costs nothing.

GitHub returns through public one-time-state callbacks, so the `SameSite=Strict` browser cookie is not required on callback GETs. Each callback redirects with `303` to the stored exact-origin `/connections/github/return` handoff page without code, state, or installation query data; that page closes the setup tab and performs no requests of its own. A restart or one-hour expiry requires a new flow.

An unfinished flow is never re-opened as a modal. Returning to **Connections** shows it inline with **Resume setup** and **Abandon setup**; abandoning discards the memory-only flow and zeroes its transient secrets rather than waiting out the hour.

If automatic conversion, installation, authorization, completion, or initial start fails, the private-App setup surface shows stage-specific guidance. **Retry this step** is reserved for an ambiguous transport failure and reuses the stable idempotency key. **Start again** is shown for a known terminal failure and creates a new flow with a new key, preventing stale auto-advance loops.

After installation, Kingdom starts explicit temporary GitHub App OAuth with PKCE. It exchanges the local callback code using the transient client secret, paginates `/user/installations` to prove the claimed installation is visible to that user and belongs to the same App, then independently validates the App, installation, exact permissions, non-suspended account identity, installation token, and repositories with the returned private key. The user token, client secret, OAuth code, and webhook secret are discarded. The private key is installed only at atomic healthy-connection finalization.

Production setup trusts neither forwarding headers nor configurable provider origins. HTTPS is required except for direct localhost or loopback Manifest origins. PATs, active webhooks, background issue polling, write-back, paste-link, GitHub Enterprise, and a Kingdom-hosted control service remain unsupported.

## Manual Existing-App Fallback

Choose **Use manual setup** and save display name, App ID, client ID, and installation ID. The slug starts empty and is populated from verified `GET /app` during health. Manual creation uses `credential_strategy=app_private_key`. The browser never accepts a private key or path. On the initialized host run:

```bash
kd connections github key provision --connection-id <id> --from ./private-key.pem
kd connections github key rotate --connection-id <id> --from ./replacement.pem
```

The key must be RSA PKCS#1 or PKCS#8 PEM. Kingdom writes it atomically with `0600` mode below a current-user-owned `0700` secret directory. SQLite receives only `github-app:<connection-id>`. Tokens are minted on demand, expire quickly, remain memory-only, and are evicted after key fingerprint changes, disablement, removal, or authentication failure.

Run **Check** after provisioning or rotation. Kingdom validates `GET /app` for exactly Metadata read-only and Issues read-only, no additional permissions, and zero subscribed events, then separately validates the installation permissions. App or installation configuration mismatch, suspension, revocation, rate limiting, and temporary service failure are reported as safe classes without provider response text. Revocation returns a dependency response with reconnect guidance, never the local-auth `401` that signs a browser out. Rate-limited health and discovery errors show the safe reset time supplied in canonical `retry_at` metadata when available.

Only the saved display name can be edited. App ID, client ID, installation ID, and numeric account ID are immutable; account login may follow a verified GitHub rename. Provisioning or rotating a key invalidates prior health and requires a new **Check**.

## Intake and Refresh

The picker enumerates installation-granted repository IDs, then browses or searches issue-only results. The repository list aggregates across the connection's installations, so a connection covering several accounts shows all of their grants together, loads more as the list is scrolled, and applies the owner/name filter across the whole loaded page. Pull requests returned by GitHub's Issues API are removed. Comment count defaults on; bodies default off, and the **Include Comments** switch sits beside the issue list so the choice is visible before an issue is selected. Selected bodies are shown in an accessible paginated preview with raw HTML, remote images, embeds, and unsafe links blocked. Retention is 100 bodies, 32 KiB each, 128 KiB aggregate. A planning session can attach multiple unique issues through **Add GitHub issue** / **Add source** inside the session itself; duplicate attachment to the same session is rejected atomically. Sources live in the **Planning Sources** panel in the session settings row, which opens anchored to its trigger. **Remove** detaches a source from future planner operations after an inline confirmation. Provenance, refresh history, and operations already bound to that source are unchanged, and the same issue can be added again — which reuses its link and adopts freshly captured content. Planner context uses no more than 64 KiB comments and 128 KiB total external text. On the Issue Decree page the prefilled decree text is the issue body plus the retained comment bodies when the import selected them, attributed and blockquoted as external evidence; it is editable before issuing, and the issued text is what execution reads.

Refresh is manual. Issue and comment ETags are checked independently, and an unchanged normalized digest creates no snapshot. A changed response appends an immutable snapshot and field status. Provider fetches follow the source's current locator after a transfer, but decree summaries retain used-snapshot coordinates and planning summaries show active-adoption coordinates until the operator adopts latest. Before decree issuance, refreshed title/body and the issuance snapshot are applied explicitly. Planning adoption is explicit and affects future operations only; **Review Update** shows the snapshot diff and adopts from there. Issued decrees always retain their exact used snapshot and immutable text: **“The GitHub source changed; this decree was not changed.”**

## Disable, Remove, and Revoke

Disable blocks provider access but keeps the strategy-specific local credential and history. Remove tombstones SQLite, evicts caches, and removes the local key or user-token file; provenance remains. The tombstone leaves the Connections view — it is retained for provenance, not as an operator surface — and the same installation can be added again. A connection whose removal did not finish stays listed as removal-pending with a retry, and startup reconciles it. Local removal does not uninstall/revoke the App or authorization. GitHub remains the external kill switch.

A connection's repository grant belongs to the installation, so it is widened or narrowed on GitHub, never in Kingdom. **Manage Repositories on GitHub** on a saved connection opens that installation's repository selection directly through `https://github.com/apps/{slug}/installations/new/permissions?suggested_target_id={account}`. Discovery reads the grant live, so a changed repository set needs no **Check**. Organization members without owner rights get GitHub's own installation-request flow.

## Backup, Restore, and Clean

Ordinary SQLite backups retain source history but exclude `.kingdom/secrets`. After restore, reprovision each key and health-check the connection. Selective provenance purge is not supported; whole-workspace deletion is the v1 deletion boundary. `kd clean` removes `.kingdom`, including all local credentials—back up keys separately or be ready to rotate/reprovision them.

## Private Content Disclosure

Selected private issue text and retained comments may be sent to the configured model provider. Kingdom frames this material as untrusted external evidence, keeps it out of transcript messages, and denies all OpenCode tools for provider operations carrying external content.

Use `make test-opencode-security-smoke` on a host licensed for **GitHub Copilot - GPT-5.4 Mini** to run the opt-in adversarial planner canary. The ordinary suite remains deterministic and proves the same wildcard denial at the provider-request boundary.

## Controlled Live Smoke Test

Deterministic adapters cover normal verification. Before release, a controlled App can verify GitHub.com itself with `make test-github-smoke`. Prepare an issue that has been transferred between two repositories granted to the installation. Set `KINGDOM_GITHUB_SMOKE_APP_ID`, `KINGDOM_GITHUB_SMOKE_CLIENT_ID`, `KINGDOM_GITHUB_SMOKE_INSTALLATION_ID`, `KINGDOM_GITHUB_SMOKE_REPOSITORY_ID`, and `KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER` to the original coordinates, plus `KINGDOM_GITHUB_SMOKE_TRANSFERRED_REPOSITORY_ID` and `KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER` to the destination coordinates. Set `KINGDOM_GITHUB_SMOKE_PRIVATE_KEY_FILE` to a protected PEM file path; never place key contents in an environment variable.

The final smoke phase permanently uninstalls the controlled installation to prove revocation handling. Set `KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL=1` only after confirming the fixture can be reinstalled. The test skips with a named missing prerequisite when the controlled App is unavailable and never runs as part of `make verify`.
