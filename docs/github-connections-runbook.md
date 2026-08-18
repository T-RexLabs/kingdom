# GitHub Connections Runbook

## Recommended Shared App Setup

Fresh `kd init` configuration uses Kingdom's official public App automatically: `kingdom-issue-importer` with client ID `Iv23li1tLBTzhr70LA1J`. Operators can override both `github.shared_app.slug` and `github.shared_app.client_id`, or blank both to disable the shared path. The App is public/installable, uses device flow and expiring user tokens, grants exactly Metadata read-only and Issues read-only, and subscribes to no events. Kingdom needs no App secret, private key, or broker service.

Select **Connect GitHub**, copy the short code, and open GitHub. Poll cadence is enforced by the local server. Kingdom paginates `/user/installations`, filters exact client ID/slug/permissions/events/suspension, and returns only safe candidates. If none exist, **Choose repositories** opens `https://github.com/apps/{slug}/installations/new`; refresh candidates without repeating authorization.

Finalization re-fetches candidates and atomically stores one healthy connection plus `.kingdom/secrets/github-user-tokens/<connection>/tokens.json`. Access is the intersection of the App and authorizing user. Refresh rotates both tokens without a client secret and without changing generation; a GitHub `401` gets one safe refresh/retry independent of the normal safe-GET transport retry budget. Reauthorization through **Reconnect** advances generation. Six months of refresh-token disuse can require reconnect. The App owner can revoke or delete the shared App centrally.

## Private Manifest Setup

Open **Connections**, select **Create a private GitHub App**, and choose a personal or organization owner. Kingdom submits a browser POST directly to GitHub with a manifest that creates a private user-owned App with:

- Metadata: read-only
- Issues: read-only

The manifest declares no events and no webhook at all (GitHub validates a manifest hook URL for public reachability even when the hook is inactive, which would reject every localhost origin), plus `request_oauth_on_install=false`, and dynamic callback URLs derived from the exact initiating browser origin. GitHub allows the App name to be edited before creation. Install the App only on repositories Kingdom should discover.

GitHub returns through public one-time-state callbacks, so the `SameSite=Strict` browser cookie is not required on callback GETs. Each callback redirects with `303` to the stored exact-origin `/connections` page without code, state, or installation query data. The page resumes from memory-only server flow status. A restart or one-hour expiry requires a new flow.

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

The picker enumerates installation-granted repository IDs, then browses or searches issue-only results. Pull requests returned by GitHub's Issues API are removed. Comment count defaults on; bodies default off. Selected bodies are shown in an accessible paginated preview with raw HTML, remote images, embeds, and unsafe links blocked. Retention is 100 bodies, 32 KiB each, 128 KiB aggregate. A planning session can attach multiple unique issues through **Add GitHub issue** / **Add source**; duplicate attachment to the same session is rejected atomically. Planner context uses no more than 64 KiB comments and 128 KiB total external text.

Refresh is manual. Issue and comment ETags are checked independently, and an unchanged normalized digest creates no snapshot. A changed response appends an immutable snapshot and field status. Provider fetches follow the source's current locator after a transfer, but decree summaries retain used-snapshot coordinates and planning summaries show active-adoption coordinates until the operator adopts latest. Before decree issuance, refreshed title/body and the issuance snapshot are applied explicitly. Planning adoption is explicit and affects future operations only. Issued decrees always retain their exact used snapshot and immutable text: **“The GitHub source changed; this decree was not changed.”**

## Disable, Remove, and Revoke

Disable blocks provider access but keeps the strategy-specific local credential and history. Remove tombstones SQLite, evicts caches, and removes the local key or user-token file; provenance remains. Local removal does not uninstall/revoke the App or authorization. GitHub remains the external kill switch.

## Backup, Restore, and Clean

Ordinary SQLite backups retain source history but exclude `.kingdom/secrets`. After restore, reprovision each key and health-check the connection. Selective provenance purge is not supported; whole-workspace deletion is the v1 deletion boundary. `kd clean` removes `.kingdom`, including all local credentials—back up keys separately or be ready to rotate/reprovision them.

## Private Content Disclosure

Selected private issue text and retained comments may be sent to the configured model provider. Kingdom frames this material as untrusted external evidence, keeps it out of transcript messages, and denies all OpenCode tools for provider operations carrying external content.

Use `make test-opencode-security-smoke` on a host licensed for **GitHub Copilot - GPT-5.4 Mini** to run the opt-in adversarial planner canary. The ordinary suite remains deterministic and proves the same wildcard denial at the provider-request boundary.

## Controlled Live Smoke Test

Deterministic adapters cover normal verification. Before release, a controlled App can verify GitHub.com itself with `make test-github-smoke`. Prepare an issue that has been transferred between two repositories granted to the installation. Set `KINGDOM_GITHUB_SMOKE_APP_ID`, `KINGDOM_GITHUB_SMOKE_CLIENT_ID`, `KINGDOM_GITHUB_SMOKE_INSTALLATION_ID`, `KINGDOM_GITHUB_SMOKE_REPOSITORY_ID`, and `KINGDOM_GITHUB_SMOKE_ISSUE_NUMBER` to the original coordinates, plus `KINGDOM_GITHUB_SMOKE_TRANSFERRED_REPOSITORY_ID` and `KINGDOM_GITHUB_SMOKE_TRANSFERRED_ISSUE_NUMBER` to the destination coordinates. Set `KINGDOM_GITHUB_SMOKE_PRIVATE_KEY_FILE` to a protected PEM file path; never place key contents in an environment variable.

The final smoke phase permanently uninstalls the controlled installation to prove revocation handling. Set `KINGDOM_GITHUB_SMOKE_ALLOW_UNINSTALL=1` only after confirming the fixture can be reinstalled. The test skips with a named missing prerequisite when the controlled App is unavailable and never runs as part of `make verify`.
