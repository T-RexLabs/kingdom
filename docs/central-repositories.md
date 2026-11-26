# Central Repository Access

GitHub repository access uses a separate operator GitHub App. Login and issue-context credentials do not grant repository access. The operator binds each project to one installation ID and numeric repository ID. Project members cannot attach other repositories available to the App.

Initial repository import is available only before the first project startup. A project owner sends `POST /api/central/v1/projects/{project}/repository/import` with the session CSRF header, a UUID `Idempotency-Key`, and `{"ref":"main"}`. `GET /api/central/v1/projects/{project}/repository` reports whether a repository is configured and whether its operator grant allows publication. Neither response contains credentials or host paths.

The first successful resolution pins the requested branch or tag to an exact commit. A retry with the same key keeps that commit even if the branch moves. A different ref or repository grant conflicts with the retained request. Import stores repository ID, commit ID, and tree ID with the immutable project source. It uses the same first-start fence as file and ZIP input. It cannot replace an existing source.

The reader requests a token restricted to that repository with Contents read permission. It reads Git objects from `api.github.com`, verifies tree and blob hashes, and preserves executable file modes. It rejects incomplete trees, symbolic links, submodules, Git LFS pointer files, unsafe paths, and repository metadata. Limits are 32,768 entries, 1,024 tree requests, 64 MiB per file, 1 GiB total file bytes, and a 128 MiB compressed archive. Empty repositories without a commit cannot be imported.

For a successful run, **Publish to GitHub** opens an explicit confirmation. Kingdom publishes only the difference between that run's retained source and its verified published result. Changed or deleted GitHub paths must still match the source bytes and executable mode. New paths must be absent. Unrelated repository files remain intact. Conflicts stop publication; Kingdom does not overwrite upstream edits. File-to-directory and directory-to-file replacements are not supported by this publication adapter and stop as conflicts. A result with no file changes does not create a branch or pull request.

Publication creates a separate `kingdom/<operation ID>` branch and a pull request. It never changes the base branch. A retry retains the exact result snapshot, base commit, title, description, and Git commit data. An uncertain pull-request submission is reconciled by its exact repository, head branch, base branch, commit, and retained marker. Kingdom does not repeat that submission because a later list is empty. The UI can recover a saved pending publication after reload.

The operator enables this capability separately from login:

```json
"repository_app": {
  "app_id": 123,
  "private_key_file": "repository-app.pem",
  "grants": [{
    "project_id": "b550566c-246d-4a36-a65b-f5d810353747",
    "installation_id": 456,
    "repository_id": 789,
    "publish": true
  }]
}
```

The private key must be a private local RSA PEM file. Configure the App with Contents read permission for imports. Publishing requires Contents write and Pull Requests write. GitHub can reject changes to workflow files without further App permissions; Kingdom does not expand the token scope automatically. Set `publish` to false for import-only access. Restart the service after changing the allowlist.

Before generating an App key, prepare a protected local destination and confirm that the chosen browser can save the download there. Save the PEM with owner-only read and write permissions (`0600`), then compare its public-key fingerprint with the key listed by GitHub. Configure the service only after this check. If a generated key cannot be retrieved, save and verify a replacement before revoking the unretrieved key. Never paste private-key contents into chat or logs.

For automated registration, GitHub's [App manifest flow](https://docs.github.com/en/apps/sharing-github-apps/registering-a-github-app-from-a-manifest) returns the private key directly to an operator-controlled setup service through the API. This avoids a browser file download. It creates a new App; it does not recover or rotate a key for an existing App. The setup service must check a one-use state value and the initiating browser session, save the key in a protected local file, and verify the returned App identity and permissions before use. An uncertain code exchange must not be repeated. Kingdom's project API does not expose this operator registration step.

The protected publication routes are `POST /api/central/v1/projects/{project}/repository/publish`, `GET .../repository/publications?run_id={run}`, and `POST .../repository/publications/{publication}/reconcile`. The first request requires a UUID retry key and `{"run_id":"…","base_branch":"main","title":"…","body":"…"}`. Both POST routes require CSRF and project-owner authorization. Listing and recovery are restricted to the actor who submitted the request. A configured grant alone never causes a publication.

The implementation follows the [installation token API](https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app), [Git trees API](https://docs.github.com/en/rest/git/trees), and [Git blobs API](https://docs.github.com/en/rest/git/blobs), with API version `2026-03-10`. Deterministic transport fixtures verify these boundaries; they do not establish live App installation compatibility.

## Verification Without a GitHub Account

Run the repository service and emulator checks with:

```sh
go test -race ./internal/testutil/githubemu ./internal/central/repositories
```

The test-only emulator checks signed App requests, repository-scoped installation tokens, Git object hashes, branch creation, and pull request creation. It can deny permissions, interrupt requests, and hide a committed pull request after its response is lost. These checks use generated credentials and make no requests to GitHub. The production client still uses the fixed GitHub API origin; the emulator transport is available only through a test-build helper.

The opt-in managed acceptance test connects repository import, a real hosted project runtime, OpenCode execution through OpenShell, canonical result publication, and restart recovery:

```sh
KINGDOM_REPOSITORY_MANAGED_ACCEPTANCE=1 \
KINGDOM_REPOSITORY_ACCEPTANCE_ENV=/absolute/path/to/managed-test-environment \
KINGDOM_REPOSITORY_ACCEPTANCE_DIR="$PWD/kd-sandbox/repository-acceptance-new" \
go test ./internal/central/repositories \
  -run '^TestManagedRepositoryAcceptance$' -count=1 -timeout=20m -v
```

The environment directory contains `config.json` with the gateway ID and endpoint, allowed worker IP, and callback and model bind addresses and HTTPS origins. It also contains `profile.sha256`, `image-pins.json`, and a `tls/` directory with the CA, operator certificate and key, and server certificate and key. Use a dedicated managed test gateway and valid certificates. Keep private keys owner-only. The evidence directory must be a new directory under `kd-sandbox/`. The test skips when not enabled and fails if enabled prerequisites are missing.

This test uses an emulated GitHub API and a deterministic model endpoint. Kingdom, OpenCode, OpenShell, file tools, command execution, and resource-closure checks run normally. Passing it does not certify a real GitHub App installation, a live model account, or production deployment.
