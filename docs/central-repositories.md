# Central Repository Access

GitHub repository access uses a separate operator GitHub App. Login and issue-context credentials do not grant repository access. The operator binds each project to one installation ID and numeric repository ID. Project members cannot attach other repositories available to the App.

Initial repository import is available only before the first project startup. A project owner sends `POST /api/central/v1/projects/{project}/repository/import` with the session CSRF header, a UUID `Idempotency-Key`, and `{"ref":"main"}`. `GET /api/central/v1/projects/{project}/repository` reports whether a repository is configured and whether its operator grant allows publication. Neither response contains credentials or host paths.

The first successful resolution pins the requested branch or tag to an exact commit. A retry with the same key keeps that commit even if the branch moves. A different ref or repository grant conflicts with the retained request. Import stores repository ID, commit ID, and tree ID with the immutable project source. It uses the same first-start fence as file and ZIP input. It cannot replace an existing source.

The reader requests a token restricted to that repository with Contents read permission. It reads Git objects from `api.github.com`, verifies tree and blob hashes, and preserves executable file modes. It rejects incomplete trees, symbolic links, submodules, Git LFS pointer files, unsafe paths, and repository metadata. Limits are 32,768 entries, 1,024 tree requests, 64 MiB per file, 1 GiB total file bytes, and a 128 MiB compressed archive. Empty repositories without a commit cannot be imported.

Repository publication is not enabled by the import routes. The publication adapter must use a verified settled result and an explicit user request. A configured publication grant alone does not cause a write.

The implementation follows the [installation token API](https://docs.github.com/en/rest/apps/apps#create-an-installation-access-token-for-an-app), [Git trees API](https://docs.github.com/en/rest/git/trees), and [Git blobs API](https://docs.github.com/en/rest/git/blobs), with API version `2026-03-10`. Deterministic transport fixtures verify these boundaries; they do not establish live App installation compatibility.
