# Initial Project Files

A project owner can upload a ZIP archive or one file before the project's first startup. Other project members can read input status. The portal does not offer replacement: a selected input or the first startup permanently closes new uploads. Create a new project to use different initial files.

The service stores a verified immutable archive manifest in the central catalog and keeps the source bytes in a private directory. Upload publication and first startup use the same database writer lock. Startup pins the destination directory identity. An interrupted initial copy can retry exact files but cannot replace different bytes or adopt a different directory. Once preparation completes, later startups do not restore the original input over project results.

ZIP uploads are limited to 128 MiB compressed, 1 GiB expanded, 64 MiB per file, 32,768 entries including directories, 64 path components, and 4,096 path bytes. Single-file uploads are limited to 64 MiB. Existing archive validation rejects links, duplicate or ambiguous names, traversal, protected project metadata, unsupported file types, and invalid paths. At most four uploads per source service are staged at once.

`kingdom-central` connects the protected upload routes and the hosted builder to the same source service. The portal's Initial Project Files section selects the project and input type. Add files before opening the project. On shutdown, the application joins project startup and active HTTP requests before closing the source service. A database reference is published only after source bytes are synchronized. An uncertain database commit retains its private bytes for exact retry; it does not activate unverified input.

## API

- `GET /api/central/v1/projects/{project}/source` returns `state` (`empty`, `imported`, or `started`) and optional source metadata.
- `POST /api/central/v1/projects/{project}/source/zip` accepts `application/zip`.
- `POST /api/central/v1/projects/{project}/source/file?name=relative-name` accepts `application/octet-stream`.

Uploads require a session, same-origin request, CSRF token, project-owner authority, and a UUID `Idempotency-Key`. Retries must retain the same content, input kind, filename, and key. Responses contain only an opaque source ID, kind, file count, byte count, and manifest SHA-256. They do not expose private host paths.

Repository import and GitHub publication use separate repository grants and are not provided by these upload routes. GitHub sign-in and the existing issue-reading connection do not grant repository content access.
