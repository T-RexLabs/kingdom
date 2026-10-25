# GitHub Repository Emulator

This test utility implements only the GitHub endpoints used by the repository
adapter. It accepts the production HTTPS origin and API version through an
in-process transport. It does not open a listener or use external credentials.

The emulator verifies fixture RSA App JWT signatures, issuer and times; exact
installation repository and permission scopes; Git blob, tree and commit
identities; branch targets; and pull request identity. It retains remote state
when a test reconstructs the central service. Fault controls cover permission
denial, one lost connection, and a committed pull request whose reply and later
list results are unavailable. Snapshot output excludes authentication material
and HTTP request bodies. Repository file bytes are controlled fixture data.

Fast service acceptance uses real central HTTP authority, source import and
canonical stored result readers. Its successful run state is seeded test data.
The separate opt-in managed acceptance executes OpenCode in OpenShell. Neither
mode establishes live GitHub App installation, authentication, or provider
compatibility. External GitHub calls are outside this emulator's evidence.
