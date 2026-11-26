# Connect a Local ChatGPT Account

Kingdom can register its own local client for ChatGPT plan usage. This connection
is for the local owner who signs in. It is not a team connection and cannot be
used by the shared central service. A hosted service needs its own approved
OpenAI client. Kingdom does not copy tokens from OpenCode, Codex, or another app.

This path requires the optional local OpenShell configuration. The default local
OpenCode workflow stays unchanged. Use the same absolute state directory, owner
ID, and protected master key file as the local OpenShell configuration:

```sh
kd chatgpt sign-in \
  --state-root /absolute/path/to/managed-state \
  --owner-id YOUR-LOCAL-OWNER-UUID \
  --master-key-file /absolute/path/to/master.key
```

The command opens the browser and waits for your consent through a temporary
loopback callback. It prints a registration ID before sign-in. Keep that ID if
sign-in fails. To retry, add `--registration-id ID`; Kingdom then reuses its
retained client and requires the same verified account.

Set the model section in the protected local OpenShell configuration:

```json
{
  "model": {
    "auth_kind": "chatgpt",
    "registration_id": "THE-PRINTED-REGISTRATION-UUID"
  }
}
```

Do not add an API key, endpoint override, private CA, model list, or team scope.
Kingdom reads the signed-in account's model list from the public OpenAI API.
The request proxy keeps the OAuth token outside the sandbox. It uses the public
Responses endpoint with streaming, no stored responses, and complete request
history. The pinned worker selects the native Responses provider. The proxy retains the Kingdom namespace when OpenCode replays function calls and results. It rejects server-side continuation and unsupported hosted tools.

Use `kd chatgpt status` or `kd chatgpt disconnect` with the same storage flags and
`--registration-id ID`. Disconnect prevents local use first. If remote
revocation cannot be confirmed, the command reports that fact and directs you to
remove the Kingdom connection in ChatGPT account settings. A lost exchange or
refresh response requires sign-in again; Kingdom does not replay a rotating
refresh token without knowing its result.

The state directory must be private to the local OS user. The master key file
must have mode `0600`; tokens are encrypted in `chatgpt.db`. Keep the directory
and key together in protected backups, and do not copy a live credential vault
between running hosts. Separate project processes use the same per-registration
lock to serialize refresh.

Synthetic tests verify the protocol, storage, account binding, and request
conversion. The pinned OpenCode worker also completed a controlled Responses
function-tool round trip and rejected a stream without its completion event.
Real account consent and inference against OpenAI remain unverified.
The current preview documentation names `earliest_refresh_at` without specifying
its wire type; Kingdom currently accepts an optional Unix-seconds integer and
rejects other formats. See OpenAI's [registration guide](https://developers.openai.com/siwc/token-sharing-open-source/sign-in),
[models and inference guide](https://developers.openai.com/siwc/token-sharing-open-source/models-and-inference),
and [preview limitations](https://developers.openai.com/siwc/token-sharing-open-source/preview-limitations).
