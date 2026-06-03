# Planner Operations Runbook

## Purpose

Use this runbook when planner reply, regenerate, or formalize work appears stuck, missing, or inconsistent after a refresh, reconnect, or runtime restart.

## Runtime Contract

- Planning sessions persist independently from decrees and runs.
- Planner work runs as durable planning operations, not request-scoped stream handlers.
- Supported operation types are `reply`, `regenerate`, and `formalize`.
- Assistant transcript entries can have variants.
- Planning operation events are replayed through `GET /api/v1/planning-operations/{id}/events` with SSE `id:` frames and `Last-Event-ID` resume.
- Unknown planning operations return `404` before a stream opens.
- Invalid `Last-Event-ID` values return `400`.
- The planner UI falls back to polling the planning-operation row if SSE retries are exhausted.
- Transcript truth comes from persisted planning messages, not from transient stream state.
- `GET /api/v1/metrics/runtime` includes planner recovery and replay counters.

## Quick Checks

### 1. Fetch the session

```bash
curl -s \
  -H 'X-Actor-Id: allfather' \
  -H 'X-Role-Id: king' \
  http://127.0.0.1:8080/api/v1/planning-sessions/<session_id>
```

Inspect:

- `conversation_state`
- `current_operation_id`
- `current_question_message_id`
- `current_draft_id`
- `context_snapshot_id`

### 2. Fetch the active operation

```bash
curl -s \
  -H 'X-Actor-Id: allfather' \
  -H 'X-Role-Id: king' \
  http://127.0.0.1:8080/api/v1/planning-operations/<operation_id>
```

Inspect:

- `type`
- `status`
- `planning_session_id`

### 3. Replay operation events

```bash
curl -N \
  -H 'X-Actor-Id: allfather' \
  -H 'X-Role-Id: king' \
  -H 'Last-Event-ID: <operation_id>:3' \
  http://127.0.0.1:8080/api/v1/planning-operations/<operation_id>/events
```

### 4. Check planner metrics

```bash
curl -s \
  -H 'X-Actor-Id: allfather' \
  -H 'X-Role-Id: king' \
  http://127.0.0.1:8080/api/v1/metrics/runtime
```

## Expected Event Sequences

### Reply

- `user_message`
- `question_ready`
- zero or more `text_delta`
- `done`

### Regenerate

- zero or more `text_delta`
- `done`

On success, the result is a new assistant-message variant. Activation determines which variant becomes the canonical transcript entry.

### Formalize

- zero or more `text_delta`
- `done`

On success, the result is a planning draft and `current_draft_id` should point at it.

### Failure

- terminal `error`

### Cancellation

- terminal `cancelled`

## Stream Semantics

- planner streams send keepalive frames while an operation is still active
- planner stream handlers also perform stuck-operation checks while the stream is open
- terminal operation states should close the stream after catch-up replay

## Recovery Checklist

1. If the session still points at an active `current_operation_id`, fetch that operation directly.
2. Replay the event stream from the last known durable event id.
3. Inspect runtime planner metrics for recovery, replay, or failure growth.
4. If SSE failed in the browser, confirm the operation row eventually reaches a terminal status.
5. If transcript content is missing, inspect `GET /planning-sessions/{id}/messages`; do not trust client-local UI state.
6. If formalize completed but the plan panel is stale, inspect `GET /planning-sessions/{id}/plan` and `current_draft_id` on the session.

## Common Failure Patterns

### Stream opens then closes immediately

Check whether the operation is already terminal. A quick close after catch-up replay is expected for terminal operations.

### `404` from the operation stream

The operation id does not exist. Confirm the correct `current_operation_id` from the session record first.

### UI shows disconnected planner state

The web client may be in poll-fallback mode after SSE retry exhaustion. Inspect the operation row directly and then runtime metrics.

### Session points at an old terminal operation

Inspect runtime logs and planner metrics for a failed terminal reconciliation.
