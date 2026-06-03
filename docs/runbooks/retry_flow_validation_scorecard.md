# Retry Flow Validation Scorecard

Use this scorecard to validate empty-output retry behavior end-to-end after runtime or UI changes.

## Metadata

- Date (UTC):
- Engineer:
- Branch:
- Commit SHA:
- Workspace path:
- Run ID:

## Scope

- Runtime retry policy: `2 retries` (`same_session` then `fresh_session`)
- Failure context hygiene for builder instructions
- Mailbox verdict pill scoping
- Effective run-agent status behavior in UI/API

## Preflight

- [ ] `go test ./internal/orchestration -run "TestShouldRetryEmptyOutputUsesOutputAndArtifactsOnly|TestCompleteExecutionRetriesDelegatedExecutionEmptyOutputWithBackoffEvents|TestCompleteExecutionFailsDelegatedExecutionAfterTwoEmptyRetries|TestLatestExecutionFailureContextsForRunUsesCompactEmptyResponseMarker"`
- [ ] `go test ./internal/state -run "TestAgentRegistryListRunAgentsDerivesBusyStatusFromRunningNodes"`
- [ ] `go test ./internal/api -run "TestServerRunMailboxMessageEndpoints|TestServerRunMailboxMessagesReturnsLatestCommunicationWindow|TestServerRunMailboxMessagesDoesNotDropOlderUserFacingMessagesWhenNewerSystemRowsExist"`
- [ ] `npm --prefix web test -- mailbox-panel`

## Live Run Setup

- [ ] `make install`
- [ ] `kd init` in isolated workspace (`kd-sandbox/...`)
- [ ] `kd summon --detach`
- [ ] Decree created and run launched

## Evidence Captured

- [ ] `run status` JSON
- [ ] `graph` JSON
- [ ] `timeline` JSON
- [ ] `mailbox/messages` JSON
- [ ] `agents` JSON
- [ ] `.kingdom/logs/summon.log` excerpt (if anomalies)

Artifacts directory:

- `run-final.json`:
- `graph.json`:
- `timeline.json`:
- `mailbox.json`:
- `agents.json`:

## Pass/Fail Matrix

### A. Retry Semantics

- [ ] **PASS** / **FAIL**: Empty-output attempts emit `attempt.retrying`
- [ ] **PASS** / **FAIL**: Retry count does not exceed `2`
- [ ] **PASS** / **FAIL**: Recovery path emits `attempt.retry_recovered` when output resumes
- [ ] **PASS** / **FAIL**: Exhausted retries produce `reason_code=provider_empty_output`

Notes:

##

### B. Context Hygiene (Builder)

- [ ] **PASS** / **FAIL**: Builder failure context includes concise marker for empty upstream response
- [ ] **PASS** / **FAIL**: Builder failure context excludes repair prose (forbidden strings)

Forbidden strings:

- `Original response was empty`
- `Formatting is now repaired`
- `required control verdict line`

Notes:

##

### C. Verdict Pill Scoping

- [ ] **PASS** / **FAIL**: Review/validation lanes show verdict pills when applicable
- [ ] **PASS** / **FAIL**: Builder lane never shows `Validation:*` or `Review:*` pill from free text

Notes:

##

### D. Agent Status UX

- [ ] **PASS** / **FAIL**: Agent with active running node appears effectively busy
- [ ] **PASS** / **FAIL**: No stale blocked-only projection while node is active

Notes:

##

## Decision

- Overall result: **PASS** / **FAIL**
- Regression severity: `none` / `low` / `medium` / `high`
- Follow-up issue(s):

## Command Log (optional)

```bash
# Paste the exact commands used for reproducibility.
```
