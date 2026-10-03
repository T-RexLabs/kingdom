import { useState } from "react";
import type { RunExecutionDetail, RunExecution } from "@/api/hooks";
import { ApiError } from "@/api/client";
import { useRunExecutionInstructions } from "@/api/hooks-execution-instructions";
import { hasSettledResponse } from "./run-conversation-state";
import { MarkdownBlock } from "@/components/ui/markdown-block";
import { isActiveExecutionStatus, isQueuedExecutionStatus } from "./run-status-semantics";

export function RunFinalResponse({
  detail,
}: {
  detail?: RunExecutionDetail | null;
}) {
  const [sourceOpen, setSourceOpen] = useState(false);
  if (!hasSettledResponse(detail)) return null;
  return (
    <section aria-label="Final response" className="group/response py-5">
      <MarkdownBlock
        content={detail.output_text}
        className="text-[15px] leading-7 text-foreground"
      />
      <details
        open={sourceOpen}
        onToggle={(event) => {
          if (event.target === event.currentTarget)
            setSourceOpen(event.currentTarget.open);
        }}
        className="mt-1 text-[11px] text-foreground-subtle"
      >
        <summary className="w-fit cursor-pointer py-1 opacity-60 hover:opacity-100 focus-visible:opacity-100 group-hover/response:opacity-100">
          Response Details
        </summary>
        {sourceOpen && (
          <>
            <p className="mb-2">
              This response was recorded by Kingdom settlement. Run approval and
              publication are separate.
            </p>
            <p className="mb-2 break-all">Attempt ID: {detail.attempt_id}</p>
            <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-xs">
              {detail.output_text}
            </pre>
          </>
        )}
      </details>
    </section>
  );
}

export function RunRuntimeResponse({
  detail,
}: {
  detail?: RunExecutionDetail | null;
}) {
  if (
    detail?.execution_mode !== "runtime_owned" ||
    detail.output_status !== "observed" ||
    !detail.output_text
  )
    return null;
  return (
    <section aria-label="Recorded runtime output" className="space-y-3 py-5">
      <MarkdownBlock
        content={detail.output_text}
        className="text-[15px] leading-7 text-foreground"
      />
      <details className="text-xs text-foreground-muted">
        <summary className="w-fit cursor-pointer py-2">
          Response Details
        </summary>
        <p className="mb-2">Recorded output from this runtime-owned step.</p>
        <pre
          aria-label="Exact recorded runtime output"
          className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-xs"
        >
          {detail.output_text}
        </pre>
      </details>
    </section>
  );
}

export function RunInstructions({
  runId,
  attemptId,
  executionMode,
  status,
}: {
  runId: string;
  attemptId: string | null;
  executionMode?: RunExecution["execution_mode"];
  status?: RunExecution["status"];
}) {
  const [sourceOpen, setSourceOpen] = useState(false);
  const runtimeOwned = executionMode === "runtime_owned";
  const pendingDispatch = isQueuedExecutionStatus(status) || isActiveExecutionStatus(status);
  const query = useRunExecutionInstructions(
    runId,
    attemptId,
    executionMode === "provider_backed",
    pendingDispatch,
  );
  const notRecorded =
    query.error instanceof ApiError && query.error.status === 404;
  return (
    <section aria-label="Attempt instructions" className="space-y-4 py-5">
      {runtimeOwned ? (
        <p className="text-sm text-foreground-muted">
          Kingdom handles this step without a provider prompt.
        </p>
      ) : !attemptId ? (
        <p className="text-sm text-foreground-muted">
          This step has not recorded an attempt yet.
        </p>
      ) : query.isLoading ? (
        <p className="text-sm text-foreground-muted">Loading instructions…</p>
      ) : notRecorded ? (
        <p className="text-sm text-foreground-muted">
          {pendingDispatch
            ? "Provider instructions will appear when this attempt is dispatched."
            : "No provider instructions were recorded for this attempt."}
        </p>
      ) : query.error ? (
        <p role="alert" className="text-sm text-status-failed">
          Instructions could not be loaded.{" "}
          <button
            type="button"
            className="underline"
            onClick={() => void query.refetch()}
          >
            Retry Instructions
          </button>
        </p>
      ) : query.data !== undefined ? (
        <>
          <p className="text-sm text-foreground-muted">
            Captured system and user instructions from Kingdom. OpenCode also adds environment and tool context.
          </p>
          <MarkdownBlock
            content={query.data}
            className="text-[15px] leading-7 text-foreground"
          />
          <details
            open={sourceOpen}
            onToggle={(event) => {
              if (event.target === event.currentTarget)
                setSourceOpen(event.currentTarget.open);
            }}
            className="text-xs text-foreground-muted"
          >
            <summary className="w-fit cursor-pointer py-2">
              Instruction Details
            </summary>
            {sourceOpen && (
              <pre
                aria-label="Exact attempt instructions"
                className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-xs"
              >
                {query.data}
              </pre>
            )}
          </details>
        </>
      ) : null}
    </section>
  );
}
