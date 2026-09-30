import { useWorkOrder, type RunExecutionDetail } from "@/api/hooks";
import { hasSettledResponse } from "./run-conversation-state";
import { MarkdownBlock } from "@/components/ui/markdown-block";

export type RunResponseDisclosure = { response: boolean; source: boolean };

export function RunFinalResponse({
  detail,
  repeated = false,
  disclosure,
  onDisclosureChange,
}: {
  detail?: RunExecutionDetail | null;
  repeated?: boolean;
  disclosure: RunResponseDisclosure;
  onDisclosureChange: (value: RunResponseDisclosure) => void;
}) {
  const { response: expanded, source: sourceOpen } = disclosure;
  if (!hasSettledResponse(detail)) return null;
  const response = (
    <MarkdownBlock
      content={detail.output_text}
      className="text-[15px] leading-7 text-foreground"
    />
  );
  return (
    <section aria-label="Final response" className="group/response py-5">
      {repeated ? (
        <details
          open={expanded}
          onToggle={(event) => {
            if (event.target === event.currentTarget)
              onDisclosureChange({
                ...disclosure,
                response: event.currentTarget.open,
              });
          }}
        >
          <summary className="cursor-pointer py-2 text-xs font-medium text-foreground-muted">
            Final Response
          </summary>
          {expanded && (
            <div className="space-y-3">
              <p className="text-xs text-foreground-muted">
                The response above matches the canonical settled response for
                this attempt.
              </p>
              {response}
            </div>
          )}
        </details>
      ) : (
        <>
          <h3 className="mb-3 text-xs font-medium text-foreground-muted">
            Final Response
          </h3>
          {response}
        </>
      )}
      {(!repeated || expanded) && (
        <details
          open={sourceOpen}
          onToggle={(event) => {
            if (event.target === event.currentTarget)
              onDisclosureChange({
                ...disclosure,
                source: event.currentTarget.open,
              });
          }}
          className="mt-1 text-[11px] text-foreground-subtle"
        >
          <summary className="w-fit cursor-pointer py-1 opacity-60 hover:opacity-100 focus-visible:opacity-100 group-hover/response:opacity-100">
            Response Details
          </summary>
          {sourceOpen && (
            <>
              <p className="mb-2">
                This response was recorded by Kingdom settlement. Run approval
                and publication are separate.
              </p>
              <p className="mb-2 break-all">Attempt ID: {detail.attempt_id}</p>
              <pre className="max-h-80 overflow-auto whitespace-pre-wrap break-words font-mono text-xs">
                {detail.output_text}
              </pre>
            </>
          )}
        </details>
      )}
    </section>
  );
}

export function RunInstructions({ workOrderId }: { workOrderId: string }) {
  const query = useWorkOrder(workOrderId);
  return (
    <section aria-label="Issued instructions" className="space-y-4 py-5">
      <p className="text-xs text-foreground-muted">
        The issued work brief for this run.
      </p>
      {query.isLoading ? (
        <p className="text-sm text-foreground-muted">Loading instructions…</p>
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
      ) : query.data?.work_order_text ? (
        <MarkdownBlock
          content={query.data.work_order_text}
          className="text-[15px] leading-7 text-foreground"
        />
      ) : (
        <p className="text-sm text-foreground-muted">
          No issued instructions are available.
        </p>
      )}
    </section>
  );
}
