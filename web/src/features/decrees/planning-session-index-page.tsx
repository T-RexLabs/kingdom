import { Card, CardContent } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { Link, useNavigate } from "@tanstack/react-router";
import { ArrowRight, ExternalLink, Scroll, Sparkles } from "lucide-react";
import { useMemo, useState } from "react";
import { ModelSelectField } from "./model-select-field";
import { ThinkingEffortField } from "./thinking-effort-field";
import { normalizeThinkingEffortSelection } from "./provider-model-selection";
import { useCreatePlanningSessionNavigation } from "./use-create-planning-session-navigation";
import { ProviderModelCatalogNotice } from "./provider-model-catalog-state";
import { useProviderModelCatalogController } from "./use-provider-model-catalog";
import { IssuePicker, type IssueSelection } from "@/features/connections/issue-picker";
import { safeGitHubIssueURL } from "@/features/connections/github-url";

const preloadPlanningSessionPage = () => import("@/features/decrees/planning-session-page");

export function PlanningSessionIndexPage() {
  const [model, setModel] = useState("");
  const [thinkingEffort, setThinkingEffort] = useState("");
  const [sourceIssue,setSourceIssue]=useState<IssueSelection>();
	const providerModelCatalogController = useProviderModelCatalogController();
	const providerModelCatalog = providerModelCatalogController.state;
  const createPlanningSession = useCreatePlanningSessionNavigation();
  const navigate = useNavigate();
  const selectedModel = useMemo(
    () => providerModelCatalog.items.find((item) => item.id === model),
    [model, providerModelCatalog.items]
  );
  const normalizedThinkingEffort = model.trim()
    ? normalizeThinkingEffortSelection(selectedModel, thinkingEffort, { preserveWhenModelMissing: true })
    : thinkingEffort;
  const recoveryPending = createPlanningSession.hasRecovery;
	const currentRequest = useMemo(() => (
		({
			...(sourceIssue ? { title:sourceIssue.issue.title, external_intake:sourceIssue.intake } : {}),
			...(model.trim() ? {
				model_selection: {
					model: model.trim(),
					...(normalizedThinkingEffort.trim() ? { thinking_effort: normalizedThinkingEffort.trim() } : {}),
				},
			} : {}),
		})
	), [model, normalizedThinkingEffort, sourceIssue]);
  const sourceURL = sourceIssue ? safeGitHubIssueURL(sourceIssue.issue.canonical_url, sourceIssue.issue.repository, sourceIssue.issue.issue_number) : null;

	const handleCreateSession = async () => {
		try {
			void preloadPlanningSessionPage();
			const session = await createPlanningSession.mutateAsync(currentRequest);
		setModel("");
		setThinkingEffort("");
		setSourceIssue(undefined);
		await navigate({
        to: "/decrees/planning/$sessionId",
        params: { sessionId: session.planning_session_id },
      });
		} catch {
			// surfaced through the inline error state
		}
	};

	const handleRetryRecovery = async () => {
		try {
			void preloadPlanningSessionPage();
			const session = await createPlanningSession.retryRecovery();
			setModel("");
			setThinkingEffort("");
			setSourceIssue(undefined);
			await navigate({
				to: "/decrees/planning/$sessionId",
				params: { sessionId: session.planning_session_id },
			});
		} catch {
			// surfaced through the inline error state
		}
	};

  return (
    <Card className="h-full border-border/80">
      <CardContent className="p-6 sm:p-8">
        <div className="space-y-6">
          <div className="space-y-2">
            <h2 className="font-display text-xl font-semibold tracking-tight text-foreground sm:text-2xl">
              Think it through with the planner.
            </h2>
            <p className="max-w-2xl text-sm leading-6 text-foreground-muted">
              Start a planner session to explore the topic, ground it in research, or shape a
              plan — the conversation follows your lead. When you're ready, generate the formal
              plan and issue the decree from it.
            </p>
          </div>

          <div className="rounded-[1.25rem] border border-border/80 bg-black/20 p-4 sm:p-5">
            <div className="mb-4 space-y-3">
              <IssuePicker actionLabel="Link to planning session" onSelect={setSourceIssue} />
              {sourceIssue ? <div aria-label="Selected GitHub source" className="flex min-w-0 flex-col gap-2 rounded-md border border-gold/20 bg-gold/10 px-3 py-2 text-xs sm:flex-row sm:items-center sm:justify-between"><div className="min-w-0"><p className="break-words font-medium text-foreground">{sourceURL ? <a href={sourceURL} target="_blank" rel="noopener noreferrer" className="inline-flex items-center gap-1 text-gold underline">{sourceIssue.issue.repository}#{sourceIssue.issue.issue_number}<ExternalLink className="h-3 w-3" /></a> : <span>{sourceIssue.issue.repository}#{sourceIssue.issue.issue_number}</span>}</p><p className="mt-0.5 break-words text-foreground-muted">{sourceIssue.issue.title}</p></div><button type="button" disabled={recoveryPending} className="self-start text-gold disabled:opacity-50 sm:self-auto" onClick={() => setSourceIssue(undefined)}>Remove source</button></div> : null}
            </div>
            <div className="flex flex-wrap items-end gap-3">
              <div className="w-full">
                <ProviderModelCatalogNotice
                  state={providerModelCatalog}
					onRetry={providerModelCatalogController.retry}
					retryPending={providerModelCatalogController.retryPending}
                />
              </div>
              <div className="w-full sm:w-[280px]">
                <ModelSelectField
                  id="planning-session-model"
                  label="Planner Model"
                  value={model}
					loading={providerModelCatalog.isLoading}
                  disabled={providerModelCatalog.disableSelection || recoveryPending}
                  options={providerModelCatalog.items}
                  triggerPlaceholder={providerModelCatalog.triggerPlaceholder}
                  emptyStateLabel={providerModelCatalog.emptyStateLabel}
                  onChange={(next) => {
                    setModel(next);
                    const nextModel = providerModelCatalog.items.find((item) => item.id === next);
                    setThinkingEffort(
                      normalizeThinkingEffortSelection(nextModel, "", { preserveWhenModelMissing: true })
                    );
                  }}
                />
              </div>
              <div className="w-full sm:w-[240px]">
                <ThinkingEffortField
                  id="planning-session-thinking-effort"
                  label="Thinking Effort"
                  value={normalizedThinkingEffort}
                  model={selectedModel}
                  disabled={providerModelCatalog.disableSelection || recoveryPending}
                  onChange={setThinkingEffort}
                />
              </div>
            </div>

			{createPlanningSession.error ? (
				recoveryPending ? (
					<div className="mt-3 rounded-md border border-status-failed/30 bg-status-failed/10 p-3 text-xs text-status-failed">
						<p>Kingdom may have accepted this planner request. Retry sends the exact same request and idempotency key so recovery cannot create a duplicate session.</p>
						<p className="mt-1 text-status-failed/90">{createPlanningSession.error.message}</p>
						<div className="mt-3 flex flex-wrap gap-2">
							<Button type="button" size="sm" onClick={() => void handleRetryRecovery()} disabled={createPlanningSession.isPending}>
								{createPlanningSession.isPending ? "Retrying..." : "Retry Planner Session"}
							</Button>
                  </div>
                </div>
              ) : (
                <p className="mt-3 text-xs text-status-failed">
                  {createPlanningSession.error.message}
                </p>
              )
            ) : null}

            <div className="mt-4 flex flex-wrap gap-3 border-t border-border/60 pt-4">
				<Button
					type="button"
					onClick={() => void handleCreateSession()}
					disabled={createPlanningSession.isPending || recoveryPending}
				>
					<Sparkles className="h-3.5 w-3.5" />
					{createPlanningSession.isPending ? "Starting..." : "Start Planner Session"}
				</Button>
              <Link to="/decrees/new" className={buttonVariants({ variant: "outline" })}>
                <Scroll className="h-3.5 w-3.5" />
                Issue Decree Directly
                <ArrowRight className="h-3.5 w-3.5" />
              </Link>
            </div>
          </div>
        </div>
      </CardContent>
    </Card>
  );
}
