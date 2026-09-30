import { useWorkspaceCapabilities } from "@/api/hooks";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { Link, useNavigate } from "@tanstack/react-router";
import { ArrowRight } from "lucide-react";
import { useThemeCopy, ThemeArtwork } from "@/theme";
import { useEffect, useMemo, useRef, useState } from "react";
import { ModelSelectField } from "./model-select-field";
import { ThinkingEffortField } from "./thinking-effort-field";
import { normalizeThinkingEffortSelection } from "./provider-model-selection";
import { useCreatePlanningSessionNavigation } from "./use-create-planning-session-navigation";
import { ProviderModelCatalogNotice } from "./provider-model-catalog-state";
import { useProviderModelCatalogController } from "./use-provider-model-catalog";

const preloadPlanningSessionPage = () => import("@/features/work-orders/planning-session-page");

export function PlanningSessionIndexPage() {
  const mounted = useRef(true);
  useEffect(() => { mounted.current = true; return () => { mounted.current = false; }; }, []);
  const emptyTitle = useThemeCopy("planning.emptyTitle");
  const emptyDescription = useThemeCopy("planning.emptyDescription");
  const workName = useThemeCopy("workOrder.singular");
  const [title, setTitle] = useState("");
  const [model, setModel] = useState("");
  const [thinkingEffort, setThinkingEffort] = useState("");
  const providerModelCatalogController = useProviderModelCatalogController();
  const providerModelCatalog = providerModelCatalogController.state;
  const workspace = useWorkspaceCapabilities();
  const createPlanningSession = useCreatePlanningSessionNavigation(workspace.data?.project_root ?? "__pending__");
  const navigate = useNavigate();
  const selectedModel = useMemo(
    () => providerModelCatalog.items.find((item) => item.id === model),
    [model, providerModelCatalog.items]
  );
  const normalizedThinkingEffort = model.trim()
    ? normalizeThinkingEffortSelection(selectedModel, thinkingEffort, { preserveWhenModelMissing: true })
    : thinkingEffort;
  const recoveryPending = createPlanningSession.hasRecovery;
  const currentRequest = useMemo(
    () =>
      model.trim()
        ? {
            ...(title.trim() ? { title: title.trim() } : {}),
            model_selection: {
              model: model.trim(),
              ...(normalizedThinkingEffort.trim()
                ? { thinking_effort: normalizedThinkingEffort.trim() }
                : {}),
            },
          }
        : title.trim() ? { title: title.trim() } : {},
    [title, model, normalizedThinkingEffort]
  );

  const startSession = async (create: () => Promise<{ planning_session_id: string }>) => {
    try {
      void preloadPlanningSessionPage();
      const session = await create();
      if (!mounted.current) return;
      setTitle("");
      setModel("");
      setThinkingEffort("");
      await navigate({
        to: "/work-orders/planning/$sessionId",
        params: { sessionId: session.planning_session_id },
      });
    } catch {
      // surfaced through the inline error state
    }
  };

  return (
    <div className="flex h-full min-h-0 overflow-y-auto px-6 py-12 sm:px-10">
      <div className="mx-auto my-auto w-full max-w-2xl py-8">
        <div className="space-y-8">
          <div className="space-y-4">
            <ThemeArtwork slot="planningEmpty" className="h-32 w-full rounded-xl object-cover sm:h-40" />
            <h2 className="font-display text-4xl leading-tight sm:text-5xl">{emptyTitle}</h2>
            <p className="max-w-xl text-sm leading-7 text-foreground-muted">{emptyDescription}</p>
          </div>

          <div className="space-y-5 rounded-2xl border border-border bg-surface p-5 sm:p-6">
            <div><label htmlFor="new-session-title" className="mb-2 block text-sm font-medium">Session Title <span className="font-normal text-foreground-muted">(Optional)</span></label><input data-ui-field="" id="new-session-title" value={title} disabled={recoveryPending} onChange={(event) => setTitle(event.target.value)} placeholder="What would you like to explore?" className="w-full rounded-lg border border-border bg-background px-3 py-2.5 text-sm outline-none focus:border-accent focus:ring-1 focus:ring-accent" /></div>
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

            {createPlanningSession.error || recoveryPending ? (
              recoveryPending ? (
                <div className="mt-3 rounded-md border border-status-failed/30 bg-status-failed/10 p-3 text-xs text-status-failed">
                  <p>A previous request may already have created a session. Resolve its original outcome before starting another.</p>
                  <p className="mt-1 text-status-failed/90">{createPlanningSession.error?.message}</p>
                  <div className="mt-3 flex flex-wrap gap-2">
                    <Button type="button" size="sm" onClick={() => void startSession(() => createPlanningSession.retryRecovery())} disabled={createPlanningSession.isPending}>
                      {createPlanningSession.isPending ? "Retrying..." : "Retry Planner Session"}
                    </Button>
                  </div>
                </div>
              ) : (
                <p className="mt-3 text-xs text-status-failed">
                  {createPlanningSession.error?.message}
                </p>
              )
            ) : null}

            {workspace.error && <p role="alert" className="text-xs text-status-failed">Project settings could not be loaded. <button className="underline" onClick={() => void workspace.refetch()}>Retry</button></p>}
            <div className="mt-4 flex flex-wrap gap-3 border-t border-border/60 pt-4">
              <Button
                type="button"
                onClick={() => void startSession(() => createPlanningSession.mutateAsync(currentRequest))}
                disabled={!workspace.data || createPlanningSession.isPending || recoveryPending}
              >
                {createPlanningSession.isPending ? "Starting..." : "Start Planner Session"}
              </Button>
              <Link to="/work-orders/new" className={buttonVariants({ variant: "outline" })}>
                Create {workName} Directly
                <ArrowRight className="h-3.5 w-3.5" />
              </Link>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
