import { Card, CardContent } from "@/components/ui/card";
import { Button, buttonVariants } from "@/components/ui/button";
import { Link, useNavigate } from "@tanstack/react-router";
import { ArrowRight, Scroll, Sparkles } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { ModelSelectField } from "./model-select-field";
import { ThinkingEffortField } from "./thinking-effort-field";
import { normalizeThinkingEffortSelection } from "./provider-model-selection";
import { useCreatePlanningSessionNavigation } from "./use-create-planning-session-navigation";
import { ProviderModelCatalogNotice, useProviderModelCatalogController } from "./provider-model-catalog-state";

const preloadPlanningSessionPage = () => import("@/features/decrees/planning-session-page");

export function PlanningSessionIndexPage() {
  const [model, setModel] = useState("");
  const [thinkingEffort, setThinkingEffort] = useState("");
	const providerModelCatalogController = useProviderModelCatalogController();
	const providerModelCatalog = providerModelCatalogController.state;
  const createPlanningSession = useCreatePlanningSessionNavigation();
  const navigate = useNavigate();
  const selectedModel = useMemo(
    () => providerModelCatalog.items.find((item) => item.id === model),
    [model, providerModelCatalog.items]
  );

  useEffect(() => {
    if (!model.trim()) return;
    setThinkingEffort((current) =>
      normalizeThinkingEffortSelection(selectedModel, current, { preserveWhenModelMissing: true })
    );
  }, [model, selectedModel]);

  const handleCreateSession = async () => {
    try {
      void preloadPlanningSessionPage();
      const session = await createPlanningSession.mutateAsync({
        ...(model.trim()
          ? {
              model_selection: {
                model: model.trim(),
                ...(thinkingEffort.trim() ? { thinking_effort: thinkingEffort.trim() } : {}),
              },
            }
          : {}),
      });
      setModel("");
      setThinkingEffort("");
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
            <div className="flex flex-wrap items-end gap-3">
              <div className="w-full">
                <ProviderModelCatalogNotice
                  state={providerModelCatalog}
					onRetry={providerModelCatalogController.retry}
					retryPending={providerModelCatalogController.retryPending}
                />
              </div>
              <div className="w-[280px]">
                <ModelSelectField
                  id="planning-session-model"
                  label="Planner Model"
                  value={model}
					loading={providerModelCatalog.isLoading}
                  disabled={providerModelCatalog.disableSelection}
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
              <div className="w-[240px]">
                <ThinkingEffortField
                  id="planning-session-thinking-effort"
                  label="Thinking Effort"
                  value={thinkingEffort}
                  model={selectedModel}
                  disabled={providerModelCatalog.disableSelection}
                  onChange={setThinkingEffort}
                />
              </div>
            </div>

            {createPlanningSession.error ? (
              <p className="mt-3 text-xs text-status-failed">
                {createPlanningSession.error.message}
              </p>
            ) : null}

            <div className="mt-4 flex flex-wrap gap-3 border-t border-border/60 pt-4">
              <Button
                type="button"
                onClick={() => void handleCreateSession()}
                disabled={createPlanningSession.isPending}
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
