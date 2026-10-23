import { runtimeMode } from "@/lib/runtime-mode";
import type { ProviderModel } from "@/api/hooks";

export function findProviderModel(models: ProviderModel[], modelId: string): ProviderModel | undefined {
  const trimmedModelID = modelId.trim();
  if (trimmedModelID === "") {
    return undefined;
  }
  return models.find((model) => model.id === trimmedModelID);
}

export function modelDisplayLabel(model: ProviderModel): string {
  return `${model.provider_name} - ${model.model_name}`;
}

export function selectedProviderModelLabel(models: ProviderModel[], modelId: string, placeholder: string): string {
  const trimmedModelID = modelId.trim();
  if (trimmedModelID === "") {
    return placeholder;
  }
  const selected = findProviderModel(models, trimmedModelID);
  return selected ? modelDisplayLabel(selected) : runtimeMode() === "hosted-project" ? "Unavailable model connection" : trimmedModelID;
}

export function normalizeThinkingEffortSelection(
  model: ProviderModel | undefined,
  thinkingEffort: string,
  options: { preserveWhenModelMissing?: boolean } = {},
): string {
  const current = thinkingEffort.trim();
  if (!model) {
    return options.preserveWhenModelMissing ? current : "";
  }
  const available = model.thinking_effort_options.map((option) => option.value.trim()).filter(Boolean);
  if (available.length === 0) {
    return "";
  }
  if (available.includes(current)) {
    return current;
  }
  return model.thinking_effort_default?.trim() || available[0] || "";
}

/** This key identifies UI state only. Never decode it into an API selection. */
export function modelSelectionKey(selection: { model: string; connection?: { id: string; revision: number } } | undefined): string {
  if (!selection) return "";
  return selection.connection ? JSON.stringify([selection.connection.id, selection.connection.revision, selection.model]) : selection.model;
}

export function selectedModelPayload(models: ProviderModel[], key: string, effort: string): { model: string; thinking_effort?: string; connection?: { id: string; revision: number } } | undefined {
  if (!key.trim()) return undefined;
  const option = findProviderModel(models, key);
  if (runtimeMode() === "hosted-project" && (!option?.connection || !option.canonical_model)) return undefined;
  return {
    model: option?.canonical_model ?? key.trim(),
    ...(effort.trim() ? { thinking_effort: effort.trim() } : {}),
    ...(option?.connection ? { connection: { ...option.connection } } : {}),
  };
}
