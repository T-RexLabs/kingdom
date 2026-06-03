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
  return selected ? modelDisplayLabel(selected) : trimmedModelID;
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
