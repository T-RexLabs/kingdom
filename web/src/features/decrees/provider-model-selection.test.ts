import { describe, expect, it } from "vitest";
import { normalizeThinkingEffortSelection, selectedProviderModelLabel } from "./provider-model-selection";

const model = {
  id: "github-copilot/gpt-5.4",
  provider_id: "github-copilot",
  provider_name: "GitHub Copilot",
  model_name: "GPT-5.4",
  thinking_effort_default: "medium",
  thinking_effort_options: [
    { value: "low", label: "Low" },
    { value: "medium", label: "Medium" },
    { value: "high", label: "High" },
  ],
};

describe("provider model selection", () => {
  it("falls back to the raw model id when the selected model is missing from the catalog", () => {
    expect(selectedProviderModelLabel([], "github-copilot/gpt-5.4", "Select model")).toBe("github-copilot/gpt-5.4");
  });

  it("preserves thinking effort when the model is temporarily missing", () => {
    expect(normalizeThinkingEffortSelection(undefined, "medium", { preserveWhenModelMissing: true })).toBe("medium");
  });

  it("normalizes thinking effort when authoritative model options are available", () => {
    expect(normalizeThinkingEffortSelection(model, "unknown", { preserveWhenModelMissing: true })).toBe("medium");
  });
});
