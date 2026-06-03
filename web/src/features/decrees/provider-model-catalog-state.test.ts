import { describe, expect, it } from "vitest";
import { ApiError } from "@/api/client";
import { resolveProviderModelCatalogState } from "./provider-model-catalog-state";

function makeApiError(message: string) {
  return new ApiError(503, {
    message,
    code: "internal_error",
    retryable: true,
    category: "core",
    details: {},
    correlationId: "api.test",
  });
}

describe("resolveProviderModelCatalogState", () => {
  it("keeps cached models selectable when a refetch error arrives", () => {
    const state = resolveProviderModelCatalogState(
      {
        catalog_status: "fresh",
        items: [
          {
            id: "github-copilot/gpt-5.4",
            provider_id: "github-copilot",
            provider_name: "GitHub Copilot",
            model_name: "GPT-5.4",
            thinking_effort_options: [{ value: "medium", label: "Medium" }],
          },
        ],
      },
      makeApiError("provider model catalog refresh failed"),
      false,
    );

    expect(state.status).toBe("stale");
    expect(state.items).toHaveLength(1);
    expect(state.disableSelection).toBe(false);
    expect(state.message).toContain("refresh failed");
  });

	it("stays unavailable when a refetch error arrives without cached models", () => {
		const state = resolveProviderModelCatalogState(
			{
				catalog_status: "fresh",
        items: [],
      },
      makeApiError("provider model catalog refresh failed"),
      false,
    );

		expect(state.status).toBe("unavailable");
		expect(state.items).toHaveLength(0);
		expect(state.disableSelection).toBe(true);
	});

	it("prefers explicit retry errors over stale response copy", () => {
		const state = resolveProviderModelCatalogState(
			{
				catalog_status: "stale",
				items: [
					{
						id: "github-copilot/gpt-5.4",
						provider_id: "github-copilot",
						provider_name: "GitHub Copilot",
						model_name: "GPT-5.4",
						thinking_effort_options: [{ value: "medium", label: "Medium" }],
					},
				],
				failure_reason: "command_failed",
				failure_message: "showing cached models",
			},
			makeApiError("live refresh failed"),
			false,
		);

		expect(state.status).toBe("stale");
		expect(state.message).toBe("live refresh failed");
		expect(state.disableSelection).toBe(false);
	});
});
