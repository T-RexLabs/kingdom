import { cleanup, render, screen } from "@testing-library/react";
import { afterEach, expect, it } from "vitest";
import { ProviderModelCatalogNotice } from "./provider-model-catalog-state";
import { resolveProviderModelCatalogState } from "./use-provider-model-catalog";

afterEach(() => { cleanup(); document.head.querySelector('meta[name="kingdom-runtime-mode"]')?.remove(); });
it("links hosted model failures to connection refresh", () => {
  const meta = document.createElement("meta");
  meta.name = "kingdom-runtime-mode";
  meta.content = "hosted-project";
  document.head.append(meta);
  render(<ProviderModelCatalogNotice state={resolveProviderModelCatalogState(undefined, new Error("Unavailable"), false)} />);
  expect(screen.getByRole("link", { name: "Manage Model Connections" })).toHaveAttribute("href", "/#connections-heading");
});
it("keeps local model failures free of central navigation", () => {
  render(<ProviderModelCatalogNotice state={resolveProviderModelCatalogState(undefined, new Error("Unavailable"), false)} />);
  expect(screen.queryByRole("link", { name: "Manage Model Connections" })).not.toBeInTheDocument();
});
