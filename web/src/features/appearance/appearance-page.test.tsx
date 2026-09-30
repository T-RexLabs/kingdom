import type { ReactNode } from "react";
import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, expect, it, vi } from "vitest";
import { setCSRFToken } from "@/api/client";
import { ThemeContext, type ThemeContextValue } from "@/theme/context";
import { defaultTheme } from "@/theme";
import { themePackage } from "@/theme/contract";
import { AppearancePage } from "./appearance-page";
import { themeAPI } from "./theme-api";

vi.mock("@tanstack/react-router", () => ({ Link: ({ children, to, className }: { children: ReactNode; to: string; className?: string }) => <a href={to} className={className}>{children}</a> }));
const digest = "b".repeat(64);
let index = 0;
function setup() {
  const context: ThemeContextValue = { catalog: { workspaceID: `appearance-${++index}`, sources: ["custom"], items: [{ id: "custom", name: "Custom Theme", version: "1.0.0", builtin: false, resolvedDigest: digest }] }, selected: null, activePackage: null, theme: defaultTheme, assetURLs: {}, isPreviewing: false, loading: false, error: null, refreshCatalog: vi.fn(async () => {}), selectTheme: vi.fn(async () => true), previewTheme: vi.fn(), cancelPreview: vi.fn() };
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  render(<QueryClientProvider client={client}><ThemeContext.Provider value={context}><AppearancePage /></ThemeContext.Provider></QueryClientProvider>);
  return context;
}
beforeEach(() => {
  sessionStorage.clear(); vi.restoreAllMocks(); setCSRFToken("csrf", "test-session");
  vi.spyOn(themeAPI, "revision").mockResolvedValue(themePackage({ manifest: { ...structuredClone(defaultTheme), id: "custom", name: "Custom Theme" }, sourceDigest: "a".repeat(64), resolvedDigest: digest }));
  vi.spyOn(themeAPI, "mutate").mockResolvedValue(undefined);
});
it("selects a personal pinned revision without modifying the shared package", async () => {
  const context = setup();
  fireEvent.click(screen.getByRole("button", { name: "Choose Theme" }));
  await waitFor(() => expect(context.selectTheme).toHaveBeenCalledWith({ id: "custom", name: "Custom Theme", version: "1.0.0", builtin: false, resolvedDigest: digest }));
  expect(themeAPI.mutate).not.toHaveBeenCalled();
});
it("unpublishes explicitly without removing editable files", async () => {
  setup();
  fireEvent.click(screen.getByRole("button", { name: "Unpublish Custom Theme" }));
  const dialog = screen.getByRole("dialog", { name: "Unpublish Theme" });
  expect(dialog).toHaveTextContent("editable files and version history remain preserved");
  fireEvent.click(within(dialog).getByRole("button", { name: "Unpublish" }));
  await waitFor(() => expect(themeAPI.mutate).toHaveBeenCalledWith({ method: "DELETE", path: "/themes/custom", body: undefined }, expect.stringMatching(/^kd1\./)));
});
it("deletes source files with their last read digest without unpublishing revisions", async () => {
  vi.spyOn(themeAPI, "source").mockResolvedValue({ source: "invalid JSON is still deletable", sourceDigest: "c".repeat(64), assets: {}, diagnostics: [] });
  setup();
  fireEvent.click(screen.getByRole("button", { name: "Delete editable source custom" }));
  const dialog = await screen.findByRole("dialog", { name: "Delete Editable Source" });
  expect(dialog).toHaveTextContent("Published revisions remain available");
  fireEvent.click(within(dialog).getByRole("button", { name: "Delete Source" }));
  await waitFor(() => expect(themeAPI.mutate).toHaveBeenCalledWith({ method: "DELETE", path: "/themes/custom/source", body: { expected_source_digest: "c".repeat(64) } }, expect.stringMatching(/^kd1\./)));
});
it("validates the exact archive privately before retaining its revision identity for import", async () => {
  const pkg = themePackage({ manifest: { ...structuredClone(defaultTheme), id: "custom", name: "Custom Theme" }, sourceDigest: "a".repeat(64), resolvedDigest: digest });
  const validate = vi.spyOn(themeAPI, "validateArchive").mockResolvedValue(pkg);
  const context = setup();
  fireEvent.change(screen.getByLabelText("Import theme package"), { target: { files: [new File(["PK"], "theme.zip", { type: "application/zip" })] } });
  await waitFor(() => expect(themeAPI.mutate).toHaveBeenCalledWith({ method: "POST", path: "/themes/import", body: { archive: "UEs=" }, verify: { id: "custom", resolvedDigest: digest } }, expect.stringMatching(/^kd1\./)));
  expect(validate).toHaveBeenCalledWith("UEs=");
  expect(context.selectTheme).not.toHaveBeenCalled();
});
