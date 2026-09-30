import type { ReactNode } from "react";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { beforeEach, expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { ApiError, setCSRFToken } from "@/api/client";
import { ThemeContext, type ThemeContextValue } from "@/theme/context";
import { defaultTheme } from "@/theme";
import { themePackage } from "@/theme/contract";
import { CreateThemePage } from "./create-theme-page";
import { themeAPI, type ThemeSource } from "./theme-api";
import { getDraftStore, parseDraft } from "./draft-store";

const router = vi.hoisted(() => ({ search: {} as { source?: string; revision?: string; base?: string }, navigate: vi.fn() }));
vi.mock("@tanstack/react-router", () => ({ useSearch: () => router.search, useNavigate: () => router.navigate, Link: ({ children, to, className }: { children: ReactNode; to: string; className?: string }) => <a href={to} className={className}>{children}</a> }));
let index = 0;
function appearanceContext(): ThemeContextValue {
  return { catalog: { workspaceID: `editor-${++index}`, sources: [], items: [] }, selected: null, activePackage: null, theme: defaultTheme, assetURLs: {}, isPreviewing: false, loading: false, error: null, refreshCatalog: vi.fn(async () => {}), selectTheme: vi.fn(async () => true), previewTheme: vi.fn(), cancelPreview: vi.fn() };
}
function setup(context = appearanceContext()) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } });
  const tree = <QueryClientProvider client={client}><ThemeContext.Provider value={context}><CreateThemePage /></ThemeContext.Provider></QueryClientProvider>;
  return { context, tree, ...render(tree) };
}
function resolved(source: string) {
  const draft = parseDraft(source)!;
  return themePackage({ manifest: { ...structuredClone(defaultTheme), id: draft.id, name: draft.name, version: draft.version }, sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
}
beforeEach(() => {
  sessionStorage.clear(); vi.restoreAllMocks(); setCSRFToken("csrf", "test-session"); router.search = {}; router.navigate.mockReset();
  vi.spyOn(themeAPI, "validate").mockImplementation(async ({ source }) => resolved(source));
});
it("keeps private preview separate from saving, publishing and explicitly selecting a revision", async () => {
  const mutate = vi.spyOn(themeAPI, "mutate").mockImplementation(async (request) => {
    const body = request.body as { source: string; assets: Record<string, string> };
    return request.method === "PUT" ? { source: body.source, assets: body.assets, sourceDigest: "c".repeat(64), diagnostics: [] } : resolved(JSON.stringify({ id: "my-theme", version: "1.0.0", name: "Evening Workshop" }));
  });
  const view = setup();
  fireEvent.change(screen.getByLabelText("Theme Name"), { target: { value: "Evening Workshop" } });
  await waitFor(() => expect(screen.getByRole("button", { name: "Preview in App" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "Preview in App" }));
  expect(view.context.previewTheme).toHaveBeenCalledTimes(1);
  expect(mutate).not.toHaveBeenCalled();
  expect(view.context.selectTheme).not.toHaveBeenCalled();
  expect(screen.getByRole("button", { name: "Publish Revision" })).toBeDisabled();
  fireEvent.click(screen.getByRole("button", { name: "Save Source" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "Publish Revision" })).toBeEnabled(), { timeout: 2500 });
  expect(mutate.mock.calls[0][0]).toMatchObject({ method: "PUT", path: "/themes/my-theme/source", body: { expected_source_digest: "" } });
  expect(JSON.parse((mutate.mock.calls[0][0].body as { source: string }).source).name).toBe("Evening Workshop");
  fireEvent.click(screen.getByRole("button", { name: "Publish Revision" }));
  await screen.findByRole("button", { name: "Use This Revision" });
  expect(mutate.mock.calls[1][0]).toEqual({ method: "POST", path: "/themes/my-theme/publish", body: { expected_source_digest: "c".repeat(64) }, verify: { id: "my-theme", resolvedDigest: "b".repeat(64) } });
  expect(view.context.selectTheme).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Use This Revision" }));
  expect(view.context.selectTheme).toHaveBeenCalledWith({ id: "my-theme", resolvedDigest: "b".repeat(64) });
});
it("preserves an invalid draft across editor remount and supports keyboard section navigation", async () => {
  const view = setup();
  const colors = screen.getByRole("tab", { name: "Colors & Type" });
  fireEvent.keyDown(colors, { key: "End" });
  expect(screen.getByRole("tab", { name: "Theme File" })).toHaveFocus();
  fireEvent.change(screen.getByLabelText("Theme JSON"), { target: { value: '{"id":' } });
  expect(screen.getByRole("button", { name: "Preview in App" })).toBeDisabled();
  view.unmount();
  render(view.tree);
  expect(screen.getByLabelText("Theme JSON")).toHaveValue('{"id":');
  expect(screen.getByRole("button", { name: "Save Source" })).toBeDisabled();
});
it("keeps source conflicts reviewable without replacing private edits", async () => {
  const context = appearanceContext();
  const store = getDraftStore(context.catalog!.workspaceID);
  const initial = store.getSnapshot();
  store.replace({ source: initial.source, assets: {} }, "source:my-theme:editable", "my-theme", "d".repeat(64));
  vi.spyOn(themeAPI, "mutate").mockRejectedValue(new ApiError(409, { message: "Source changed", code: "dependency_blocked", category: "core", retryable: false, details: {}, correlationId: "api" }));
  setup(context);
  fireEvent.change(screen.getByLabelText("Theme Name"), { target: { value: "My Unsaved Name" } });
  await waitFor(() => expect(screen.getByRole("button", { name: "Save Source" })).toBeEnabled());
  fireEvent.click(screen.getByRole("button", { name: "Save Source" }));
  await screen.findByRole("button", { name: "Reload Shared Source" });
  expect(screen.getByLabelText("Theme Name")).toHaveValue("My Unsaved Name");
  expect(store.getSnapshot()).toMatchObject({ expectedDigest: "d".repeat(64), dirty: true });
  expect(screen.getByRole("button", { name: "Publish Revision" })).toBeDisabled();
});
it("loads revision authoring intent into an unsaved copy instead of editing the published identity", async () => {
  const source: ThemeSource = { source: JSON.stringify({ schemaVersion: 1, resolverVersion: 1, id: "shared-theme", version: "2.0.0", name: "Shared Theme", base: { id: defaultTheme.id, version: defaultTheme.version }, palette: { accent: "#ffffff" } }), sourceDigest: "c".repeat(64), assets: {}, diagnostics: [] };
  vi.spyOn(themeAPI, "source").mockResolvedValue(source);
  router.search = { source: "shared-theme", revision: "b".repeat(64) };
  const view = setup();
  await waitFor(() => expect(screen.getByLabelText(/Theme ID/)).toHaveValue("shared-theme-copy"));
  const draft = getDraftStore(view.context.catalog!.workspaceID).getSnapshot();
  expect(parseDraft(draft.source)).toMatchObject({ base: { id: defaultTheme.id }, palette: { accent: "#ffffff" }, version: "1.0.0" });
  expect(draft).toMatchObject({ sourceID: null, expectedDigest: "", dirty: true });
  expect(themeAPI.source).toHaveBeenCalledWith("shared-theme", "b".repeat(64), expect.any(AbortSignal));
});

it("exposes canonical starting themes and preserves font product spellings", async () => {
  const user = userEvent.setup();
  setup();
  expect(screen.getByRole("button", { name: "Start With" })).toHaveAccessibleDescription(defaultTheme.name);
  for (const slot of ["Body Font", "Display Font", "Mono Font"]) {
    await user.click(screen.getByRole("button", { name: slot }));
    expect(screen.getByRole("option", { name: "DM Sans" })).toBeInTheDocument();
    expect(screen.getByRole("option", { name: "JetBrains Mono" })).toBeInTheDocument();
    await user.keyboard("{Escape}");
  }
  await user.click(screen.getByRole("button", { name: "Start With" }));
  await user.click(screen.getByRole("option", { name: "Nautilus" }));
  expect(router.navigate).toHaveBeenCalledWith({ to: "/appearance/create", search: { base: "nautilus" } });
});

it("updates the preview and permits saving and app preview when contrast advice is returned", async () => {
  vi.spyOn(themeAPI, "validate").mockImplementation(async ({ source }) => {
    const pkg = resolved(source);
    const authored = parseDraft(source)!;
    return themePackage({ ...pkg, manifest: { ...pkg.manifest, typography: { ...pkg.manifest.typography, ...authored.typography } }, warnings: [{ path: "/tokens/text.primary", code: "contrast", message: "Low contrast is advisory." }] });
  });
  const view = setup();
  fireEvent.change(screen.getByRole("spinbutton", { name: /Heading Weight/ }), { target: { value: "925.5" } });
  await waitFor(() => expect(screen.getByRole("button", { name: "Preview in App" })).toBeEnabled());
  expect(screen.getByRole("button", { name: "Save Source" })).toBeEnabled();
  expect(screen.getByText("Preview is up to date.")).toBeInTheDocument();
  expect(screen.getByText(/still preview, save and publish/)).toBeInTheDocument();
  fireEvent.click(screen.getByRole("button", { name: "Preview in App" }));
  expect(view.context.previewTheme).toHaveBeenCalledWith(expect.objectContaining({ manifest: expect.objectContaining({ typography: expect.objectContaining({ headingWeight: 925.5 }) }) }), expect.any(Object));
});
