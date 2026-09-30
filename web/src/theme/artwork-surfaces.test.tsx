import { fireEvent, render, screen, within } from "@testing-library/react";
import type { ReactElement, ReactNode } from "react";
import { expect, it, vi } from "vitest";
import { DashboardPage } from "@/features/dashboard/dashboard-page";
import { AgentsPage } from "@/features/agents/agents-page";
import { PlanningSessionIndexPage } from "@/features/work-orders/planning-session-index-page";
import type { ThemeAssetURLs } from "./assets";
import { ThemeContext } from "./context";
import { ThemeController } from "./controller";
import { defaultTheme, type ArtworkSlot } from "./contract";

const emptyQuery = { data: { items: [] }, isLoading: false, refetch: vi.fn() };
vi.mock("@/api/hooks", () => ({
  useDashboardSnapshot: () => ({ isLoading: false }),
  useInfiniteRuns: () => ({ data: { pages: [{ items: [] }] } }),
  usePlanningSessions: () => emptyQuery,
  useAgents: () => emptyQuery,
  useRoles: () => ({ data: { items: [{ role_id: "builder", description: "Builds the work." }] } }),
  useWorkspaceCapabilities: () => ({ data: { project_root: "/test" } }),
}));
vi.mock("@/features/work-orders/use-create-planning-session-navigation", () => ({
  useCreatePlanningSessionNavigation: () => ({ hasRecovery: false, isPending: false }),
}));
vi.mock("@/features/work-orders/use-provider-model-catalog", () => ({
  useProviderModelCatalogController: () => ({ state: { items: [], isLoading: false, disableSelection: false } }),
}));
vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, to }: { children: ReactNode; to: string }) => <a href={to}>{children}</a>,
  useNavigate: () => vi.fn(),
}));

function renderSurface(ui: ReactElement, artwork: Partial<Record<ArtworkSlot, string>> = {
  overview: "asset:overview", planningEmpty: "asset:planning", agentBackdrop: "asset:agents",
}, missingAsset = false) {
  const controller = new ThemeController();
  const theme = structuredClone(defaultTheme);
  theme.artwork = artwork;
  theme.assets = Object.fromEntries(["overview", "planning", "agents", "portrait"].map((id) => [id, { path: `assets/${id}.png`, mediaType: "image/png" }]));
  theme.identities.builder.portrait = "asset:portrait";
  const assetURLs: ThemeAssetURLs = missingAsset ? {} : { overview: "/overview.png", planning: "/planning.png", agents: "/agents.png", portrait: "/portrait.png" };
  const appearance = {
    ...controller.getSnapshot(), theme, assetURLs,
    refreshCatalog: controller.refreshCatalog, selectTheme: controller.selectTheme,
    previewTheme: controller.previewTheme, cancelPreview: controller.cancelPreview,
  };
  return render(<ThemeContext.Provider value={appearance}>{ui}</ThemeContext.Provider>);
}

it("shows only Overview artwork above its operational queues", () => {
  const view = renderSurface(<DashboardPage />);
  const art = view.container.querySelector('img[src="/overview.png"]');
  expect(art).toHaveAttribute("alt", "");
  expect(view.container.querySelectorAll("img")).toHaveLength(1);
  expect(art!.compareDocumentPosition(screen.getByRole("region", { name: "Needs Your Decision" })) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
});

it("shows planning artwork above the new-session welcome without replacing its controls", () => {
  const view = renderSurface(<PlanningSessionIndexPage />);
  const art = view.container.querySelector('img[src="/planning.png"]');
  expect(art).toHaveAttribute("alt", "");
  expect(view.container.querySelectorAll("img")).toHaveLength(1);
  expect(art!.compareDocumentPosition(screen.getByRole("heading", { level: 2 })) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(screen.getByRole("button", { name: "Start Planner Session" })).toBeEnabled();
});

it("keeps an agent card illustration separate from the role portrait in its details", () => {
  const view = renderSurface(<AgentsPage />);
  const card = screen.getByRole("button", { name: `About ${defaultTheme.identities.builder.name}` });
  const art = card.querySelector('img[src="/agents.png"]');
  expect(art).toHaveAttribute("alt", "");
  expect(art!.compareDocumentPosition(within(card).getByRole("heading")) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy();
  expect(view.container.querySelector('img[src="/portrait.png"]')).not.toBeInTheDocument();
  fireEvent.click(card);
  expect(screen.getByRole("dialog").querySelector('img[src="/portrait.png"]')).toHaveAttribute("alt", "");
  expect(screen.getByRole("dialog").querySelector('img[src="/agents.png"]')).not.toBeInTheDocument();
});

it.each([false, true])("leaves no artwork frame when no displayable image is available (missing=%s)", (missing) => {
  const view = renderSurface(<><DashboardPage /><PlanningSessionIndexPage /><AgentsPage /></>, missing ? { overview: "asset:overview", planningEmpty: "asset:planning", agentBackdrop: "asset:agents" } : {}, missing);
  expect(view.container.querySelectorAll("img")).toHaveLength(0);
});

it("removes an undecodable optional illustration instead of leaving a broken frame", () => {
  const view = renderSurface(<DashboardPage />);
  const art = view.container.querySelector('img[src="/overview.png"]');
  expect(art).not.toBeNull();
  fireEvent.error(art!);
  expect(view.container.querySelectorAll("img")).toHaveLength(0);
});
