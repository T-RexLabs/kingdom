import {
  createRouter,
  createRootRoute,
  createRoute,
  lazyRouteComponent,
  redirect,
} from "@tanstack/react-router";
import { validateAnalyticsSearch } from "@/lib/analytics-time-range";
import { NotFoundPage } from "@/components/error-boundary";
import { RootLayout } from "@/components/root-layout";
import { parseRunDetailSearch } from "@/features/runs/run-detail-tabs";
import { isRunsStateFilter, type RunsStateFilter } from "@/lib/run-filters";
import { validateWorkOrderNewSearch } from "@/features/work-orders/work-order-route-search";
import { GITHUB_SETUP_RETURN_PATH } from "@/features/connections/github-setup-return-path";

import { appBase } from "@/lib/runtime-mode";

const rootRoute = createRootRoute({
  component: RootLayout,
  notFoundComponent: NotFoundPage,
});

const githubSetupReturnRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: GITHUB_SETUP_RETURN_PATH,
  component: lazyRouteComponent(
    () => import("@/features/connections/github-setup-return-page"),
    "GitHubSetupReturnPage"
  ),
});

// Dashboard stays eagerly loaded (landing page — must be instant)
import { DashboardPage } from "@/features/dashboard/dashboard-page";

const dashboardRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/",
  component: DashboardPage,
});

// Lazy-loaded routes — split into separate chunks
const runsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runs",
  validateSearch: (search: Record<string, unknown>): { state_filter?: RunsStateFilter; q?: string } => ({
    ...(isRunsStateFilter(search.state_filter) ? { state_filter: search.state_filter } : {}),
    ...(typeof search.q === "string" && search.q.trim() ? { q: search.q.trim() } : {}),
  }),
  component: lazyRouteComponent(
    () => import("@/features/runs/runs-page"),
    "RunsPage"
  ),
});

const runDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runs/$runId",
  validateSearch: parseRunDetailSearch,
  component: lazyRouteComponent(
    () => import("@/features/runs/run-detail-page"),
    "RunDetailPage"
  ),
});

const workOrdersRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/work-orders",
  component: lazyRouteComponent(
    () => import("@/features/work-orders/work-orders-workspace-layout"),
    "WorkOrdersWorkspaceLayout"
  ),
});

const workOrdersIndexRoute = createRoute({
  getParentRoute: () => workOrdersRoute,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/work-orders/planning" });
  },
  component: lazyRouteComponent(
    () => import("@/features/work-orders/planning-session-index-page"),
    "PlanningSessionIndexPage"
  ),
});

const workOrdersPlanningRoute = createRoute({
  getParentRoute: () => workOrdersRoute,
  path: "/planning",
  component: lazyRouteComponent(
    () => import("@/features/work-orders/planning-session-index-page"),
    "PlanningSessionIndexPage"
  ),
});

const workOrdersPlanningSessionRoute = createRoute({
  getParentRoute: () => workOrdersRoute,
  path: "/planning/$sessionId",
  validateSearch: (search: Record<string, unknown>): { show_plan?: boolean } => {
    if (search.show_plan === true || search.show_plan === "true") {
      return { show_plan: true };
    }
    return {};
  },
  component: lazyRouteComponent(
    () => import("@/features/work-orders/planning-session-page"),
    "PlanningSessionPage"
  ),
});

const workOrdersNewRoute = createRoute({
  getParentRoute: () => workOrdersRoute,
  path: "/new",
  validateSearch: validateWorkOrderNewSearch,
  component: lazyRouteComponent(
    () => import("@/features/work-orders/issue-work-order-page"),
    "IssueWorkOrderPage"
  ),
});

const agentsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/agents",
  component: lazyRouteComponent(
    () => import("@/features/agents/agents-page"),
    "AgentsPage"
  ),
});

const connectionsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/connections",
  validateSearch: (search: Record<string, unknown>): { connection?: string } => typeof search.connection === "string" && search.connection.trim() ? { connection: search.connection } : {},
  component: lazyRouteComponent(
    () => import("@/features/connections/connections-page"),
    "ConnectionsPage"
  ),
});

const analyticsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/analytics",
  validateSearch: validateAnalyticsSearch,
  component: lazyRouteComponent(
    () => import("@/features/analytics/analytics-page"),
    "AnalyticsPage"
  ),
});

const appearanceRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/appearance",
  component: lazyRouteComponent(() => import("@/features/appearance/appearance-page"), "AppearancePage"),
});
const createThemeRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/appearance/create",
  validateSearch: (search: Record<string, unknown>): { source?: string; revision?: string; base?: string } => {
    const next: { source?: string; revision?: string; base?: string } = {};
    for (const key of ["source", "revision", "base"] as const) {
      if (typeof search[key] === "string" && search[key].length > 0) next[key] = search[key];
    }
    return next;
  },
  component: lazyRouteComponent(() => import("@/features/appearance/create-theme-page"), "CreateThemePage"),
});

const routeTree = rootRoute.addChildren([
  dashboardRoute,
  runsRoute,
  runDetailRoute,
  workOrdersRoute.addChildren([
    workOrdersIndexRoute,
    workOrdersPlanningRoute,
    workOrdersPlanningSessionRoute,
    workOrdersNewRoute,
  ]),
  agentsRoute,
  connectionsRoute,
  appearanceRoute,
  createThemeRoute,
  analyticsRoute,
  githubSetupReturnRoute,
]);

export const router = createRouter({
  routeTree,
  basepath: appBase(),
  defaultPendingComponent: () => (
    <div className="flex h-64 items-center justify-center">
      <div className="h-8 w-8 animate-spin rounded-full border-2 border-accent border-t-transparent" />
    </div>
  ),
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
