import {
  createRouter,
  createRootRoute,
  createRoute,
  Outlet,
  lazyRouteComponent,
  redirect,
} from "@tanstack/react-router";
import { AppShell } from "@/components/app-shell";
import { NotFoundPage } from "@/components/error-boundary";
import { GovernanceNotifierMount } from "@/features/runs/governance-notifier";
import { isRunDetailTab, type RunDetailTab } from "@/features/runs/run-detail-tabs";
import { isRunsStateFilter, type RunsStateFilter } from "@/lib/run-filters";

const rootRoute = createRootRoute({
  component: () => (
    <AppShell>
      <GovernanceNotifierMount />
      <Outlet />
    </AppShell>
  ),
  notFoundComponent: NotFoundPage,
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
  validateSearch: (search: Record<string, unknown>): { state_filter?: RunsStateFilter } => {
    const rawStateFilter = search.state_filter;
    if (isRunsStateFilter(rawStateFilter)) {
      return { state_filter: rawStateFilter };
    }
    return {};
  },
  component: lazyRouteComponent(
    () => import("@/features/runs/runs-page"),
    "RunsPage"
  ),
});

const runDetailRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/runs/$runId",
  validateSearch: (search: Record<string, unknown>): { tab?: RunDetailTab } => {
    const tab = search.tab;
    if (isRunDetailTab(tab)) {
      return { tab };
    }
    return {};
  },
  component: lazyRouteComponent(
    () => import("@/features/runs/run-detail-page"),
    "RunDetailPage"
  ),
});

const decreesRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/decrees",
  component: lazyRouteComponent(
    () => import("@/features/decrees/decrees-workspace-layout"),
    "DecreesWorkspaceLayout"
  ),
});

const decreesIndexRoute = createRoute({
  getParentRoute: () => decreesRoute,
  path: "/",
  beforeLoad: () => {
    throw redirect({ to: "/decrees/planning" });
  },
  component: lazyRouteComponent(
    () => import("@/features/decrees/planning-session-index-page"),
    "PlanningSessionIndexPage"
  ),
});

const decreesPlanningRoute = createRoute({
  getParentRoute: () => decreesRoute,
  path: "/planning",
  component: lazyRouteComponent(
    () => import("@/features/decrees/planning-session-index-page"),
    "PlanningSessionIndexPage"
  ),
});

const decreesPlanningSessionRoute = createRoute({
  getParentRoute: () => decreesRoute,
  path: "/planning/$sessionId",
  validateSearch: (search: Record<string, unknown>): { show_plan?: boolean } => {
    if (search.show_plan === true || search.show_plan === "true") {
      return { show_plan: true };
    }
    return {};
  },
  component: lazyRouteComponent(
    () => import("@/features/decrees/planning-session-page"),
    "PlanningSessionPage"
  ),
});

const decreesNewRoute = createRoute({
  getParentRoute: () => decreesRoute,
  path: "/new",
  validateSearch: (search: Record<string, unknown>): {
    planning_session_id?: string;
    planning_draft_id?: string;
    planning_context_snapshot_id?: string;
  } => {
    const next: {
      planning_session_id?: string;
      planning_draft_id?: string;
      planning_context_snapshot_id?: string;
    } = {};

    for (const key of ["planning_session_id", "planning_draft_id", "planning_context_snapshot_id"] as const) {
      const value = search[key];
      if (typeof value === "string" && value.trim() !== "") {
        next[key] = value;
      }
    }

    return next;
  },
  component: lazyRouteComponent(
    () => import("@/features/decrees/issue-decree-page"),
    "IssueDecreePage"
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

const realmAnalyticsRoute = createRoute({
  getParentRoute: () => rootRoute,
  path: "/realm-analytics",
  validateSearch: (search: Record<string, unknown>): { window_minutes?: number; run_id?: string } => {
    const next: { window_minutes?: number; run_id?: string } = {};

    const rawMinutes = search.window_minutes;
    if (typeof rawMinutes === "number" && Number.isFinite(rawMinutes)) {
      const rounded = Math.round(rawMinutes);
      if (rounded >= 1 && rounded <= 525600) {
        next.window_minutes = rounded;
      }
    } else if (typeof rawMinutes === "string") {
      const parsed = Number.parseInt(rawMinutes, 10);
      if (Number.isFinite(parsed) && parsed >= 1 && parsed <= 525600) {
        next.window_minutes = parsed;
      }
    }

    const rawRunID = search.run_id;
    if (typeof rawRunID === "string" && rawRunID.trim().length > 0) {
      next.run_id = rawRunID;
    }

    return next;
  },
  component: lazyRouteComponent(
    () => import("@/features/realm-analytics/realm-analytics-page"),
    "RealmAnalyticsPage"
  ),
});

const routeTree = rootRoute.addChildren([
  dashboardRoute,
  runsRoute,
  runDetailRoute,
  decreesRoute.addChildren([
    decreesIndexRoute,
    decreesPlanningRoute,
    decreesPlanningSessionRoute,
    decreesNewRoute,
  ]),
  agentsRoute,
  realmAnalyticsRoute,
]);

export const router = createRouter({
  routeTree,
  defaultPendingComponent: () => (
    <div className="flex h-64 items-center justify-center">
      <div className="h-8 w-8 animate-spin rounded-full border-2 border-gold border-t-transparent" />
    </div>
  ),
});

declare module "@tanstack/react-router" {
  interface Register {
    router: typeof router;
  }
}
