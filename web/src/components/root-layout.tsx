import { Outlet, useRouterState } from "@tanstack/react-router";
import { AppShell } from "@/components/app-shell";
import { GovernanceNotifierMount } from "@/features/runs/governance-notifier";
import { GITHUB_SETUP_RETURN_PATH } from "@/features/connections/github-setup-return-path";

/**
 * Decides whether a route carries app chrome.
 *
 * The GitHub setup tab lands on a chromeless handoff page, so the shell is
 * applied here rather than wrapping the root unconditionally. A pathless layout
 * route would namespace every route id and leak that id into feature code.
 */
const chromelessPaths = new Set<string>([GITHUB_SETUP_RETURN_PATH]);

export function RootLayout() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  if (chromelessPaths.has(pathname)) return <Outlet />;
  return (
    <AppShell>
      <GovernanceNotifierMount />
      <Outlet />
    </AppShell>
  );
}
