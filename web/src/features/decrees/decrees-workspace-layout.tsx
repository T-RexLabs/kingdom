import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import { PageHeader } from "@/components/page-layout";
import { buttonVariants } from "@/components/ui/button-variants";
import { PlanningSessionList } from "./planning-session-list";

export function DecreesWorkspaceLayout() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const mode = pathname.startsWith("/decrees/new") ? "issue" : "planning";

  return (
    <div className="flex h-full min-h-0 flex-col">
      <PageHeader>
        <div className="flex w-full items-center justify-between gap-4">
          <div>
            <h1 className="font-display text-2xl font-bold text-gold">Royal Decrees</h1>
            <p className="text-sm text-foreground-muted">
              Talk it through with the planner when you're still exploring, or issue a decree
              directly when the path is already clear.
            </p>
          </div>
          <div className="hidden items-center gap-2 md:flex">
            <Link
              to="/decrees/planning"
              className={buttonVariants({
                variant: mode === "planning" ? "outline" : "ghost",
                size: "sm",
              })}
            >
              Planning Sessions
            </Link>
            <Link
              to="/decrees/new"
              className={buttonVariants({
                variant: mode === "issue" ? "default" : "outline",
                size: "sm",
              })}
            >
              Issue Decree
            </Link>
          </div>
        </div>
      </PageHeader>

      <div className="flex flex-1 min-h-0 flex-col px-page py-4">
        <div className="flex gap-2 md:hidden mb-4">
          <Link
            to="/decrees/planning"
            className={buttonVariants({
              variant: mode === "planning" ? "outline" : "ghost",
              size: "sm",
            })}
          >
            Planning Sessions
          </Link>
          <Link
            to="/decrees/new"
            className={buttonVariants({
              variant: mode === "issue" ? "default" : "outline",
              size: "sm",
            })}
          >
            Issue Decree
          </Link>
        </div>

        <div
          className={`grid flex-1 min-h-0 grid-cols-1 gap-4 ${
            mode === "planning"
              ? "xl:grid-cols-[280px_minmax(0,1fr)]"
              : "xl:grid-cols-[minmax(0,1fr)]"
          }`}
        >
          {mode === "planning" ? (
            <div className="min-h-0 h-full">
              <PlanningSessionList />
            </div>
          ) : null}
          <div className="min-h-0 h-full">
            <Outlet />
          </div>
        </div>
      </div>
    </div>
  );
}
