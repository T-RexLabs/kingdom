import { useState } from "react";
import { Link, Outlet, useRouterState } from "@tanstack/react-router";
import { PanelLeft, ArrowLeft } from "lucide-react";
import { PageContent, PageHeader } from "@/components/page-layout";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { Modal } from "@/components/ui/modal-shell";
import { useThemeCopy } from "@/theme";
import { PlanningSessionList } from "./planning-session-list";

export function WorkOrdersWorkspaceLayout() {
  const pathname = useRouterState({ select: (state) => state.location.pathname });
  const creating = pathname.startsWith("/work-orders/new");
  const [sessionsOpen, setSessionsOpen] = useState(false);
  const workName = useThemeCopy("workOrder.singular");
  return <div className="flex h-full min-h-0 flex-col">
    <PageHeader className="shrink-0">
      <div className="flex w-full items-center justify-between gap-3">
        {creating ? <Link to="/work-orders/planning" className="inline-flex items-center gap-2 text-sm text-foreground-muted hover:text-foreground"><ArrowLeft className="h-4 w-4" />Planning</Link> : <h1 className="text-sm font-medium text-foreground-muted">Planning</h1>}
        {!creating && <div className="flex items-center gap-2">
          <Button variant="ghost" size="sm" className="lg:hidden" onClick={() => setSessionsOpen(true)}><PanelLeft className="h-4 w-4" />Sessions</Button>
          <Link to="/work-orders/new" className={buttonVariants({ variant: "outline", size: "sm" })}>New {workName}</Link>
        </div>}
      </div>
    </PageHeader>
    {creating ? <PageContent className="py-8 sm:py-10"><Outlet /></PageContent> : <div className="grid min-h-0 flex-1 grid-cols-1 lg:grid-cols-[248px_minmax(0,1fr)] xl:grid-cols-[272px_minmax(0,1fr)]">
      <aside className="hidden min-h-0 border-r border-border bg-surface/30 pt-5 lg:block"><PlanningSessionList /></aside>
      <main className="min-h-0 min-w-0"><Outlet /></main>
    </div>}
    <Modal open={sessionsOpen} onClose={() => setSessionsOpen(false)} title="Planning Sessions" size="sm"><div className="h-[min(65vh,40rem)]"><PlanningSessionList onSelect={() => setSessionsOpen(false)} /></div></Modal>
  </div>;
}
