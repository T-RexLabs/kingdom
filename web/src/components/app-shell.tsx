import { Link, useRouterState } from "@tanstack/react-router";
import { Menu, X } from "lucide-react";
import { type ReactNode, useRef, useState } from "react";
import { useHealth, useWorkspaceCapabilities } from "@/api/hooks";
import { cn } from "@/lib/utils";
import { useKeyboardShortcuts } from "@/hooks/use-keyboard-shortcuts";
import { ShortcutsHelp } from "@/components/shortcuts-help";
import { NotificationStack } from "@/components/notifications";
import { ModalDialog } from "@/components/ui/modal-dialog";
import { ThemeIcon, useTheme } from "@/theme";

const primaryNavigation = [
  { name: "Overview", href: "/", icon: "nav.overview" },
  { name: "Planning", href: "/work-orders/planning", icon: "nav.planning" },
  { name: "Runs", href: "/runs", icon: "nav.runs" },
  { name: "Agents", href: "/agents", icon: "nav.agents" },
  { name: "Analytics", href: "/analytics", icon: "nav.analytics" },
] as const;
const secondaryNavigation = [
  { name: "Connections", href: "/connections", icon: "nav.connections" },
  { name: "Appearance", href: "/appearance", icon: "nav.appearance" },
] as const;

export function AppShell({ children }: { children: ReactNode }) {
  const { location } = useRouterState();
  const { helpOpen, closeHelp, shortcuts, pendingG } = useKeyboardShortcuts();
  const { isPreviewing, cancelPreview, theme } = useTheme();
  const [openAt, setOpenAt] = useState<object | null>(null);
  const menuRef = useRef<HTMLButtonElement>(null);
  const mobileOpen = openAt === location;
  return (
    <div className="flex h-dvh min-h-0 bg-background text-foreground">
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[100] focus:rounded-lg focus:bg-surface focus:p-3"
      >
        Skip to content
      </a>
      <aside
        className="hidden w-56 shrink-0 flex-col border-r border-border bg-background-secondary md:flex"
        aria-label="Workspace navigation"
      >
        <SidebarContent />
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="flex h-14 shrink-0 items-center gap-3 border-b border-border px-4 md:hidden">
          <button
            ref={menuRef}
            type="button"
            onClick={() => setOpenAt(location)}
            aria-label="Open navigation"
            aria-expanded={mobileOpen}
            className="rounded-md p-2 hover:bg-surface-raised"
          >
            <Menu size={20} />
          </button>
          <Brand />
        </header>
        {isPreviewing && (
          <div
            className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-accent/30 bg-accent/10 px-page py-2 text-sm"
            role="status"
          >
            <span>
              Previewing <strong>{theme.name}</strong>
              <span className="ml-2 text-foreground-muted">
                Only you can see this preview.
              </span>
            </span>
            <div className="flex gap-4">
              <Link
                to="/appearance/create"
                className="underline underline-offset-4"
              >
                Back to Editor
              </Link>
              <button
                type="button"
                onClick={cancelPreview}
                className="underline underline-offset-4"
              >
                Exit Preview
              </button>
            </div>
          </div>
        )}
        <main
          id="main-content"
          tabIndex={-1}
          className="min-h-0 flex-1 overflow-hidden outline-none"
        >
          {children}
        </main>
      </div>
      <ModalDialog
        open={mobileOpen}
        onClose={() => setOpenAt(null)}
        ariaLabel="Navigation"
        getFinalFocus={() => menuRef.current}
        overlayClassName="justify-start p-0"
        panelClassName="relative flex h-dvh w-72 max-w-[85vw] flex-col rounded-none bg-background-secondary p-0"
      >
        <button
          type="button"
          onClick={() => setOpenAt(null)}
          aria-label="Close navigation"
          className="absolute right-3 top-5 rounded-md p-2 hover:bg-surface-raised"
        >
          <X size={18} />
        </button>
        <SidebarContent />
      </ModalDialog>
      <ShortcutsHelp
        open={helpOpen}
        onClose={closeHelp}
        shortcuts={shortcuts}
        pendingG={pendingG}
      />
      <NotificationStack />
    </div>
  );
}
function Brand() {
  return (
    <Link
      to="/"
      className="flex w-fit items-center gap-2.5 rounded-md text-accent"
    >
      <ThemeIcon slot="brand" className="h-5 w-5" />
      <span className="font-display text-2xl">Kingdom</span>
    </Link>
  );
}
function SidebarContent() {
  const { location } = useRouterState();
  const { theme } = useTheme();
  const health = useHealth();
  const workspace = useWorkspaceCapabilities();
  const projectPath = workspace.data?.project_root;
  const name =
    projectPath?.split(/[\\/]/).filter(Boolean).at(-1) ?? "Workspace";
  function links(items: typeof primaryNavigation | typeof secondaryNavigation) {
    return items.map((item) => {
      const active =
        item.href === "/"
          ? location.pathname === "/"
          : location.pathname.startsWith(item.href);
      return (
        <Link
          key={item.href}
          to={item.href}
          aria-current={active ? "page" : undefined}
          className={cn(
            "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm transition-colors",
            active
              ? "bg-accent/10 text-accent"
              : "text-foreground-muted hover:bg-surface-raised hover:text-foreground",
          )}
        >
          <ThemeIcon slot={item.icon} className="h-[18px] w-[18px] shrink-0" />
          {item.name}
        </Link>
      );
    });
  }
  return (
    <>
      <div className="px-6 pb-7 pt-7">
        <Brand />
      </div>
      <div
        className="mx-4 mb-7 rounded-lg border border-border bg-surface px-3 py-3"
        title={projectPath}
      >
        <div className="truncate text-sm font-medium">{name}</div>
        <div className="mt-1 text-xs text-foreground-muted">
          Local Workspace
        </div>
      </div>
      <nav aria-label="Main" className="space-y-1 px-4">
        {links(primaryNavigation)}
      </nav>
      <div className="min-h-8 flex-1" />
      <nav aria-label="Settings" className="space-y-1 px-4">
        {links(secondaryNavigation)}
      </nav>
      <footer className="space-y-4 px-7 pb-6 pt-6">
        <div className="flex items-center gap-2.5 text-xs text-foreground-muted">
          <span
            aria-hidden="true"
            className={cn(
              "h-1.5 w-1.5 rounded-full",
              health.isError
                ? "bg-status-failed"
                : health.data?.status === "ok"
                  ? "bg-status-succeeded"
                  : "bg-foreground-subtle",
            )}
          />
          {health.isError
            ? "Runtime Unavailable"
            : health.data?.status === "ok"
              ? "Local Runtime"
              : "Connecting…"}
        </div>
        <p className="text-xs leading-relaxed text-foreground-subtle">
          {theme.copy["brand.tagline"]}
        </p>
      </footer>
    </>
  );
}
