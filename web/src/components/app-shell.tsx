import { Link, useRouterState } from "@tanstack/react-router";
import { Menu, PanelLeftClose, PanelLeftOpen, X } from "lucide-react";
import { type ReactNode, useRef, useState } from "react";
import { useWorkspaceCapabilities } from "@/api/hooks";
import { cn } from "@/lib/utils";
import { useKeyboardShortcuts } from "@/hooks/use-keyboard-shortcuts";
import { ShortcutsHelp } from "@/components/shortcuts-help";
import { NotificationStack } from "@/components/notifications";
import { ModalDialog } from "@/components/ui/modal-dialog";
import { KingdomBrand } from "@/components/kingdom-brand";
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

const navigationPreferenceKey = "kingdom:navigation:v1:collapsed";
function readNavigationPreference() {
  try { return localStorage.getItem(navigationPreferenceKey) === "true"; }
  catch { return false; }
}

export function AppShell({ children }: { children: ReactNode }) {
  const { location } = useRouterState();
  const { helpOpen, closeHelp, shortcuts, pendingG } = useKeyboardShortcuts();
  const { isPreviewing, cancelPreview, theme, error: themeError } = useTheme();
  const [openAt, setOpenAt] = useState<object | null>(null);
  const [collapsed, setCollapsed] = useState(readNavigationPreference);
  const menuRef = useRef<HTMLButtonElement>(null);
  const mobileOpen = openAt === location;
  function toggleNavigation() {
    const next = !collapsed;
    setCollapsed(next);
    try { localStorage.setItem(navigationPreferenceKey, String(next)); }
    catch { /* Navigation remains usable when personal storage is unavailable. */ }
  }
  return (
    <div className="ui-app-shell flex h-dvh min-h-0 bg-background text-foreground">
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:fixed focus:left-4 focus:top-4 focus:z-[100] focus:rounded-lg focus:bg-surface focus:p-3"
      >
        Skip to content
      </a>
      <aside
        id="workspace-navigation"
        className={cn("ui-sidebar hidden shrink-0 flex-col overflow-y-auto border-r border-border bg-background-secondary md:flex motion-safe:transition-[width] motion-safe:duration-200", collapsed ? "w-16" : "w-56")}
        aria-label="Workspace navigation"
      >
        <SidebarContent collapsed={collapsed} onToggle={toggleNavigation} />
      </aside>
      <div className="flex min-w-0 flex-1 flex-col">
        <header className="ui-mobile-header flex min-h-16 shrink-0 items-center gap-3 border-b border-border px-4 md:hidden">
          <button
            ref={menuRef}
            type="button"
            onClick={() => setOpenAt(location)}
            aria-label="Open navigation"
            aria-expanded={mobileOpen}
            className="ui-button ui-button--ghost rounded-md p-2 hover:bg-surface-raised"
          >
            <Menu size={20} />
          </button>
          <KingdomBrand />
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
        {themeError && !location.pathname.startsWith("/appearance") && (
          <div role="alert" className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-border bg-surface px-page py-2 text-sm">
            <span>{themeError}</span>
            <Link to="/appearance" className="text-accent underline underline-offset-4">
              Review Appearance
            </Link>
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
        panelClassName="ui-sidebar relative flex h-dvh w-72 max-w-[85vw] flex-col rounded-none bg-background-secondary p-0"
      >
        <button
          type="button"
          onClick={() => setOpenAt(null)}
          aria-label="Close navigation"
          className="ui-button ui-button--ghost absolute right-3 top-5 rounded-md p-2 hover:bg-surface-raised"
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
function SidebarContent({ collapsed = false, onToggle }: { collapsed?: boolean; onToggle?: () => void }) {
  const { location } = useRouterState();
  const { theme } = useTheme();
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
          aria-label={item.name}
          title={collapsed ? item.name : undefined}
          aria-current={active ? "page" : undefined}
          className={cn(
            "ui-nav-item flex min-h-10 items-center rounded-lg py-2.5 text-sm transition-colors",
            collapsed ? "justify-center px-2" : "gap-3 px-3",
            active
              ? "bg-accent/10 text-accent"
              : "text-foreground-muted hover:bg-surface-raised hover:text-foreground",
          )}
        >
          <ThemeIcon slot={item.icon} className="ui-nav-icon h-[18px] w-[18px] shrink-0" />
          <span className={collapsed ? "sr-only" : undefined}>{item.name}</span>
        </Link>
      );
    });
  }
  return (
    <>
      <div className={cn("flex gap-2 pb-6 pt-6", collapsed ? "flex-col items-center px-2" : "items-center justify-between pl-4 pr-3")}>
        <KingdomBrand collapsed={collapsed} />
        {onToggle && <button
          type="button"
          onClick={onToggle}
          aria-label={collapsed ? "Expand navigation" : "Collapse navigation"}
          title={collapsed ? "Expand navigation" : "Collapse navigation"}
          aria-expanded={!collapsed}
          aria-controls="workspace-navigation"
          className="ui-button ui-button--ghost flex h-8 w-8 shrink-0 items-center justify-center rounded-md text-foreground-muted transition-colors hover:bg-surface-raised hover:text-foreground"
        >
          {collapsed ? <PanelLeftOpen size={16} aria-hidden="true" /> : <PanelLeftClose size={16} aria-hidden="true" />}
        </button>}
      </div>
      {!collapsed && <div
        className="ui-workspace-info mx-4 mb-7 rounded-lg border border-border bg-surface px-3 py-3"
        title={projectPath}
      >
        <div className="truncate text-sm font-medium">{name}</div>
        <div className="mt-1 text-xs text-foreground-muted">
          Local Workspace
        </div>
      </div>}
      <nav aria-label="Main" className={cn("space-y-1", collapsed ? "px-2" : "px-4")}>
        {links(primaryNavigation)}
      </nav>
      <div className="min-h-8 flex-1" />
      <nav aria-label="Settings" className={cn("space-y-1", collapsed ? "px-2" : "px-4")}>
        {links(secondaryNavigation)}
      </nav>
      <footer className={cn("space-y-4 pb-6 pt-6", collapsed ? "px-2" : "px-7")}>
        {!collapsed && <p className="text-xs leading-relaxed text-foreground-subtle">
          {theme.copy["brand.tagline"]}
        </p>}
      </footer>
    </>
  );
}
