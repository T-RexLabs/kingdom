import { Link, useRouterState } from "@tanstack/react-router";
import { cn } from "@/lib/utils";
import {
  Crown,
  Scroll,
  Play,
  Users,
  Shield,
  Menu,
  X,
  Keyboard,
  BarChart3,
  Link2,
} from "lucide-react";
import { type ReactNode, useCallback, useEffect, useRef, useState } from "react";
import { useHealth } from "@/api/hooks";
import { useKeyboardShortcuts } from "@/hooks/use-keyboard-shortcuts";
import { ShortcutsHelp } from "@/components/shortcuts-help";
import { NotificationStack } from "@/components/notifications";

const navigation = [
  { name: "Throne Room", href: "/", icon: Crown },
  { name: "Decrees", href: "/decrees", icon: Scroll },
  { name: "Runs", href: "/runs", icon: Play },
  { name: "Agents", href: "/agents", icon: Users },
  { name: "Connections", href: "/connections", icon: Link2 },
  { name: "Realm Analytics", href: "/realm-analytics", icon: BarChart3 },
] as const;

export function AppShell({ children }: { children: ReactNode }) {
  const router = useRouterState();
  const { helpOpen, closeHelp, shortcuts, pendingG } = useKeyboardShortcuts();

  return (
    <div className="flex h-screen">
      <NavigationFrame routeLocation={router.location} />

      {/* Main content */}
      <main className="flex-1 overflow-auto pt-12 md:pt-0">{children}</main>

      {/* Keyboard shortcuts overlay */}
      <ShortcutsHelp
        open={helpOpen}
        onClose={closeHelp}
        shortcuts={shortcuts}
        pendingG={pendingG}
      />

      {/* Global notification cards */}
      <NotificationStack />
    </div>
  );
}

function NavigationFrame({ routeLocation }: { routeLocation: object }) {
  const [mobileOpenLocation, setMobileOpenLocation] = useState<object | null>(null);
  const toggleRef = useRef<HTMLButtonElement>(null);
  const sidebarRef = useRef<HTMLElement>(null);
  const wasMobileOpenRef = useRef(false);
  const mobileOpen = mobileOpenLocation === routeLocation;
  const toggleMobile = useCallback(
    () => setMobileOpenLocation((current) => (current === routeLocation ? null : routeLocation)),
    [routeLocation],
  );

  useEffect(() => {
    const wasMobileOpen = wasMobileOpenRef.current;
    wasMobileOpenRef.current = mobileOpen;
    if (wasMobileOpen && !mobileOpen && sidebarRef.current?.contains(document.activeElement)) {
      toggleRef.current?.focus();
    }
  }, [mobileOpen]);

  return (
    <>
      {/* Mobile header */}
      <div className="fixed top-0 left-0 right-0 z-40 flex h-12 items-center border-b border-border bg-surface px-3 md:hidden">
        <button
          ref={toggleRef}
          onClick={toggleMobile}
          className="rounded-md p-1.5 text-foreground-muted hover:text-foreground"
          aria-label="Toggle navigation"
        >
          {mobileOpen ? <X className="h-5 w-5" /> : <Menu className="h-5 w-5" />}
        </button>
        <Link to="/" className="ml-3 flex items-center gap-2 rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40">
          <Shield className="h-5 w-5 text-gold" />
          <span className="font-display text-sm font-bold text-gold">Kingdom</span>
        </Link>
      </div>

      {/* Backdrop */}
      {mobileOpen && (
        <div
          className="fixed inset-0 z-40 bg-black/50 md:hidden"
          onClick={() => setMobileOpenLocation(null)}
        />
      )}

      {/* Sidebar */}
      <aside
        ref={sidebarRef}
        className={cn(
          "fixed inset-y-0 left-0 z-50 flex w-56 flex-col border-r border-border bg-surface transition-transform duration-200 md:visible md:static md:translate-x-0",
          mobileOpen ? "visible translate-x-0" : "invisible -translate-x-full",
        )}
      >
        <SidebarContent />
      </aside>
    </>
  );
}

function SidebarContent() {
  const router = useRouterState();
  const currentPath = router.location.pathname;
  const { data: health } = useHealth();

  return (
    <>
      {/* Logo / Brand */}
      <Link
        to="/"
        className="flex h-[76px] items-center gap-3 border-b border-border px-4 transition-colors hover:bg-surface-raised/50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-gold/40"
      >
        <Shield className="h-7 w-7 text-gold" />
        <div>
          <h1 className="font-display text-lg font-bold tracking-wide text-gold">
            Kingdom
          </h1>
          <p className="text-[10px] uppercase tracking-widest text-foreground-subtle">
            Orchestrator
          </p>
        </div>
      </Link>

      {/* Navigation */}
      <nav className="flex-1 space-y-0.5 px-2 py-3">
        {navigation.map((item) => {
          const isActive =
            item.href === "/"
              ? currentPath === "/"
              : currentPath.startsWith(item.href);

          return (
            <Link
              key={item.href}
              to={item.href}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-gold/10 text-gold"
                  : "text-foreground-muted hover:bg-surface-raised hover:text-foreground"
              )}
            >
              <item.icon className="h-4 w-4" />
              {item.name}
            </Link>
          );
        })}
      </nav>

      {/* Keyboard shortcut hint + system status footer */}
      <div className="border-t border-border px-4 py-3">
        <div className="mb-2 flex items-center gap-2 text-xs text-foreground-subtle">
          <Keyboard className="h-3 w-3" />
          <span>
            Press <kbd className="rounded border border-border bg-background px-1 font-mono text-[10px]">?</kbd> for shortcuts
          </span>
        </div>
        <div className="flex items-center gap-2">
          <div
            className={cn(
              "h-2 w-2 rounded-full",
              health?.status === "ok" ? "bg-status-succeeded" : "bg-status-failed"
            )}
          />
          <span className="text-xs text-foreground-subtle">
            {health?.status === "ok" ? "System Online" : "Connecting..."}
          </span>
        </div>
      </div>
    </>
  );
}
