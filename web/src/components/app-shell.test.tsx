import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { AppShell } from "./app-shell";

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, to, className }: { children: React.ReactNode; to: string; className?: string }) => (
    <a href={to} className={className}>
      {children}
    </a>
  ),
  useRouterState: () => ({ location: { pathname: "/runs" } }),
}));

vi.mock("@/api/hooks", () => ({
  useHealth: () => ({ data: { status: "ok" } }),
}));

vi.mock("@/hooks/use-keyboard-shortcuts", () => ({
  useKeyboardShortcuts: () => ({ helpOpen: false, closeHelp: vi.fn(), shortcuts: [], pendingG: false }),
}));

vi.mock("@/components/shortcuts-help", () => ({
  ShortcutsHelp: () => null,
}));

vi.mock("@/components/notifications", () => ({
  NotificationStack: () => null,
}));

describe("app shell", () => {
  it("routes both brand labels to throne room", () => {
    render(
      <AppShell>
        <div>content</div>
      </AppShell>,
    );

    const kingdomLinks = screen.getAllByRole("link", { name: /Kingdom/i });
    expect(kingdomLinks.length).toBeGreaterThan(1);
    for (const link of kingdomLinks) {
      expect(link).toHaveAttribute("href", "/");
    }
  });
});
