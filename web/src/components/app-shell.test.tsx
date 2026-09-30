import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AppShell } from "./app-shell";

let routerLocation = { pathname: "/runs" };

vi.mock("@tanstack/react-router", () => ({
  Link: ({ children, to, className }: { children: React.ReactNode; to: string; className?: string }) => (
    <a href={to} className={className}>
      {children}
    </a>
  ),
  useRouterState: () => ({ location: routerLocation }),
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
  beforeEach(() => {
    routerLocation = { pathname: "/runs" };
  });

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

  it("closes mobile navigation on route changes without remounting desktop navigation", async () => {
    const user = userEvent.setup();
    const view = render(
      <AppShell>
        <div>content</div>
      </AppShell>,
    );

    await user.click(screen.getByRole("button", { name: "Toggle navigation" }));
    const toggle = screen.getByRole("button", { name: "Toggle navigation" });
    const sidebar = screen.getByRole("complementary");
    expect(sidebar).toHaveClass("translate-x-0");

    const runsLink = screen.getByRole("link", { name: "Runs" });
    runsLink.focus();
    expect(runsLink).toHaveFocus();

    routerLocation = { pathname: "/agents" };
    view.rerender(
      <AppShell>
        <div>content</div>
      </AppShell>,
    );

    expect(screen.getByRole("complementary")).toBe(sidebar);
    expect(sidebar).toHaveClass("-translate-x-full");
    expect(toggle).toHaveFocus();
  });
});
