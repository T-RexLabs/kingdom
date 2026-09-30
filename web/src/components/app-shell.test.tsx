import { render, screen, within, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AppShell } from "./app-shell";

let routerLocation = { pathname: "/runs" };
let previewing = false;
let healthError = false;
const cancelPreview = vi.fn();
vi.mock("@tanstack/react-router", () => ({
  Link: ({
    children,
    to,
    ...props
  }: {
    children: React.ReactNode;
    to: string;
  }) => (
    <a href={to} {...props}>
      {children}
    </a>
  ),
  useRouterState: () => ({ location: routerLocation }),
}));
vi.mock("@/api/hooks", () => ({
  useHealth: () => ({ data: { status: "ok" }, isError: healthError }),
  useWorkspaceCapabilities: () => ({
    data: { project_root: "/projects/Atlas" },
  }),
}));
vi.mock("@/theme", () => ({
  ThemeIcon: ({ slot }: { slot: string }) => (
    <span aria-hidden="true" data-icon={slot} />
  ),
  useTheme: () => ({
    isPreviewing: previewing,
    cancelPreview,
    theme: { name: "My Theme", copy: { "brand.tagline": "Clear intent." } },
  }),
}));
vi.mock("@/hooks/use-keyboard-shortcuts", () => ({
  useKeyboardShortcuts: () => ({
    helpOpen: false,
    closeHelp: vi.fn(),
    shortcuts: [],
    pendingG: false,
  }),
}));
vi.mock("@/components/shortcuts-help", () => ({ ShortcutsHelp: () => null }));
vi.mock("@/components/notifications", () => ({
  NotificationStack: () => null,
}));

describe("app shell", () => {
  beforeEach(() => {
    routerLocation = { pathname: "/runs" };
    previewing = false;
    healthError = false;
    cancelPreview.mockClear();
  });
  it("keeps navigation stable across themes and appearance in settings", () => {
    render(<AppShell>content</AppShell>);
    const mainNav = screen.getByRole("navigation", { name: "Main" });
    expect(
      within(mainNav)
        .getAllByRole("link")
        .map((link) => link.textContent),
    ).toEqual(["Overview", "Planning", "Runs", "Agents", "Analytics"]);
    expect(within(mainNav).getByRole("link", { name: "Runs" })).toHaveAttribute(
      "aria-current",
      "page",
    );
    expect(screen.getByRole("link", { name: "Appearance" })).toHaveAttribute(
      "href",
      "/appearance",
    );
    expect(screen.queryByRole("combobox")).not.toBeInTheDocument();
    expect(screen.getByText("Atlas")).toBeInTheDocument();
  });
  it("does not claim the runtime is healthy after a failed refresh", () => {
    const view = render(<AppShell>content</AppShell>);
    expect(screen.getByText("Local Runtime")).toBeInTheDocument();
    healthError = true;
    view.rerender(<AppShell>content</AppShell>);
    expect(screen.getByText("Runtime Unavailable")).toBeInTheDocument();
    expect(screen.queryByText("Local Runtime")).not.toBeInTheDocument();
  });
  it("closes the mobile drawer on navigation and restores focus", async () => {
    const user = userEvent.setup();
    const view = render(<AppShell>content</AppShell>);
    const desktop = screen.getByRole("complementary");
    const toggle = screen.getByRole("button", { name: "Open navigation" });
    await user.click(toggle);
    const dialog = screen.getByRole("dialog", { name: "Navigation" });
    within(dialog).getByRole("link", { name: "Runs" }).focus();
    routerLocation = { pathname: "/agents" };
    view.rerender(<AppShell>content</AppShell>);
    expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
    expect(screen.getByRole("complementary")).toBe(desktop);
    await waitFor(() => expect(toggle).toHaveFocus());
  });
  it("makes full-app preview reversible without a global selector", async () => {
    previewing = true;
    render(<AppShell>content</AppShell>);
    expect(screen.getByRole("status")).toHaveTextContent(
      "Only you can see this preview.",
    );
    expect(
      screen.getByRole("link", { name: "Back to Editor" }),
    ).toHaveAttribute("href", "/appearance/create");
    await userEvent.click(screen.getByRole("button", { name: "Exit Preview" }));
    expect(cancelPreview).toHaveBeenCalledOnce();
  });
});
