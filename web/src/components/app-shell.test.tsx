import { render, screen, within, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { AppShell } from "./app-shell";

let routerLocation = { pathname: "/runs" };
let previewing = false;
let healthError = false;
let themeError: string | null = null;
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
    error: themeError,
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
  afterEach(() => vi.restoreAllMocks());
  beforeEach(() => {
    localStorage.clear();
    routerLocation = { pathname: "/runs" };
    previewing = false;
    healthError = false;
    themeError = null;
    cancelPreview.mockClear();
  });
  it("remembers a collapsed desktop rail while keeping every destination named", async () => {
    const user = userEvent.setup();
    const view = render(<AppShell>content</AppShell>);
    const sidebar = screen.getByRole("complementary", { name: "Workspace navigation" });
    const toggle = within(sidebar).getByRole("button", { name: "Collapse navigation" });
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    await user.click(toggle);
    expect(within(sidebar).getByRole("button", { name: "Expand navigation" })).toHaveAttribute("aria-expanded", "false");
    for (const name of ["Overview", "Planning", "Runs", "Agents", "Analytics", "Connections", "Appearance"]) {
      expect(within(sidebar).getByRole("link", { name })).toHaveAttribute("title", name);
    }
    expect(within(sidebar).getByRole("link", { name: "Runs" })).toHaveAttribute("aria-current", "page");
    expect(toggle).toHaveFocus();
    view.unmount();
    render(<AppShell>reloaded content</AppShell>);
    expect(screen.getByRole("button", { name: "Expand navigation" })).toBeInTheDocument();
    await user.click(screen.getByRole("button", { name: "Expand navigation" }));
    expect(screen.getByRole("button", { name: "Collapse navigation" })).toBeInTheDocument();
  });
  it("keeps mobile navigation expanded independently from the desktop preference", async () => {
    const user = userEvent.setup();
    render(<AppShell>content</AppShell>);
    await user.click(screen.getByRole("button", { name: "Collapse navigation" }));
    await user.click(screen.getByRole("button", { name: "Open navigation" }));
    const drawer = screen.getByRole("dialog", { name: "Navigation" });
    expect(within(drawer).getByRole("link", { name: "Planning" }).textContent).toBe("Planning");
    expect(within(drawer).getByText("Local Workspace")).toBeInTheDocument();
    expect(within(drawer).queryByRole("button", { name: "Expand navigation" })).not.toBeInTheDocument();
  });
  it("still collapses when browser preference storage is unavailable", async () => {
    vi.spyOn(Storage.prototype, "getItem").mockImplementation(() => { throw new Error("Storage unavailable"); });
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new Error("Storage unavailable"); });
    render(<AppShell>content</AppShell>);
    await userEvent.click(screen.getByRole("button", { name: "Collapse navigation" }));
    expect(screen.getByRole("button", { name: "Expand navigation" })).toBeInTheDocument();
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
  it("exposes theme fallback from work screens without repeating the appearance error", () => {
    themeError = "Your selected theme is unavailable. Your saved preference is preserved.";
    const view = render(<AppShell>current work</AppShell>);
    expect(screen.getByRole("alert")).toHaveTextContent(themeError);
    expect(screen.getByRole("link", { name: "Review Appearance" })).toHaveAttribute("href", "/appearance");
    expect(screen.getByText("current work")).toBeInTheDocument();
    routerLocation = { pathname: "/appearance" };
    view.rerender(<AppShell>appearance controls</AppShell>);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
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
