import { act, render, screen } from "@testing-library/react";
import { beforeEach, expect, it, vi } from "vitest";
import { ThemeProvider } from "./theme-provider";
import { useTheme } from "./context";
import { builtInThemes, themePackage, type ThemePackage } from "./contract";
import { preferenceKey } from "./preferences";

function Consumer() { const { theme } = useTheme(); return <div data-testid="app">{theme.name}</div>; }
beforeEach(() => localStorage.clear());
it("keeps application children behind the boundary until the pinned choice loads", async () => {
  const pkg = themePackage({ manifest: builtInThemes[3], sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
  localStorage.setItem(preferenceKey("workspace"), JSON.stringify({ id: pkg.manifest.id, resolvedDigest: pkg.resolvedDigest }));
  let resolve!: (value: ThemePackage) => void;
  const pending = new Promise<ThemePackage>((done) => { resolve = done; });
  const view = render(<ThemeProvider client={{ catalog: vi.fn(async () => ({ workspaceID: "workspace", sources: [], items: [{ id: pkg.manifest.id, version: "1.0.0", name: pkg.manifest.name, builtin: true, resolvedDigest: pkg.resolvedDigest }] })), revision: vi.fn(() => pending) }}><Consumer /></ThemeProvider>);
  expect(screen.queryByTestId("app")).not.toBeInTheDocument();
  expect(screen.getByRole("status")).toHaveTextContent("Loading your workspace");
  await act(async () => { resolve(pkg); await pending; });
  expect(screen.getByTestId("app")).toHaveTextContent("Software Lab");
  expect(document.documentElement.style.colorScheme).toBe("light");
  view.unmount();
  expect(document.documentElement.hasAttribute("data-theme")).toBe(false);
});
