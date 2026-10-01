import { render, screen } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { builtInThemes } from "@/theme";
import { resolvedManifest, themePackage } from "@/theme/contract";
import { ThemePreviewCard } from "./theme-preview-card";

vi.mock("lucide-react/dynamic", () => ({ iconNames: ["circle", "flask-conical", "crown"], DynamicIcon: ({ name }: { name: string }) => <span data-testid="package-icon">{name}</span> }));
it("keeps High and Ludicrous Mode previews independent of each other and the active document", () => {
  const activeOrnamentation = document.documentElement.getAttribute("data-ornamentation");
  document.documentElement.dataset.ornamentation = "balanced";
  const high = resolvedManifest({ ...builtInThemes[0], name: "High Sample", surfaces: { ...builtInThemes[0].surfaces, ornamentation: "high" } });
  const ludicrous = resolvedManifest({ ...builtInThemes[0], name: "Ludicrous Sample", surfaces: { ...builtInThemes[0].surfaces, ornamentation: "ludicrous" } });
  try {
    const view = render(<><ThemePreviewCard manifest={high} /><ThemePreviewCard manifest={ludicrous} /></>);
    const highSample = screen.getByLabelText("High Sample presentation sample");
    const ludicrousSample = screen.getByLabelText("Ludicrous Sample presentation sample");
    expect(highSample).toHaveAttribute("data-ornamentation", "high");
    expect(ludicrousSample).toHaveAttribute("data-ornamentation", "ludicrous");
    expect(document.documentElement).toHaveAttribute("data-ornamentation", "balanced");
    view.rerender(<><ThemePreviewCard manifest={{ ...high, surfaces: { ...high.surfaces, ornamentation: "minimal" } }} /><ThemePreviewCard manifest={ludicrous} /></>);
    expect(highSample).toHaveAttribute("data-ornamentation", "minimal");
    expect(ludicrousSample).toHaveAttribute("data-ornamentation", "ludicrous");
    expect(document.documentElement).toHaveAttribute("data-ornamentation", "balanced");
    view.unmount();
    expect(document.documentElement).toHaveAttribute("data-ornamentation", "balanced");
  } finally {
    if (activeOrnamentation === null) delete document.documentElement.dataset.ornamentation;
    else document.documentElement.setAttribute("data-ornamentation", activeOrnamentation);
  }
});

it("uses each package's typography and brand icon without changing the active document theme", () => {
  const lab = builtInThemes.find((theme) => theme.id === "software-lab")!;
  const theme = { ...structuredClone(lab), icons: { ...lab.icons, brand: "lucide:flask-conical" } };
  document.documentElement.dataset.theme = "another-active-theme";
  render(<ThemePreviewCard manifest={theme} compact />);
  expect(screen.getByTestId("package-icon")).toHaveTextContent("flask-conical");
  const sample = screen.getByLabelText(`${theme.name} presentation sample`);
  expect(sample.style.getPropertyValue("--theme-font-display")).toContain("DM Sans");
  expect(document.documentElement.dataset.theme).toBe("another-active-theme");
  delete document.documentElement.dataset.theme;
});
it("renders a custom brand from its exact immutable revision asset", () => {
  const manifest = { ...structuredClone(builtInThemes[0]), id: "custom", assets: { mark: { path: "assets/mark.svg", mediaType: "image/svg+xml" as const } }, icons: { ...builtInThemes[0].icons, brand: "asset:mark" } };
  const pkg = themePackage({ manifest, resolvedDigest: "a".repeat(64), sourceDigest: "b".repeat(64) });
  const view = render(<ThemePreviewCard manifest={pkg.manifest} pkg={pkg} />);
  expect(view.container.querySelector("img")).toHaveAttribute("src", `/api/v1/themes/custom/revisions/${"a".repeat(64)}/assets/mark`);
});
it("keeps private validation bytes paired with their corresponding preview", () => {
  const manifest = { ...structuredClone(builtInThemes[0]), id: "private", assets: { mark: { path: "assets/mark.svg", mediaType: "image/svg+xml" as const } }, icons: { ...builtInThemes[0].icons, brand: "asset:mark" } };
  const pkg = themePackage({ manifest, resolvedDigest: "a".repeat(64), sourceDigest: "b".repeat(64) });
  const encoded = btoa('<svg xmlns="http://www.w3.org/2000/svg"/>');
  const view = render(<ThemePreviewCard manifest={pkg.manifest} pkg={pkg} files={{ "assets/mark.svg": encoded }} />);
  expect(view.container.querySelector("img")).toHaveAttribute("src", `data:image/svg+xml;base64,${encoded}`);
});

it("isolates flat sample text from classic chrome and leaves classic bevels application-owned", () => {
  const flat = builtInThemes.find((theme) => theme.id === "obsidian-court")!;
  const classic = builtInThemes.find((theme) => theme.id === "workstation-98")!;
  const view = render(<ThemePreviewCard manifest={flat} compact />);
  expect(screen.getByText("Theme Sample").parentElement).toHaveStyle({ color: flat.tokens["text.secondary"] });
  expect(screen.getByText("Plan").style.borderRadius).toBe("var(--theme-radius)");
  expect(screen.getByLabelText(`${flat.name} presentation sample`)).toHaveAttribute("data-chrome", "flat");
  view.rerender(<ThemePreviewCard manifest={classic} compact />);
  expect(screen.getByText("Theme Sample").parentElement?.style.color).toBe("");
  expect(screen.getByText("Plan").style.borderColor).toBe("");
  expect(screen.getByLabelText(`${classic.name} presentation sample`)).toHaveAttribute("data-chrome", "classic");
});


it("previews surface settings and native controls inside the draft boundary", () => {
  const manifest = structuredClone(builtInThemes[0]);
  manifest.surfaces = { ...manifest.surfaces, borderWidth: 6, radius: 12, elevation: "soft", ornamentation: "balanced" };
  manifest.colorScheme = "light";
  const view = render(<ThemePreviewCard manifest={manifest} />);
  const sample = screen.getByLabelText(`${manifest.name} presentation sample`);
  expect(sample).toHaveClass("theme-overlay");
  expect(sample.style.getPropertyValue("--theme-border-width")).toBe("6px");
  expect(sample.style.getPropertyValue("--theme-radius")).toBe("12px");
  expect(sample.style.getPropertyValue("--theme-shadow")).not.toBe("none");
  expect(sample).toHaveAttribute("data-ornamentation", "balanced");
  expect(sample.style.colorScheme).toBe("light");
  expect(screen.getByLabelText("Checkbox")).toHaveAttribute("type", "checkbox");
  expect(screen.getByLabelText("Date")).toHaveAttribute("type", "date");
  expect(view.container.querySelector(".theme-ornament")).not.toBeNull();
  view.rerender(<ThemePreviewCard manifest={{ ...manifest, colorScheme: "dark", surfaces: { ...manifest.surfaces, elevation: "flat", ornamentation: "minimal" } }} />);
  expect(sample.style.getPropertyValue("--theme-shadow")).toBe("none");
  expect(sample.style.colorScheme).toBe("dark");
  expect(sample).toHaveAttribute("data-ornamentation", "minimal");
});

it("updates body, display and monospace typography inside its scoped preview", () => {
  const manifest = structuredClone(builtInThemes[0]);
  manifest.typography = { body: "bundled:jetbrains-mono", display: "bundled:system-sans", mono: "bundled:classic-sans", bodyWeight: 500, headingWeight: 600 };
  const view = render(<ThemePreviewCard manifest={manifest} context="controls" />);
  const sample = screen.getByLabelText(`${manifest.name} presentation sample`);
  expect(sample).toHaveStyle({ fontWeight: "var(--theme-weight-body)" });
  expect(sample.style.getPropertyValue("--theme-font-body")).toContain("JetBrains Mono");
  expect(sample.style.getPropertyValue("--theme-font-display")).toContain("system-ui");
  expect(screen.getByText("const progress = 98;")).toHaveStyle({ fontFamily: "var(--theme-font-mono)" });
  view.rerender(<ThemePreviewCard manifest={{ ...manifest, typography: { ...manifest.typography, bodyWeight: 400 } }} context="controls" />);
  expect(sample.style.getPropertyValue("--theme-weight-body")).toBe("400");
});
it("previews the selected role and its validated portrait, rather than always Builder", () => {
  const manifest = structuredClone(builtInThemes[0]);
  manifest.identities.reviewer = { name: "Test Reviewer", description: "A changed responsibility description.", lore: "New role lore.", portrait: "asset:portrait" };
  manifest.icons["role.reviewer"] = "lucide:flask-conical";
  manifest.assets.portrait = { path: "assets/portrait.png", mediaType: "image/png" };
  const pkg = themePackage({ manifest, sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
  render(<ThemePreviewCard manifest={manifest} pkg={pkg} files={{ "assets/portrait.png": btoa("validated image") }} role="reviewer" context="agent" />);
  expect(screen.getByText("Test Reviewer")).toBeInTheDocument();
  expect(screen.getByText("A changed responsibility description.")).toBeInTheDocument();
  expect(screen.getByText("New role lore.")).toBeInTheDocument();
  expect(screen.getByRole("img", { name: "Test Reviewer portrait" })).toHaveAttribute("src", `data:image/png;base64,${btoa("validated image")}`);
  expect(screen.getByRole("img", { name: "Reviewer icon" })).toHaveTextContent("flask-conical");
});
it("shows authored copy and artwork in selectable contexts with keyboard navigation", async () => {
  const user = userEvent.setup();
  const manifest = structuredClone(builtInThemes[0]);
  manifest.copy["overview.welcome"] = "A Changed Welcome";
  manifest.copy["planning.emptyTitle"] = "An Updated Planning Start";
  manifest.copy["planning.emptyDescription"] = "A changed planning description.";
  manifest.assets.scene = { path: "assets/scene.png", mediaType: "image/png" };
  manifest.artwork = { overview: "asset:scene", planningEmpty: "asset:scene", agentBackdrop: "asset:scene" };
  const pkg = themePackage({ manifest, sourceDigest: "a".repeat(64), resolvedDigest: "b".repeat(64) });
  render(<ThemePreviewCard manifest={manifest} pkg={pkg} context="overview" />);
  expect(screen.getByText("A Changed Welcome")).toBeInTheDocument();
  expect(screen.getByRole("img", { name: "Overview artwork" })).toHaveAttribute("src", `/api/v1/themes/${manifest.id}/revisions/${"b".repeat(64)}/assets/scene`);
  screen.getByRole("tab", { name: "Overview" }).focus();
  await user.keyboard("{ArrowRight}");
  expect(screen.getByRole("tab", { name: "Planning" })).toHaveFocus();
  expect(screen.getByText("An Updated Planning Start")).toBeInTheDocument();
  expect(screen.getByText("A changed planning description.")).toBeInTheDocument();
  expect(screen.getByRole("img", { name: "Planning artwork" })).toBeInTheDocument();
  expect(screen.queryByRole("img", { name: "Planning icon" })).not.toBeInTheDocument();
  expect(screen.getByRole("img", { name: "Planning navigation icon" })).toBeInTheDocument();
  await user.click(screen.getByRole("tab", { name: "Agent" }));
  expect(screen.getByRole("img", { name: "Agent backdrop artwork" })).toBeInTheDocument();
});
