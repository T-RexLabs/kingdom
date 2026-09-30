import { render, screen } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import { builtInThemes } from "@/theme";
import { themePackage } from "@/theme/contract";
import { ThemePreviewCard } from "./theme-preview-card";

vi.mock("lucide-react/dynamic", () => ({ iconNames: ["circle", "flask-conical", "crown"], DynamicIcon: ({ name }: { name: string }) => <span data-testid="package-icon">{name}</span> }));
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
