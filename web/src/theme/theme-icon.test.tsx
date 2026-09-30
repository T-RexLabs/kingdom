import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import { ManifestIcon } from "./theme-icon";
import { defaultTheme } from "./contract";

function fixture(original: boolean) {
  const theme = structuredClone(defaultTheme);
  theme.assets = { mark: { path: "assets/mark.svg", mediaType: "image/svg+xml" } };
  theme.icons.brand = "asset:mark";
  theme.surfaces.iconRendering = original ? "original" : "mask";
  return theme;
}
it("renders original icon colors as an isolated accessible image and falls back on decoding failure", () => {
  const view = render(<ManifestIcon theme={fixture(true)} assetURLs={{ mark: "/mark.svg" }} slot="brand" title="Kingdom Workstation" />);
  const icon = screen.getByRole("img", { name: "Kingdom Workstation" });
  const image = view.container.querySelector("img")!;
  expect(image).toHaveAttribute("src", "/mark.svg");
  expect(image).not.toHaveAttribute("hidden");
  expect(icon.style.maskImage).toBe("");
  expect(view.container.querySelector("svg")).toBeNull();
  fireEvent.error(image);
  expect(screen.getByRole("img", { name: "Kingdom Workstation" })).toBeVisible();
  expect(view.container.querySelector("img")).toBeNull();
  expect(view.container.querySelector("svg")).not.toBeNull();
});
it("keeps monochrome assets masked and decorative icons out of the accessibility tree", () => {
  const view = render(<ManifestIcon theme={fixture(false)} assetURLs={{ mark: "/mark.svg" }} slot="brand" />);
  expect(view.container.querySelector("span")?.style.maskImage).toContain("/mark.svg");
  expect(view.container.querySelector("img")).toHaveAttribute("hidden");
  expect(screen.queryByRole("img")).not.toBeInTheDocument();
});
