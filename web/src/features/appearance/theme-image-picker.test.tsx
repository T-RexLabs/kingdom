import { useState } from "react";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { expect, it, vi } from "vitest";
import { builtInThemes, type ThemeManifest } from "@/theme";
import { EditorIdentity } from "./editor-fields";
import { newDraft, parseDraft } from "./draft-store";
import { Modal } from "@/components/ui/modal";
import { ThemeImagePicker } from "./theme-image-picker";

vi.mock("lucide-react/dynamic", () => ({
  iconNames: ["circle", "crown", "hammer", "palette", "axe"],
  DynamicIcon: ({ name }: { name: string }) => <svg data-icon={name} />,
}));

it("previews and searches real icon choices and writes the canonical reference", async () => {
  const user = userEvent.setup();
  function Editor() {
    const [manifest, change] = useState(parseDraft(newDraft().source)!);
    return <><EditorIdentity manifest={manifest} role="builder" setRole={() => {}} disabled={false} change={change} /><output data-testid="draft">{JSON.stringify(manifest)}</output></>;
  }
  render(<Editor />);
  const trigger = screen.getByRole("combobox", { name: "Role Icon" });
  expect(trigger.querySelector("svg[data-icon]")).not.toBeNull();
  await user.click(trigger);
  await user.type(screen.getByRole("textbox", { name: "Search Role Icon" }), "hammer");
  const option = screen.getByRole("option", { name: /^Hammer/ });
  expect(option.querySelector('svg[data-icon="hammer"]')).not.toBeNull();
  await user.keyboard("{ArrowDown}{Enter}");
  expect(JSON.parse(screen.getByTestId("draft").textContent!)).toMatchObject({ icons: { "role.builder": "lucide:hammer" } });
  expect(trigger.querySelector('svg[data-icon="hammer"]')).not.toBeNull();
});

it("bounds browsing, searches the entire catalog, and preserves modal keyboard focus", async () => {
  const user = userEvent.setup();
  const change = vi.fn();
  render(<Modal open onClose={vi.fn()} title="Edit Theme"><ThemeImagePicker id="bounded-icon" label="Icon" value="lucide:crown" kind="icon" theme={builtInThemes[0]} assetURLs={{}} disabled={false} onChange={change} /></Modal>);
  const trigger = screen.getByRole("combobox", { name: "Icon" });
  await user.click(trigger);
  expect(screen.getAllByRole("option")).toHaveLength(48);
  await user.click(screen.getByRole("button", { name: "Show More Choices" }));
  expect(screen.getAllByRole("option")).toHaveLength(96);
  const search = screen.getByRole("textbox", { name: "Search Icon" });
  await user.type(search, "no-such-symbol-qxz");
  expect(screen.getByText("No matching images or symbols.")).toBeInTheDocument();
  await user.clear(search);
  await user.type(search, "zoom-out");
  expect(screen.getAllByRole("option")).toHaveLength(1);
  expect(screen.getByRole("option", { name: "Zoom Out" })).toBeInTheDocument();
  await user.keyboard("{ArrowDown}{Enter}");
  expect(change).toHaveBeenCalledWith("lucide:zoom-out");
  await waitFor(() => expect(trigger).toHaveFocus());
  expect(screen.getByRole("dialog", { name: "Edit Theme" })).toBeInTheDocument();
});

it("offers inherited SVG icons and validated images for brand, portraits and artwork", async () => {
  const user = userEvent.setup();
  const base = builtInThemes.find((theme) => theme.id === "workstation-98")!;
  const resolved = structuredClone(base);
  resolved.assets.portrait = { path: "assets/portrait.png", mediaType: "image/png" };
  const files = { "assets/portrait.png": btoa("validated-image-fixture") };
  function Editor() {
    const [manifest, change] = useState<ThemeManifest>({ ...parseDraft(newDraft("workstation-98").source)!, assets: { portrait: resolved.assets.portrait } });
    return <><EditorIdentity manifest={manifest} assetPreview={{ manifest: resolved, files }} role="builder" setRole={() => {}} disabled={false} change={change} /><output data-testid="draft">{JSON.stringify(manifest)}</output></>;
  }
  render(<Editor />);
  expect(screen.getByRole("combobox", { name: "Role Icon" }).querySelector("img")).not.toBeNull();
  await user.click(screen.getByRole("combobox", { name: "Portrait" }));
  const portrait = screen.getByRole("option", { name: /^Portrait \(Asset\)/ });
  expect(portrait.querySelector("img")).toHaveAttribute("src", `data:image/png;base64,${files["assets/portrait.png"]}`);
  await user.click(portrait);
  expect(screen.getByRole("combobox", { name: "Portrait" }).querySelector("img")).not.toBeNull();
  await user.click(screen.getByText("Navigation and Workflow Icons"));
  await user.click(screen.getByRole("combobox", { name: "Brand" }));
  expect(screen.queryByRole("option", { name: /^Portrait \(Asset\)/ })).not.toBeInTheDocument();
  await user.type(screen.getByRole("textbox", { name: "Search Brand" }), "workstation");
  const logo = screen.getByRole("option", { name: /^Workstation \(Asset\)/ });
  expect(logo.querySelector("img")).not.toBeNull();
  await user.click(logo);
  await user.click(screen.getByText("Artwork"));
  await user.click(screen.getByRole("combobox", { name: "Overview" }));
  await user.click(screen.getByRole("option", { name: /^Portrait \(Asset\)/ }));
  expect(JSON.parse(screen.getByTestId("draft").textContent!)).toMatchObject({ icons: { brand: "asset:workstation" }, identities: { builder: { portrait: "asset:portrait" } }, artwork: { overview: "asset:portrait" } });
  expect(within(screen.getByRole("combobox", { name: "Overview" })).getByText("Portrait (Asset)")).toBeInTheDocument();
});
