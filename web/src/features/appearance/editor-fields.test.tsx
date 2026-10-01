import { useState } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { EditorColors, EditorIdentity } from "./editor-fields";
import { newDraft, parseDraft } from "./draft-store";
import { defaultTheme } from "@/theme";

it("offers four ornamentation levels and changes only the selected setting", async () => {
  const user = userEvent.setup();
  const initial = parseDraft(newDraft().source)!;
  initial.surfaces = { radius: 12, borderWidth: 2, elevation: "soft", chrome: "classic", iconRendering: "original" };
  initial.palette = { accent: "#c5aa77" };
  function Editor() {
    const [manifest, change] = useState(initial);
    return <><EditorColors manifest={manifest} disabled={false} change={change} /><output data-testid="draft">{JSON.stringify(manifest)}</output></>;
  }
  render(<Editor />);
  const choices = [
    ["Minimal", "minimal", /plain surfaces/i],
    ["Balanced", "balanced", /small accent lines/i],
    ["High", "high", /stronger header accents/i],
    ["Ludicrous Mode", "ludicrous", /mountain kingdom/i],
  ] as const;
  for (const [label, value, description] of choices) {
    await user.click(screen.getByRole("button", { name: "Ornamentation" }));
    expect(screen.getAllByRole("option").map((option) => option.textContent)).toEqual(choices.map(([name]) => name));
    await user.click(screen.getByRole("option", { name: label }));
    expect(screen.getByRole("button", { name: "Ornamentation" })).toHaveTextContent(label);
    expect(screen.getByText(description)).toBeInTheDocument();
    expect(JSON.parse(screen.getByTestId("draft").textContent!)).toEqual({ ...initial, surfaces: { ...initial.surfaces, ornamentation: value } });
  }
});

it("describes inherited ornamentation and prevents changes while the editor is disabled", async () => {
  const user = userEvent.setup();
  const change = vi.fn();
  const manifest = parseDraft(newDraft().source)!;
  const resolved = { ...defaultTheme, surfaces: { ...defaultTheme.surfaces, ornamentation: "ludicrous" as const } };
  render(<EditorColors manifest={manifest} resolved={resolved} disabled change={change} />);
  const selector = screen.getByRole("button", { name: "Ornamentation" });
  expect(selector).toHaveTextContent("Ludicrous Mode");
  expect(selector).toBeDisabled();
  expect(screen.getByText(/mountain kingdom/i)).toBeInTheDocument();
  await user.click(selector);
  expect(screen.queryByRole("listbox")).not.toBeInTheDocument();
  expect(change).not.toHaveBeenCalled();
  expect(manifest.surfaces).toBeUndefined();
});

it("lets users clear inherited text before typing a replacement without changing canonical responsibility", () => {
  function Editor() {
    const [manifest, change] = useState(parseDraft(newDraft().source)!);
    return (
      <EditorIdentity
        manifest={manifest}
        role="builder"
        setRole={() => {}}
        disabled={false}
        change={change}
      />
    );
  }
  render(<Editor />);
  const name = screen.getByLabelText("Display Name");
  expect(name).not.toHaveValue("");
  fireEvent.change(name, { target: { value: "" } });
  expect(name).toHaveValue("");
  fireEvent.change(name, { target: { value: "My Builder" } });
  expect(name).toHaveValue("My Builder");
  expect(screen.getByRole("button", { name: "Responsibility" })).toHaveAccessibleDescription("Builder");
  expect(
    screen.getByText(/generated replies stay the same/),
  ).toBeInTheDocument();
});

it("keeps canonical font, numeric, and surface values when choosing through shared controls", async () => {
  const user = userEvent.setup();
  function Editor() {
    const initial = parseDraft(newDraft().source)!;
    initial.assets = { local: { path: "assets/local.woff2", mediaType: "font/woff2" } };
    const [manifest, change] = useState(initial);
    return <><EditorColors manifest={manifest} disabled={false} change={change} /><output data-testid="draft">{JSON.stringify(manifest)}</output></>;
  }
  render(<Editor />);
  await user.click(screen.getByRole("button", { name: "Body Font" }));
  await user.click(screen.getByRole("option", { name: "local (Local Font)" }));
  fireEvent.change(screen.getByRole("spinbutton", { name: /Heading Weight/ }), { target: { value: "600" } });
  fireEvent.change(screen.getByRole("spinbutton", { name: /Border Width/ }), { target: { value: "2" } });
  await user.click(screen.getByRole("button", { name: "Native Control Scheme" }));
  await user.click(screen.getByRole("option", { name: "Light" }));
  await user.click(screen.getByRole("button", { name: "Control Style" }));
  await user.click(screen.getByRole("option", { name: "Classic Desktop" }));
  await user.click(screen.getByRole("button", { name: "Icon Colors" }));
  await user.click(screen.getByRole("option", { name: "Original Artwork" }));
  expect(JSON.parse(screen.getByTestId("draft").textContent!)).toMatchObject({ typography: { body: "asset:local", headingWeight: 600 }, surfaces: { borderWidth: 2, chrome: "classic", iconRendering: "original" }, colorScheme: "light" });
});

it("edits and resets a semantic color without dropping other customizations", () => {
  function Editor() {
    const initial = parseDraft(newDraft().source)!;
    initial.tokens = {
      "text.primary": "#ffffff",
      "status.running.foreground": "#aabbcc",
    };
    initial.palette = { accent: "#d0ad66" };
    const [manifest, change] = useState(initial);
    return (
      <>
        <EditorColors manifest={manifest} disabled={false} change={change} />
        <output data-testid="draft">{JSON.stringify(manifest)}</output>
      </>
    );
  }
  render(<Editor />);
  fireEvent.click(screen.getByText("Advanced Colors"));
  fireEvent.change(screen.getByLabelText("Text Primary Color"), {
    target: { value: "#eeeeee" },
  });
  fireEvent.change(screen.getByLabelText("Accent Color"), {
    target: { value: "#c5aa77" },
  });
  let value = JSON.parse(screen.getByTestId("draft").textContent!);
  expect(value.tokens["text.primary"]).toBe("#eeeeee");
  expect(value.tokens["status.running.foreground"]).toBe("#aabbcc");
  fireEvent.click(
    screen.getByRole("button", { name: "Reset text primary color" }),
  );
  value = JSON.parse(screen.getByTestId("draft").textContent!);
  expect(value.tokens["text.primary"]).toBeUndefined();
  expect(value.tokens["status.running.foreground"]).toBe("#aabbcc");
  expect(value.palette.accent).toBe("#c5aa77");
  expect(
    screen.getByRole("button", { name: "Reset text primary color" }),
  ).toBeDisabled();
});


it("lets numeric surface values be cleared and replaced, including wider borders", async () => {
  const user = userEvent.setup();
  function Editor() {
    const [manifest, change] = useState(parseDraft(newDraft("workstation-98").source)!);
    return <><EditorColors manifest={manifest} disabled={false} change={change} /><output data-testid="draft">{JSON.stringify(manifest)}</output></>;
  }
  render(<Editor />);
  const radius = screen.getByRole("spinbutton", { name: /Corner Radius/ });
  await user.clear(radius);
  expect(radius).toHaveValue(null);
  await user.type(radius, "12");
  expect(radius).toHaveValue(12);
  const border = screen.getByRole("spinbutton", { name: /Border Width/ });
  await user.clear(border);
  expect(border).toHaveValue(null);
  await user.type(border, "6");
  expect(border).toHaveValue(6);
  expect(JSON.parse(screen.getByTestId("draft").textContent!)).toMatchObject({ surfaces: { radius: 12, borderWidth: 6 } });
});

it("uses the same editable numeric control for every number setting", async () => {
  const user = userEvent.setup();
  function Editor() {
    const [manifest, change] = useState(parseDraft(newDraft().source)!);
    return <EditorColors manifest={manifest} disabled={false} change={change} />;
  }
  render(<Editor />);
  for (const [name, value] of [["Heading Weight", "600"], ["Body Weight", "500"], ["Corner Radius", "8"], ["Border Width", "4"]]) {
    const input = screen.getByRole("spinbutton", { name: new RegExp(name) });
    expect(input).toHaveClass("appearance-input");
    expect(screen.queryByRole("button", { name })).not.toBeInTheDocument();
    await user.clear(input);
    expect(input).toHaveValue(null);
    await user.type(input, value);
    expect(input).toHaveValue(Number(value));
  }
});

it("allows CSS font weights and uncapped nonnegative surface sizes", () => {
  function Editor() {
    const [manifest, change] = useState(parseDraft(newDraft().source)!);
    return <><EditorColors manifest={manifest} disabled={false} change={change} /><output data-testid="draft">{JSON.stringify(manifest)}</output></>;
  }
  render(<Editor />);
  for (const [name, value] of [["Heading Weight", "925.5"], ["Body Weight", "250"], ["Corner Radius", "48.5"], ["Border Width", "12.5"]]) {
    const input = screen.getByRole("spinbutton", { name: new RegExp(name) }) as HTMLInputElement;
    fireEvent.change(input, { target: { value } });
    expect(input.validity.valid, `${name} must accept ${value}`).toBe(true);
  }
  const border = screen.getByRole("spinbutton", { name: /Border Width/ });
  fireEvent.change(border, { target: { value: "0" } });
  expect((border as HTMLInputElement).validity.valid).toBe(true);
  expect(screen.getByRole("spinbutton", { name: /Corner Radius/ })).not.toHaveAttribute("max");
  expect(border).not.toHaveAttribute("max");
  expect(JSON.parse(screen.getByTestId("draft").textContent!)).toMatchObject({ typography: { headingWeight: 925.5, bodyWeight: 250 }, surfaces: { radius: 48.5, borderWidth: 0 } });
});
