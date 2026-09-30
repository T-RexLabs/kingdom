import { useState } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
import userEvent from "@testing-library/user-event";
import { EditorColors, EditorIdentity } from "./editor-fields";
import { newDraft, parseDraft } from "./draft-store";

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
  await user.click(screen.getByRole("button", { name: "Heading Weight" }));
  await user.click(screen.getByRole("option", { name: "600" }));
  await user.click(screen.getByRole("button", { name: "Border Width" }));
  await user.click(screen.getByRole("option", { name: "2 Pixels" }));
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
