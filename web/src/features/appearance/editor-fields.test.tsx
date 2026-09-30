import { useState } from "react";
import { fireEvent, render, screen } from "@testing-library/react";
import { expect, it } from "vitest";
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
  expect(screen.getByLabelText("Responsibility")).toHaveValue("builder");
  expect(
    screen.getByText(/generated replies stay the same/),
  ).toBeInTheDocument();
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
