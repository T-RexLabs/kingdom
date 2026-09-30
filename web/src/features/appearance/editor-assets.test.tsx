import { act, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, expect, it, vi } from "vitest";
import * as api from "./theme-api";
import { EditorAssets } from "./editor-assets";
import { newDraft, parseDraft } from "./draft-store";

afterEach(() => vi.restoreAllMocks());
it("does not overwrite intervening edits after an asynchronous asset read", async () => {
  let complete!: (value: string) => void;
  vi.spyOn(api, "fileBase64").mockImplementation(() => new Promise((resolve) => { complete = resolve; }));
  let current = 1;
  const change = vi.fn();
  render(<EditorAssets manifest={parseDraft(newDraft().source)!} assets={{}} disabled={false} generation={1} currentGeneration={() => current} change={change} />);
  fireEvent.change(screen.getByLabelText("Upload theme asset"), { target: { files: [new File(["<svg/>"], "mark.svg", { type: "image/svg+xml" })] } });
  current = 2;
  await act(async () => complete("encoded"));
  expect(change).not.toHaveBeenCalled();
  expect(screen.getByRole("alert")).toHaveTextContent("draft changed while reading");
});
