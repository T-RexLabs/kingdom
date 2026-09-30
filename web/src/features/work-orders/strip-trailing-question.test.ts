import { describe, expect, it } from "vitest";
import { normalizeForPromptMatch, stripTrailingQuestion } from "./strip-trailing-question";

describe("normalizeForPromptMatch", () => {
  it("trims whitespace", () => {
    expect(normalizeForPromptMatch("  hello  ")).toBe("hello");
  });

  it("strips trailing punctuation", () => {
    expect(normalizeForPromptMatch("Which style?")).toBe("which style");
    expect(normalizeForPromptMatch("Choose one.")).toBe("choose one");
    expect(normalizeForPromptMatch("Ready!")).toBe("ready");
    expect(normalizeForPromptMatch("Select:")).toBe("select");
    expect(normalizeForPromptMatch("Pick one;")).toBe("pick one");
    expect(normalizeForPromptMatch("Multiple??")).toBe("multiple");
  });

  it("strips wrapping bold markdown", () => {
    expect(normalizeForPromptMatch("**Which style?**")).toBe("which style");
  });

  it("strips wrapping italic markdown", () => {
    expect(normalizeForPromptMatch("*Which style?*")).toBe("which style");
    expect(normalizeForPromptMatch("_Which style?_")).toBe("which style");
  });

  it("lowercases", () => {
    expect(normalizeForPromptMatch("Which Style")).toBe("which style");
  });

  it("handles combined formatting", () => {
    expect(normalizeForPromptMatch("  **Which style?**  ")).toBe("which style");
  });

  it("returns empty string for empty input", () => {
    expect(normalizeForPromptMatch("")).toBe("");
    expect(normalizeForPromptMatch("   ")).toBe("");
  });
});

describe("stripTrailingQuestion", () => {
  it("strips question from last paragraph", () => {
    const prose = "Got it — cursor-based is more scalable.\n\nWhich pagination style should the endpoint use?";
    const prompt = "Which pagination style should the endpoint use?";
    expect(stripTrailingQuestion(prose, prompt)).toBe("Got it — cursor-based is more scalable.");
  });

  it("strips question from last line", () => {
    const prose = "Context here.\nWhich style?";
    const prompt = "Which style?";
    expect(stripTrailingQuestion(prose, prompt)).toBe("Context here.");
  });

  it("returns empty string when entire prose is the question", () => {
    expect(stripTrailingQuestion("Which style?", "Which style?")).toBe("");
  });

  it("handles punctuation differences", () => {
    const prose = "Context.\n\nWhich style?";
    expect(stripTrailingQuestion(prose, "Which style")).toBe("Context.");
    expect(stripTrailingQuestion(prose, "Which style?")).toBe("Context.");
    expect(stripTrailingQuestion(prose, "Which style??")).toBe("Context.");
  });

  it("handles markdown bold in prose", () => {
    const prose = "Context.\n\n**Which style?**";
    expect(stripTrailingQuestion(prose, "Which style?")).toBe("Context.");
  });

  it("handles case differences", () => {
    const prose = "Context.\n\nwhich style?";
    expect(stripTrailingQuestion(prose, "Which Style?")).toBe("Context.");
  });

  it("does not strip when prompt is not at the end", () => {
    const prose = "Which style? Let me explain more context.";
    expect(stripTrailingQuestion(prose, "Which style?")).toBe("Which style? Let me explain more context.");
  });

  it("does not strip partial matches", () => {
    const prose = "Context.\n\nWhich pagination style should the endpoint use for best performance?";
    expect(stripTrailingQuestion(prose, "Which pagination style should the endpoint use?"))
      .toBe("Context.\n\nWhich pagination style should the endpoint use for best performance?");
  });

  it("returns prose unchanged when prompt is undefined", () => {
    expect(stripTrailingQuestion("Hello world", undefined)).toBe("Hello world");
  });

  it("returns prose unchanged when prompt is empty", () => {
    expect(stripTrailingQuestion("Hello world", "")).toBe("Hello world");
    expect(stripTrailingQuestion("Hello world", "   ")).toBe("Hello world");
  });

  it("preserves trailing whitespace style of original prose when no match", () => {
    const prose = "No question here.\n\nJust context.";
    expect(stripTrailingQuestion(prose, "Something else?")).toBe(prose);
  });
});
