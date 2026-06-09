import { describe, expect, it } from "vitest";
import { parseStreamingQuestion } from "./parse-streaming-question";

describe("parseStreamingQuestion", () => {
  it("returns full text when no block is present", () => {
    const result = parseStreamingQuestion("Just some prose text.");
    expect(result.content).toBe("Just some prose text.");
    expect(result.question).toBeNull();
  });

  it("returns text before block when block is incomplete", () => {
    const raw = "Context here.\n\n```structured_question\n{\"prompt\": \"Choose";
    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Context here.");
    expect(result.question).toBeNull();
  });

  it("hides a trailing partial structured_question opener before the full tag arrives", () => {
    const raw = "Context here.\n\n```str";
    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Context here.");
    expect(result.question).toBeNull();
  });

  it("never leaks partial structured-question JSON while the block is still streaming", () => {
    const raw = "Context here.\n\n```structured_question\n{\"prompt\":\"Cho";
    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Context here.");
    expect(result.question).toBeNull();
  });

  it("parses complete block and strips trailing prompt from prose", () => {
    const raw = [
      "Got it — cursor-based is more scalable.",
      "",
      "Which pagination style should the endpoint use?",
      "",
      "```structured_question",
      '{"prompt": "Which pagination style should the endpoint use?", "type": "single_select", "options": [{"label": "Page", "value": "page"}]}',
      "```",
    ].join("\n");

    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Got it — cursor-based is more scalable.");
    expect(result.question).not.toBeNull();
    expect(result.question?.prompt).toBe("Which pagination style should the endpoint use?");
  });

  it("returns empty content when entire prose is the question prompt", () => {
    const raw = [
      "Which style?",
      "",
      "```structured_question",
      '{"prompt": "Which style?", "type": "single_select", "options": [{"label": "A", "value": "a"}]}',
      "```",
    ].join("\n");

    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("");
    expect(result.question).not.toBeNull();
  });

  it("preserves prose when prompt is not at the end", () => {
    const raw = [
      "Which style? Here is more context about the decision.",
      "",
      "```structured_question",
      '{"prompt": "Which style?", "type": "single_select", "options": [{"label": "A", "value": "a"}]}',
      "```",
    ].join("\n");

    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Which style? Here is more context about the decision.");
    expect(result.question).not.toBeNull();
  });

  it("returns prose without block on invalid JSON", () => {
    const raw = "Context.\n\n```structured_question\n{invalid json}\n```";
    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Context.");
    expect(result.question).toBeNull();
  });

  it("returns full text when parsed question has no prompt", () => {
    const raw = "Context.\n\n```structured_question\n{\"type\": \"yes_no\"}\n```";
    const result = parseStreamingQuestion(raw);
    expect(result.question).toBeNull();
  });

  it("rejects unsupported question types instead of leaking them through the UI", () => {
    const raw = [
      "Context.",
      "",
      "```structured_question",
      '{"prompt": "Choose scope", "type": "pick_many"}',
      "```",
    ].join("\n");

    const result = parseStreamingQuestion(raw);

    expect(result.content).toBe("Context.");
    expect(result.question).toBeNull();
  });

  it("normalizes valid options and drops malformed option entries", () => {
    const raw = [
      "Context.",
      "",
      "Choose scope?",
      "",
      "```structured_question",
      '{"prompt": "Choose scope?", "type": "single_select", "options": [{"label": "Lean", "value": "lean"}, "Thorough", {"label": "", "value": ""}, 7]}',
      "```",
    ].join("\n");

    const result = parseStreamingQuestion(raw);

    expect(result.content).toBe("Context.");
    expect(result.question).toEqual({
      prompt: "Choose scope?",
      type: "single_select",
      options: [
        { label: "Lean", value: "lean" },
        { label: "Thorough", value: "Thorough" },
      ],
    });
  });

  it("handles prompt with different punctuation in prose", () => {
    const raw = [
      "Some context.",
      "",
      "**Which style??**",
      "",
      "```structured_question",
      '{"prompt": "Which style?", "type": "single_select", "options": [{"label": "A", "value": "a"}]}',
      "```",
    ].join("\n");

    const result = parseStreamingQuestion(raw);
    expect(result.content).toBe("Some context.");
    expect(result.question).not.toBeNull();
  });
});
