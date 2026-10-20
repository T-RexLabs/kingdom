import { describe, expect, it } from "vitest";
import { activityStatusLine } from "./activity-status";
import type { ActivityItem } from "./activity-view";

let sequence = 0;
const tool = (toolName: string, input?: Record<string, unknown>): ActivityItem => ({
  id: `t${++sequence}`, eventId: `t${sequence}`, sequence, kind: "tool", toolName, label: toolName, input: input && JSON.stringify(input),
});
const of = (kind: ActivityItem["kind"]): ActivityItem => ({ id: `k${++sequence}`, eventId: `k${sequence}`, sequence, kind, label: kind });

describe("activityStatusLine", () => {
  it.each<[string, ActivityItem[], string]>([
    ["nothing yet", [], "Thinking…"],
    ["only routine progress", [of("progress")], "Thinking…"],
    ["thinking", [tool("read"), of("thinking")], "Thinking…"],
    ["writing the reply", [tool("read"), of("message")], "Writing the reply…"],
    ["one named file", [tool("read", { filePath: "/repo/internal/planner.go" })], "Reading planner.go…"],
    ["a run of reads", [of("thinking"), tool("read"), tool("read"), tool("read")], "Reading 3 files…"],
    ["only the newest run counts", [tool("read"), tool("grep"), tool("read"), tool("read")], "Reading 2 files…"],
    ["edits", [tool("edit"), tool("edit")], "Editing 2 files…"],
    ["an unnamed write", [tool("write")], "Writing a file…"],
    ["web pages", [tool("webfetch"), tool("webfetch")], "Reading 2 web pages…"],
    ["code search", [tool("grep")], "Searching the code…"],
    ["commands", [tool("shell"), tool("shell"), tool("shell")], "Running 3 commands…"],
    ["an unknown tool", [tool("lsp")], "Using lsp…"],
  ])("%s", (_, items, expected) => {
    expect(activityStatusLine(items)).toBe(expected);
  });
});
