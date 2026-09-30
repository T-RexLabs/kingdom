import { describe, expect, it } from "vitest";
import type { ActivityItem } from "./activity-view";
import { activityToolSummary } from "./activity-tool-summary";

const tool = (overrides: Partial<ActivityItem>): ActivityItem => ({
  id: "p",
  eventId: "e",
  sequence: 1,
  kind: "tool",
  label: "Provider Tool",
  ...overrides,
});
describe("quiet tool summaries", () => {
  it("shows a filename without exposing a full path in the default summary", () => {
    expect(
      activityToolSummary(
        tool({
          toolName: "read",
          inputContent: {
            availability: "available",
            preview: JSON.stringify({
              filePath: "/private/project/src/plan.ts",
            }),
          },
        }),
      ),
    ).toBe("Read plan.ts");
  });
  it("does not use a raw command or a multiline provider title as the action", () => {
    expect(
      activityToolSummary(
        tool({
          toolName: "bash",
          toolTitle: "python3 - <<'PY'\nprint('long output')",
          inputContent: {
            availability: "available",
            preview: '{"command":"python3 script.py"}',
          },
        }),
      ),
    ).toBe("Run Command");
  });
  it("keeps unnamed tools and incomplete inputs honest without inventing a filename", () => {
    expect(
      activityToolSummary(
        tool({
          toolName: "read",
          inputContent: {
            availability: "available",
            preview: '{"filePath":"/partial',
          },
        }),
      ),
    ).toBe("Read");
    expect(activityToolSummary(tool({ label: "Tool Call" }))).toBe("Tool Call");
  });
});
