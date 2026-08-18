import { describe, expect, it } from "vitest";
import { safeGitHubIssueURL } from "./github-url";

describe("safeGitHubIssueURL", () => {
  it("accepts only the exact GitHub issue provenance URL", () => {
    expect(safeGitHubIssueURL("https://github.com/owner/repo/issues/7", "owner/repo", 7)).toBe("https://github.com/owner/repo/issues/7");
    for (const value of [
      "javascript:alert(1)",
      "http://github.com/owner/repo/issues/7",
      "https://github.com.evil/owner/repo/issues/7",
      "https://user@github.com/owner/repo/issues/7",
      "https://github.com:444/owner/repo/issues/7",
      "https://github.com/owner/repo/issues/7?token=secret",
      "https://github.com/owner/repo/issues/7#fragment",
      "https://github.com/owner/other/issues/7",
      "https://github.com/owner/repo/issues/8",
    ]) expect(safeGitHubIssueURL(value, "owner/repo", 7)).toBeNull();
  });
});
