import { describe, expect, it } from "vitest";
import { safeGitHubInstallationManageURL, safeGitHubIssueURL } from "./github-url";

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

describe("safeGitHubInstallationManageURL", () => {
  it("preselects the account that owns the installation", () => {
    expect(safeGitHubInstallationManageURL("kingdom-issue-importer", "10")).toBe(
      "https://github.com/apps/kingdom-issue-importer/installations/new/permissions?suggested_target_id=10",
    );
  });

  it("builds the same shape for an organization account", () => {
    expect(safeGitHubInstallationManageURL("kingdom-issue-importer", "98765432")).toBe(
      "https://github.com/apps/kingdom-issue-importer/installations/new/permissions?suggested_target_id=98765432",
    );
  });

  it.each([
    ["empty slug", "", "10"],
    ["uppercase slug", "Kingdom", "10"],
    ["slug with a path separator", "kingdom/evil", "10"],
    ["slug with a query separator", "kingdom?x=1", "10"],
    ["slug starting with a hyphen", "-kingdom", "10"],
    ["overlong slug", "k".repeat(101), "10"],
    ["empty account id", "kingdom", ""],
    ["non-numeric account id", "kingdom", "10a"],
    ["zero-leading account id", "kingdom", "010"],
    ["account id with a query separator", "kingdom", "10&scope=all"],
    ["overlong account id", "kingdom", "1".repeat(33)],
  ])("rejects %s", (_label, slug, accountID) => {
    expect(safeGitHubInstallationManageURL(slug, accountID)).toBeNull();
  });
});
