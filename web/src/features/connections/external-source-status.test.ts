import { describe, expect, it } from "vitest";
import { externalSourceFailureGuidance, externalSourceStatusNotice } from "./external-source-status";

describe("externalSourceStatusNotice", () => {
  // The whole point of the notice: a source speaks only when the operator has
  // something to do about it. Everything else is silence.
  it.each(["never_checked", "unchanged", "superseded"])("stays silent for %s", (status) => {
    expect(externalSourceStatusNotice(status)).toBeUndefined();
  });

  it("announces an available update regardless of the recorded status", () => {
    expect(externalSourceStatusNotice("unchanged", true)).toBe("Update Available");
    expect(externalSourceStatusNotice("never_checked", true)).toBe("Update Available");
    expect(externalSourceStatusNotice("changed")).toBe("Update Available");
  });

  it("names every failure the operator has to act on", () => {
    expect(externalSourceStatusNotice("revoked")).toBe("Access Revoked");
    expect(externalSourceStatusNotice("deleted")).toBe("Issue Deleted");
    expect(externalSourceStatusNotice("rate_limited")).toBe("Check Delayed");
  });

  it("falls back rather than rendering a raw provider status", () => {
    expect(externalSourceStatusNotice("something_new_from_the_api")).toBe("Check Failed");
  });
});

describe("externalSourceFailureGuidance", () => {
  it("guides only the statuses a source can recover from", () => {
    expect(externalSourceFailureGuidance("revoked")).toMatch(/health check/);
    expect(externalSourceFailureGuidance("unchanged")).toBeUndefined();
    expect(externalSourceFailureGuidance("never_checked")).toBeUndefined();
  });
});
