import { describe, expect, it } from "vitest";
import { developmentServer } from "./dev-backend";

describe("development backend", () => {
  it.each([
    undefined,
    "",
    "not a URL",
    "http://127.0.0.1",
    "http://127.0.0.1:8080",
    "http://localhost:8080",
    "http://[::1]:8080",
    "https://127.0.0.1:18080",
    "http://0.0.0.0:18080",
    "http://example.com:18080",
    "http://localhost.example.com:18080",
    "http://user:secret@127.0.0.1:18080",
    "http://127.0.0.1:18080/api/v1",
    "http://127.0.0.1:18080/?token=secret",
    "http://127.0.0.1:18080/#fragment",
  ])("rejects an unsafe or missing backend: %s", (backend) => {
    expect(() =>
      developmentServer(
        { command: "serve" },
        { KINGDOM_DEV_BACKEND_URL: backend },
      ),
    ).toThrow("KINGDOM_DEV_BACKEND_URL must be the HTTP URL of a sandbox runtime");
  });

  it.each(["127.0.0.1", "localhost", "[::1]"])(
    "routes API requests to the selected loopback sandbox: %s",
    (host) => {
      const target = `http://${host}:18081`;
      expect(
        developmentServer(
          { command: "serve" },
          { KINGDOM_DEV_BACKEND_URL: `${target}/` },
        ),
      ).toEqual({
        host: "127.0.0.1",
        port: 5173,
        proxy: {
          "/api": { target, changeOrigin: false },
          "/healthz": { target, changeOrigin: false },
        },
      });
    },
  );

  it.each([undefined, "http://127.0.0.1:8080"])(
    "does not require or validate a runtime during builds, preview, or tests: %s",
    (backend) => {
      const environment = { KINGDOM_DEV_BACKEND_URL: backend };
      expect(developmentServer({ command: "build" }, environment)).toBeUndefined();
      expect(
        developmentServer({ command: "serve", isPreview: true }, environment),
      ).toBeUndefined();
      expect(
        developmentServer({ command: "serve" }, { ...environment, VITEST: "true" }),
      ).toBeUndefined();
    },
  );
});
