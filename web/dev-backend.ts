import type { ConfigEnv, ServerOptions } from "vite";

const backendSetting = "KINGDOM_DEV_BACKEND_URL";

/** Configure only the development server; builds and tests need no runtime. */
export function developmentServer(
  { command, isPreview }: Pick<ConfigEnv, "command" | "isPreview">,
  environment: Record<string, string | undefined>,
): ServerOptions | undefined {
  if (command !== "serve" || isPreview || environment.VITEST === "true") {
    return undefined;
  }

  const input = environment[backendSetting]?.trim();
  const usage = `${backendSetting} must be the HTTP URL of a sandbox runtime on localhost, 127.0.0.1, or [::1], with an explicit port other than 8080 (for example, http://127.0.0.1:18080).`;
  if (!input) throw new Error(usage);

  let backend: URL;
  try {
    backend = new URL(input);
  } catch {
    throw new Error(usage);
  }

  if (
    backend.protocol !== "http:" ||
    !["localhost", "127.0.0.1", "[::1]"].includes(backend.hostname) ||
    !backend.port ||
    backend.port === "8080" ||
    backend.username ||
    backend.password ||
    backend.pathname !== "/" ||
    backend.search ||
    backend.hash
  ) {
    throw new Error(usage);
  }

  return {
    host: "127.0.0.1",
    port: 5173,
    proxy: {
      "/api": { target: backend.origin, changeOrigin: false },
      "/healthz": { target: backend.origin, changeOrigin: false },
    },
  };
}
