export type RuntimeMode = "local" | "central" | "hosted-project";

export function runtimeMode(root: Document = document): RuntimeMode {
  const mode = root.querySelector<HTMLMetaElement>('meta[name="kingdom-runtime-mode"]')?.content;
  return mode === "central" || mode === "hosted-project" ? mode : "local";
}

export function appBase(root: Document = document): string {
  if (runtimeMode(root) !== "hosted-project") return "/";
  const base = root.querySelector<HTMLMetaElement>('meta[name="kingdom-app-base"]')?.content ?? "";
  if (!/^\/projects\/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(base)) {
    throw new Error("Kingdom project routing is not configured.");
  }
  return base;
}
