import type { ThemeManifest } from "@/theme";

export function objectValue(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
export function optionalStringAt(value: unknown, ...path: string[]): string | undefined {
  let current: unknown = value;
  for (const key of path) current = objectValue(current)[key];
  return typeof current === "string" ? current : undefined;
}
export function stringAt(value: unknown, ...path: string[]): string { return optionalStringAt(value, ...path) ?? ""; }
export function numberAt(value: unknown, slot: string, key: string, fallback: number): number { const number = objectValue(objectValue(value)[slot])[key]; return typeof number === "number" ? number : fallback; }
export function setDraftField(manifest: ThemeManifest, path: string[], value: unknown): ThemeManifest {
  const copy = structuredClone(manifest);
  let target = copy as unknown as Record<string, unknown>;
  for (const key of path.slice(0, -1)) { target[key] = { ...objectValue(target[key]) }; target = target[key] as Record<string, unknown>; }
  if (value === undefined) delete target[path[path.length - 1]];
  else target[path[path.length - 1]] = value;
  return copy;
}

export function referencesAsset(value: unknown, id: string): boolean {
  if (typeof value === "string") return value === `asset:${id}`;
  if (!value || typeof value !== "object") return false;
  return Object.values(value).some((entry) => referencesAsset(entry, id));
}
