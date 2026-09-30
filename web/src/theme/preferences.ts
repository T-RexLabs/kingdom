import { validReference, type ThemeReference } from "./contract";

export function preferenceKey(workspaceID: string): string {
  return `kingdom:appearance:v1:${workspaceID}`;
}
export function readPreference(workspaceID: string): ThemeReference | null {
  try {
    const raw = localStorage.getItem(preferenceKey(workspaceID));
    const parsed: unknown = raw ? JSON.parse(raw) : null;
    return validReference(parsed) ? { id: parsed.id, resolvedDigest: parsed.resolvedDigest } : null;
  } catch {
    return null;
  }
}
export function writePreference(workspaceID: string, ref: ThemeReference): boolean {
  if (!validReference(ref)) return false;
  try {
    localStorage.setItem(preferenceKey(workspaceID), JSON.stringify({ id: ref.id, resolvedDigest: ref.resolvedDigest }));
    return true;
  } catch {
    return false;
  }
}
