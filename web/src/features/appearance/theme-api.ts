import { API_BASE, ApiError, apiFetch, buildAuthHeaders, signalUnauthorized } from "@/api/client";
import type { components } from "@/api/schema";
import { newIdempotencyKey } from "@/lib/idempotency";
import { themePackage, type ThemeReference, type ThemeCatalog } from "@/theme/contract";

export type ThemeSource = components["schemas"]["ThemeSource"];
export type ThemeDiagnostic = components["schemas"]["ThemeDiagnostic"];
export type ThemeDraft = components["schemas"]["ThemeDraftRequest"];
export type SharedThemeRequest = { method: "POST" | "PUT" | "DELETE"; path: string; body?: unknown; verify?: ThemeReference };
export const themeAPI = {
  catalog: () => apiFetch<ThemeCatalog>("/themes"),
  validateArchive: async (archive: string) => themePackage(await apiFetch<components["schemas"]["ThemePackage"]>("/themes/validate-archive", { method: "POST", body: JSON.stringify({ archive }), headers: { "Idempotency-Key": newIdempotencyKey("theme-archive-preview") } })),
  source: (id: string, revision?: string, signal?: AbortSignal) => apiFetch<ThemeSource>(`/themes/${encodeURIComponent(id)}/source`, { params: { revision }, signal }),
  revision: async (ref: ThemeReference) => themePackage(await apiFetch<components["schemas"]["ThemePackage"]>(`/themes/${encodeURIComponent(ref.id)}/revisions/${ref.resolvedDigest}`), ref),
  validate: async (draft: ThemeDraft, signal?: AbortSignal) => themePackage(await apiFetch<components["schemas"]["ThemePackage"]>("/themes/validate", { method: "POST", body: JSON.stringify(draft), signal, headers: { "Idempotency-Key": newIdempotencyKey("theme-preview") } })),
  mutate: (request: SharedThemeRequest, key: string) => apiFetch<unknown>(request.path, { method: request.method, body: request.body === undefined ? undefined : JSON.stringify(request.body), headers: { "Idempotency-Key": key } }),
};
export function themeError(error: unknown): string { return error instanceof Error ? error.message : "The theme operation could not be completed."; }
export function themeDiagnostics(error: unknown): ThemeDiagnostic[] {
  if (!(error instanceof ApiError) || !Array.isArray(error.details.diagnostics)) return [];
  return error.details.diagnostics.filter((entry): entry is ThemeDiagnostic => !!entry && typeof entry === "object" && typeof entry.path === "string" && typeof entry.message === "string" && typeof entry.code === "string");
}
export function fileBase64(file: File, maxBytes = 8 << 20): Promise<string> {
  if (!file.size || file.size > maxBytes) return Promise.reject(new Error("This file exceeds the supported size limit."));
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result).split(",", 2)[1]);
    reader.onerror = () => reject(new Error("The selected file could not be read."));
    reader.readAsDataURL(file);
  });
}
export function downloadJSON(name: string, value: unknown) {
  const data = typeof value === "string" ? value : JSON.stringify(value, null, 2);
  downloadBlob(name, new Blob([data], { type: "application/json" }));
}
function downloadBlob(name: string, blob: Blob) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url; link.download = name; link.click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
export async function exportTheme(ref: ThemeReference) {
  const response = await fetch(`${API_BASE}/themes/${encodeURIComponent(ref.id)}/revisions/${ref.resolvedDigest}/export`, { credentials: "same-origin", headers: buildAuthHeaders() });
  if (!response.ok) {
    if (response.status === 401) signalUnauthorized();
    throw new Error("The package could not be exported. Refresh the catalog and try again.");
  }
  downloadBlob(`${ref.id}.zip`, await response.blob());
}
