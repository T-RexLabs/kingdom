import { validReference } from "@/theme/contract";
import type { SharedThemeRequest } from "./theme-api";

export type ThemeAttempt = { request: SharedThemeRequest; key: string; origin: string; authBinding: string };
const id = /^[a-z][a-z0-9-]{0,63}$/;
const digest = /^[a-f0-9]{64}$/;
const key = /^kd1\.[0-9]{1,12}\.[0-9]{1,8}\.[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?\.[A-Za-z0-9_-]{22}$/;
function record(value: unknown): value is Record<string, unknown> { return !!value && typeof value === "object" && !Array.isArray(value); }
function keys(value: Record<string, unknown>, names: string[]) { return Object.keys(value).every((name) => names.includes(name)); }
function base64(value: unknown, max: number): value is string { return typeof value === "string" && value.length > 0 && value.length <= max && value.length % 4 === 0 && /^[A-Za-z0-9+/]*={0,2}$/.test(value); }
function assets(value: unknown): boolean {
  if (!record(value) || Object.keys(value).length > 64) return false;
  let total = 0;
  return Object.entries(value).every(([path, content]) => { if (!/^assets\/(?:[A-Za-z0-9_-]+\/)*[A-Za-z0-9_-]+\.(?:svg|png|jpg|jpeg|webp|avif|woff2)$/.test(path) || path.length > 512 || !base64(content, Math.ceil((8 << 20) / 3) * 4)) return false; total += content.length; return total <= Math.ceil((32 << 20) / 3) * 4 + 128; });
}
export function validSharedRequest(value: unknown): value is SharedThemeRequest {
  if (!record(value) || !keys(value, ["method", "path", "body", "verify"]) || typeof value.path !== "string") return false;
  const body = value.body;
  if (value.path === "/themes/import") return value.method === "POST" && record(body) && keys(body, ["archive"]) && base64(body.archive, 44739244) && validReference(value.verify) && keys(value.verify, ["id", "resolvedDigest"]);
  const match = /^\/themes\/([^/]+)(?:\/(source|publish))?$/.exec(value.path);
  if (!match || !id.test(match[1])) return false;
  if (match[2] === "source") {
    if (!record(body) || typeof body.expected_source_digest !== "string" || (body.expected_source_digest !== "" && !digest.test(body.expected_source_digest)) || value.verify !== undefined) return false;
    if (value.method === "DELETE") return keys(body, ["expected_source_digest"]) && digest.test(body.expected_source_digest);
    if (value.method !== "PUT" || !keys(body, ["source", "assets", "expected_source_digest"]) || typeof body.source !== "string" || !body.source.length || new TextEncoder().encode(body.source).length > 256 << 10 || !assets(body.assets)) return false;
    try { const source: unknown = JSON.parse(body.source); return record(source) && source.id === match[1]; } catch { return false; }
  }
  if (match[2] === "publish") return value.method === "POST" && record(body) && keys(body, ["expected_source_digest"]) && typeof body.expected_source_digest === "string" && digest.test(body.expected_source_digest) && validReference(value.verify) && keys(value.verify, ["id", "resolvedDigest"]) && value.verify.id === match[1];
  return value.method === "DELETE" && body === undefined && value.verify === undefined;
}
export function validStoredAttempt(value: unknown): value is ThemeAttempt {
  if (!record(value) || !keys(value, ["request", "key", "origin", "authBinding"]) || !validSharedRequest(value.request) || typeof value.key !== "string" || !key.test(value.key) || typeof value.authBinding !== "string" || !value.authBinding.length || value.authBinding.length > 256 || (/\s/.test(value.authBinding) || [...value.authBinding].some((char) => char.charCodeAt(0) < 32 || char.charCodeAt(0) === 127))) return false;
  if (typeof value.origin !== "string") return false;
  try { const origin = new URL(value.origin); return (origin.protocol === "http:" || origin.protocol === "https:") && origin.origin === value.origin; } catch { return false; }
}
