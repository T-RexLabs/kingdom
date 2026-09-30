import Ajv from "ajv";
import type { components } from "@/api/schema";
import schemaText from "../../../contracts/theme/theme.schema.json?raw";
import obsidianText from "../../../contracts/theme/builtin/obsidian-court.json?raw";
import nautilusText from "../../../contracts/theme/builtin/nautilus.json?raw";
import astralText from "../../../contracts/theme/builtin/astral-council.json?raw";
import labText from "../../../contracts/theme/builtin/software-lab.json?raw";
import workstationText from "../../../contracts/theme/builtin/workstation-98.json?raw";

export type ThemeManifest = components["schemas"]["ThemeManifest"];
export type ThemePackage = Omit<components["schemas"]["ThemePackage"], "manifest"> & { manifest: ResolvedThemeManifest };
export type ThemeCatalog = components["schemas"]["ThemeCatalog"];
export type ThemeReference = Pick<ThemeCatalog["items"][number], "id" | "resolvedDigest">;
export type ThemeAsset = components["schemas"]["ThemeAsset"];
export type RoleID = keyof NonNullable<ThemeManifest["identities"]>;
export type TokenSlot = keyof NonNullable<ThemeManifest["tokens"]>;
export type IconSlot = keyof NonNullable<ThemeManifest["icons"]>;
export type CopySlot = keyof NonNullable<ThemeManifest["copy"]>;
export type ArtworkSlot = keyof NonNullable<ThemeManifest["artwork"]>;

type Complete<T> = Required<NonNullable<T>>;
export type ResolvedThemeManifest = Omit<ThemeManifest, "base" | "palette" | "tokens" | "typography" | "surfaces" | "icons" | "identities" | "copy" | "artwork" | "assets" | "colorScheme"> & {
  palette: Complete<ThemeManifest["palette"]>;
  tokens: Complete<ThemeManifest["tokens"]>;
  typography: Complete<ThemeManifest["typography"]>;
  surfaces: Complete<Omit<NonNullable<ThemeManifest["surfaces"]>, "chrome" | "iconRendering">> & Pick<NonNullable<ThemeManifest["surfaces"]>, "chrome" | "iconRendering">;
  icons: Complete<ThemeManifest["icons"]>;
  identities: { [K in RoleID]: components["schemas"]["ThemeIdentity"] & { name: string; description: string } };
  copy: Complete<ThemeManifest["copy"]>;
  artwork: NonNullable<ThemeManifest["artwork"]>;
  assets: NonNullable<ThemeManifest["assets"]>;
  colorScheme: NonNullable<ThemeManifest["colorScheme"]>;
};

// The authoring schema is imported from the same file as the Go validator.
// Browser validation accepts complete resolver responses only, never derivation.
const validate = new Ajv({ strict: false, allErrors: false }).compile(JSON.parse(schemaText));
export function resolvedManifest(value: unknown): ResolvedThemeManifest {
  if (!validate(value) || !value || typeof value !== "object" || "base" in value) {
    throw new Error("The theme response is not a complete supported presentation.");
  }
  return freezeTree(structuredClone(value)) as ResolvedThemeManifest;
}
function freezeTree<T>(value: T): T {
  if (value && typeof value === "object") {
    Object.values(value).forEach((child) => freezeTree(child));
    Object.freeze(value);
  }
  return value;
}
export function themePackage(value: components["schemas"]["ThemePackage"], expected?: ThemeReference): ThemePackage {
  const manifest = resolvedManifest(value.manifest);
  if (!validReference({ id: manifest.id, resolvedDigest: value.resolvedDigest }) || !/^[a-f0-9]{64}$/.test(value.sourceDigest)) {
    throw new Error("The theme revision has an invalid identity.");
  }
  if (expected && (manifest.id !== expected.id || value.resolvedDigest !== expected.resolvedDigest)) {
    throw new Error("The theme response does not match the selected revision.");
  }
  return { ...value, manifest };
}
export function validReference(value: unknown): value is ThemeReference {
  if (!value || typeof value !== "object") return false;
  const ref = value as Partial<ThemeReference>;
  return typeof ref.id === "string" && /^[a-z][a-z0-9-]{0,63}$/.test(ref.id) && typeof ref.resolvedDigest === "string" && /^[a-f0-9]{64}$/.test(ref.resolvedDigest);
}
export const builtInThemes: readonly ResolvedThemeManifest[] = [obsidianText, nautilusText, astralText, labText, workstationText].map((raw) => resolvedManifest(JSON.parse(raw)));
export const defaultTheme = builtInThemes[0];
const neutralTheme = builtInThemes[3];
export function canonicalRoleName(role: string): string {
  return neutralTheme.identities[role as RoleID]?.name ?? role.replaceAll("-", " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}
export function roleIdentity(manifest: ResolvedThemeManifest, role: string) {
  const canonicalName = canonicalRoleName(role);
  return {
    role,
    canonicalName,
    ...{ name: canonicalName, description: "Role details are unavailable.", lore: undefined, portrait: undefined },
    ...manifest.identities[role as RoleID],
  };
}
