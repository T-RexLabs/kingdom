import type { ReactNode } from "react";
import { SelectField } from "@/components/ui/select-field";
import { AdvancedColors } from "./advanced-colors";
import {
  builtInThemes,
  canonicalRoleName,
  defaultTheme,
  type ResolvedThemeManifest,
  type RoleID,
  type ThemeManifest,
} from "@/theme";
import schemaText from "../../../../contracts/theme/theme.schema.json?raw";
import iconCatalogText from "../../../../contracts/theme/lucide-icons.json?raw";

const fontReferences: string[] =
  JSON.parse(schemaText).definitions.font.anyOf[0].enum;
const fontLabels: Record<string, string> = {
  "bundled:dm-sans": "DM Sans",
  "bundled:jetbrains-mono": "JetBrains Mono",
};
const iconReferences: string[] = JSON.parse(iconCatalogText);
const roles = Object.keys(defaultTheme.identities) as RoleID[];
import {
  objectValue,
  stringAt,
  optionalStringAt,
  numberAt,
  setDraftField,
} from "./editor-values";
export function Field({
  label,
  help,
  children,
}: {
  label: string;
  help?: string;
  children: ReactNode;
}) {
  return (
    <label className="appearance-field">
      <span>{label}</span>
      {children}
      {help && <small className="appearance-help">{help}</small>}
    </label>
  );
}
export function EditorColors({
  manifest,
  resolved,
  disabled,
  change,
}: {
  manifest: ThemeManifest;
  resolved?: ResolvedThemeManifest;
  disabled: boolean;
  change: (next: ThemeManifest) => void;
}) {
  const base =
    builtInThemes.find(
      (theme) => theme.id === stringAt(manifest, "base", "id"),
    ) ?? defaultTheme;
  const view = resolved ?? base;
  const explicitTokens = Object.keys(objectValue(manifest.tokens)).length > 0;
  const update = (path: string[], value: unknown) =>
    change(setDraftField(manifest, path, value));
  return (
    <fieldset disabled={disabled} className="space-y-7">
      <section>
        <h3 className="mb-4 text-sm font-medium">Palette</h3>
        <div className="grid grid-cols-2 gap-4">
          {(["accent", "canvas", "surface", "text"] as const).map((key) => (
            <Field key={key} label={key[0].toUpperCase() + key.slice(1)}>
              <div className="flex items-center gap-2">
                <input
                  type="color"
                  aria-label={`${key} color picker`}
                  value={
                    /^#[a-f0-9]{6}$/i.test(stringAt(manifest, "palette", key))
                      ? stringAt(manifest, "palette", key)
                      : view.palette[key]
                  }
                  onChange={(event) =>
                    update(["palette", key], event.target.value)
                  }
                  className="h-10 w-11 shrink-0 rounded border border-border bg-background p-1"
                />
                <input
                  className="appearance-input font-mono text-xs"
                  value={
                    optionalStringAt(manifest, "palette", key) ??
                    view.palette[key]
                  }
                  onChange={(event) =>
                    update(["palette", key], event.target.value)
                  }
                  aria-label={`${key[0].toUpperCase() + key.slice(1)} Color`}
                />
              </div>
            </Field>
          ))}
        </div>
        {(explicitTokens || !manifest.base) && (
          <div className="mt-4 rounded-lg border border-border p-4 text-xs text-foreground-muted">
            <p>
              Explicit semantic colors take priority over the palette.
              Regenerating removes these color overrides and uses the palette to
              derive accessible states.
            </p>
            <button
              type="button"
              className="appearance-link mt-2"
              onClick={() => {
                const next = structuredClone(manifest);
                next.base = next.base ?? {
                  id: defaultTheme.id as NonNullable<
                    ThemeManifest["base"]
                  >["id"],
                  version: defaultTheme.version,
                };
                delete next.tokens;
                change(next);
              }}
            >
              Use Palette Colors
            </button>
          </div>
        )}
      </section>
      <AdvancedColors manifest={manifest} resolved={view} change={change} />
      <section className="appearance-form-section">
        <h3>Typography</h3>
        <div className="grid gap-4 sm:grid-cols-2">
          {(["body", "display", "mono"] as const).map((slot) => (
            <SelectField
              key={slot}
              id={`theme-${slot}-font`}
              label={`${slot[0].toUpperCase() + slot.slice(1)} Font`}
              disabled={disabled}
                value={
                  optionalStringAt(manifest, "typography", slot) ??
                  view.typography[slot]
                }
                onChange={(next) =>
                  update(["typography", slot], next)
                }
              options={[
                ...fontReferences.map((font) => ({ value: font, label: fontLabels[font] ??
                      font
                        .slice(8)
                        .replaceAll("-", " ")
                        .replace(/\b\w/g, (char) => char.toUpperCase()) })),
                ...Object.entries(objectValue(manifest.assets))
                  .filter(
                    ([, asset]) =>
                      objectValue(asset).mediaType === "font/woff2",
                  )
                  .map(([id]) => ({ value: `asset:${id}`, label: `${id} (Local Font)` })),
              ]}
            />
          ))}
          <SelectField id="theme-heading-weight" label="Heading Weight" disabled={disabled}
              value={String(numberAt(
                manifest,
                "typography",
                "headingWeight",
                view.typography.headingWeight,
              ))}
              onChange={(next) =>
                update(
                  ["typography", "headingWeight"],
                  Number(next),
                )
              }
              options={[400, 500, 600, 700].map((weight) => ({ value: String(weight), label: String(weight) }))}
          />
          <SelectField id="theme-body-weight" label="Body Weight" disabled={disabled}
              value={String(numberAt(
                manifest,
                "typography",
                "bodyWeight",
                view.typography.bodyWeight,
              ))}
              onChange={(next) =>
                update(["typography", "bodyWeight"], Number(next))
              }
              options={[400, 500].map((weight) => ({ value: String(weight), label: String(weight) }))}
          />
        </div>
      </section>
      <section className="appearance-form-section">
        <h3>Surfaces</h3>
        <div className="grid gap-4 sm:grid-cols-2">
          <Field label="Corner Radius" help="Between 0 and 20 pixels.">
            <input
              type="number"
              className="appearance-input"
              min={0}
              max={20}
              value={numberAt(
                manifest,
                "surfaces",
                "radius",
                view.surfaces.radius,
              )}
              onChange={(event) =>
                update(["surfaces", "radius"], Number(event.target.value))
              }
            />
          </Field>
          <SelectField id="theme-border-width" label="Border Width" disabled={disabled}
              value={String(numberAt(
                manifest,
                "surfaces",
                "borderWidth",
                view.surfaces.borderWidth,
              ))}
              onChange={(next) =>
                update(["surfaces", "borderWidth"], Number(next))
              }
              options={[{ value: "1", label: "1 Pixel" }, { value: "2", label: "2 Pixels" }]}
          />
          <SelectField id="theme-elevation" label="Elevation" disabled={disabled}
              value={
                optionalStringAt(manifest, "surfaces", "elevation") ??
                view.surfaces.elevation
              }
              onChange={(next) =>
                update(["surfaces", "elevation"], next)
              }
              options={[{ value: "flat", label: "Flat" }, { value: "soft", label: "Soft" }]}
          />
          <SelectField id="theme-ornamentation" label="Ornamentation" disabled={disabled}
              value={
                optionalStringAt(manifest, "surfaces", "ornamentation") ??
                view.surfaces.ornamentation
              }
              onChange={(next) =>
                update(["surfaces", "ornamentation"], next)
              }
              options={[{ value: "minimal", label: "Minimal" }, { value: "balanced", label: "Balanced" }]}
          />
          <SelectField id="theme-color-scheme" label="Native Control Scheme" disabled={disabled}
              value={
                optionalStringAt(manifest, "colorScheme") ?? view.colorScheme
              }
              onChange={(next) => update(["colorScheme"], next)}
              options={[{ value: "dark", label: "Dark" }, { value: "light", label: "Light" }]}
          />
        </div>
      </section>
    </fieldset>
  );
}
export function EditorIdentity({
  manifest,
  resolved,
  disabled,
  role,
  setRole,
  change,
}: {
  manifest: ThemeManifest;
  resolved?: ResolvedThemeManifest;
  disabled: boolean;
  role: RoleID;
  setRole: (role: RoleID) => void;
  change: (next: ThemeManifest) => void;
}) {
  const base =
    builtInThemes.find(
      (theme) => theme.id === stringAt(manifest, "base", "id"),
    ) ?? defaultTheme;
  const view = resolved ?? base;
  const identity = view.identities[role];
  const update = (path: string[], value: unknown) =>
    change(setDraftField(manifest, path, value));
  const images = Object.entries(objectValue(manifest.assets)).filter(
    ([, asset]) => String(objectValue(asset).mediaType).startsWith("image/"),
  );
  return (
    <fieldset disabled={disabled} className="space-y-4">
      <SelectField id="theme-responsibility" label="Responsibility" disabled={disabled}
          value={role}
          onChange={(next) => setRole(next as RoleID)}
          options={roles.map((id) => ({ value: id, label: canonicalRoleName(id) }))}
      />
      <p className="appearance-help">
        The canonical responsibility stays visible. Names and lore change
        presentation; permissions, actions and generated replies stay the same.
      </p>
      <Field label="Display Name">
        <input
          className="appearance-input"
          maxLength={80}
          value={
            optionalStringAt(manifest, "identities", role, "name") ??
            identity.name
          }
          onChange={(event) =>
            update(["identities", role, "name"], event.target.value)
          }
        />
      </Field>
      <Field label="Description">
        <textarea
          className="appearance-input min-h-24"
          value={
            optionalStringAt(manifest, "identities", role, "description") ??
            identity.description
          }
          onChange={(event) =>
            update(["identities", role, "description"], event.target.value)
          }
        />
      </Field>
      <Field label="Lore">
        <textarea
          className="appearance-input min-h-32"
          value={
            optionalStringAt(manifest, "identities", role, "lore") ??
            identity.lore ??
            ""
          }
          onChange={(event) =>
            update(["identities", role, "lore"], event.target.value)
          }
        />
      </Field>
      <Field
        label="Role Icon"
        help="Use a Lucide symbol or asset:<id> for a validated local SVG."
      >
        <input
          className="appearance-input font-mono text-xs"
          list="theme-icon-options"
          value={
            optionalStringAt(manifest, "icons", `role.${role}`) ??
            view.icons[`role.${role}`]
          }
          onChange={(event) =>
            update(["icons", `role.${role}`], event.target.value)
          }
        />
      </Field>
      <SelectField id="theme-portrait" label="Portrait" disabled={disabled}
          value={stringAt(manifest, "identities", role, "portrait")}
          onChange={(next) =>
            update(
              ["identities", role, "portrait"],
              next || undefined,
            )
          }
          options={[{ value: "", label: "None / Inherited" }, ...images.map(([id]) => ({ value: `asset:${id}`, label: id }))]}
      />
      <datalist id="theme-icon-options">
        {iconReferences.map((name) => (
          <option key={name} value={`lucide:${name}`} />
        ))}
      </datalist>
      <details className="appearance-form-section">
        <summary className="text-sm">Navigation and Workflow Icons</summary>
        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          {Object.keys(defaultTheme.icons)
            .filter((key) => !key.startsWith("role."))
            .map((key) => (
              <Field
                key={key}
                label={key
                  .replaceAll(".", " · ")
                  .replaceAll("-", " ")
                  .replace(/\b\w/g, (char) => char.toUpperCase())}
              >
                <input
                  className="appearance-input font-mono text-xs"
                  list="theme-icon-options"
                  value={
                    optionalStringAt(manifest, "icons", key) ??
                    stringAt(view, "icons", key)
                  }
                  onChange={(event) =>
                    update(["icons", key], event.target.value)
                  }
                />
              </Field>
            ))}
        </div>
      </details>
      <details className="appearance-form-section">
        <summary className="text-sm">UI Copy</summary>
        <p className="appearance-help mt-3">
          Plain text only. Operational outcomes and generated responses are not
          theme copy.
        </p>
        <div className="mt-4 space-y-4">
          {Object.keys(defaultTheme.copy).map((key) => (
            <Field
              key={key}
              label={key
                .replaceAll(".", " · ")
                .replace(/([A-Z])/g, " $1")
                .replace(/\b\w/g, (char) => char.toUpperCase())}
            >
              <textarea
                className="appearance-input"
                value={
                  optionalStringAt(manifest, "copy", key) ??
                  stringAt(view, "copy", key)
                }
                onChange={(event) => update(["copy", key], event.target.value)}
              />
            </Field>
          ))}
        </div>
      </details>
      <details className="appearance-form-section">
        <summary className="text-sm">Artwork</summary>
        <div className="mt-4 space-y-4">
          {["overview", "planningEmpty", "agentBackdrop"].map((key) => (
            <SelectField
              key={key}
              id={`theme-artwork-${key}`}
              label={key
                .replace(/([A-Z])/g, " $1")
                .replace(/\b\w/g, (char) => char.toUpperCase())}
              disabled={disabled}
                value={stringAt(manifest, "artwork", key)}
                onChange={(next) =>
                  update(["artwork", key], next || undefined)
                }
              options={[{ value: "", label: "None / Inherited" }, ...images.map(([id]) => ({ value: `asset:${id}`, label: id }))]}
            />
          ))}
        </div>
      </details>
    </fieldset>
  );
}
