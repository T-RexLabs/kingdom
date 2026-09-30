import type { ReactNode } from "react";
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
            <Field
              key={slot}
              label={`${slot[0].toUpperCase() + slot.slice(1)} Font`}
            >
              <select
                className="appearance-input"
                value={
                  optionalStringAt(manifest, "typography", slot) ??
                  view.typography[slot]
                }
                onChange={(event) =>
                  update(["typography", slot], event.target.value)
                }
              >
                {fontReferences.map((font) => (
                  <option key={font} value={font}>
                    {fontLabels[font] ??
                      font
                        .slice(8)
                        .replaceAll("-", " ")
                        .replace(/\b\w/g, (char) => char.toUpperCase())}
                  </option>
                ))}
                {Object.entries(objectValue(manifest.assets))
                  .filter(
                    ([, asset]) =>
                      objectValue(asset).mediaType === "font/woff2",
                  )
                  .map(([id]) => (
                    <option key={id} value={`asset:${id}`}>
                      {id} (Local Font)
                    </option>
                  ))}
              </select>
            </Field>
          ))}
          <Field label="Heading Weight">
            <select
              className="appearance-input"
              value={numberAt(
                manifest,
                "typography",
                "headingWeight",
                view.typography.headingWeight,
              )}
              onChange={(event) =>
                update(
                  ["typography", "headingWeight"],
                  Number(event.target.value),
                )
              }
            >
              {[400, 500, 600, 700].map((weight) => (
                <option key={weight} value={weight}>
                  {weight}
                </option>
              ))}
            </select>
          </Field>
          <Field label="Body Weight">
            <select
              className="appearance-input"
              value={numberAt(
                manifest,
                "typography",
                "bodyWeight",
                view.typography.bodyWeight,
              )}
              onChange={(event) =>
                update(["typography", "bodyWeight"], Number(event.target.value))
              }
            >
              {[400, 500].map((weight) => (
                <option key={weight} value={weight}>
                  {weight}
                </option>
              ))}
            </select>
          </Field>
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
          <Field label="Border Width">
            <select
              className="appearance-input"
              value={numberAt(
                manifest,
                "surfaces",
                "borderWidth",
                view.surfaces.borderWidth,
              )}
              onChange={(event) =>
                update(["surfaces", "borderWidth"], Number(event.target.value))
              }
            >
              <option value={1}>1 Pixel</option>
              <option value={2}>2 Pixels</option>
            </select>
          </Field>
          <Field label="Elevation">
            <select
              className="appearance-input"
              value={
                optionalStringAt(manifest, "surfaces", "elevation") ??
                view.surfaces.elevation
              }
              onChange={(event) =>
                update(["surfaces", "elevation"], event.target.value)
              }
            >
              <option value="flat">Flat</option>
              <option value="soft">Soft</option>
            </select>
          </Field>
          <Field label="Ornamentation">
            <select
              className="appearance-input"
              value={
                optionalStringAt(manifest, "surfaces", "ornamentation") ??
                view.surfaces.ornamentation
              }
              onChange={(event) =>
                update(["surfaces", "ornamentation"], event.target.value)
              }
            >
              <option value="minimal">Minimal</option>
              <option value="balanced">Balanced</option>
            </select>
          </Field>
          <Field label="Native Control Scheme">
            <select
              className="appearance-input"
              value={
                optionalStringAt(manifest, "colorScheme") ?? view.colorScheme
              }
              onChange={(event) => update(["colorScheme"], event.target.value)}
            >
              <option value="dark">Dark</option>
              <option value="light">Light</option>
            </select>
          </Field>
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
      <Field label="Responsibility">
        <select
          className="appearance-input"
          value={role}
          onChange={(event) => setRole(event.target.value as RoleID)}
        >
          {roles.map((id) => (
            <option key={id} value={id}>
              {canonicalRoleName(id)}
            </option>
          ))}
        </select>
      </Field>
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
      <Field label="Portrait">
        <select
          className="appearance-input"
          value={stringAt(manifest, "identities", role, "portrait")}
          onChange={(event) =>
            update(
              ["identities", role, "portrait"],
              event.target.value || undefined,
            )
          }
        >
          <option value="">None / Inherited</option>
          {images.map(([id]) => (
            <option value={`asset:${id}`} key={id}>
              {id}
            </option>
          ))}
        </select>
      </Field>
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
            <Field
              key={key}
              label={key
                .replace(/([A-Z])/g, " $1")
                .replace(/\b\w/g, (char) => char.toUpperCase())}
            >
              <select
                className="appearance-input"
                value={stringAt(manifest, "artwork", key)}
                onChange={(event) =>
                  update(["artwork", key], event.target.value || undefined)
                }
              >
                <option value="">None / Inherited</option>
                {images.map(([id]) => (
                  <option key={id} value={`asset:${id}`}>
                    {id}
                  </option>
                ))}
              </select>
            </Field>
          ))}
        </div>
      </details>
    </fieldset>
  );
}
