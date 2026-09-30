import { useState, type ReactNode } from "react";
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
import { ThemeImagePicker } from "./theme-image-picker";
import { useValidatedAssetURLs, type ValidatedAssetPreview } from "./use-theme-picker-assets";

const fontReferences: string[] =
  JSON.parse(schemaText).definitions.font.anyOf[0].enum;
const fontLabels: Record<string, string> = {
  "bundled:dm-sans": "DM Sans",
  "bundled:classic-sans": "Classic Windows (Tahoma)",
  "bundled:jetbrains-mono": "JetBrains Mono",
};
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
// Keep a temporary empty value while editing instead of coercing it to zero.
// Only complete numbers enter the canonical draft; blur restores an empty field.
function NumericSetting({ label, value, min, max, help, change }: { label: string; value: number; min?: number; max?: number; help?: string; change: (value: number) => void }) {
  const [editing, setEditing] = useState<{ source: number; text: string } | null>(null);
  const text = editing?.source === value ? editing.text : String(value);
  return <Field label={label} help={help}>
    <input type="number" className="appearance-input" min={min} max={max} step="any" value={text}
      onChange={(event) => {
        const next = event.target.value;
        const numeric = Number(next);
        const complete = next !== "" && Number.isFinite(numeric);
        setEditing({ source: complete ? numeric : value, text: next });
        if (complete) change(numeric);
      }}
      onBlur={() => setEditing(null)} />
  </Field>;
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
                  className="ui-color-field h-10 w-11 shrink-0 rounded border border-border bg-background p-1"
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
              derive the other colors.
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
          <NumericSetting label="Heading Weight" min={1} max={1000} help="Use any CSS font weight from 1 to 1,000. The result depends on the font."
            value={numberAt(manifest, "typography", "headingWeight", view.typography.headingWeight)}
            change={(next) => update(["typography", "headingWeight"], next)} />
          <NumericSetting label="Body Weight" min={1} max={1000} help="Use any CSS font weight from 1 to 1,000. The result depends on the font."
            value={numberAt(manifest, "typography", "bodyWeight", view.typography.bodyWeight)}
            change={(next) => update(["typography", "bodyWeight"], next)} />
        </div>
      </section>
      <section className="appearance-form-section">
        <h3>Surfaces</h3>
        <div className="grid gap-4 sm:grid-cols-2">
          <SelectField id="theme-chrome" label="Control Style" disabled={disabled}
            value={optionalStringAt(manifest, "surfaces", "chrome") ?? view.surfaces.chrome ?? "flat"}
            onChange={(next) => update(["surfaces", "chrome"], next)}
            options={[{ value: "flat", label: "Modern" }, { value: "classic", label: "Classic Desktop" }]} />
          <SelectField id="theme-icon-rendering" label="Icon Colors" disabled={disabled}
            value={optionalStringAt(manifest, "surfaces", "iconRendering") ?? view.surfaces.iconRendering ?? "mask"}
            onChange={(next) => update(["surfaces", "iconRendering"], next)}
            options={[{ value: "mask", label: "Match Text" }, { value: "original", label: "Original Artwork" }]} />
          <NumericSetting label="Corner Radius" min={0} help="Pixels. Use 0 for square corners; decimal values are welcome."
            value={numberAt(manifest, "surfaces", "radius", view.surfaces.radius)}
            change={(next) => update(["surfaces", "radius"], next)} />
          <NumericSetting label="Border Width" min={0} help="Pixels. Use 0 for no border; decimal values are welcome."
            value={numberAt(manifest, "surfaces", "borderWidth", view.surfaces.borderWidth)}
            change={(next) => update(["surfaces", "borderWidth"], next)} />
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
          <div className="space-y-2 sm:col-span-2">
          <SelectField id="theme-color-scheme" label="Native Control Scheme" disabled={disabled}
              value={
                optionalStringAt(manifest, "colorScheme") ?? view.colorScheme
              }
              onChange={(next) => update(["colorScheme"], next)}
              options={[{ value: "dark", label: "Dark" }, { value: "light", label: "Light" }]}
          />
          <p className="appearance-help">Changes browser-drawn controls, such as checkboxes and date pickers. The palette stays the same.</p>
          </div>
        </div>
      </section>
    </fieldset>
  );
}
export function EditorIdentity({
  manifest,
  resolved,
  assetPreview,
  disabled,
  role,
  setRole,
  change,
}: {
  manifest: ThemeManifest;
  resolved?: ResolvedThemeManifest;
  assetPreview?: ValidatedAssetPreview;
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
  const assetTheme = assetPreview?.manifest ?? view;
  const assetURLs = useValidatedAssetURLs(assetTheme, assetPreview);
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
      <ThemeImagePicker
        id="theme-role-icon" label="Role Icon" kind="icon" disabled={disabled}
        theme={assetTheme} assetURLs={assetURLs}
        value={optionalStringAt(manifest, "icons", `role.${role}`) ?? view.icons[`role.${role}`]}
        onChange={(next) => update(["icons", `role.${role}`], next)}
      />
      <ThemeImagePicker id="theme-portrait" label="Portrait" help="An optional avatar for this role, shown in its agent details. Choose an uploaded image; the role icon is used separately in navigation and activity." kind="image" disabled={disabled}
          theme={assetTheme} assetURLs={assetURLs}
          value={stringAt(manifest, "identities", role, "portrait")}
          inheritedValue={base.identities[role].portrait}
          onChange={(next) =>
            update(
              ["identities", role, "portrait"],
              next || undefined,
            )
          }
      />
      <details className="appearance-form-section">
        <summary className="text-sm">Navigation and Workflow Icons</summary>
        <div className="mt-4 grid gap-4 sm:grid-cols-2">
          {Object.keys(defaultTheme.icons)
            .filter((key) => !key.startsWith("role."))
            .map((key) => (
              <ThemeImagePicker
                key={key}
                id={`theme-icon-${key}`} kind="icon" disabled={disabled}
                theme={assetTheme} assetURLs={assetURLs}
                label={key
                  .replaceAll(".", " · ")
                  .replaceAll("-", " ")
                  .replace(/\b\w/g, (char) => char.toUpperCase())}
                value={optionalStringAt(manifest, "icons", key) ?? stringAt(view, "icons", key)}
                onChange={(next) => update(["icons", key], next)}
              />
            ))}
        </div>
      </details>
      <details className="appearance-form-section">
        <summary className="text-sm">App Wording</summary>
        <p className="appearance-help mt-3">
          Customize the headings, welcome messages, and work order names shown
          throughout the App.
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
        <p className="appearance-help mt-3">Optional illustrations for specific areas of the app. Upload an image under Local Assets, then choose where it appears.</p>
        <div className="mt-4 space-y-4">
          {["overview", "planningEmpty", "agentBackdrop"].map((key) => (
            <ThemeImagePicker
              key={key}
              id={`theme-artwork-${key}`}
              kind="image" theme={assetTheme} assetURLs={assetURLs}
              label={key
                .replace(/([A-Z])/g, " $1")
                .replace(/\b\w/g, (char) => char.toUpperCase())}
              help={{ overview: "An illustration below the Overview welcome header.", planningEmpty: "An illustration above the welcome message in a new planning session.", agentBackdrop: "A banner above each role card on the Agents page. Text stays outside the image." }[key]}
              disabled={disabled}
                value={stringAt(manifest, "artwork", key)}
                inheritedValue={stringAt(base, "artwork", key)}
                onChange={(next) =>
                  update(["artwork", key], next || undefined)
                }
            />
          ))}
        </div>
      </details>
    </fieldset>
  );
}
