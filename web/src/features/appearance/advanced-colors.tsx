import {
  defaultTheme,
  type ResolvedThemeManifest,
  type ThemeManifest,
  type TokenSlot,
} from "@/theme";
import schemaText from "../../../../contracts/theme/theme.schema.json?raw";
import { objectValue, optionalStringAt, setDraftField } from "./editor-values";

const groupNames: Record<string, string> = {
  surface: "Surfaces",
  text: "Text",
  border: "Borders",
  accent: "Accent",
  focus: "Focus",
  selection: "Selection",
  control: "Controls",
  code: "Code",
  tool: "Tools",
  chart: "Charts",
  dag: "Workflow",
  status: "Status",
};
function displayName(value: string) {
  return value
    .replaceAll(".", " ")
    .replace(/([a-z])([A-Z0-9])/g, "$1 $2")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}
// The canonical schema owns the token catalog. Only presentation group names
// live here; new schema tokens automatically receive a control and reset.
const tokenGroups = new Map<string, TokenSlot[]>();
for (const token of Object.keys(
  JSON.parse(schemaText).properties.tokens.properties,
) as TokenSlot[]) {
  const group = token.split(".")[0];
  tokenGroups.set(group, [...(tokenGroups.get(group) ?? []), token]);
}

export function AdvancedColors({
  manifest,
  resolved,
  change,
}: {
  manifest: ThemeManifest;
  resolved: ResolvedThemeManifest;
  change: (manifest: ThemeManifest) => void;
}) {
  const overrides = objectValue(manifest.tokens);
  function reset(token: TokenSlot) {
    // A complete baseless file has no inherited slots. Preserve its authored
    // palette and every other explicit value when enabling a per-slot reset.
    const next = setDraftField(manifest, ["tokens", token], undefined);
    next.base ??= {
      id: defaultTheme.id as NonNullable<ThemeManifest["base"]>["id"],
      version: defaultTheme.version,
    };
    change(next);
  }
  return (
    <details className="appearance-form-section">
      <summary className="cursor-pointer text-sm font-medium">
        Advanced Colors
      </summary>
      <p className="appearance-help mt-3">
        Override individual semantic colors. Inherited colors follow the base
        and palette; explicit overrides remain when the palette changes. Reset
        restores inheritance for one color.
      </p>
      <div className="mt-4 space-y-3">
        {[...tokenGroups].map(([group, tokens]) => (
          <details key={group} className="rounded-lg border border-border p-3">
            <summary className="cursor-pointer text-sm">
              {groupNames[group] ?? displayName(group)}
            </summary>
            <div className="mt-4 grid gap-4 sm:grid-cols-2">
              {tokens.map((token) => {
                const label = displayName(token);
                const explicit = Object.hasOwn(overrides, token);
                const value =
                  optionalStringAt(manifest, "tokens", token) ??
                  resolved.tokens[token];
                return (
                  <div key={token} className="min-w-0 space-y-2">
                    <label
                      className="block text-xs font-medium"
                      htmlFor={`theme-color-${token}`}
                    >
                      {label} Color
                    </label>
                    <div className="flex items-center gap-2">
                      <input
                        type="color"
                        aria-label={`${label.toLowerCase()} color picker`}
                        value={
                          /^#[a-f0-9]{6}$/i.test(value)
                            ? value
                            : resolved.tokens[token]
                        }
                        onChange={(event) =>
                          change(
                            setDraftField(
                              manifest,
                              ["tokens", token],
                              event.target.value,
                            ),
                          )
                        }
                        className="ui-color-field h-10 w-11 shrink-0 rounded border border-border bg-background p-1"
                      />
                      <input
                        id={`theme-color-${token}`}
                        className="appearance-input min-w-0 font-mono text-xs"
                        value={value}
                        onChange={(event) =>
                          change(
                            setDraftField(
                              manifest,
                              ["tokens", token],
                              event.target.value,
                            ),
                          )
                        }
                      />
                    </div>
                    <div className="flex items-center justify-between gap-2 text-xs">
                      <span
                        className={
                          explicit ? "text-accent" : "text-foreground-muted"
                        }
                      >
                        {explicit ? "Override" : "Inherited"}
                      </span>
                      <button
                        type="button"
                        className="appearance-link text-xs disabled:cursor-default disabled:opacity-40"
                        aria-label={`Reset ${label.toLowerCase()} color`}
                        disabled={!explicit}
                        onClick={() => reset(token)}
                      >
                        Reset
                      </button>
                    </div>
                  </div>
                );
              })}
            </div>
          </details>
        ))}
      </div>
    </details>
  );
}
