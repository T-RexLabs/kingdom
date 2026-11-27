import { useId, useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from "react";
import { builtinAssetURLs, imageAssetURL, revisionAssets, type ThemeAssetURLs } from "@/theme/assets";
import { canonicalRoleName, type IconSlot, type RoleID } from "@/theme/contract";
import { applyPresentation } from "@/theme/presentation";
import { ManifestIcon } from "@/theme/theme-icon";
import { LudicrousRegalia } from "@/components/ludicrous-regalia";
import type { ResolvedThemeManifest, ThemePackage } from "@/theme";
import "./theme-preview-card.css";

export type ThemePreviewContext = "overview" | "planning" | "agent" | "controls";
const contexts: { value: ThemePreviewContext; label: string }[] = [
  { value: "overview", label: "Overview" },
  { value: "planning", label: "Planning" },
  { value: "agent", label: "Agent" },
  { value: "controls", label: "Controls" },
];
const navigation: { slot: IconSlot; label: string }[] = [
  { slot: "nav.overview", label: "Overview" }, { slot: "nav.planning", label: "Planning" },
  { slot: "nav.runs", label: "Runs" }, { slot: "nav.agents", label: "Agents" },
  { slot: "nav.analytics", label: "Analytics" }, { slot: "nav.connections", label: "Connections" },
  { slot: "nav.appearance", label: "Appearance" },
];

type SampleProps = { manifest: ResolvedThemeManifest; urls: ThemeAssetURLs };
function PreviewArtwork({ manifest, urls, slot, label }: SampleProps & { slot: "overview" | "planningEmpty" | "agentBackdrop"; label: string }) {
  const src = imageAssetURL(manifest, urls, manifest.artwork[slot]);
  return src ? <img className="theme-preview-artwork" src={src} alt={label} /> : null;
}
function SampleHeading({ children }: { children: string }) {
  return <p className="theme-preview-heading" style={{ fontFamily: "var(--theme-font-display)", fontWeight: "var(--theme-weight-heading)" }}>{children}</p>;
}
function SampleButton({ children, active = false }: { children: React.ReactNode; active?: boolean }) {
  return <span className="ui-button ui-button--outline theme-preview-button" data-active={active || undefined}>{children}</span>;
}
function OverviewSample({ manifest, urls }: SampleProps) {
  return <div className="theme-preview-stack">
    <SampleHeading>{manifest.copy["overview.welcome"]}</SampleHeading>
    <p>Keep work moving. Pick up a plan, review a decision, or follow your agents.</p>
    <PreviewArtwork manifest={manifest} urls={urls} slot="overview" label="Overview artwork" />
    <div className="theme-preview-actions"><SampleButton>Plan Work</SampleButton><SampleButton active><ManifestIcon theme={manifest} assetURLs={urls} slot="work-order" size={14} />New {manifest.copy["workOrder.singular"]}</SampleButton></div>
    <div className="theme-surface theme-preview-panel"><p className="theme-preview-secondary">{manifest.copy["workOrder.plural"]}</p><div className="theme-preview-status-row"><span>A Sample Run</span><span className="ui-badge theme-preview-success">Complete</span></div></div>
    <p className="theme-preview-secondary">{manifest.copy["brand.tagline"]}</p>
  </div>;
}
function PlanningSample({ manifest, urls }: SampleProps) {
  return <div className="theme-preview-stack">
    <PreviewArtwork manifest={manifest} urls={urls} slot="planningEmpty" label="Planning artwork" />
    <SampleHeading>{manifest.copy["planning.emptyTitle"]}</SampleHeading>
    <p>{manifest.copy["planning.emptyDescription"]}</p>
    <div className="theme-surface theme-preview-panel theme-preview-stack">
      <label className="theme-preview-stack">Session Title<input className="ui-field theme-preview-field" placeholder="What would you like to explore?" aria-label="Sample session title" /></label>
      <div className="theme-preview-actions"><SampleButton active>Start Planning</SampleButton><SampleButton>New {manifest.copy["workOrder.singular"]}</SampleButton></div>
    </div>
  </div>;
}
function AgentSample({ manifest, urls, role }: SampleProps & { role: RoleID }) {
  const identity = manifest.identities[role];
  const portrait = imageAssetURL(manifest, urls, identity.portrait);
  return <div className="theme-preview-stack">
    <p className="theme-preview-secondary">{manifest.copy["agents.heading"]}</p>
    <PreviewArtwork manifest={manifest} urls={urls} slot="agentBackdrop" label="Agent backdrop artwork" />
    <div className="theme-preview-identity"><ManifestIcon theme={manifest} assetURLs={urls} slot={`role.${role}`} title={`${canonicalRoleName(role)} icon`} size={24} /><div><SampleHeading>{identity.name}</SampleHeading><p className="theme-preview-secondary">{canonicalRoleName(role)}</p></div></div>
    <p>{identity.description}</p>
    {portrait && <img className="theme-preview-portrait" src={portrait} alt={`${identity.name} portrait`} />}
    {identity.lore && <p className="theme-preview-lore" style={{ fontFamily: "var(--theme-font-display)" }}>{identity.lore}</p>}
  </div>;
}
function ControlSample({ manifest, urls }: SampleProps) {
  const workflow = Object.keys(manifest.icons).filter((slot) => slot.startsWith("node.")) as IconSlot[];
  return <div className="theme-preview-stack">
    <SampleHeading>An Idea Becomes Reality</SampleHeading>
    <p>A clear plan. A team with purpose. Every step in view.</p>
    <code className="theme-code theme-preview-code" style={{ fontFamily: "var(--theme-font-mono)" }}>const progress = 98;</code>
    <div className="theme-surface theme-preview-panel theme-preview-stack">
      <div className="theme-preview-actions"><SampleButton>Plan</SampleButton><SampleButton active>Build</SampleButton><SampleButton>Review</SampleButton></div>
      <div className="theme-preview-status-row"><span>Status</span><span className="ui-badge theme-preview-success">Complete</span></div>
    </div>
    <div className="theme-preview-divider theme-preview-stack">
      <p className="theme-preview-secondary">Native Controls</p>
      <label className="theme-preview-native-check"><input type="checkbox" defaultChecked className="theme-native-checkbox" /> Checkbox</label>
      <label className="theme-preview-stack">Date<input type="date" defaultValue="1998-06-25" className="theme-native-sample" /></label>
    </div>
    <div className="theme-preview-divider theme-preview-stack"><p className="theme-preview-secondary">Workflow Icons</p><div className="theme-preview-workflow">{workflow.map((slot) => <span key={slot} className="theme-preview-workflow-item"><ManifestIcon theme={manifest} assetURLs={urls} slot={slot} size={20} /><span>{slot.slice(5).replaceAll("-", " ").replace(/\b\w/g, (letter) => letter.toUpperCase())}</span></span>)}</div></div>
    <p className="theme-preview-secondary">{manifest.copy["appearance.description"]}</p>
  </div>;
}

export function ThemePreviewCard({ manifest, pkg, files, compact = false, role = "builder", context = "controls" }: {
  manifest: ResolvedThemeManifest; pkg?: ThemePackage; files?: Readonly<Record<string, string>>; compact?: boolean; role?: RoleID; context?: ThemePreviewContext;
}) {
  const root = useRef<HTMLDivElement>(null);
  const tabs = useRef<HTMLDivElement>(null);
  const id = useId();
  const [selection, setSelection] = useState({ context, pane: context });
  // Follow an editor section change, while preserving manual sample navigation
  // during normal draft updates and role selection.
  if (selection.context !== context) setSelection({ context, pane: context });
  const pane = selection.context === context ? selection.pane : context;
  const urls = useMemo(() => {
    if (!pkg) return builtinAssetURLs(manifest);
    if (!files) return revisionAssets(pkg);
    const inherited = builtinAssetURLs(manifest);
    return Object.fromEntries(Object.entries(pkg.manifest.assets).flatMap(([assetID, asset]) => {
      const url = typeof files[asset.path] === "string" ? `data:${asset.mediaType};base64,${files[asset.path]}` : inherited[assetID];
      return url ? [[assetID, url]] : [];
    }));
  }, [pkg, files, manifest]);
  useLayoutEffect(() => root.current ? applyPresentation({ theme: manifest, activePackage: pkg ?? null, assetURLs: urls }, root.current) : undefined, [manifest, pkg, urls]);
  const colors = manifest.tokens;
  const classic = manifest.surfaces.chrome === "classic";
  // Classic and storybook chrome draw their own control shapes.
  const ownsControlShape = classic || manifest.surfaces.chrome === "storybook";
  const sampleStyle: CSSProperties = { background: colors.canvas, color: colors["text.primary"], borderColor: classic ? undefined : colors["border.default"], fontFamily: "var(--theme-font-body)", fontWeight: "var(--theme-weight-body)" };
  const selectPane = (next: ThemePreviewContext) => setSelection({ context, pane: next });
  return <div className="theme-preview-container">
    {!compact && <div className="theme-preview-tabs" role="tablist" aria-label="Preview context" ref={tabs}>{contexts.map(({ value, label }) => <button key={value} id={`${id}-${value}`} type="button" role="tab" aria-selected={pane === value} aria-controls={`${id}-panel`} tabIndex={pane === value ? 0 : -1} onClick={() => selectPane(value)} onKeyDown={(event) => {
      const index = contexts.findIndex((item) => item.value === value);
      const next = event.key === "ArrowRight" ? (index + 1) % contexts.length : event.key === "ArrowLeft" ? (index + contexts.length - 1) % contexts.length : event.key === "Home" ? 0 : event.key === "End" ? contexts.length - 1 : -1;
      if (next < 0) return;
      event.preventDefault(); selectPane(contexts[next].value);
      tabs.current?.querySelectorAll<HTMLButtonElement>("button")[next].focus();
    }}>{label}</button>)}</div>}
    <div ref={root} className={`ui-window theme-overlay theme-study-preview ${compact ? "compact" : ""}`} style={sampleStyle} aria-label={`${manifest.name} presentation sample`}>
      <div className="theme-study-rail" style={{ borderColor: colors["border.default"], color: colors["accent.base"] }}><ManifestIcon theme={manifest} assetURLs={urls} slot="brand" title={compact ? undefined : "Brand icon"} size={24} />{compact ? <><i /><i /><i /></> : navigation.map(({ slot, label }) => <ManifestIcon key={slot} theme={manifest} assetURLs={urls} slot={slot} title={`${label} navigation icon`} size={18} />)}</div>
      <div className="theme-preview-content">
        <div className="ui-window-titlebar theme-preview-titlebar" style={classic ? undefined : { color: colors["text.secondary"] }}><span>Theme Sample</span><span>{compact ? "Typography & Color" : contexts.find((item) => item.value === pane)?.label}</span></div>
        <LudicrousRegalia />
        <div className="theme-ornament theme-preview-ornament" aria-hidden="true" />
        {compact ? <><SampleHeading>An Idea Becomes Reality</SampleHeading><div className="theme-preview-actions" aria-hidden="true">{["Plan", "Build", "Review"].map((step) => <span key={step} className="ui-button ui-button--outline theme-preview-button" style={ownsControlShape ? undefined : { borderRadius: "var(--theme-radius)" }}>{step}</span>)}</div></> : <div id={`${id}-panel`} role="tabpanel" aria-labelledby={`${id}-${pane}`} tabIndex={0} className="theme-preview-panel-content" key={pane}>
          {pane === "overview" && <OverviewSample manifest={manifest} urls={urls} />}
          {pane === "planning" && <PlanningSample manifest={manifest} urls={urls} />}
          {pane === "agent" && <AgentSample manifest={manifest} urls={urls} role={role} />}
          {pane === "controls" && <ControlSample manifest={manifest} urls={urls} />}
        </div>}
      </div>
    </div>
  </div>;
}
