import { useEffect, useId, useState } from "react";
import { X } from "lucide-react";
import { Button } from "@/components/ui/button";
import type { ThemeDiagnostic } from "./theme-api";

/** Advice never controls draft validity; identical advice stays dismissed across edits. */
export function ThemeColorAdvice({ warnings }: { warnings: readonly ThemeDiagnostic[] }) {
  if (!warnings.length) return null;
  return <ColorAdvice key={JSON.stringify(warnings)} warnings={warnings} />;
}

function ColorAdvice({ warnings }: { warnings: readonly ThemeDiagnostic[] }) {
  const [visible, setVisible] = useState(true);
  const [details, setDetails] = useState(false);
  const detailsID = useId();
  useEffect(() => {
    if (!visible || details) return;
    const timeout = setTimeout(() => setVisible(false), 8000);
    return () => clearTimeout(timeout);
  }, [visible, details]);
  if (!visible) return <Button variant="ghost" size="sm" onClick={() => { setDetails(false); setVisible(true); }}>Color Advice ({warnings.length})</Button>;
  return <section className="rounded-md border border-border bg-surface p-4 text-xs leading-relaxed" aria-label="Color advice">
    <div className="flex items-start gap-3">
      <p role="status" className="min-w-0 flex-1">Some colors may be hard to read together. You can still preview, save and publish this theme.</p>
      <Button variant="ghost" size="icon" aria-label="Dismiss color advice" onClick={() => setVisible(false)}><X size={14} /></Button>
    </div>
    <Button variant="ghost" size="sm" className="mt-2" aria-expanded={details} aria-controls={detailsID} onClick={() => setDetails(!details)}>{details ? "Hide Details" : "Show Details"}</Button>
    {details && <ul id={detailsID} className="mt-3 space-y-3 text-foreground-muted">
      {warnings.map((warning, index) => <li key={`${warning.path}:${index}`}><span className="block font-medium text-foreground">{warning.path.replace(/^\/tokens\//, "").replaceAll(".", " · ").replace(/([a-z])([A-Z])/g, "$1 $2").replace(/\b\w/g, (letter) => letter.toUpperCase())}</span>{warning.message}</li>)}
    </ul>}
  </section>;
}
