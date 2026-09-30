import { Github } from "lucide-react";
import { useEffect, useState } from "react";

/**
 * Landing page for the dedicated GitHub setup tab.
 *
 * Every GitHub manifest callback redirects here instead of to /connections so
 * this tab never boots a second copy of the app that would race the driving
 * tab's flow advancement. It performs no API calls: the driving tab owns the
 * flow and polls for the stage change this redirect just produced.
 */
export function GitHubSetupReturnPage() {
  const [closeBlocked, setCloseBlocked] = useState(false);

  useEffect(() => {
    window.close();
    // A browser may refuse to close a tab it did not open by script. Give it a
    // moment, then tell the operator what to do instead of leaving a tab that
    // looks stuck.
    const timer = window.setTimeout(() => setCloseBlocked(true), 400);
    return () => window.clearTimeout(timer);
  }, []);

  return (
    <main className="flex min-h-screen items-center justify-center bg-background p-6 text-center">
      <div className="max-w-sm space-y-4">
        <div className="mx-auto w-fit rounded-xl border border-accent/30 bg-accent/10 p-3">
          <Github className="h-6 w-6 text-accent" />
        </div>
        <h1 className="font-display text-lg font-semibold text-foreground">
          Step Captured
        </h1>
        <p className="text-sm leading-relaxed text-foreground-muted" aria-live="polite">
          {closeBlocked
            ? "You can close this tab and continue in Kingdom."
            : "Returning to Kingdom…"}
        </p>
      </div>
    </main>
  );
}
