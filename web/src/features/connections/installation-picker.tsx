import { ExternalLink } from "lucide-react";
import type { components } from "@/api/schema";
import { cn } from "@/lib/utils";
import { safeGitHubInstallationManageURL } from "./github-url";

type Candidate = components["schemas"]["GitHubInstallationCandidate"];

/**
 * The accounts an App is installed on, as checkboxes.
 *
 * One connection covers whichever of these the operator ticks, so setup and the
 * saved connection ask the question the same way and can never drift apart.
 */
export function InstallationPicker({
  candidates,
  selected,
  onToggle,
  disabled = false,
  managementLinksDisabled = false,
  name,
}: {
  candidates: Candidate[];
  selected: ReadonlySet<string>;
  onToggle: (installationID: string) => void;
  disabled?: boolean;
  managementLinksDisabled?: boolean;
  name: string;
}) {
  return (
    <div className="grid gap-2 sm:grid-cols-2">
      {candidates.map((candidate) => {
        const isSelected = selected.has(candidate.installation_id);
        const manageURL = safeGitHubInstallationManageURL(
          candidate.app_slug,
          candidate.account_id,
        );
        return (
          <label
            key={candidate.installation_id}
            className={cn(
              "rounded-xl border p-4 transition",
              disabled ? "cursor-not-allowed opacity-60" : "cursor-pointer",
              isSelected
                ? "border-gold bg-gold/10"
                : "border-border bg-background/40 hover:border-gold/40",
            )}
          >
            <input
              className="sr-only"
              type="checkbox"
              name={name}
              value={candidate.installation_id}
              checked={isSelected}
              disabled={disabled}
              onChange={() => onToggle(candidate.installation_id)}
            />
            <span className="flex items-start gap-2">
              <span
                aria-hidden
                className={cn(
                  "mt-0.5 flex h-4 w-4 flex-none items-center justify-center border text-[10px] font-bold",
                  "rounded",
                  isSelected ? "border-gold bg-gold text-background" : "border-border",
                )}
              >
                {isSelected ? "✓" : ""}
              </span>
              <span className="min-w-0">
                <span className="block truncate font-medium">@{candidate.account_login}</span>
                {candidate.repository_selection && (
                  <span className="mt-1 block text-xs text-foreground-muted">
                    {candidate.repository_selection === "all"
                      ? "All Repositories"
                      : "Selected Repositories"}
                  </span>
                )}
                {manageURL && (managementLinksDisabled ? (
                  <span
                    aria-disabled="true"
                    className="mt-1 inline-flex items-center gap-1 text-xs text-foreground-muted"
                  >
                    Manage Repositories on GitHub
                    <ExternalLink className="h-3 w-3" />
                  </span>
                ) : (
                  <a
                    className="mt-1 inline-flex items-center gap-1 text-xs text-gold underline-offset-2 hover:underline"
                    href={manageURL}
                    target="_blank"
                    rel="noreferrer"
                    onClick={(event) => event.stopPropagation()}
                  >
                    Manage Repositories on GitHub
                    <ExternalLink className="h-3 w-3" />
                  </a>
                ))}
              </span>
            </span>
          </label>
        );
      })}
    </div>
  );
}
