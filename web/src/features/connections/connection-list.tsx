import { Github } from "lucide-react";
import type { components } from "@/api/schema";
import { Button } from "@/components/ui/button";
import { RelativeTime } from "@/components/ui/relative-time";

type Connection = components["schemas"]["Connection"];
function connectionStatus(connection: Connection) {
  const key =
    connection.lifecycle !== "enabled"
      ? connection.lifecycle
      : connection.credential_state === "missing"
        ? "missing_credential"
        : connection.health_class;
  const labels: Record<Connection["health_class"], string> = {
    healthy: "Healthy",
    missing_credential: "Credentials Missing",
    never_checked: "Not Checked",
    permission_mismatch: "Review Permissions",
    revoked: "Reconnect Required",
    suspended: "Suspended",
    rate_limited: "Rate Limited",
    unavailable: "Unavailable",
    disabled: "Disabled",
    removal_pending: "Removal Pending",
    removed: "Removed",
  };
  return {
    label: labels[key],
    tone:
      key === "healthy"
        ? "succeeded"
        : key === "disabled" || key === "never_checked"
          ? "neutral"
          : "attention",
  };
}
export function ConnectionStatus({ connection }: { connection: Connection }) {
  const status = connectionStatus(connection);
  return (
    <span
      className="inline-flex items-center gap-2 text-xs"
      style={{ color: `var(--theme-status-${status.tone}-foreground)` }}
    >
      <span
        aria-hidden="true"
        className="h-1.5 w-1.5 rounded-full bg-current"
      />
      {status.label}
    </span>
  );
}
export function ConnectionList({
  items,
  onManage,
}: {
  items: Connection[];
  onManage: (id: string) => void;
}) {
  return (
    <div className="divide-y divide-border overflow-hidden rounded-xl border border-border bg-surface">
      {items.map((connection) => (
        <article
          key={connection.connection_id}
          className="flex flex-wrap items-center justify-between gap-5 p-5 sm:p-6"
        >
          <div className="flex min-w-0 items-start gap-4">
            <Github
              className="mt-1 h-5 w-5 shrink-0 text-foreground-muted"
              aria-hidden="true"
            />
            <div className="min-w-0">
              <h2 className="break-words text-base font-medium">
                {connection.display_name}
              </h2>
              <p className="mt-1 text-sm text-foreground-muted">
                GitHub ·{" "}
                {connection.installations
                  .map((installation) =>
                    installation.account_login
                      ? `@${installation.account_login}`
                      : "Account Awaiting Validation",
                  )
                  .join(", ") || "No Accounts"}
              </p>
              {connection.last_checked_at && (
                <p className="mt-2 text-xs text-foreground-subtle">
                  Checked <RelativeTime iso={connection.last_checked_at} />
                </p>
              )}
            </div>
          </div>
          <div className="flex items-center gap-5">
            <ConnectionStatus connection={connection} />
            <Button
              variant="outline"
              size="sm"
              onClick={() => onManage(connection.connection_id)}
              aria-label={`Manage ${connection.display_name}`}
            >
              Manage
            </Button>
          </div>
        </article>
      ))}
    </div>
  );
}
