import { useNavigate, useSearch } from "@tanstack/react-router";
import { ApiError, apiFetch } from "@/api/client";
import { CopyButton } from "@/components/ui/copy-button";
import { fieldClass } from "@/components/ui/field-styles";
import { cn } from "@/lib/utils";
import {
  openSetupWindow,
  postManifestToSetupWindow,
  setupWindowBlockedMessage,
} from "./github-setup-window";
import { safeGitHubInstallationManageURL } from "./github-url";
import { InstallationPicker } from "./installation-picker";
import type { components } from "@/api/schema";
import { WorkspacePage } from "@/components/workspace-page";
import { ConnectionList, ConnectionStatus } from "./connection-list";
import { RelativeTime } from "@/components/ui/relative-time";
import { newIdempotencyKey as idem } from "@/lib/idempotency";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { Card, CardContent } from "@/components/ui/card";
import {
  ConfirmDialog,
  Modal,
  ModalFooter,
  MODAL_BODY_TEXT_CLASS,
} from "@/components/ui/modal-shell";
import {
  CheckCircle2,
  ChevronDown,
  ExternalLink,
  Github,
  KeyRound,
  Loader2,
  Pencil,
  ShieldCheck,
  X,
} from "lucide-react";
import { useQueryClient } from "@tanstack/react-query";
import { connectionKeys, useConnections } from "@/api/hooks";
import { useCallback, useEffect, useRef, useState } from "react";
import type { FormEvent } from "react";
import { externalSourceFailureGuidance } from "./external-source-status";
import {
  shouldRetainMutationRequest,
  useRecoverableMutation,
} from "@/lib/use-recoverable-mutation";

type Connection = components["schemas"]["Connection"];
type FlowStatus = components["schemas"]["GitHubManifestFlowStatus"];
type StartManifestFlowPayload = {
  flow_id: string;
  form_action: string;
  manifest: string;
  expires_at: string;
};
type ConnectionInstallation = components["schemas"]["ConnectionInstallation"];
type ConnectionForm = Pick<
  Connection,
  "display_name" | "app_id" | "client_id"
> & {
  installation_id: string;
};
type SetupOptions = components["schemas"]["GitHubSetupOptions"];
type GeneratedDeviceFlow = components["schemas"]["GitHubDeviceFlowStatus"];
type DeviceFlow = GeneratedDeviceFlow;
type RequestError = { message: string; retryAt?: string };
const noConnections: Connection[] = [];

function isAmbiguousSetupFailure(cause: unknown): boolean {
  return shouldRetainMutationRequest(cause);
}
type ManifestFailureStage = "start" | FlowStatus["stage"];
type ManifestRecovery = "retry" | "restart";

const emptyForm: ConnectionForm = {
  display_name: "",
  app_id: "",
  client_id: "",
  installation_id: "",
};
const devicePollFailure =
  "GitHub authorization could not be completed. Start again.";
const devicePollRetry =
  "GitHub authorization status could not be checked. Retrying safely.";

// Stages where GitHub, not Kingdom, is holding the flow.
const manifestWaitingStages = new Set<FlowStatus["stage"]>([
  "awaiting_app_creation",
  "awaiting_installation",
  "awaiting_authorization",
]);
const manifestPollIntervalMs = 2000;
const sharedStatusPollIntervalMs = 250;
const sharedConflictDiscoveryAttempts = 5;
const sharedConflictDiscoveryIntervalMs = 250;
const sharedTransientStages = new Set<DeviceFlow["stage"]>([
  "starting_authorization",
  "discovering_installations",
  "finalizing",
]);

const flowCopy: Record<FlowStatus["stage"], string> = {
  awaiting_app_creation: "Finish creating the private App on GitHub.",
  app_created: "Securing the App credentials returned by GitHub.",
  converting_app: "GitHub is creating the App credentials.",
  awaiting_installation:
    "Choose the account and repositories for this installation.",
  installation_received:
    "Confirm which GitHub user can access this installation.",
  awaiting_authorization: "Finish the temporary GitHub authorization.",
  authorization_received: "Proving the installation before anything is saved.",
  validating_connection: "Validating the App key and repository access.",
  completed: "The connection is healthy and ready.",
  failed: "This setup flow cannot continue.",
};

const manifestFailureCopy: Record<ManifestFailureStage, string> = {
  start: "Private App setup could not start.",
  awaiting_app_creation: "GitHub App creation did not return to Kingdom.",
  app_created:
    "Kingdom could not secure the App credentials returned by GitHub.",
  converting_app: "GitHub App credential creation did not finish.",
  awaiting_installation: "Kingdom could not open GitHub App installation.",
  installation_received:
    "Kingdom could not start temporary GitHub authorization.",
  awaiting_authorization: "GitHub authorization did not return to Kingdom.",
  authorization_received:
    "Kingdom could not validate and save the GitHub connection.",
  validating_connection: "GitHub connection validation did not finish.",
  completed: "The private GitHub App connection is ready.",
  failed: "This private App setup flow cannot continue.",
};

function InstallationList({
  connection,
  managementLinksDisabled = false,
}: {
  connection: Connection;
  managementLinksDisabled?: boolean;
}) {
  if (connection.installations.length === 0) {
    return (
      <p className="mt-2 text-sm text-foreground-muted">
        No installations yet.
      </p>
    );
  }
  return (
    <ul className="mt-2 space-y-1">
      {connection.installations.map((installation: ConnectionInstallation) => {
        const href = safeGitHubInstallationManageURL(
          connection.app_slug,
          installation.account_id ?? "",
        );
        return (
          <li
            key={installation.installation_id}
            className="flex flex-wrap items-center gap-2 text-sm"
          >
            <span className="font-medium">
              @{installation.account_login ?? "Account pending validation"}
            </span>
            {href &&
              (managementLinksDisabled ? (
                <span
                  aria-disabled="true"
                  className="inline-flex items-center gap-1 text-xs text-foreground-muted"
                >
                  Manage Repositories on GitHub
                  <ExternalLink className="h-3 w-3" />
                </span>
              ) : (
                <a
                  className="inline-flex items-center gap-1 text-xs text-accent underline-offset-2 hover:underline"
                  href={href}
                  target="_blank"
                  rel="noreferrer"
                >
                  Manage Repositories on GitHub
                  <ExternalLink className="h-3 w-3" />
                </a>
              ))}
          </li>
        );
      })}
    </ul>
  );
}

function requestError(cause: unknown): RequestError {
  if (cause instanceof ApiError)
    return { message: cause.message, retryAt: cause.retryAt };
  return {
    message: cause instanceof Error ? cause.message : "GitHub request failed",
  };
}

function MetadataFields({
  value,
  onChange,
  disabled = false,
}: {
  value: ConnectionForm;
  onChange: (value: ConnectionForm) => void;
  disabled?: boolean;
}) {
  const field = (
    key: keyof ConnectionForm,
    label: string,
    maxLength: number,
  ) => (
    <label className="text-xs font-medium text-foreground-muted">
      {label}
      <input
        required
        disabled={disabled}
        maxLength={maxLength}
        className={cn(fieldClass, "mt-1")}
        value={value[key]}
        onChange={(event) => onChange({ ...value, [key]: event.target.value })}
      />
    </label>
  );
  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {field("display_name", "Display Name", 120)}
      {field("app_id", "App ID", 64)}
      {field("client_id", "Client ID", 128)}
      {field("installation_id", "Installation ID", 64)}
    </div>
  );
}

function ErrorNotice({ error }: { error?: RequestError }) {
  if (!error) return null;
  return (
    <div
      role="alert"
      className="rounded-lg border border-destructive/40 bg-destructive/10 px-4 py-3 text-sm text-destructive"
    >
      {error.message}
      {error.retryAt ? (
        <p className="mt-1 text-xs">
          Retry after{" "}
          <time dateTime={error.retryAt}>
            {new Date(error.retryAt).toLocaleString()}
          </time>
          .
        </p>
      ) : null}
    </div>
  );
}

function ManifestSetupStatus({
  flow,
  error,
  failureStage,
  recovery,
  busy,
  onRecover,
}: {
  flow?: FlowStatus;
  error?: RequestError;
  failureStage?: ManifestFailureStage;
  recovery?: ManifestRecovery;
  busy: boolean;
  onRecover: () => void;
}) {
  if (!flow && !error) return null;
  const failed = Boolean(error) || flow?.stage === "failed";
  const stage = failureStage ?? flow?.stage ?? "start";
  return (
    <div
      role={failed ? "alert" : "status"}
      className={`rounded-lg border p-3 ${failed ? "border-destructive/40 bg-destructive/10" : "border-border/80 bg-background/70"}`}
    >
      <div className="flex items-start gap-2 text-sm font-medium">
        {busy ? (
          <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin text-accent" />
        ) : (
          <Github className="mt-0.5 h-4 w-4 shrink-0 text-accent" />
        )}
        <div className="min-w-0">
          <p>{flow?.app_name || "Private GitHub App setup"}</p>
          <p
            className={`mt-1 text-xs font-normal leading-relaxed ${failed ? "text-destructive" : "text-foreground-muted"}`}
          >
            {error
              ? `${manifestFailureCopy[stage]} ${error.message}`
              : flow?.failure ||
                (flow ? flowCopy[flow.stage] : manifestFailureCopy[stage])}
          </p>
          {error?.retryAt ? (
            <p className="mt-1 text-xs text-destructive">
              Retry after{" "}
              <time dateTime={error.retryAt}>
                {new Date(error.retryAt).toLocaleString()}
              </time>
              .
            </p>
          ) : null}
        </div>
      </div>
      {failed ? (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="mt-3"
          disabled={busy}
          onClick={onRecover}
        >
          {recovery === "retry" ? "Retry This Step" : "Start Again"}
        </Button>
      ) : null}
    </div>
  );
}

const manifestHandoffCopy: Partial<Record<FlowStatus["stage"], string>> = {
  awaiting_installation: "Choose Repositories on GitHub",
  awaiting_authorization: "Authorize on GitHub",
};

/**
 * Opens the next GitHub hop in the dedicated setup tab. The operator clicks it,
 * because a tab opened from an effect is blocked as an unsolicited pop-up and
 * because nothing should navigate this tab away mid-flow.
 */
function ManifestHandoffAction({
  flow,
  url,
  busy,
  onBlocked,
}: {
  flow?: FlowStatus;
  url: string;
  busy: boolean;
  onBlocked: () => void;
}) {
  const label = flow ? manifestHandoffCopy[flow.stage] : undefined;
  if (!flow || !label || !url) return null;
  return (
    <div className="space-y-2">
      <Button
        type="button"
        className="w-full"
        disabled={busy}
        onClick={() => {
          if (!openSetupWindow(url)) onBlocked();
        }}
      >
        <ExternalLink className="h-4 w-4" />
        {label}
      </Button>
      <p
        className="flex items-center justify-center gap-2 text-xs text-foreground-muted"
        aria-live="polite"
      >
        <Loader2 className="h-3.5 w-3.5 animate-spin text-accent" />
        Kingdom continues as soon as GitHub returns
      </p>
    </div>
  );
}

export function ConnectionsPage() {
  // The shared cache is the list. This page manages connections, so it is the
  // one that invalidates, but it reads the same entry as every other surface
  // rather than keeping a copy that can fall behind them.
  const queryClient = useQueryClient();
  const connections = useConnections();
  const items = connections.data?.items ?? noConnections;
  const loading = connections.isPending;
  const selectedConnectionID = useSearch({
    from: "/connections",
    select: (search) => search.connection ?? "",
  });
  const navigate = useNavigate({ from: "/connections" });
  const setSelectedConnectionID = (connection: string) => {
    void navigate({ search: connection ? { connection } : {} });
  };
  const [addOpen, setAddOpen] = useState(false);
  const [error, setError] = useState<RequestError>();
  const [manifestError, setManifestError] = useState<RequestError>();
  const [manifestFailureStage, setManifestFailureStage] =
    useState<ManifestFailureStage>();
  const [manifestRecovery, setManifestRecovery] = useState<ManifestRecovery>();
  const [manifestRetryAttempt, setManifestRetryAttempt] = useState(0);
  const [manifestPollAttempt, setManifestPollAttempt] = useState(0);
  const [sharedError, setSharedError] = useState<RequestError>();
  const [operation, setOperation] = useState("");
  const [flow, setFlow] = useState<FlowStatus>();
  const [setupOptions, setSetupOptions] = useState<SetupOptions>({
    shared_enabled: false,
  });
  const [sharedFlow, setSharedFlow] = useState<DeviceFlow>();
  const [sharedStatusAttempt, setSharedStatusAttempt] = useState(0);
  const [sharedOpen, setSharedOpen] = useState(false);
  const [selectedInstallations, setSelectedInstallations] = useState<
    Set<string>
  >(new Set());
  // Which saved connection has its installation editor open, and what that
  // editor is offering.
  const [installationsEditingID, setInstallationsEditingID] = useState("");
  const [installationCandidates, setInstallationCandidates] = useState<
    DeviceFlow["candidates"]
  >([]);
  const [installationSelection, setInstallationSelection] = useState<
    Set<string>
  >(new Set());
  const [sharedReconnectID, setSharedReconnectID] = useState("");
  const [manifestOpen, setManifestOpen] = useState(false);
  // The GitHub URL for the current stage, opened only when the operator asks.
  const [manifestHandoffURL, setManifestHandoffURL] = useState("");
  const [ownerType, setOwnerType] = useState<"personal" | "organization">(
    "personal",
  );
  const [organization, setOrganization] = useState("");
  const [manualOpen, setManualOpen] = useState(false);
  const [manualForm, setManualForm] = useState<ConnectionForm>(emptyForm);
  const [manualSaved, setManualSaved] = useState<Connection>();
  const [editingID, setEditingID] = useState("");
  const [editName, setEditName] = useState("");
  const [pendingRemoval, setPendingRemoval] = useState<Connection>();
  const [removalError, setRemovalError] = useState<RequestError>();
  const advancing = useRef("");
  const setupRequestKeys = useRef(new Map<string, string>());
  const mounted = useRef(false);
  const sharedFlowID = useRef<string | undefined>(undefined);
  const sharedStartAttempt = useRef(0);
  const sharedResumeRef = useRef<HTMLButtonElement | null>(null);
  const itemsRef = useRef(items);
  itemsRef.current = items;
  const createConnection = useRecoverableMutation(
    "connection",
    (form: ConnectionForm, key) =>
      apiFetch<Connection>("/connections", {
        method: "POST",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({
          provider: "github",
          credential_strategy: "app_private_key",
          ...form,
        }),
      }),
  );
  const editConnection = useRecoverableMutation(
    "connection-edit",
    (request: { connectionId: string; displayName: string }, key) =>
      apiFetch(`/connections/${request.connectionId}`, {
        method: "PATCH",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({ display_name: request.displayName }),
      }),
  );
  const checkConnection = useRecoverableMutation(
    "check",
    (connectionId: string, key) =>
      apiFetch(`/connections/${connectionId}/health-checks`, {
        method: "POST",
        headers: { "Idempotency-Key": key },
      }),
  );
  const toggleConnection = useRecoverableMutation(
    "toggle",
    (request: { connectionId: string; enabled: boolean }, key) =>
      apiFetch(`/connections/${request.connectionId}`, {
        method: "PATCH",
        headers: { "Idempotency-Key": key },
        body: JSON.stringify({ enabled: request.enabled }),
      }),
  );
  const removeConnection = useRecoverableMutation(
    "remove",
    (connectionId: string, key) =>
      apiFetch(`/connections/${connectionId}`, {
        method: "DELETE",
        headers: { "Idempotency-Key": key },
      }),
  );
  const ordinaryRecoveryPending =
    createConnection.hasPendingRecovery ||
    editConnection.hasPendingRecovery ||
    checkConnection.hasPendingRecovery ||
    toggleConnection.hasPendingRecovery ||
    removeConnection.hasPendingRecovery;

  const setupRequestKey = (requestID: string, scope: string) => {
    const existing = setupRequestKeys.current.get(requestID);
    if (existing) return existing;
    const key = idem(scope);
    setupRequestKeys.current.set(requestID, key);
    return key;
  };

  const settleSetupRequest = (requestID: string, cause?: unknown) => {
    if (!cause || !isAmbiguousSetupFailure(cause))
      setupRequestKeys.current.delete(requestID);
  };

  // Stable so the manifest-flow effect can depend on it without re-running the
  // flow advance on every render.
  const reloadConnections = useCallback(
    () => queryClient.invalidateQueries({ queryKey: connectionKeys.list() }),
    [queryClient],
  );

  const adoptSharedFlow = useCallback(
    (next?: DeviceFlow, preserveFreshSelection = false) => {
      const sameFlow = !!next && sharedFlowID.current === next.flow_id;
      sharedFlowID.current = next?.flow_id;
      setSharedFlow(next);

      const reconnectID = next?.reconnect_connection_id ?? "";
      setSharedReconnectID(reconnectID);
      if (!next) {
        setSelectedInstallations(new Set());
        return;
      }
      if (reconnectID) {
        const reconnect = itemsRef.current.find(
          (connection) => connection.connection_id === reconnectID,
        );
        setSelectedInstallations(
          new Set(
            reconnect?.installations.map(
              (installation) => installation.installation_id,
            ) ?? [],
          ),
        );
        return;
      }
      if (preserveFreshSelection && sameFlow) {
        const candidateIDs = new Set(
          next.candidates.map((candidate) => candidate.installation_id),
        );
        setSelectedInstallations(
          (existing) =>
            new Set(
              [...existing].filter((installationID) =>
                candidateIDs.has(installationID),
              ),
            ),
        );
        return;
      }
      setSelectedInstallations(new Set());
    },
    [],
  );

  const canAcceptSharedFlow = useCallback(
    (flowID: string) => mounted.current && sharedFlowID.current === flowID,
    [],
  );

  const acceptSharedStatus = useCallback(
    async (
      next: DeviceFlow,
      expectedFlowID?: string,
      preserveFreshSelection = true,
    ) => {
      if (expectedFlowID && !canAcceptSharedFlow(expectedFlowID)) return;
      if (next.stage === "completed") {
        await reloadConnections();
        if (expectedFlowID && !canAcceptSharedFlow(expectedFlowID)) return;
        adoptSharedFlow(undefined);
        setSharedError(undefined);
        setSharedOpen(false);
        return;
      }
      adoptSharedFlow(next, preserveFreshSelection);
      setSharedError(undefined);
    },
    [adoptSharedFlow, canAcceptSharedFlow, reloadConnections],
  );

  const discoverCurrentSharedFlow = useCallback(async () => {
    let lastError: unknown;
    for (
      let attempt = 0;
      attempt < sharedConflictDiscoveryAttempts;
      attempt += 1
    ) {
      if (!mounted.current) return undefined;
      try {
        const current = await apiFetch<DeviceFlow | undefined>(
          "/connections/github/device-flows",
        );
        if (current?.flow_id) return current;
      } catch (cause) {
        if (cause instanceof ApiError && cause.status === 401) throw cause;
        lastError = cause;
      }
      if (attempt + 1 < sharedConflictDiscoveryAttempts) {
        await new Promise((resolve) =>
          window.setTimeout(resolve, sharedConflictDiscoveryIntervalMs),
        );
      }
    }
    if (lastError) throw lastError;
    return undefined;
  }, []);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      sharedFlowID.current = undefined;
      sharedStartAttempt.current += 1;
    };
  }, []);

  useEffect(() => {
    let active = true;
    void (async () => {
      let options: SetupOptions = { shared_enabled: false };
      try {
        options = await apiFetch<SetupOptions>(
          "/connections/github/setup-options",
        );
        if (active) setSetupOptions(options);
      } catch {
        if (active) setSetupOptions({ shared_enabled: false });
      }
      try {
        const currentFlow = await apiFetch<FlowStatus>(
          "/connections/github/manifest-flows",
        );
        // Server flow state is memory-only and lives for an hour, so it
        // outlives an abandoned setup. Adopt it as resumable state, but never
        // open a modal on the operator's behalf: returning to this page is not
        // a request to resume.
        if (
          active &&
          currentFlow &&
          typeof currentFlow.flow_id === "string" &&
          currentFlow.stage in flowCopy
        ) {
          setFlow(currentFlow);
        }
      } catch (cause) {
        if (active && cause instanceof ApiError && cause.status !== 404)
          setManifestError(requestError(cause));
      }
      if (options.shared_enabled) {
        try {
          const currentSharedFlow = await apiFetch<DeviceFlow>(
            "/connections/github/device-flows",
          );
          if (active && currentSharedFlow?.flow_id) {
            adoptSharedFlow(currentSharedFlow);
          }
        } catch {
          // Discovery is best-effort. A start still receives the canonical
          // binding/intent conflict if an undiscovered flow is active.
        }
      }
    })();
    return () => {
      active = false;
    };
  }, [adoptSharedFlow]);

  useEffect(() => {
    if (!sharedFlow?.reconnect_connection_id) return;
    const reconnect = items.find(
      (connection) =>
        connection.connection_id === sharedFlow.reconnect_connection_id,
    );
    if (!reconnect) return;
    setSharedReconnectID(reconnect.connection_id);
    setSelectedInstallations(
      new Set(
        reconnect.installations.map(
          (installation) => installation.installation_id,
        ),
      ),
    );
  }, [items, sharedFlow?.flow_id, sharedFlow?.reconnect_connection_id]);

  useEffect(() => {
    const advanceID = flow
      ? `${flow.flow_id}:${flow.stage}:${manifestRetryAttempt}`
      : "";
    if (!flow || advancing.current === advanceID) return;
    const advance = async () => {
      advancing.current = advanceID;
      const requestID = `manifest:${flow.flow_id}:${flow.stage}`;
      setManifestError(undefined);
      setManifestFailureStage(undefined);
      setManifestRecovery(undefined);
      // Server work happens here; opening GitHub does not. A tab opened from an
      // effect is blocked as an unsolicited pop-up, and silently navigating the
      // operator away is what this flow is being fixed to stop doing.
      setManifestHandoffURL("");
      try {
        if (flow.stage === "app_created") {
          setOperation("flow-conversion");
          // Conversion's installation URL is not used: the awaiting_installation
          // step below fetches it, so resuming a flow and completing one take
          // exactly the same path.
          await apiFetch(
            `/connections/github/manifest-flows/${flow.flow_id}/conversion`,
            {
              method: "POST",
              headers: {
                "Idempotency-Key": setupRequestKey(
                  requestID,
                  "manifest-conversion",
                ),
              },
            },
          );
          settleSetupRequest(requestID);
          // The server has already moved on; mirroring the stage here is what
          // surfaces the next hop and starts watching for GitHub's return.
          setFlow({ ...flow, stage: "awaiting_installation" });
        } else if (flow.stage === "awaiting_installation") {
          setOperation("flow-installation");
          const result = await apiFetch<{ installation_url: string }>(
            `/connections/github/manifest-flows/${flow.flow_id}/installation`,
            {
              method: "POST",
              headers: {
                "Idempotency-Key": setupRequestKey(
                  requestID,
                  "manifest-installation",
                ),
              },
            },
          );
          settleSetupRequest(requestID);
          setManifestHandoffURL(result.installation_url);
        } else if (flow.stage === "installation_received") {
          setOperation("flow-authorization");
          const result = await apiFetch<{ authorization_url: string }>(
            `/connections/github/manifest-flows/${flow.flow_id}/authorization`,
            {
              method: "POST",
              headers: {
                "Idempotency-Key": setupRequestKey(
                  requestID,
                  "manifest-authorization",
                ),
              },
            },
          );
          settleSetupRequest(requestID);
          setManifestHandoffURL(result.authorization_url);
          setFlow({ ...flow, stage: "awaiting_authorization" });
        } else if (flow.stage === "authorization_received") {
          setOperation("flow-completion");
          await apiFetch<Connection>(
            `/connections/github/manifest-flows/${flow.flow_id}/completion`,
            {
              method: "POST",
              headers: {
                "Idempotency-Key": setupRequestKey(
                  requestID,
                  "manifest-completion",
                ),
              },
            },
          );
          settleSetupRequest(requestID);
          await reloadConnections();
          setFlow({ ...flow, stage: "completed" });
        }
      } catch (cause) {
        settleSetupRequest(requestID, cause);
        setManifestError(requestError(cause));
        setManifestFailureStage(flow.stage);
        setManifestRecovery(
          isAmbiguousSetupFailure(cause) ? "retry" : "restart",
        );
      } finally {
        setOperation("");
      }
    };
    if (
      [
        "app_created",
        "awaiting_installation",
        "installation_received",
        "authorization_received",
      ].includes(flow.stage)
    )
      void advance();
  }, [flow, manifestRetryAttempt, reloadConnections]);

  // While GitHub holds the setup tab, this tab watches for the stage change the
  // callback produces. Without it the operator would have to reload the page to
  // see their own progress.
  useEffect(() => {
    if (!flow || !manifestWaitingStages.has(flow.stage) || manifestError)
      return;
    let active = true;
    const timer = window.setTimeout(() => {
      void (async () => {
        try {
          const next = await apiFetch<FlowStatus>(
            `/connections/github/manifest-flows/${flow.flow_id}`,
          );
          if (!active || !next || next.stage === flow.stage) {
            if (active) setManifestPollAttempt((attempt) => attempt + 1);
            return;
          }
          setFlow(next);
        } catch {
          // A transient status read must not fail the flow; the operator's own
          // GitHub tab is still the authority on progress.
          if (active) setManifestPollAttempt((attempt) => attempt + 1);
        }
      })();
    }, manifestPollIntervalMs);
    return () => {
      active = false;
      window.clearTimeout(timer);
    };
  }, [flow, manifestError, manifestPollAttempt]);

  useEffect(() => {
    if (!sharedOpen || sharedFlow?.stage !== "awaiting_authorization") return;
    const polledFlowID = sharedFlow.flow_id;
    const retryAt = sharedFlow.retry_at
      ? Date.parse(sharedFlow.retry_at)
      : Date.now() + sharedFlow.interval_seconds * 1000;
    const timer = window.setTimeout(
      () => {
        void (async () => {
          const requestID = `device-poll:${sharedFlow.flow_id}`;
          try {
            const next = await apiFetch<DeviceFlow>(
              `/connections/github/device-flows/${sharedFlow.flow_id}/poll`,
              {
                method: "POST",
                headers: {
                  "Idempotency-Key": setupRequestKey(requestID, "device-poll"),
                },
              },
            );
            settleSetupRequest(requestID);
            if (!canAcceptSharedFlow(polledFlowID)) return;
            await acceptSharedStatus(next, polledFlowID);
          } catch (cause) {
            settleSetupRequest(requestID, cause);
            if (!canAcceptSharedFlow(polledFlowID)) return;
            if (cause instanceof ApiError && cause.status === 429) {
              const nextRetryAt =
                cause.retryAt ??
                new Date(
                  Date.now() + sharedFlow.interval_seconds * 1000,
                ).toISOString();
              setSharedError(undefined);
              setSharedFlow((current) =>
                current?.flow_id === sharedFlow.flow_id
                  ? { ...current, retry_at: nextRetryAt }
                  : current,
              );
            } else if (
              !isAmbiguousSetupFailure(cause) &&
              cause instanceof ApiError
            ) {
              setSharedError(undefined);
              setSharedFlow((current) =>
                current?.flow_id === sharedFlow.flow_id
                  ? {
                      ...current,
                      stage: "failed",
                      user_code: undefined,
                      verification_uri: undefined,
                      retry_at: undefined,
                      failure: devicePollFailure,
                    }
                  : current,
              );
            } else {
              const nextRetryAt = new Date(
                Date.now() + sharedFlow.interval_seconds * 1000,
              ).toISOString();
              setSharedError({
                message: devicePollRetry,
                retryAt: nextRetryAt,
              });
              setSharedFlow((current) =>
                current?.flow_id === sharedFlow.flow_id
                  ? { ...current, retry_at: nextRetryAt }
                  : current,
              );
            }
          }
        })();
      },
      Math.min(2_147_000_000, Math.max(0, retryAt - Date.now())),
    );
    return () => {
      window.clearTimeout(timer);
    };
  }, [acceptSharedStatus, canAcceptSharedFlow, sharedFlow, sharedOpen]);

  useEffect(() => {
    if (!sharedFlow || !sharedTransientStages.has(sharedFlow.stage)) return;
    const watchedFlowID = sharedFlow.flow_id;
    const timer = window.setTimeout(() => {
      void (async () => {
        try {
          const next = await apiFetch<DeviceFlow>(
            `/connections/github/device-flows/${watchedFlowID}`,
          );
          if (!canAcceptSharedFlow(watchedFlowID)) return;
          await acceptSharedStatus(next, watchedFlowID);
        } catch {
          if (canAcceptSharedFlow(watchedFlowID)) {
            setSharedStatusAttempt((attempt) => attempt + 1);
          }
        }
      })();
    }, sharedStatusPollIntervalMs);
    return () => window.clearTimeout(timer);
  }, [
    acceptSharedStatus,
    canAcceptSharedFlow,
    sharedFlow,
    sharedStatusAttempt,
  ]);

  const startShared = async (connectionID?: string) => {
    if (sharedFlowLive) {
      setSharedOpen(true);
      return;
    }
    const startAttempt = ++sharedStartAttempt.current;
    const requestID = `device-start:${connectionID || "new"}`;
    setOperation("shared-start");
    setSharedError(undefined);
    adoptSharedFlow(undefined);
    const reconnect = items.find(
      (connection) => connection.connection_id === connectionID,
    );
    setSelectedInstallations(
      new Set(
        reconnect?.installations.map(
          (installation) => installation.installation_id,
        ) ?? [],
      ),
    );
    setSharedReconnectID(connectionID || "");
    setSharedOpen(true);
    try {
      const next = await apiFetch<DeviceFlow>(
        "/connections/github/device-flows",
        {
          method: "POST",
          headers: {
            "Idempotency-Key": setupRequestKey(requestID, "device-start"),
          },
          body: JSON.stringify(
            connectionID ? { connection_id: connectionID } : {},
          ),
        },
      );
      settleSetupRequest(requestID);
      if (!mounted.current || sharedStartAttempt.current !== startAttempt)
        return;
      await acceptSharedStatus(next, undefined, false);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      if (!mounted.current || sharedStartAttempt.current !== startAttempt)
        return;
      if (cause instanceof ApiError && cause.status === 409) {
        try {
          const current = await discoverCurrentSharedFlow();
          if (!mounted.current || sharedStartAttempt.current !== startAttempt)
            return;
          if (current?.flow_id) {
            await acceptSharedStatus(current, undefined, false);
            setSharedOpen(false);
            return;
          }
        } catch {
          // Keep the start conflict when canonical rediscovery is unavailable.
        }
      }
      setSharedError(requestError(cause));
    } finally {
      if (mounted.current && sharedStartAttempt.current === startAttempt) {
        setOperation("");
      }
    }
  };

  const refreshCandidates = async () => {
    if (!sharedFlow || operation === "shared-finalize") return;
    const refreshedFlowID = sharedFlow.flow_id;
    const requestID = `device-candidates:${refreshedFlowID}`;
    setOperation("shared-refresh");
    setSharedError(undefined);
    try {
      const next = await apiFetch<DeviceFlow>(
        `/connections/github/device-flows/${refreshedFlowID}/candidates`,
        {
          method: "POST",
          headers: {
            "Idempotency-Key": setupRequestKey(requestID, "device-candidates"),
          },
        },
      );
      settleSetupRequest(requestID);
      if (!canAcceptSharedFlow(refreshedFlowID)) return;
      await acceptSharedStatus(next, refreshedFlowID);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      if (!canAcceptSharedFlow(refreshedFlowID)) return;
      try {
        const canonical = await apiFetch<DeviceFlow>(
          `/connections/github/device-flows/${refreshedFlowID}`,
        );
        if (!canAcceptSharedFlow(refreshedFlowID)) return;
        await acceptSharedStatus(canonical, refreshedFlowID);
        if (
          ["selecting_installation", "needs_installation"].includes(
            canonical.stage,
          ) &&
          canAcceptSharedFlow(refreshedFlowID)
        ) {
          setSharedError(requestError(cause));
        }
      } catch {
        if (!canAcceptSharedFlow(refreshedFlowID)) return;
        if (!isAmbiguousSetupFailure(cause) && cause instanceof ApiError) {
          adoptSharedFlow(undefined);
        }
        setSharedError(requestError(cause));
      }
    } finally {
      if (mounted.current) setOperation("");
    }
  };

  // The editor asks the connection's own credential what it can see, so adding
  // an account never needs a fresh authorization.
  const openInstallationEditor = async (connection: Connection) => {
    if (installationsEditingID === connection.connection_id) {
      setInstallationsEditingID("");
      return;
    }
    setInstallationsEditingID(connection.connection_id);
    setInstallationCandidates([]);
    setInstallationSelection(
      new Set(
        connection.installations.map(
          (item: ConnectionInstallation) => item.installation_id,
        ),
      ),
    );
    setOperation(`${connection.connection_id}:installations-load`);
    setError(undefined);
    try {
      const page = await apiFetch<{ items: DeviceFlow["candidates"] }>(
        `/connections/${connection.connection_id}/installation-candidates`,
      );
      setInstallationCandidates(page.items);
    } catch (cause) {
      setError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  const saveInstallations = async (connection: Connection) => {
    setOperation(`${connection.connection_id}:installations-save`);
    setError(undefined);
    try {
      await apiFetch(`/connections/${connection.connection_id}/installations`, {
        method: "PUT",
        headers: { "Idempotency-Key": idem("connection-installations") },
        body: JSON.stringify({
          installation_ids: [...installationSelection].sort(),
        }),
      });
      await reloadConnections();
      setInstallationsEditingID("");
    } catch (cause) {
      setError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  const toggleInstallation =
    (setter: typeof setInstallationSelection) => (installationID: string) => {
      setter((existing) => {
        const next = new Set(existing);
        if (next.has(installationID)) next.delete(installationID);
        else next.add(installationID);
        return next;
      });
    };

  const finalizeShared = async () => {
    if (!sharedFlow || operation) return;
    const chosen = sharedFlow.candidates.filter((item) =>
      selectedInstallations.has(item.installation_id),
    );
    if (
      sharedReconnectID &&
      (!reconnectConnection || missingReconnectInstallations.length !== 0)
    )
      return;
    if (!sharedReconnectID && chosen.length === 0) return;
    const installationIDs = sharedReconnectID
      ? reconnectConnection!.installations
          .map((item) => item.installation_id)
          .sort()
      : chosen.map((item) => item.installation_id).sort();
    // The key follows the whole selection: changing what is ticked after an
    // ambiguous failure must mint a new key rather than replay the old body.
    const requestID = `device-finalize:${sharedFlow.flow_id}:${installationIDs.join("+")}`;
    setOperation("shared-finalize");
    setSharedError(undefined);
    try {
      await apiFetch<Connection>(
        `/connections/github/device-flows/${sharedFlow.flow_id}/finalize`,
        {
          method: "POST",
          headers: {
            "Idempotency-Key": setupRequestKey(requestID, "device-finalize"),
          },
          body: JSON.stringify({
            installation_ids: installationIDs,
            display_name: sharedReconnectID
              ? reconnectConnection!.display_name
              : chosen.length === 1
                ? chosen[0].display_name
                : `${chosen[0].display_name} +${chosen.length - 1}`,
          }),
        },
      );
      settleSetupRequest(requestID);
      await reloadConnections();
      setSharedOpen(false);
      adoptSharedFlow(undefined);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      if (!canAcceptSharedFlow(sharedFlow.flow_id)) return;
      try {
        const canonical = await apiFetch<DeviceFlow>(
          `/connections/github/device-flows/${sharedFlow.flow_id}`,
        );
        if (!canAcceptSharedFlow(sharedFlow.flow_id)) return;
        await acceptSharedStatus(canonical, sharedFlow.flow_id);
        if (
          canonical.stage === "selecting_installation" &&
          canAcceptSharedFlow(sharedFlow.flow_id)
        ) {
          setSharedError(requestError(cause));
        }
      } catch {
        if (canAcceptSharedFlow(sharedFlow.flow_id)) {
          setSharedError(requestError(cause));
        }
      }
    } finally {
      if (mounted.current) setOperation("");
    }
  };

  const discardShared = async () => {
    if (!sharedFlow || operation === "shared-finalize") return;
    const requestID = `device-discard:${sharedFlow.flow_id}`;
    setOperation("shared-discard");
    setSharedError(undefined);
    try {
      await apiFetch(`/connections/github/device-flows/${sharedFlow.flow_id}`, {
        method: "DELETE",
        headers: {
          "Idempotency-Key": setupRequestKey(requestID, "device-discard"),
        },
      });
      settleSetupRequest(requestID);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      if (!(cause instanceof ApiError && cause.status === 404)) {
        setSharedError(requestError(cause));
        setOperation("");
        return;
      }
    }
    adoptSharedFlow(undefined);
    setSharedOpen(false);
    setOperation("");
  };

  // One source of truth for "a private-App setup is live and unfinished" so the
  // inline and modal surfaces cannot disagree about what is startable.
  const manifestFlowLive =
    !!flow && flow.stage !== "completed" && flow.stage !== "failed";
  const sharedFlowLive =
    !!sharedFlow &&
    !["completed", "access_denied", "expired", "failed"].includes(
      sharedFlow.stage,
    );
  const reconnectConnection = sharedReconnectID
    ? items.find((connection) => connection.connection_id === sharedReconnectID)
    : undefined;
  const reconnectCandidateIDs = new Set(
    sharedFlow?.candidates
      .filter((candidate) =>
        reconnectConnection?.installations.some(
          (installation) =>
            installation.installation_id === candidate.installation_id &&
            installation.account_id === candidate.account_id,
        ),
      )
      .map((candidate) => candidate.installation_id) ?? [],
  );
  const reconnectCandidates =
    sharedFlow?.candidates.filter((candidate) =>
      reconnectCandidateIDs.has(candidate.installation_id),
    ) ?? [];
  const missingReconnectInstallations =
    reconnectConnection?.installations.filter(
      (installation) =>
        !reconnectCandidateIDs.has(installation.installation_id),
    ) ?? [];
  const startManifestDisabled =
    !!operation ||
    !!manifestError ||
    manifestFlowLive ||
    flow?.stage === "failed" ||
    (ownerType === "organization" && !organization.trim());

  const reportSetupWindowBlocked = () => {
    setManifestError({ message: setupWindowBlockedMessage });
    setManifestFailureStage(flow?.stage ?? "start");
    setManifestRecovery("retry");
  };

  const startManifest = async () => {
    const requestID = `manifest-start:${ownerType}:${organization.trim()}`;
    // The tab must be opened by the click itself; a tab opened after the await
    // below is blocked as an unsolicited pop-up.
    if (!openSetupWindow()) {
      reportSetupWindowBlocked();
      return;
    }
    setOperation("flow-start");
    setManifestError(undefined);
    setManifestFailureStage(undefined);
    setManifestRecovery(undefined);
    try {
      const response = await apiFetch<StartManifestFlowPayload | FlowStatus>(
        "/connections/github/manifest-flows",
        {
          method: "POST",
          headers: {
            "Idempotency-Key": setupRequestKey(requestID, "manifest-start"),
          },
          body: JSON.stringify({
            owner_type: ownerType,
            ...(ownerType === "organization"
              ? { organization: organization.trim() }
              : {}),
          }),
        },
      );
      settleSetupRequest(requestID);
      if ("form_action" in response) {
        postManifestToSetupWindow(response.form_action, response.manifest);
        // Adopting the started flow is what puts this tab in charge: it shows the
        // stage and starts watching for GitHub's return instead of waiting for a
        // reload to discover progress the operator already made.
        setFlow({
          flow_id: response.flow_id,
          stage: "awaiting_app_creation",
          expires_at: response.expires_at,
        });
      } else {
        setFlow(response);
      }
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      setManifestError(requestError(cause));
      setManifestFailureStage("start");
      setManifestRecovery(isAmbiguousSetupFailure(cause) ? "retry" : "restart");
    } finally {
      setOperation("");
    }
  };

  const abandonManifest = async () => {
    if (!flow) {
      setManifestOpen(false);
      return;
    }
    const requestID = `manifest-discard:${flow.flow_id}`;
    setOperation("flow-discard");
    try {
      await apiFetch(`/connections/github/manifest-flows/${flow.flow_id}`, {
        method: "DELETE",
        headers: {
          "Idempotency-Key": setupRequestKey(requestID, "manifest-discard"),
        },
      });
      settleSetupRequest(requestID);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      // A flow the server no longer knows about is already abandoned; anything
      // else is surfaced so the operator is not left with invisible state.
      if (!(cause instanceof ApiError && cause.status === 404)) {
        setManifestError(requestError(cause));
        setManifestFailureStage(flow.stage);
        setManifestRecovery("restart");
        setOperation("");
        return;
      }
    }
    advancing.current = "";
    setFlow(undefined);
    setManifestError(undefined);
    setManifestFailureStage(undefined);
    setManifestRecovery(undefined);
    setManifestOpen(false);
    setOperation("");
  };

  const recoverManifest = () => {
    if (
      manifestRecovery === "retry" &&
      flow &&
      manifestFailureStage !== "start"
    ) {
      advancing.current = "";
      setManifestRetryAttempt((attempt) => attempt + 1);
      return;
    }
    advancing.current = "";
    setFlow(undefined);
    void startManifest();
  };

  const createManual = async (event: FormEvent) => {
    event.preventDefault();
    setOperation("manual-create");
    setError(undefined);
    try {
      const created = await createConnection.mutateAsync({ ...manualForm });
      setManualSaved(created);
      setManualForm(emptyForm);
      await reloadConnections();
    } catch (cause) {
      setError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  const saveEdit = async (event: FormEvent) => {
    event.preventDefault();
    const id = editingID;
    setOperation(`${id}:edit`);
    setError(undefined);
    try {
      await editConnection.mutateAsync({
        connectionId: id,
        displayName: editName,
      });
      setEditingID("");
      await reloadConnections();
    } catch (cause) {
      setError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  const mutate = async (
    connection: Connection,
    action: "check" | "toggle" | "remove",
  ) => {
    setOperation(`${connection.connection_id}:${action}`);
    if (action === "remove") setRemovalError(undefined);
    else setError(undefined);
    try {
      if (action === "check") {
        await checkConnection.mutateAsync(connection.connection_id);
      } else if (action === "toggle") {
        await toggleConnection.mutateAsync({
          connectionId: connection.connection_id,
          enabled: connection.lifecycle !== "enabled",
        });
      } else {
        await removeConnection.mutateAsync(connection.connection_id);
      }
      if (action === "remove") setPendingRemoval(undefined);
    } catch (cause) {
      if (action === "remove") setRemovalError(requestError(cause));
      else setError(requestError(cause));
    } finally {
      await reloadConnections();
      setOperation("");
    }
  };

  const selectedConnection = items.find(
    (item) => item.connection_id === selectedConnectionID,
  );
  return (
    <WorkspacePage
      section="Connections"
      title={selectedConnection?.display_name ?? "Connections"}
      description={
        selectedConnection
          ? `GitHub · ${selectedConnection.credential_strategy === "user_access_token" ? "Kingdom App" : "Private App"}`
          : "Bring work from your tools into Kingdom."
      }
      actions={
        selectedConnectionID ? (
          <Button variant="ghost" onClick={() => setSelectedConnectionID("")}>
            All Connections
          </Button>
        ) : (
          <Button
            disabled={!!operation || sharedFlowLive || manifestFlowLive}
            onClick={() => setAddOpen(true)}
          >
            Add Connection
          </Button>
        )
      }
    >
      <div className="space-y-6">
        <ErrorNotice
          error={
            error ??
            (connections.error ? requestError(connections.error) : undefined)
          }
        />
        {sharedFlowLive && !sharedOpen ? (
          <section
            className="mt-6 max-w-2xl rounded-lg border border-accent/30 bg-accent/5 p-4"
            aria-labelledby="shared-setup-status"
          >
            <p id="shared-setup-status" className="text-sm font-medium">
              {sharedReconnectID
                ? "GitHub Reconnect in Progress"
                : "GitHub Setup in Progress"}
            </p>
            <p
              className="mt-1 text-xs text-foreground-muted"
              aria-live="polite"
            >
              This setup is preserved for this browser session until it is
              completed, discarded, or expires.
            </p>
            <div className="mt-3 flex flex-wrap gap-2">
              <Button
                ref={sharedResumeRef}
                type="button"
                size="sm"
                onClick={() => setSharedOpen(true)}
              >
                Resume Setup
              </Button>
              <Button
                type="button"
                size="sm"
                variant="outline"
                disabled={!!operation}
                onClick={() => void discardShared()}
              >
                {operation === "shared-discard" ? (
                  <Loader2 className="h-3.5 w-3.5 animate-spin" />
                ) : null}
                Discard Setup
              </Button>
            </div>
            <ErrorNotice error={sharedError} />
          </section>
        ) : null}
        {!manifestOpen && (flow || manifestError) ? (
          <section
            className="mt-6 max-w-2xl space-y-3"
            aria-label="Private GitHub App Setup Status"
          >
            <ManifestSetupStatus
              flow={flow}
              error={manifestError}
              failureStage={manifestFailureStage}
              recovery={manifestRecovery}
              busy={operation.startsWith("flow-")}
              onRecover={recoverManifest}
            />
            <ManifestHandoffAction
              flow={flow}
              url={manifestHandoffURL}
              busy={!!operation}
              onBlocked={reportSetupWindowBlocked}
            />
            {setupOptions.shared_enabled && manifestFlowLive ? (
              <div className="flex flex-wrap gap-2">
                <Button
                  type="button"
                  size="sm"
                  onClick={() => setManifestOpen(true)}
                >
                  Resume Setup
                </Button>
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={!!operation}
                  onClick={() => void abandonManifest()}
                >
                  {operation === "flow-discard" ? (
                    <Loader2 className="h-3.5 w-3.5 animate-spin" />
                  ) : null}
                  Abandon Setup
                </Button>
              </div>
            ) : null}
          </section>
        ) : null}
        {loading ? (
          <div
            aria-label="Loading connections"
            className="grid gap-3 sm:grid-cols-2"
          >
            <div className="h-40 animate-pulse rounded-xl border border-border bg-card/60" />
            <div className="h-40 animate-pulse rounded-xl border border-border bg-card/60" />
          </div>
        ) : selectedConnectionID &&
          !selectedConnection &&
          !connections.error ? (
          <div role="alert" className="rounded-xl border border-border p-6">
            <h2 className="text-lg font-medium">Connection Unavailable</h2>
            <p className="mt-2 text-sm text-foreground-muted">
              This connection is no longer in the workspace. Return to All
              Connections to choose another.
            </p>
          </div>
        ) : items.length === 0 && !connections.error ? (
          <div className="rounded-xl border border-dashed border-border px-6 py-12 text-center">
            <h2 className="font-display text-2xl">Connect Your First Tool</h2>
            <p className="mb-6 mt-2 text-sm text-foreground-muted">
              Bring GitHub issues into planning and new work.
            </p>
            <Button
              disabled={!!operation || sharedFlowLive || manifestFlowLive}
              onClick={() => setAddOpen(true)}
            >
              Connect GitHub
            </Button>
          </div>
        ) : !selectedConnection ? (
          <ConnectionList items={items} onManage={setSelectedConnectionID} />
        ) : (
          <div className="space-y-6">
            {[selectedConnection].map((connection) => {
              const mutable =
                connection.lifecycle === "enabled" ||
                connection.lifecycle === "disabled";
              // A connection whose removal did not finish is still on its way
              // out, so it keeps only the action that finishes the job.
              // Tombstones never reach the list at all.
              const removable =
                mutable || connection.lifecycle === "removal_pending";
              return (
                <Card key={connection.connection_id}>
                  <CardContent className="p-5">
                    {editingID === connection.connection_id ? (
                      <form onSubmit={saveEdit} className="space-y-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <h3 className="font-semibold">Edit Connection</h3>
                            <p className="mt-1 text-xs text-foreground-muted">
                              App, client, and installation identity are
                              immutable.
                            </p>
                          </div>
                          <Button
                            type="button"
                            size="sm"
                            variant="ghost"
                            onClick={() => setEditingID("")}
                            disabled={editConnection.hasPendingRecovery}
                          >
                            <X className="h-4 w-4" />
                            Cancel
                          </Button>
                        </div>
                        <label className="block text-xs font-medium text-foreground-muted">
                          Display Name
                          <input
                            required
                            maxLength={120}
                            className={cn(fieldClass, "mt-1")}
                            value={editName}
                            disabled={editConnection.hasPendingRecovery}
                            onChange={(event) =>
                              setEditName(event.target.value)
                            }
                          />
                        </label>
                        <Button
                          type="submit"
                          size="sm"
                          disabled={
                            operation === `${connection.connection_id}:edit`
                          }
                        >
                          {editConnection.hasPendingRecovery
                            ? "Retry save display name"
                            : "Save display name"}
                        </Button>
                      </form>
                    ) : (
                      <>
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="flex flex-wrap items-center gap-2">
                              <ConnectionStatus connection={connection} />
                              {connection.last_checked_at && (
                                <span className="text-xs text-foreground-muted">
                                  Checked{" "}
                                  <RelativeTime
                                    iso={connection.last_checked_at}
                                  />
                                </span>
                              )}
                            </div>

                            <div className="mt-3">
                              <div className="flex items-center gap-2">
                                <h4 className="text-xs font-semibold uppercase tracking-wide text-foreground-muted">
                                  Accounts and Access
                                </h4>
                                {mutable && (
                                  <button
                                    type="button"
                                    className="text-xs text-accent underline-offset-2 hover:underline"
                                    onClick={() =>
                                      void openInstallationEditor(connection)
                                    }
                                  >
                                    {installationsEditingID ===
                                    connection.connection_id
                                      ? "Close"
                                      : "Change"}
                                  </button>
                                )}
                              </div>
                              <InstallationList
                                connection={connection}
                                managementLinksDisabled={
                                  operation === "shared-finalize"
                                }
                              />
                              {installationsEditingID ===
                                connection.connection_id && (
                                <div className="mt-3 rounded-xl border border-border bg-background/40 p-4">
                                  <p className="text-xs text-foreground-muted">
                                    Kingdom reads every repository these
                                    installations grant. Install the App on
                                    another account on GitHub, then reopen this
                                    list.
                                  </p>
                                  <div className="mt-3">
                                    {operation ===
                                    `${connection.connection_id}:installations-load` ? (
                                      <p
                                        role="status"
                                        className="flex items-center gap-2 text-xs text-foreground-muted"
                                      >
                                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                        Loading installations...
                                      </p>
                                    ) : installationCandidates.length === 0 ? (
                                      <p className="rounded-lg border border-dashed border-border p-6 text-center text-sm text-foreground-muted">
                                        No installations are available to this
                                        connection.
                                      </p>
                                    ) : (
                                      <InstallationPicker
                                        name={`installations-${connection.connection_id}`}
                                        candidates={installationCandidates}
                                        selected={installationSelection}
                                        onToggle={toggleInstallation(
                                          setInstallationSelection,
                                        )}
                                      />
                                    )}
                                  </div>
                                  <div className="mt-3 flex justify-end gap-2">
                                    <Button
                                      size="sm"
                                      variant="ghost"
                                      onClick={() =>
                                        setInstallationsEditingID("")
                                      }
                                    >
                                      Cancel
                                    </Button>
                                    <Button
                                      size="sm"
                                      disabled={
                                        installationSelection.size === 0 ||
                                        operation ===
                                          `${connection.connection_id}:installations-save`
                                      }
                                      onClick={() =>
                                        void saveInstallations(connection)
                                      }
                                    >
                                      {operation ===
                                        `${connection.connection_id}:installations-save` && (
                                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                                      )}
                                      Save Installations
                                    </Button>
                                  </div>
                                </div>
                              )}
                            </div>
                            {connection.health_class ===
                              "permission_mismatch" && (
                              <p className="mt-2 max-w-2xl text-xs leading-5 text-status-blocked">
                                {externalSourceFailureGuidance(
                                  connection.health_class,
                                )}
                              </p>
                            )}
                          </div>
                          {removable && (
                            <div className="flex flex-wrap items-start gap-3">
                              {mutable && (
                                <Button
                                  size="sm"
                                  variant="outline"
                                  disabled={
                                    !!operation ||
                                    (ordinaryRecoveryPending &&
                                      checkConnection.pendingRequest !==
                                        connection.connection_id) ||
                                    connection.lifecycle !== "enabled" ||
                                    connection.credential_state !== "present"
                                  }
                                  onClick={() =>
                                    void mutate(connection, "check")
                                  }
                                >
                                  <ShieldCheck className="h-3.5 w-3.5" />
                                  {checkConnection.pendingRequest ===
                                  connection.connection_id
                                    ? "Retry check"
                                    : "Check"}
                                </Button>
                              )}
                              <details className="relative">
                                <summary className="cursor-pointer rounded-lg border border-border px-3 py-2 text-sm">
                                  Manage Connection
                                </summary>
                                <div className="mt-3 flex max-w-sm flex-wrap gap-2">
                                  {mutable && (
                                    <>
                                      <Button
                                        size="sm"
                                        variant="outline"
                                        disabled={
                                          !!operation || ordinaryRecoveryPending
                                        }
                                        onClick={() => {
                                          setEditingID(
                                            connection.connection_id,
                                          );
                                          setEditName(connection.display_name);
                                        }}
                                      >
                                        <Pencil className="h-3.5 w-3.5" />
                                        Edit
                                      </Button>
                                      {connection.credential_strategy ===
                                        "user_access_token" && (
                                        <Button
                                          size="sm"
                                          variant="outline"
                                          disabled={
                                            !!operation ||
                                            ordinaryRecoveryPending ||
                                            sharedFlowLive
                                          }
                                          onClick={() =>
                                            void startShared(
                                              connection.connection_id,
                                            )
                                          }
                                        >
                                          Reconnect Connection
                                        </Button>
                                      )}

                                      <Button
                                        size="sm"
                                        variant="outline"
                                        disabled={
                                          !!operation ||
                                          (ordinaryRecoveryPending &&
                                            toggleConnection.pendingRequest
                                              ?.connectionId !==
                                              connection.connection_id)
                                        }
                                        onClick={() =>
                                          void mutate(connection, "toggle")
                                        }
                                      >
                                        {toggleConnection.pendingRequest
                                          ?.connectionId ===
                                        connection.connection_id
                                          ? `Retry ${toggleConnection.pendingRequest.enabled ? "enable" : "disable"}`
                                          : connection.lifecycle === "enabled"
                                            ? "Disable"
                                            : "Enable"}
                                      </Button>
                                    </>
                                  )}
                                  <Button
                                    size="sm"
                                    variant="outline"
                                    disabled={
                                      !!operation || ordinaryRecoveryPending
                                    }
                                    onClick={() => {
                                      setPendingRemoval(connection);
                                      setRemovalError(undefined);
                                    }}
                                  >
                                    {connection.lifecycle === "removal_pending"
                                      ? "Retry Removal"
                                      : "Remove"}
                                  </Button>
                                </div>
                              </details>
                            </div>
                          )}
                        </div>
                        <section className="mt-6 border-t border-border pt-5">
                          <h3 className="text-sm font-medium">Permissions</h3>
                          <p className="mt-2 text-sm text-foreground-muted">
                            Repository metadata and issues · Read only
                          </p>
                          <p className="mt-1 text-xs text-foreground-muted">
                            Choose issues from a planning session or when
                            creating work. You decide when source content is
                            refreshed.
                          </p>
                        </section>
                        <details className="group mt-4 rounded-lg border border-border/70 bg-background/40">
                          <summary className="flex cursor-pointer list-none items-center justify-between px-3 py-2 text-xs font-medium text-foreground-muted">
                            Technical Details
                            <ChevronDown className="h-4 w-4 transition group-open:rotate-180" />
                          </summary>
                          <div className="border-t border-border/70 px-3 py-3 text-xs text-foreground-muted">
                            <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
                              <dt>Strategy</dt>
                              <dd>{connection.credential_strategy}</dd>
                              <dt>App Slug</dt>
                              <dd>{connection.app_slug}</dd>
                              <dt>App ID</dt>
                              <dd className="break-all">{connection.app_id}</dd>
                              <dt>Client ID</dt>
                              <dd className="break-all">
                                {connection.client_id}
                              </dd>
                              <dt>Installation IDs</dt>
                              <dd className="break-all">
                                {connection.installations
                                  .map(
                                    (installation: ConnectionInstallation) =>
                                      installation.installation_id,
                                  )
                                  .join(", ") || "None"}
                              </dd>
                              <dt>Connection ID</dt>
                              <dd className="break-all">
                                {connection.connection_id}
                              </dd>
                            </dl>
                            {mutable &&
                              connection.credential_strategy ===
                                "app_private_key" && (
                                <div className="mt-3 rounded-md border border-accent/20 bg-accent/5 p-2">
                                  <div className="flex items-center gap-2 font-medium text-foreground">
                                    <KeyRound className="h-3.5 w-3.5 text-accent" />
                                    {connection.credential_state === "present"
                                      ? "Rotate the private key from the host"
                                      : "Provision the private key from the host"}
                                  </div>
                                  <code className="mt-2 block overflow-x-auto">
                                    kd connections github key{" "}
                                    {connection.credential_state === "present"
                                      ? "rotate"
                                      : "provision"}{" "}
                                    --connection-id {connection.connection_id}{" "}
                                    --from ./private-key.pem
                                  </code>
                                  <p className="mt-2">
                                    Run Check after{" "}
                                    {connection.credential_state === "present"
                                      ? "rotation"
                                      : "provisioning"}
                                    .
                                  </p>
                                </div>
                              )}
                          </div>
                        </details>
                        {connection.lifecycle === "removal_pending" && (
                          <p className="mt-4 text-xs text-foreground-muted">
                            Removal did not finish, so this connection is still
                            listed. Retry removal to retire its credential.
                          </p>
                        )}
                      </>
                    )}
                  </CardContent>
                </Card>
              );
            })}
          </div>
        )}

        {!selectedConnection && items.length > 0 && (
          <p className="text-sm text-foreground-muted">
            Choose issues from a planning session or when creating work. You
            control when source content is refreshed.
          </p>
        )}
        {addOpen && (
          <Modal
            open
            onClose={() => setAddOpen(false)}
            title="Add Connection"
            description="Choose how to connect GitHub."
            size="md"
          >
            <div className="space-y-5">
              <div className="flex items-center gap-3">
                <Github className="h-6 w-6 text-foreground-muted" />
                <h3 className="text-lg font-medium">GitHub</h3>
              </div>
              <p className="text-sm text-foreground-muted">
                Read issues and repository metadata. Kingdom does not write to
                GitHub or subscribe to webhooks.
              </p>
              <div
                className="space-y-3"
                role="group"
                aria-label="GitHub Setup Options"
              >
                {setupOptions.shared_enabled && (
                  <Button
                    className="w-full"
                    disabled={!!operation || sharedFlowLive}
                    onClick={() => {
                      setAddOpen(false);
                      void startShared();
                    }}
                  >
                    Connect GitHub
                  </Button>
                )}
                <Button
                  className="w-full"
                  variant={setupOptions.shared_enabled ? "outline" : "default"}
                  onClick={() => {
                    setAddOpen(false);
                    setManifestOpen(true);
                  }}
                >
                  Create a Private GitHub App
                </Button>
                <button
                  type="button"
                  className="w-full text-sm text-foreground-muted underline underline-offset-4"
                  onClick={() => {
                    setAddOpen(false);
                    setManualOpen(true);
                    setManualSaved(undefined);
                  }}
                >
                  Use Manual Setup
                </button>
              </div>
            </div>
          </Modal>
        )}

        {sharedOpen ? (
          <Modal
            open
            onClose={() => setSharedOpen(false)}
            getFinalFocus={
              sharedFlowLive ? () => sharedResumeRef.current : undefined
            }
            size="lg"
            title={sharedReconnectID ? "Reconnect GitHub" : "Connect GitHub"}
            description={
              sharedReconnectID
                ? "Reauthorize every installation already saved on this connection."
                : "Authorize on GitHub, then choose the installations Kingdom may use."
            }
            footer={
              <ModalFooter>
                {sharedFlowLive ? (
                  <Button
                    variant="outline"
                    disabled={!!operation}
                    onClick={() => void discardShared()}
                  >
                    {operation === "shared-discard" ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : null}
                    Discard Setup
                  </Button>
                ) : null}
                <Button variant="ghost" onClick={() => setSharedOpen(false)}>
                  Close
                </Button>
              </ModalFooter>
            }
          >
            <div className="space-y-5">
              <ErrorNotice error={sharedError} />
              {operation === "shared-start" ? (
                <div className="flex min-h-40 items-center justify-center gap-2 text-sm text-foreground-muted">
                  <Loader2 className="h-4 w-4 animate-spin text-accent" />
                  Preparing GitHub...
                </div>
              ) : !sharedFlow ? (
                <p className="py-10 text-center text-sm text-foreground-muted">
                  No active GitHub setup is available.
                </p>
              ) : sharedFlow.stage === "awaiting_authorization" ? (
                <div className="space-y-5">
                  <div className="rounded-xl border border-accent/30 bg-accent/5 p-5 text-center">
                    <p className="text-xs font-semibold uppercase tracking-[0.16em] text-foreground-muted">
                      Enter This Code on GitHub
                    </p>
                    <div className="mt-3 flex items-center justify-center gap-2">
                      <code className="font-display text-3xl font-semibold tracking-[0.18em] text-accent">
                        {sharedFlow.user_code}
                      </code>
                      <CopyButton
                        value={sharedFlow.user_code || ""}
                        label="Copy GitHub code"
                      />
                    </div>
                  </div>
                  <a
                    className={buttonVariants({ className: "w-full" })}
                    href={sharedFlow.verification_uri}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Open GitHub
                    <ExternalLink className="h-4 w-4" />
                  </a>
                  <p className="text-center text-xs text-foreground-muted">
                    Authorizing lets Kingdom see the installations you already
                    have. You can install the App on more accounts in the next
                    step.
                  </p>
                  <p
                    className="flex items-center justify-center gap-2 text-xs text-foreground-muted"
                    aria-live="polite"
                  >
                    <Loader2 className="h-3.5 w-3.5 animate-spin text-accent" />
                    Waiting safely for GitHub authorization
                  </p>
                </div>
              ) : sharedFlow.stage === "needs_installation" ? (
                <div className="space-y-4 text-center">
                  <div className="rounded-xl border border-border bg-background/50 p-6">
                    <h3 className="font-display text-lg font-semibold">
                      Choose Repositories on GitHub
                    </h3>
                    <p className="mt-2 text-sm text-foreground-muted">
                      {sharedReconnectID
                        ? "Install or restore the App on every account this connection already covers, then refresh the list. Use Change on the saved connection if the covered accounts should change."
                        : "Install the App for an account and select the repositories Kingdom should be able to discover."}
                    </p>
                  </div>
                  <a
                    className={buttonVariants({ className: "w-full" })}
                    href={sharedFlow.install_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    Choose Repositories
                    <ExternalLink className="h-4 w-4" />
                  </a>
                  <Button
                    variant="outline"
                    className="w-full"
                    disabled={!!operation}
                    onClick={() => void refreshCandidates()}
                  >
                    {operation === "shared-refresh" && (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    )}
                    I Installed the App
                  </Button>
                </div>
              ) : sharedFlow.stage === "selecting_installation" ? (
                <div className="space-y-4">
                  <div>
                    <h3 className="font-display text-lg font-semibold">
                      {sharedReconnectID
                        ? "Saved Installations"
                        : "Choose Installations"}
                    </h3>
                    <p className="mt-1 text-sm text-foreground-muted">
                      {sharedReconnectID
                        ? "Reconnect reauthorizes the complete saved set. To add or remove accounts, close this setup and use Change on the connection."
                        : "One connection is created covering every account you tick, and it reads every repository those installations grant."}
                    </p>
                  </div>
                  <InstallationPicker
                    name="installation"
                    candidates={
                      sharedReconnectID
                        ? reconnectCandidates
                        : sharedFlow.candidates
                    }
                    selected={selectedInstallations}
                    disabled={
                      !!sharedReconnectID || operation === "shared-finalize"
                    }
                    managementLinksDisabled={operation === "shared-finalize"}
                    onToggle={(installationID: string) =>
                      setSelectedInstallations((existing) => {
                        const next = new Set(existing);
                        if (next.has(installationID))
                          next.delete(installationID);
                        else next.add(installationID);
                        return next;
                      })
                    }
                  />
                  <p
                    className="text-xs text-foreground-muted"
                    aria-live="polite"
                  >
                    {sharedReconnectID
                      ? `${selectedInstallations.size} saved`
                      : `${selectedInstallations.size} selected`}
                  </p>
                  {sharedReconnectID &&
                  missingReconnectInstallations.length > 0 ? (
                    <div
                      role="alert"
                      className="rounded-xl border border-status-blocked/40 bg-status-blocked/10 p-4 text-sm text-status-blocked"
                    >
                      <p className="font-medium">
                        Some Saved Installations Are Missing
                      </p>
                      <p className="mt-1 text-xs leading-5">
                        Restore App installation and user access for{" "}
                        {missingReconnectInstallations
                          .map(
                            (installation) =>
                              `@${installation.account_login ?? installation.installation_id}`,
                          )
                          .join(", ")}
                        , then refresh. Use Change only if these accounts should
                        no longer belong to the connection.
                      </p>
                    </div>
                  ) : null}
                  <Button
                    className="w-full"
                    disabled={
                      selectedInstallations.size === 0 ||
                      missingReconnectInstallations.length > 0 ||
                      !!operation
                    }
                    onClick={() => void finalizeShared()}
                  >
                    {operation === "shared-finalize" && (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    )}
                    {sharedReconnectID
                      ? "Reconnect Connection"
                      : "Connect Selected Installations"}
                  </Button>
                  <div className="rounded-xl border border-border bg-background/40 p-4">
                    <p className="text-sm text-foreground-muted">
                      {sharedReconnectID
                        ? "Missing a saved account? Restore the App installation and your access on GitHub, then refresh. The saved set cannot be edited during reconnect."
                        : "Missing an account, or need different repositories? Install the App on another account or change which repositories it can see, then refresh this list."}
                    </p>
                    <div className="mt-3 flex flex-col gap-2 sm:flex-row">
                      {operation === "shared-finalize" ? (
                        <span
                          aria-disabled="true"
                          className={buttonVariants({
                            variant: "outline",
                            className: "flex-1 opacity-50",
                          })}
                        >
                          Install on Another Account
                          <ExternalLink className="h-4 w-4" />
                        </span>
                      ) : (
                        <a
                          className={buttonVariants({
                            variant: "outline",
                            className: "flex-1",
                          })}
                          href={sharedFlow.install_url}
                          target="_blank"
                          rel="noreferrer"
                        >
                          Install on Another Account
                          <ExternalLink className="h-4 w-4" />
                        </a>
                      )}
                      <Button
                        variant="outline"
                        className="flex-1"
                        disabled={!!operation}
                        onClick={() => void refreshCandidates()}
                      >
                        {operation === "shared-refresh" && (
                          <Loader2 className="h-4 w-4 animate-spin" />
                        )}
                        Refresh Installations
                      </Button>
                    </div>
                  </div>
                </div>
              ) : (
                <div className="rounded-xl border border-border p-6 text-center">
                  <p className="font-medium">
                    {sharedFlow.failure ||
                      (sharedFlow.stage === "access_denied"
                        ? "GitHub authorization was declined."
                        : sharedFlow.stage === "expired"
                          ? "The GitHub code expired."
                          : "Finishing your GitHub connection…")}
                  </p>
                  {["failed", "access_denied", "expired"].includes(
                    sharedFlow.stage,
                  ) && (
                    <Button
                      className="mt-4"
                      onClick={() =>
                        void startShared(sharedReconnectID || undefined)
                      }
                    >
                      Start Again
                    </Button>
                  )}
                </div>
              )}
            </div>
          </Modal>
        ) : null}

        {manifestOpen ? (
          <Modal
            open
            onClose={() => setManifestOpen(false)}
            size="sm"
            title="Create a Private GitHub App"
            description="Create and own a dedicated App for this Kingdom workspace."
            footer={
              <ModalFooter>
                {manifestFlowLive ? (
                  <Button
                    variant="outline"
                    disabled={!!operation}
                    onClick={() => void abandonManifest()}
                  >
                    {operation === "flow-discard" ? (
                      <Loader2 className="h-3.5 w-3.5 animate-spin" />
                    ) : null}
                    Abandon Setup
                  </Button>
                ) : null}
                <Button variant="ghost" onClick={() => setManifestOpen(false)}>
                  Close
                </Button>
              </ModalFooter>
            }
          >
            <div className="space-y-4">
              <ManifestSetupStatus
                flow={flow}
                error={manifestError}
                failureStage={manifestFailureStage}
                recovery={manifestRecovery}
                busy={operation.startsWith("flow-")}
                onRecover={recoverManifest}
              />
              <ManifestHandoffAction
                flow={flow}
                url={manifestHandoffURL}
                busy={!!operation}
                onBlocked={reportSetupWindowBlocked}
              />
              <div className="grid grid-cols-2 gap-2 rounded-lg border border-border bg-background/70 p-1">
                {(["personal", "organization"] as const).map((owner) => (
                  <button
                    key={owner}
                    type="button"
                    className={`rounded-md px-3 py-2 text-xs font-medium capitalize ${ownerType === owner ? "bg-accent text-primary-foreground" : "text-foreground-muted"}`}
                    onClick={() => setOwnerType(owner)}
                  >
                    {owner}
                  </button>
                ))}
              </div>
              {ownerType === "organization" && (
                <label className="block text-xs font-medium text-foreground-muted">
                  Organization Login
                  <input
                    className={cn(fieldClass, "mt-1")}
                    value={organization}
                    onChange={(event) => setOrganization(event.target.value)}
                  />
                </label>
              )}
              <Button
                className="w-full"
                disabled={startManifestDisabled}
                onClick={() => void startManifest()}
              >
                Create Private App on GitHub
              </Button>
            </div>
          </Modal>
        ) : null}

        {manualOpen ? (
          <Modal
            open
            onClose={() => {
              if (!createConnection.hasPendingRecovery) setManualOpen(false);
            }}
            size="lg"
            title="Manual GitHub App Setup"
            description="Save existing App metadata, then provision its private key from the host."
            footer={
              <ModalFooter>
                <Button
                  type="button"
                  variant="ghost"
                  onClick={() => setManualOpen(false)}
                  disabled={createConnection.hasPendingRecovery}
                >
                  Close
                </Button>
              </ModalFooter>
            }
          >
            <ErrorNotice error={error} />
            {manualSaved ? (
              <div className="space-y-4">
                <div className="flex items-start gap-3 rounded-lg border border-success/30 bg-success/10 p-3">
                  <CheckCircle2 className="mt-0.5 h-4 w-4 text-success" />
                  <div>
                    <p className="text-sm font-medium">Metadata saved</p>
                    <p className="text-xs text-foreground-muted">
                      The connection stays unavailable until its key is
                      provisioned and Check succeeds.
                    </p>
                  </div>
                </div>
                <div className="rounded-lg border border-border bg-background p-3">
                  <code className="block overflow-x-auto text-xs">
                    kd connections github key provision --connection-id{" "}
                    {manualSaved.connection_id} --from ./private-key.pem
                  </code>
                  <p className="mt-2 text-xs text-foreground-muted">
                    Then return here and run Check.
                  </p>
                </div>
              </div>
            ) : (
              <form className="space-y-4" onSubmit={createManual}>
                <MetadataFields
                  value={manualForm}
                  onChange={setManualForm}
                  disabled={createConnection.hasPendingRecovery}
                />
                <p className={MODAL_BODY_TEXT_CLASS}>
                  The App must have exactly Metadata read and Issues read, no
                  events, and no additional permissions. Private keys never
                  enter this browser form.
                </p>
                <Button type="submit" disabled={operation === "manual-create"}>
                  {operation === "manual-create" && (
                    <Loader2 className="h-4 w-4 animate-spin" />
                  )}
                  {createConnection.hasPendingRecovery
                    ? "Retry Save Connection Metadata"
                    : "Save Connection Metadata"}
                </Button>
              </form>
            )}
          </Modal>
        ) : null}

        {pendingRemoval ? (
          <ConfirmDialog
            open
            onClose={() => {
              if (!removeConnection.hasPendingRecovery)
                setPendingRemoval(undefined);
            }}
            onConfirm={() => void mutate(pendingRemoval, "remove")}
            title={pendingRemoval.display_name}
            caption="Remove Connection"
            description={`This tombstones the connection and removes its ${pendingRemoval.credential_strategy === "user_access_token" ? "local user tokens" : "local private key"}.`}
            confirmLabel={
              operation === `${pendingRemoval.connection_id}:remove`
                ? "Removing..."
                : removeConnection.hasPendingRecovery
                  ? "Retry Remove Connection"
                  : "Remove Connection"
            }
            confirmDisabled={
              operation === `${pendingRemoval.connection_id}:remove`
            }
            dismissDisabled={removeConnection.hasPendingRecovery}
          >
            <div className="space-y-3">
              <p className={MODAL_BODY_TEXT_CLASS}>
                Historical provenance remains available. This does not uninstall
                or revoke the GitHub App; use GitHub as the external kill
                switch.
              </p>
              <ErrorNotice error={removalError} />
            </div>
          </ConfirmDialog>
        ) : null}
      </div>
    </WorkspacePage>
  );
}
