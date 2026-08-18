import { ApiError, apiFetch } from "@/api/client";
import { CopyButton } from "@/components/ui/copy-button";
import { fieldClass } from "@/components/ui/field-styles";
import { cn } from "@/lib/utils";
import {
  openSetupWindow,
  postManifestToSetupWindow,
  setupWindowBlockedMessage,
} from "./github-setup-window";
import type { components } from "@/api/schema";
import { PageContent, PageHeader } from "@/components/page-layout";
import { newIdempotencyKey as idem } from "@/lib/idempotency";
import { Button } from "@/components/ui/button";
import { buttonVariants } from "@/components/ui/button-variants";
import { Card, CardContent } from "@/components/ui/card";
import { ModalDialog } from "@/components/ui/modal-dialog";
import {
  ModalShell,
  modalShellSizeClass,
  MODAL_BODY_TEXT_CLASS,
} from "@/components/ui/modal-shell";
import {
  CheckCircle2,
  ChevronDown,
  ExternalLink,
  Github,
  KeyRound,
  Link2,
  Loader2,
  Pencil,
  ShieldCheck,
  X,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import type { FormEvent } from "react";
import { externalSourceFailureGuidance } from "./external-source-status";
import { shouldRetainMutationRequest, useRecoverableMutation } from "@/lib/use-recoverable-mutation";

type Connection = components["schemas"]["Connection"];
type FlowStatus = components["schemas"]["GitHubManifestFlowStatus"];
type ConnectionForm = Pick<
  Connection,
  "display_name" | "app_id" | "client_id" | "installation_id"
>;
type SetupOptions = components["schemas"]["GitHubSetupOptions"];
type DeviceFlow = components["schemas"]["GitHubDeviceFlowStatus"];
type RequestError = { message: string; retryAt?: string };

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
  app_created: "Kingdom could not secure the App credentials returned by GitHub.",
  converting_app: "GitHub App credential creation did not finish.",
  awaiting_installation: "Kingdom could not open GitHub App installation.",
  installation_received: "Kingdom could not start temporary GitHub authorization.",
  awaiting_authorization: "GitHub authorization did not return to Kingdom.",
  authorization_received: "Kingdom could not validate and save the GitHub connection.",
  validating_connection: "GitHub connection validation did not finish.",
  completed: "The private GitHub App connection is ready.",
  failed: "This private App setup flow cannot continue.",
};

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
      {field("display_name", "Display name", 120)}
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
        {busy ? <Loader2 className="mt-0.5 h-4 w-4 shrink-0 animate-spin text-gold" /> : <Github className="mt-0.5 h-4 w-4 shrink-0 text-gold" />}
        <div className="min-w-0">
          <p>{flow?.app_name || "Private GitHub App setup"}</p>
          <p className={`mt-1 text-xs font-normal leading-relaxed ${failed ? "text-destructive" : "text-foreground-muted"}`}>
            {error ? `${manifestFailureCopy[stage]} ${error.message}` : flow?.failure || (flow ? flowCopy[flow.stage] : manifestFailureCopy[stage])}
          </p>
          {error?.retryAt ? <p className="mt-1 text-xs text-destructive">Retry after <time dateTime={error.retryAt}>{new Date(error.retryAt).toLocaleString()}</time>.</p> : null}
        </div>
      </div>
      {failed ? (
        <Button type="button" size="sm" variant="outline" className="mt-3" disabled={busy} onClick={onRecover}>
          {recovery === "retry" ? "Retry this step" : "Start again"}
        </Button>
      ) : null}
    </div>
  );
}

const manifestHandoffCopy: Partial<Record<FlowStatus["stage"], string>> = {
  awaiting_installation: "Choose repositories on GitHub",
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
        <Loader2 className="h-3.5 w-3.5 animate-spin text-gold" />
        Kingdom continues as soon as GitHub returns
      </p>
    </div>
  );
}

export function ConnectionsPage() {
  const [items, setItems] = useState<Connection[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<RequestError>();
  const [manifestError, setManifestError] = useState<RequestError>();
  const [manifestFailureStage, setManifestFailureStage] = useState<ManifestFailureStage>();
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
  const [sharedOpen, setSharedOpen] = useState(false);
  const [selectedInstallation, setSelectedInstallation] = useState("");
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
  const createConnection = useRecoverableMutation(
    "connection",
    (form: ConnectionForm, key) => apiFetch<Connection>("/connections", {
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
    (request: { connectionId: string; displayName: string }, key) => apiFetch(`/connections/${request.connectionId}`, {
      method: "PATCH",
      headers: { "Idempotency-Key": key },
      body: JSON.stringify({ display_name: request.displayName }),
    }),
  );
  const checkConnection = useRecoverableMutation(
    "check",
    (connectionId: string, key) => apiFetch(`/connections/${connectionId}/health-checks`, {
      method: "POST",
      headers: { "Idempotency-Key": key },
    }),
  );
  const toggleConnection = useRecoverableMutation(
    "toggle",
    (request: { connectionId: string; enabled: boolean }, key) => apiFetch(`/connections/${request.connectionId}`, {
      method: "PATCH",
      headers: { "Idempotency-Key": key },
      body: JSON.stringify({ enabled: request.enabled }),
    }),
  );
  const removeConnection = useRecoverableMutation(
    "remove",
    (connectionId: string, key) => apiFetch(`/connections/${connectionId}`, {
      method: "DELETE",
      headers: { "Idempotency-Key": key },
    }),
  );
  const ordinaryRecoveryPending = createConnection.hasPendingRecovery
    || editConnection.hasPendingRecovery
    || checkConnection.hasPendingRecovery
    || toggleConnection.hasPendingRecovery
    || removeConnection.hasPendingRecovery;

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

  const loadConnections = async () => {
    const value = await apiFetch<{ items: Connection[] }>("/connections");
    setItems(value.items);
  };

  useEffect(() => {
    let active = true;
    void (async () => {
      try {
        const connections = await apiFetch<{ items: Connection[] }>(
          "/connections",
        );
        if (!active) return;
        setItems(connections.items);
      } catch (cause) {
        if (active) setError(requestError(cause));
      }
      try {
        const options = await apiFetch<SetupOptions>(
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
      if (active) setLoading(false);
    })();
    return () => {
      active = false;
    };
  }, []);

  useEffect(() => {
    const advanceID = flow ? `${flow.flow_id}:${flow.stage}:${manifestRetryAttempt}` : "";
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
          await loadConnections();
          setFlow({ ...flow, stage: "completed" });
        }
      } catch (cause) {
        settleSetupRequest(requestID, cause);
        setManifestError(requestError(cause));
        setManifestFailureStage(flow.stage);
        setManifestRecovery(isAmbiguousSetupFailure(cause) ? "retry" : "restart");
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
  }, [flow, manifestRetryAttempt]);

  // While GitHub holds the setup tab, this tab watches for the stage change the
  // callback produces. Without it the operator would have to reload the page to
  // see their own progress.
  useEffect(() => {
    if (!flow || !manifestWaitingStages.has(flow.stage) || manifestError) return;
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
    let active = true;
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
                  "Idempotency-Key": setupRequestKey(
                    requestID,
                    "device-poll",
                  ),
                },
              },
            );
            settleSetupRequest(requestID);
            if (!active) return;
            setSharedError(undefined);
            setSharedFlow(next);
          } catch (cause) {
            settleSetupRequest(requestID, cause);
            if (!active) return;
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
            } else if (!isAmbiguousSetupFailure(cause) && cause instanceof ApiError) {
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
              setSharedError({ message: devicePollRetry, retryAt: nextRetryAt });
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
      active = false;
      window.clearTimeout(timer);
    };
  }, [sharedFlow, sharedOpen]);

  const startShared = async (connectionID?: string) => {
    const requestID = `device-start:${connectionID || "new"}`;
    setOperation("shared-start");
    setSharedError(undefined);
    setSharedFlow(undefined);
    setSelectedInstallation("");
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
      setSharedFlow(next);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      setSharedError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  const refreshCandidates = async () => {
    if (!sharedFlow) return;
    const requestID = `device-candidates:${sharedFlow.flow_id}`;
    setOperation("shared-refresh");
    setSharedError(undefined);
    try {
      setSharedFlow(
        await apiFetch<DeviceFlow>(
          `/connections/github/device-flows/${sharedFlow.flow_id}/candidates`,
          {
            method: "POST",
            headers: {
              "Idempotency-Key": setupRequestKey(
                requestID,
                "device-candidates",
              ),
            },
          },
        ),
      );
      settleSetupRequest(requestID);
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      setSharedError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  const finalizeShared = async () => {
    if (!sharedFlow) return;
    const candidate = sharedFlow.candidates.find(
      (item) => item.installation_id === selectedInstallation,
    );
    if (!candidate) return;
    const requestID = `device-finalize:${sharedFlow.flow_id}:${candidate.installation_id}`;
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
            installation_id: candidate.installation_id,
            display_name: candidate.display_name,
          }),
        },
      );
      settleSetupRequest(requestID);
      await loadConnections();
      setSharedOpen(false);
      setSharedFlow(undefined);
      setSharedReconnectID("");
    } catch (cause) {
      settleSetupRequest(requestID, cause);
      setSharedError(requestError(cause));
    } finally {
      setOperation("");
    }
  };

  // One source of truth for "a private-App setup is live and unfinished" so the
  // inline and modal surfaces cannot disagree about what is startable.
  const manifestFlowLive = !!flow && flow.stage !== "completed" && flow.stage !== "failed";
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
      const response = await apiFetch<{
        flow_id: string;
        form_action: string;
        manifest: string;
        expires_at: string;
      }>("/connections/github/manifest-flows", {
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
      });
      settleSetupRequest(requestID);
      postManifestToSetupWindow(response.form_action, response.manifest);
      // Adopting the started flow is what puts this tab in charge: it shows the
      // stage and starts watching for GitHub's return instead of waiting for a
      // reload to discover progress the operator already made.
      setFlow({
        flow_id: response.flow_id,
        stage: "awaiting_app_creation",
        expires_at: response.expires_at,
      });
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
    if (manifestRecovery === "retry" && flow && manifestFailureStage !== "start") {
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
      await loadConnections();
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
      await editConnection.mutateAsync({ connectionId: id, displayName: editName });
      setEditingID("");
      await loadConnections();
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
      await loadConnections().catch((cause) => setError(requestError(cause)));
      setOperation("");
    }
  };

  return (
    <div className="flex h-full flex-col">
      <PageHeader>
        <div>
          <div className="flex items-center gap-2">
            <Link2 className="h-5 w-5 text-gold" />
            <h1 className="font-display text-xl font-semibold">Connections</h1>
          </div>
          <p className="mt-1 text-xs text-foreground-muted">
            Private GitHub issue access, owned and controlled by you.
          </p>
        </div>
      </PageHeader>
      <PageContent className="space-y-5">
        <ErrorNotice error={error} />
        <Card className="overflow-hidden border-gold/30 bg-[radial-gradient(circle_at_top_right,hsl(var(--gold)/0.12),transparent_42%)]">
          <CardContent className="p-5 sm:p-7">
            <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_18rem] lg:items-end">
              <div>
                <div className="flex items-center gap-3">
                  <div className="rounded-xl border border-gold/30 bg-gold/10 p-2.5">
                    <Github className="h-6 w-6 text-gold" />
                  </div>
                  <div>
                    <p className="text-[11px] font-semibold uppercase tracking-[0.18em] text-gold">
                      Recommended
                    </p>
                    <h2 className="font-display text-xl font-semibold">
                      {setupOptions.shared_enabled
                        ? "Connect GitHub"
                        : "Create a private GitHub App"}
                    </h2>
                  </div>
                </div>
                <p className="mt-4 max-w-2xl text-sm leading-relaxed text-foreground-muted">
                  {setupOptions.shared_enabled
                    ? "Authorize Kingdom's public GitHub App, then choose one installation. Access remains the intersection of the App installation and your GitHub account."
                    : "GitHub creates an App owned by your personal account or organization. Kingdom requests only repository metadata read and Issues read, with no events and no webhook."}
                </p>
                {flow || manifestError ? (
                  <div className="mt-4 space-y-3">
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
                          Resume setup
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
                          Abandon setup
                        </Button>
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
              {setupOptions.shared_enabled ? (
                <div className="space-y-3">
                  <Button
                    className="w-full"
                    disabled={!!operation}
                    onClick={() => void startShared()}
                  >
                    {operation === "shared-start" ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Github className="h-4 w-4" />
                    )}
                    Connect GitHub
                  </Button>
                  <Button
                    type="button"
                    className="w-full"
                    variant="outline"
                    onClick={() => setManifestOpen(true)}
                  >
                    Create a private GitHub App
                  </Button>
                  <button
                    type="button"
                    className="w-full text-center text-xs text-foreground-muted underline decoration-border underline-offset-4 hover:text-foreground"
                    onClick={() => {
                      setManualOpen(true);
                      setManualSaved(undefined);
                    }}
                  >
                    Use manual setup
                  </button>
                </div>
              ) : (
                <div className="space-y-3">
                  <div className="grid grid-cols-2 gap-2 rounded-lg border border-border bg-background/70 p-1">
                    <button
                      type="button"
                      className={`rounded-md px-3 py-2 text-xs font-medium ${ownerType === "personal" ? "bg-gold text-primary-foreground" : "text-foreground-muted"}`}
                      onClick={() => setOwnerType("personal")}
                    >
                      Personal
                    </button>
                    <button
                      type="button"
                      className={`rounded-md px-3 py-2 text-xs font-medium ${ownerType === "organization" ? "bg-gold text-primary-foreground" : "text-foreground-muted"}`}
                      onClick={() => setOwnerType("organization")}
                    >
                      Organization
                    </button>
                  </div>
                  {ownerType === "organization" && (
                    <label className="block text-xs font-medium text-foreground-muted">
                      Organization login
                      <input
                        required
                        aria-label="Organization login"
                        className={cn(fieldClass, "mt-1")}
                        value={organization}
                        onChange={(event) =>
                          setOrganization(event.target.value)
                        }
                      />
                    </label>
                  )}
                  <Button
                    className="w-full"
                    disabled={startManifestDisabled}
                    onClick={() => void startManifest()}
                  >
                    {operation === "flow-start" ? (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    ) : (
                      <Github className="h-4 w-4" />
                    )}
                    Create a private GitHub App
                  </Button>
                  <button
                    type="button"
                    className="w-full text-center text-xs text-foreground-muted underline decoration-border underline-offset-4 hover:text-foreground"
                    onClick={() => {
                      setManualOpen(true);
                      setManualSaved(undefined);
                    }}
                  >
                    Use manual setup instead
                  </button>
                </div>
              )}
            </div>
          </CardContent>
        </Card>

        <div className="flex items-center justify-between">
          <div>
            <h2 className="font-display text-lg font-semibold">
              Saved connections
            </h2>
            <p className="text-xs text-foreground-muted">
              Installation access is validated independently before use.
            </p>
          </div>
        </div>
        {loading ? (
          <div
            aria-label="Loading connections"
            className="grid gap-3 sm:grid-cols-2"
          >
            <div className="h-40 animate-pulse rounded-xl border border-border bg-card/60" />
            <div className="h-40 animate-pulse rounded-xl border border-border bg-card/60" />
          </div>
        ) : items.length === 0 ? (
          <div className="rounded-xl border border-dashed border-border p-8 text-center text-sm text-foreground-muted">
            No connections yet.{" "}
            {setupOptions.shared_enabled
              ? "Connect GitHub"
              : "Create your private App"}{" "}
            above.
          </div>
        ) : (
          <div className="grid gap-4 xl:grid-cols-2">
            {items.map((connection) => {
              const mutable =
                connection.lifecycle === "enabled" ||
                connection.lifecycle === "disabled";
              const account =
                connection.installation_account_login ||
                "Account pending validation";
              return (
                <Card key={connection.connection_id}>
                  <CardContent className="p-5">
                    {editingID === connection.connection_id ? (
                      <form onSubmit={saveEdit} className="space-y-4">
                        <div className="flex items-center justify-between">
                          <div>
                            <h3 className="font-semibold">Edit connection</h3>
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
                          Display name
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
                          {editConnection.hasPendingRecovery ? "Retry save display name" : "Save display name"}
                        </Button>
                      </form>
                    ) : (
                      <>
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div className="min-w-0">
                            <div className="flex flex-wrap items-center gap-2">
                              <h3 className="truncate font-semibold">
                                {connection.display_name}
                              </h3>
                              <span className="rounded-full border border-border px-2 py-0.5 text-[10px] uppercase tracking-wide">
                                {connection.health_class.replaceAll("_", " ")}
                              </span>
                            </div>
                            <p className="mt-1 text-sm text-foreground-muted">
                              @{account} /{" "}
                              {connection.credential_strategy ===
                              "user_access_token"
                                ? "Shared GitHub App"
                                : "Private App key"}{" "}
                              / {connection.lifecycle}
                            </p>
                            {connection.health_class === "permission_mismatch" && (
                              <p className="mt-2 max-w-2xl text-xs leading-5 text-amber-300">
                                {externalSourceFailureGuidance(connection.health_class)}
                              </p>
                            )}
                          </div>
                          {mutable && (
                            <div className="flex flex-wrap gap-2">
                              <Button
                                size="sm"
                                variant="outline"
                                 disabled={!!operation || ordinaryRecoveryPending}
                                onClick={() => {
                                  setEditingID(connection.connection_id);
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
                                  disabled={!!operation || ordinaryRecoveryPending}
                                  onClick={() =>
                                    void startShared(connection.connection_id)
                                  }
                                >
                                  Reconnect
                                </Button>
                              )}
                              <Button
                                size="sm"
                                variant="outline"
                                disabled={
                                   !!operation || (ordinaryRecoveryPending && checkConnection.pendingRequest !== connection.connection_id) ||
                                  connection.lifecycle !== "enabled" ||
                                  connection.credential_state !== "present"
                                }
                                onClick={() => void mutate(connection, "check")}
                              >
                                <ShieldCheck className="h-3.5 w-3.5" />
                                {checkConnection.pendingRequest === connection.connection_id ? "Retry check" : "Check"}
                              </Button>
                              <Button
                                size="sm"
                                variant="outline"
                                 disabled={!!operation || (ordinaryRecoveryPending && toggleConnection.pendingRequest?.connectionId !== connection.connection_id)}
                                onClick={() =>
                                  void mutate(connection, "toggle")
                                }
                              >
                                {toggleConnection.pendingRequest?.connectionId === connection.connection_id
                                  ? `Retry ${toggleConnection.pendingRequest.enabled ? "enable" : "disable"}`
                                  : connection.lifecycle === "enabled" ? "Disable" : "Enable"}
                              </Button>
                              <Button
                                size="sm"
                                variant="outline"
                                 disabled={!!operation || ordinaryRecoveryPending}
                                onClick={() => {
                                  setPendingRemoval(connection);
                                  setRemovalError(undefined);
                                }}
                              >
                                Remove
                              </Button>
                            </div>
                          )}
                        </div>
                        <details className="group mt-4 rounded-lg border border-border/70 bg-background/40">
                          <summary className="flex cursor-pointer list-none items-center justify-between px-3 py-2 text-xs font-medium text-foreground-muted">
                            Technical details
                            <ChevronDown className="h-4 w-4 transition group-open:rotate-180" />
                          </summary>
                          <div className="border-t border-border/70 px-3 py-3 text-xs text-foreground-muted">
                            <dl className="grid grid-cols-[auto_minmax(0,1fr)] gap-x-3 gap-y-1">
                              <dt>Strategy</dt>
                              <dd>{connection.credential_strategy}</dd>
                              <dt>App slug</dt>
                              <dd>{connection.app_slug}</dd>
                              <dt>App ID</dt>
                              <dd className="break-all">{connection.app_id}</dd>
                              <dt>Client ID</dt>
                              <dd className="break-all">
                                {connection.client_id}
                              </dd>
                              <dt>Installation ID</dt>
                              <dd className="break-all">
                                {connection.installation_id}
                              </dd>
                              <dt>Connection ID</dt>
                              <dd className="break-all">
                                {connection.connection_id}
                              </dd>
                            </dl>
                            {mutable &&
                              connection.credential_strategy ===
                                "app_private_key" && (
                                <div className="mt-3 rounded-md border border-gold/20 bg-gold/5 p-2">
                                  <div className="flex items-center gap-2 font-medium text-foreground">
                                    <KeyRound className="h-3.5 w-3.5 text-gold" />
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
                        {!mutable && (
                          <p className="mt-4 text-xs text-foreground-muted">
                            {connection.lifecycle === "removed"
                              ? "Read-only tombstone retained for historical provenance."
                              : "Credential removal is pending."}
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

        {sharedOpen ? (
          <ModalDialog
            open
            onClose={() => setSharedOpen(false)}
            ariaLabel="Connect GitHub"
            panelClassName={`w-full ${modalShellSizeClass("lg")}`}
          >
            <ModalShell
              title="Connect GitHub"
              caption="One secure connection"
              description="Authorize on GitHub, then choose the installation Kingdom may use."
              onClose={() => setSharedOpen(false)}
              footer={
                <div className="flex justify-end">
                  <Button variant="ghost" onClick={() => setSharedOpen(false)}>
                    Close
                  </Button>
                </div>
              }
            >
              <div className="space-y-5">
                <ErrorNotice error={sharedError} />
                {!sharedFlow || operation === "shared-start" ? (
                  <div className="flex min-h-40 items-center justify-center gap-2 text-sm text-foreground-muted">
                    <Loader2 className="h-4 w-4 animate-spin text-gold" />
                    Preparing GitHub…
                  </div>
                ) : sharedFlow.stage === "awaiting_authorization" ? (
                  <div className="space-y-5">
                    <div className="rounded-xl border border-gold/30 bg-gold/5 p-5 text-center">
                      <p className="text-xs font-semibold uppercase tracking-[0.16em] text-foreground-muted">
                        Enter this code on GitHub
                      </p>
                      <div className="mt-3 flex items-center justify-center gap-2">
                        <code className="font-display text-3xl font-semibold tracking-[0.18em] text-gold">
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
                    <p
                      className="flex items-center justify-center gap-2 text-xs text-foreground-muted"
                      aria-live="polite"
                    >
                      <Loader2 className="h-3.5 w-3.5 animate-spin text-gold" />
                      Waiting safely for GitHub authorization
                    </p>
                  </div>
                ) : sharedFlow.stage === "needs_installation" ? (
                  <div className="space-y-4 text-center">
                    <div className="rounded-xl border border-border bg-background/50 p-6">
                      <h3 className="font-display text-lg font-semibold">
                        Choose repositories on GitHub
                      </h3>
                      <p className="mt-2 text-sm text-foreground-muted">
                        Install the App for an account and select the
                        repositories Kingdom should be able to discover.
                      </p>
                    </div>
                    <a
                      className={buttonVariants({ className: "w-full" })}
                      href={sharedFlow.install_url}
                      target="_blank"
                      rel="noreferrer"
                    >
                      Choose repositories
                      <ExternalLink className="h-4 w-4" />
                    </a>
                    <Button
                      variant="outline"
                      className="w-full"
                      disabled={operation === "shared-refresh"}
                      onClick={() => void refreshCandidates()}
                    >
                      {operation === "shared-refresh" && (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      )}
                      I installed the App
                    </Button>
                  </div>
                ) : sharedFlow.stage === "selecting_installation" ? (
                  <div className="space-y-4">
                    <div>
                      <h3 className="font-display text-lg font-semibold">
                        Choose an installation
                      </h3>
                      <p className="mt-1 text-sm text-foreground-muted">
                        One connection is created for the account you select.
                      </p>
                    </div>
                    <div className="grid gap-2 sm:grid-cols-2">
                      {sharedFlow.candidates.map((candidate) => (
                        <label
                          key={candidate.installation_id}
                          className={`cursor-pointer rounded-xl border p-4 transition ${selectedInstallation === candidate.installation_id ? "border-gold bg-gold/10" : "border-border bg-background/40 hover:border-gold/40"}`}
                        >
                          <input
                            className="sr-only"
                            type="radio"
                            name="installation"
                            value={candidate.installation_id}
                            checked={
                              selectedInstallation === candidate.installation_id
                            }
                            onChange={() =>
                              setSelectedInstallation(candidate.installation_id)
                            }
                          />
                          <span className="block font-medium">
                            @{candidate.account_login}
                          </span>
                          <span className="mt-1 block text-xs text-foreground-muted">
                            {candidate.display_name}
                          </span>
                        </label>
                      ))}
                    </div>
                    <Button
                      className="w-full"
                      disabled={
                        !selectedInstallation || operation === "shared-finalize"
                      }
                      onClick={() => void finalizeShared()}
                    >
                      {operation === "shared-finalize" && (
                        <Loader2 className="h-4 w-4 animate-spin" />
                      )}
                      Connect selected installation
                    </Button>
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
                        Start again
                      </Button>
                    )}
                  </div>
                )}
              </div>
            </ModalShell>
          </ModalDialog>
        ) : null}

        {manifestOpen ? (
          <ModalDialog
            open
            onClose={() => setManifestOpen(false)}
            ariaLabel="Create a private GitHub App"
            panelClassName={`w-full ${modalShellSizeClass("sm")}`}
          >
            <ModalShell
              title="Create a private GitHub App"
              caption="Private App option"
              description="Create and own a dedicated App for this Kingdom workspace."
              onClose={() => setManifestOpen(false)}
              footer={
                <div className="flex flex-wrap justify-end gap-2">
                  {manifestFlowLive ? (
                    <Button
                      variant="outline"
                      disabled={!!operation}
                      onClick={() => void abandonManifest()}
                    >
                      {operation === "flow-discard" ? (
                        <Loader2 className="h-3.5 w-3.5 animate-spin" />
                      ) : null}
                      Abandon setup
                    </Button>
                  ) : null}
                  <Button variant="ghost" onClick={() => setManifestOpen(false)}>
                    Close
                  </Button>
                </div>
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
                      className={`rounded-md px-3 py-2 text-xs font-medium capitalize ${ownerType === owner ? "bg-gold text-primary-foreground" : "text-foreground-muted"}`}
                      onClick={() => setOwnerType(owner)}
                    >
                      {owner}
                    </button>
                  ))}
                </div>
                {ownerType === "organization" && (
                  <label className="block text-xs font-medium text-foreground-muted">
                    Organization login
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
                  Create private App on GitHub
                </Button>
              </div>
            </ModalShell>
          </ModalDialog>
        ) : null}

        {manualOpen ? (
          <ModalDialog
            open
            onClose={() => {
              if (!createConnection.hasPendingRecovery) setManualOpen(false);
            }}
            ariaLabel="Manual GitHub App setup"
            panelClassName={`w-full ${modalShellSizeClass("lg")}`}
          >
            <ModalShell
              title="Manual GitHub App setup"
              caption="Secondary setup path"
              description="Save existing App metadata, then provision its private key from the host."
              onClose={() => {
                if (!createConnection.hasPendingRecovery) setManualOpen(false);
              }}
              footer={
                <div className="flex justify-end">
                  <Button
                    type="button"
                    variant="ghost"
                    onClick={() => setManualOpen(false)}
                    disabled={createConnection.hasPendingRecovery}
                  >
                    Close
                  </Button>
                </div>
              }
            >
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
                  <MetadataFields value={manualForm} onChange={setManualForm} disabled={createConnection.hasPendingRecovery} />
                  <p className={MODAL_BODY_TEXT_CLASS}>
                    The App must have exactly Metadata read and Issues read, no
                    events, and no additional permissions. Private keys never
                    enter this browser form.
                  </p>
                  <Button
                    type="submit"
                    disabled={operation === "manual-create"}
                  >
                    {operation === "manual-create" && (
                      <Loader2 className="h-4 w-4 animate-spin" />
                    )}
                    {createConnection.hasPendingRecovery ? "Retry save connection metadata" : "Save connection metadata"}
                  </Button>
                </form>
              )}
            </ModalShell>
          </ModalDialog>
        ) : null}

        {pendingRemoval ? (
          <ModalDialog
            open
            onClose={() => {
              if (!removeConnection.hasPendingRecovery) setPendingRemoval(undefined);
            }}
            ariaLabel="Remove GitHub connection"
            panelClassName={`w-full ${modalShellSizeClass("sm")}`}
          >
            <ModalShell
              title={pendingRemoval.display_name}
              caption="Remove connection"
              description={`This tombstones the connection and removes its ${pendingRemoval.credential_strategy === "user_access_token" ? "local user tokens" : "local private key"}.`}
              onClose={() => {
                if (!removeConnection.hasPendingRecovery) setPendingRemoval(undefined);
              }}
              footer={
                <div className="flex justify-end gap-2">
                  <Button
                    type="button"
                    variant="ghost"
                    onClick={() => setPendingRemoval(undefined)}
                    disabled={removeConnection.hasPendingRecovery}
                  >
                    Cancel
                  </Button>
                  <Button
                    type="button"
                    variant="destructive"
                    disabled={
                      operation === `${pendingRemoval.connection_id}:remove`
                    }
                    onClick={() => void mutate(pendingRemoval, "remove")}
                  >
                    {operation === `${pendingRemoval.connection_id}:remove`
                      ? "Removing..."
                      : removeConnection.hasPendingRecovery ? "Retry remove connection" : "Remove connection"}
                  </Button>
                </div>
              }
            >
              <div className="space-y-3">
                <p className={MODAL_BODY_TEXT_CLASS}>
                  Historical provenance remains available. This does not
                  uninstall or revoke the GitHub App; use GitHub as the external
                  kill switch.
                </p>
                <ErrorNotice error={removalError} />
              </div>
            </ModalShell>
          </ModalDialog>
        ) : null}
      </PageContent>
    </div>
  );
}
