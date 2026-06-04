import type { RunExecutionDetail } from "@/api/hooks";
import { RelativeTime } from "@/components/ui/relative-time";
import { cn } from "@/lib/utils";
import { ChevronDown, ChevronRight } from "lucide-react";
import { useEffect, useId, useState, type ReactNode } from "react";
import { runStatusBannerClass } from "./run-detail-tones";
import { executionProgressLabel, isActiveExecutionStatus, isBlockedExecutionStatus, isExecutionReworkOutcome, isQueuedExecutionStatus, operatorExecutionStatusText } from "./run-status-semantics";

type PresentedExecutionDetail = RunExecutionDetail & { live_signal?: { live_label?: string; activity_at: string } };

export function RunExecutionsPanel({
	detail,
	isDetailLoading,
	detailErrorMessage,
	detailPlaceholderMessage,
	onRetryDetail,
}: {
	detail: PresentedExecutionDetail | null;
	isDetailLoading: boolean;
	detailErrorMessage: string | null;
	detailPlaceholderMessage?: string | null;
	onRetryDetail: () => void;
}) {
	const [outcomeOpen, setOutcomeOpen] = useState(false);
	const [diagnosticsOpen, setDiagnosticsOpen] = useState(false);

	useEffect(() => {
		setOutcomeOpen(false);
		setDiagnosticsOpen(false);
	}, [detail?.attempt_id]);

	if (isDetailLoading) {
		return <SectionPlaceholder label="Outcome" message="Loading the selected step..." />;
	}

	if (detailErrorMessage) {
		return (
			<section className="space-y-3 rounded-xl border border-status-failed/30 bg-status-failed/10 px-4 py-5 text-sm text-status-failed">
				<div className="text-xs font-medium uppercase tracking-[0.16em]">Outcome</div>
				<p>{detailErrorMessage}</p>
				<button
					type="button"
					onClick={onRetryDetail}
					className="rounded-md border border-status-failed/30 px-3 py-1.5 text-xs font-medium text-status-failed hover:bg-status-failed/10"
				>
					Retry
				</button>
			</section>
		);
	}

	if (!detail) {
		return <SectionPlaceholder label="Outcome" message={detailPlaceholderMessage ?? "Select a step to inspect the outcome."} />;
	}

	const outcomeBody = outcomeText(detail);
	const outcomeTone = outcomeToneForDetail(detail);
	const diagnosticsFields = diagnosticsFieldsForDetail(detail);

	return (
		<div className="space-y-3">
			<DisclosureSection
				label="Outcome"
				open={outcomeOpen}
				onToggle={() => setOutcomeOpen((current) => !current)}
			>
				<div className="space-y-3">
					{outcomeBody.body ? <p className="text-wrap-anywhere text-sm leading-relaxed text-foreground-subtle">{outcomeBody.body}</p> : null}
					{outcomeBody.note ? (
						<div className={cn(
							"rounded-lg border px-3 py-3 text-sm",
							outcomeTone === "neutral"
								? "border-border bg-surface/60 text-foreground-subtle"
								: runStatusBannerClass(outcomeTone),
						)}>
							{outcomeBody.note}
						</div>
					) : null}
					{detail.output_text ? (
						<pre className="overflow-x-auto whitespace-pre-wrap rounded-lg border border-border bg-surface px-3 py-3 text-wrap-anywhere text-xs leading-relaxed text-foreground">
							{detail.output_text}
						</pre>
					) : null}
				</div>
			</DisclosureSection>

			<DisclosureSection
				label="Diagnostics"
				open={diagnosticsOpen}
				onToggle={() => setDiagnosticsOpen((current) => !current)}
			>
				{diagnosticsFields.length > 0 ? (
					<div className="grid gap-3 text-sm text-foreground-subtle sm:grid-cols-2 xl:grid-cols-3">
						{diagnosticsFields.map((field) => (
							<DetailField key={field.label} label={field.label}>{field.value}</DetailField>
						))}
					</div>
				) : (
					<div className="text-sm text-foreground-subtle">No additional diagnostics for this step.</div>
				)}
			</DisclosureSection>
		</div>
	);
}

function DisclosureSection({
	label,
	open,
	onToggle,
	children,
}: {
	label: string;
	open: boolean;
	onToggle: () => void;
	children: ReactNode;
}) {
	const panelId = useId();
	return (
		<section className="rounded-xl border border-border bg-background/70">
			<button
				type="button"
				onClick={onToggle}
				aria-expanded={open}
				aria-controls={panelId}
				className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left"
			>
				<div className="text-xs font-medium uppercase tracking-[0.16em] text-foreground-muted">{label}</div>
				{open ? <ChevronDown className="h-4 w-4 text-foreground-subtle" /> : <ChevronRight className="h-4 w-4 text-foreground-subtle" />}
			</button>
			{open ? <div id={panelId} className="border-t border-border px-4 py-4">{children}</div> : null}
		</section>
	);
}

function SectionPlaceholder({ label, message }: { label: string; message: string }) {
	return (
		<section className="rounded-xl border border-border bg-background/70 px-4 py-6">
			<div className="text-xs font-medium uppercase tracking-[0.16em] text-foreground-muted">{label}</div>
			<div className="mt-3 text-sm text-foreground-subtle">{message}</div>
		</section>
	);
}

function DetailField({ label, children }: { label: string; children: ReactNode }) {
	return (
		<div className="rounded-lg border border-border/70 bg-surface px-3 py-2">
			<div className="text-[11px] uppercase tracking-[0.16em] text-foreground-muted">{label}</div>
			<div className="mt-1 break-words text-sm text-foreground">{children}</div>
		</div>
	);
}

function diagnosticsFieldsForDetail(detail: PresentedExecutionDetail): Array<{ label: string; value: ReactNode }> {
	const fields: Array<{ label: string; value: ReactNode }> = [];
	if (detail.failure_code) {
		fields.push({ label: "Failure code", value: detail.failure_code });
	}
	if (detail.launch_started_at) {
		fields.push({ label: "Launching", value: <RelativeTime iso={detail.launch_started_at} /> });
	}
	if (detail.started_at) {
		fields.push({ label: "Started", value: <RelativeTime iso={detail.started_at} /> });
	}
	return fields;
}

function outcomeText(detail: PresentedExecutionDetail): { body?: string; note?: string } {
	const progressLabel = executionProgressLabel(
		detail.status,
		detail.progress_reason_code,
		detail.progress_event_type,
		detail.live_signal?.live_label,
	);
	if (detail.output_verdict === "REWORK") {
		return {
			body: "Changes were requested for this step.",
			note: detail.output_text ? "The output below contains the explanation for what needs to change." : undefined,
		};
	}
	if (detail.settlement_status === "pending") {
		return {
			body: detail.output_text ? undefined : operatorExecutionStatusText("verifying", progressLabel) ?? "Kingdom is still confirming the final result for this step.",
			note: detail.output_text ? "The output below may still change." : undefined,
		};
	}
	if (isQueuedExecutionStatus(detail.status)) {
		return {
			body: operatorExecutionStatusText(detail.status, progressLabel) ?? "This step is queued to start.",
			note: undefined,
		};
	}
	if (isActiveExecutionStatus(detail.status)) {
		return {
			body: detail.output_text ? undefined : operatorExecutionStatusText(detail.status, progressLabel) ?? "This step is still running.",
			note: detail.output_text ? "The output below may still change." : undefined,
		};
	}
	if (isBlockedExecutionStatus(detail.status)) {
		return {
			body: operatorExecutionStatusText(detail.status, progressLabel) ?? "This step is blocked while Kingdom decides or performs recovery.",
			note: detail.output_text ? undefined : missingOutputNote(detail),
		};
	}
	const failureText = operatorFacingFailureText(detail);
	if (failureText) {
		return {
			body: failureText,
			note: detail.output_text ? undefined : missingOutputNote(detail),
		};
	}
	if (detail.status === "cancelled") {
		return { body: "This step stopped before completion.", note: missingOutputNote(detail) };
	}
	if (detail.output_text) {
		return {};
	}
	return { note: missingOutputNote(detail) };
}

function outcomeToneForDetail(detail: PresentedExecutionDetail): "neutral" | "running" | "failed" | "rework" {
	if (isExecutionReworkOutcome(detail)) {
		return "rework";
	}
	if (detail.status === "failed") {
		return "failed";
	}
	if (detail.settlement_status === "pending" || isActiveExecutionStatus(detail.status)) {
		return "running";
	}
	return "neutral";
}

function operatorFacingFailureText(detail: PresentedExecutionDetail): string | null {
	if (detail.failure_code === "provider_incomplete_turn") {
		return "Kingdom could not confirm a clean finish for this step.";
	}
	if (detail.failure_code === "provider_cancelled") {
		return "This step was stopped before it could finish.";
	}
	const text = detail.failure_text?.trim();
	return text ? text : null;
}

function missingOutputNote(detail: PresentedExecutionDetail): string | undefined {
	if (detail.output_text) {
		return undefined;
	}
	if (detail.output_status === "unavailable") {
		return "No user-facing output was captured for this step.";
	}
	if (detail.execution_mode === "runtime_owned") {
		return "This was a runtime-owned step, so no user-facing output was expected.";
	}
	return undefined;
}
