import type { RunNodeExecution } from "@/api/hooks";
import { Badge } from "@/components/ui/badge";
import { cn, nodeLabel } from "@/lib/utils";
import { statusLabel } from "@/lib/status-system";
import {
	Activity,
	CheckCircle2,
	CircleAlert,
	Play,
	RotateCw,
	XCircle,
} from "lucide-react";
import {
	presentRunNodeExecution,
	type RunNodeExecutionPresentation,
	type RecoveryIconKey,
} from "./run-execution-presentation";

const RECOVERY_ICONS: Record<RecoveryIconKey, typeof Activity> = {
	activity: Activity,
	play: Play,
	check: CheckCircle2,
	x: XCircle,
	rotate: RotateCw,
	alert: CircleAlert,
};

export function WorkflowNodeCards({
	nodes,
	selectedNodeId,
	onSelectNode,
	action,
	emptyMessage,
	unavailableNotice,
}: {
	nodes: RunNodeExecution[];
	selectedNodeId?: string | null;
	onSelectNode?: (nodeId: string) => void;
	action?: { label: (node: RunNodeExecution) => string; onClick: (nodeId: string) => void };
	emptyMessage?: string;
	unavailableNotice?: { message: string; onRetry?: () => void };
}) {
	return (
		<section aria-label="Workflow nodes" className="space-y-2">
			<p className="px-1 text-xs font-medium uppercase tracking-[0.16em] text-foreground-muted">
				Workflow Activity
			</p>
			{unavailableNotice ? (
				<section
					role="alert"
					className="rounded-xl border border-status-blocked/40 bg-status-blocked/10 px-3 py-3 text-sm text-status-blocked"
				>
					<p className="font-medium text-foreground">Execution Step List Unavailable</p>
					<p className="mt-1">{unavailableNotice.message}</p>
					{unavailableNotice.onRetry ? (
						<button
							type="button"
							onClick={unavailableNotice.onRetry}
							className="mt-2 inline-flex min-h-11 items-center rounded-md border border-status-blocked/40 px-3 text-sm font-medium hover:bg-status-blocked/10"
						>
							Retry Step List
						</button>
					) : null}
				</section>
			) : null}
			{nodes.map((node) => {
				const presentation = presentRunNodeExecution(node, node.current_execution);
				const Icon = RECOVERY_ICONS[presentation.iconKey];
				const selected = node.node_id === selectedNodeId;

				return (
					<article
						key={node.node_id}
						className={cn(
							"rounded-xl border bg-background/70",
							selected ? "border-gold/50" : "border-border",
						)}
					>
						{onSelectNode ? (
							<button
								type="button"
								onClick={() => onSelectNode(node.node_id)}
								aria-pressed={selected}
								className="flex min-h-11 w-full items-start gap-3 p-3 text-left hover:bg-surface-raised/40"
							>
								<NodeCardContent icon={Icon} node={node} presentation={presentation} />
							</button>
						) : (
							<div className="flex min-h-11 items-start gap-3 p-3">
								<NodeCardContent icon={Icon} node={node} presentation={presentation} />
							</div>
						)}
						{action ? (
							<div className="border-t border-border px-3 py-2">
								<button
									type="button"
									onClick={() => action.onClick(node.node_id)}
									className="min-h-11 rounded-md border border-border px-3 text-sm font-medium text-foreground hover:bg-surface-raised"
								>
									{action.label(node)}
								</button>
							</div>
						) : null}
					</article>
				);
			})}
			{nodes.length === 0 && emptyMessage ? (
				<p className="rounded-xl border border-border px-4 py-6 text-sm text-foreground-subtle">
					{emptyMessage}
				</p>
			) : null}
		</section>
	);
}

function NodeCardContent({
	icon: Icon,
	node,
	presentation,
}: {
	icon: typeof Activity;
	node: RunNodeExecution;
	presentation: RunNodeExecutionPresentation;
}) {
	return (
		<>
			<Icon aria-hidden className="mt-0.5 h-4 w-4 shrink-0 text-foreground-subtle" />
			<span className="min-w-0 flex-1">
				<span className="flex flex-wrap items-center gap-2">
					<span className="text-wrap-anywhere font-medium text-foreground">
						{nodeLabel(node.node_id)}
					</span>
					<Badge variant="status" status={presentation.status}>
						{statusLabel(presentation.status)}
					</Badge>
				</span>
				<span className="mt-1 block text-sm font-medium text-foreground">
					{presentation.heading}
				</span>
				<span
					role={presentation.announcement?.role}
					aria-live={presentation.announcement?.ariaLive}
					className="mt-1 block text-wrap-anywhere text-sm leading-relaxed text-foreground-subtle"
				>
					{presentation.description}
				</span>
				{presentation.failureDetail ? (
					<span className="mt-1 block text-xs text-status-failed">
						Failure: {presentation.failureDetail}
					</span>
				) : null}
				{presentation.recoveryKind ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						{presentation.recoveryKind}
						{presentation.retryDetail ? ` · ${presentation.retryDetail}` : ""}
					</span>
				) : null}
				{presentation.recoveryReason ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						Recovery Reason: {presentation.recoveryReason}
					</span>
				) : null}
				{presentation.scheduledAt ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						Scheduled: <time dateTime={presentation.scheduledAt} title={presentation.scheduledAt}>{presentation.scheduledAt}</time>
					</span>
				) : null}
				{presentation.pauseReason ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						Pause Reason: {presentation.pauseReason}
					</span>
				) : null}
				{node.attempt_count > 1 ? (
					<span className="mt-1 block text-xs text-foreground-subtle">
						{node.attempt_count} attempts
					</span>
				) : null}
				{presentation.requiredAction ? (
					<span className="mt-2 block text-sm font-medium text-foreground">
						{presentation.requiredAction}
					</span>
				) : null}
			</span>
		</>
	);
}
