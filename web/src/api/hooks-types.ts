import type { components } from "./schema";

/**
 * Mutation argument shape used by every mutation in this module.
 *
 * The `key` is the Idempotency-Key the server will see. By making it part of
 * the mutation's variables, retries from TanStack Query (or the caller) reuse
 * the same key - that's the point of an idempotency key. The previous
 * implementation generated `Date.now()-Math.random()` inside `mutationFn`,
 * which produced a fresh key on every call and silently defeated server-side
 * deduplication.
 *
 * Callers obtain a key once via `newIdempotencyKey(scope)` at the call site.
 */
export type MutationWithBody<TBody> = { body: TBody; key: string };
export type MutationWithValue<TValue> = { value: TValue; key: string };
export type MutationKeyOnly = { key: string };

/*
 * Re-export generated schema types for convenience.
 * These are the canonical API shapes - never hand-roll interfaces
 * that duplicate OpenAPI contract types.
 */
export type RunSummary = components["schemas"]["RunSummary"];
export type RunDetailSummary = components["schemas"]["RunDetailSummary"];
export type RunTabCounts = components["schemas"]["RunTabCounts"];
export type RunPlanSnapshot = components["schemas"]["RunPlanSnapshot"];
export type RunPlanReview = components["schemas"]["RunPlanReview"];
export type RunPlanReviewsResponse = components["schemas"]["RunPlanReviewsResponse"];
export type GovernanceDecision = components["schemas"]["GovernanceDecision"];
export type GovernanceDecisionsResponse = components["schemas"]["GovernanceDecisionsResponse"];
export type RunListResponse = components["schemas"]["RunListResponse"];
export type RunGraphNode = components["schemas"]["RunGraphNode"];
export type RunGraphEdge = components["schemas"]["RunGraphEdge"];
export type RunGraphResponse = components["schemas"]["RunGraphResponse"];
export type RunAgent = components["schemas"]["RunAgent"];
export type RunParticipant = components["schemas"]["RunParticipant"];
export type RunExecution = components["schemas"]["RunExecution"];
export type RunExecutionDetail = components["schemas"]["RunExecutionDetail"];
export type RunExecutionsResponse = components["schemas"]["RunExecutionsResponse"];
export type RunNodeExecution = components["schemas"]["RunNodeExecution"];
export type RunNodeAttemptsResponse = components["schemas"]["RunNodeAttemptsResponse"];
export type RunExecutionFailure = components["schemas"]["RunExecutionFailure"];
export type RunRecoveryState = components["schemas"]["RunRecoveryState"];
export type RunNodeAttemptHistoryItem = components["schemas"]["RunNodeAttemptHistoryItem"];
export type RunAttemptRetryRelationship = components["schemas"]["RunAttemptRetryRelationship"];
export type AgentsResponse = components["schemas"]["AgentsResponse"];
export type Role = components["schemas"]["Role"];
export type RolesResponse = components["schemas"]["RolesResponse"];
export type ProviderModel = components["schemas"]["ProviderModel"];
export type ProviderModelsResponse = components["schemas"]["ProviderModelsResponse"];
export type WorkspaceCapabilities = components["schemas"]["WorkspaceCapabilities"];
export type ThinkingEffortOption = components["schemas"]["ThinkingEffortOption"];
export type RunModelSelection = components["schemas"]["RunModelSelection"];
export type RunModelSelectionEntry = components["schemas"]["RunModelSelectionEntry"];
export type TimelineEvent = components["schemas"]["TimelineEvent"];
export type RunTimelineResponse = components["schemas"]["RunTimelineResponse"];
export type WorkOrderDetail = components["schemas"]["WorkOrderDetail"];
export type PlanningSessionSummary = components["schemas"]["PlanningSessionSummary"];
export type PlanningSessionDetail = components["schemas"]["PlanningSessionDetail"];
export type AcpAgent = components["schemas"]["AcpAgent"];
export type AcpAgentListResponse = components["schemas"]["AcpAgentListResponse"];
export type AcpRun = components["schemas"]["AcpRun"];
export type AcpRunEvent = components["schemas"]["AcpRunEvent"];
export type AcpRunActivityItem = components["schemas"]["AcpRunActivityItem"];
export type AcpRunActivityResponse = components["schemas"]["AcpRunActivityResponse"];
export type CreatePlanningSessionResult = components["schemas"]["CreatePlanningSessionResult"];
export type PlanningSessionListResponse = components["schemas"]["PlanningSessionListResponse"];
export type PlanningMessage = components["schemas"]["PlanningMessage"];
export type PlanningMessagesPage = components["schemas"]["PlanningMessagesPage"];
export type PlanningDraft = components["schemas"]["PlanningDraft"];
export type PlanningContextSnapshot = components["schemas"]["PlanningContextSnapshot"];
export type PlanningAgentModelSelection = components["schemas"]["PlanningAgentModelSelection"];
export type PlanningMessageVariantsResponse = components["schemas"]["PlanningMessageVariantsResponse"];
export type PlannerRuntimeMetrics = components["schemas"]["PlannerRuntimeMetrics"];
export type RuntimeMetrics = components["schemas"]["RuntimeMetrics"];
export type DashboardSnapshot = components["schemas"]["DashboardSnapshot"];
export type AnalyticsOutcomeBucket = components["schemas"]["AnalyticsOutcomeBucket"];
export type AnalyticsRoleDuration = components["schemas"]["AnalyticsRoleDuration"];
export type AnalyticsOverview = components["schemas"]["AnalyticsOverview"];
export type AnalyticsRunAnalytics = components["schemas"]["AnalyticsRunAnalytics"];
export type TokenUsageByRole = components["schemas"]["TokenUsageByRole"];
export type TokenUsageByRoleModel = components["schemas"]["TokenUsageByRoleModel"];
export type AnalyticsTokenSummary = components["schemas"]["AnalyticsTokenSummary"];
export type AnalyticsArtifactSummary = components["schemas"]["AnalyticsArtifactSummary"];
export type AnalyticsNodeSummary = components["schemas"]["AnalyticsNodeSummary"];
export type AnalyticsSelectedRun = components["schemas"]["AnalyticsSelectedRun"];
export type AnalyticsExecutionDetail = components["schemas"]["AnalyticsExecutionDetail"];
export type AnalyticsExecutionDetailSnapshot = components["schemas"]["AnalyticsExecutionDetailSnapshot"];
export type AnalyticsSnapshot = components["schemas"]["AnalyticsSnapshot"];

export type CreateWorkOrderRequest = components["schemas"]["CreateWorkOrderRequest"];
export type CreateWorkOrderResult = components["schemas"]["CreateWorkOrderResult"];
export type CreatePlanningSessionRequest = components["schemas"]["CreatePlanningSessionRequest"];
export type UpdatePlanningSessionRequest = components["schemas"]["UpdatePlanningSessionRequest"];
export type CreateAcpRunRequest = components["schemas"]["CreateAcpRunRequest"];
export type CancelAcpRunRequest = components["schemas"]["CancelAcpRunRequest"];
export type CreateRunRequest = components["schemas"]["CreateRunRequest"];
export type CancelRunRequest = components["schemas"]["CancelRunRequest"];
export type PublishWorkflowRequest = components["schemas"]["PublishWorkflowRequest"];
export type PublishWorkflowResult = components["schemas"]["PublishWorkflowResult"];
export type CreateFinalApprovalRequest = components["schemas"]["CreateFinalApprovalRequest"];
export type CreateFinalApprovalResult = components["schemas"]["CreateFinalApprovalResult"];
export type CreatePlanApprovalRequest = components["schemas"]["CreatePlanApprovalRequest"];
export type CreatePlanApprovalResult = components["schemas"]["CreatePlanApprovalResult"];
export type RetryNodeResult = components["schemas"]["RetryNodeResult"];
export type RetryExecutionStopResult = components["schemas"]["RetryExecutionStopResult"];
export type RerunFromNodeResult = components["schemas"]["RerunFromNodeResult"];
