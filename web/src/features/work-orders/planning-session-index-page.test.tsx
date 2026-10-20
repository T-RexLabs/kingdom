import { render as renderPlanning } from "./planning-test-render";
import { screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { PlanningSessionIndexPage } from "./planning-session-index-page";

const recovery = vi.hoisted(() => ({
	isPending: false,
	isCheckingRecovery: false,
	retryRecovery: vi.fn(async () => ({ planning_session_id: "session-1" })),
}));

vi.mock("@tanstack/react-router", () => ({
	Link: ({ children, ...props }: { children: ReactNode }) => <a href="#" {...props}>{children}</a>,
	useNavigate: () => vi.fn(),
}));

vi.mock("@/api/hooks", () => ({
	useWorkspaceCapabilities: () => ({ data: { project_root: "/tmp/project" }, error: null }),
}));

vi.mock("./use-create-planning-session-navigation", () => ({
	useCreatePlanningSessionNavigation: () => ({
		 hasRecovery: true,
		isPending: recovery.isPending,
		isCheckingRecovery: recovery.isCheckingRecovery,
		error: null,
		retryRecovery: recovery.retryRecovery,
	}),
}));

vi.mock("./use-provider-model-catalog", () => ({
	useProviderModelCatalogController: () => ({
		state: {
			isLoading: false,
			items: [],
			message: null,
			disableSelection: false,
			triggerPlaceholder: "Select model",
			emptyStateLabel: "No models found.",
		},
		retry: vi.fn(),
		retryPending: false,
	}),
}));

describe("planning session creation recovery", () => {
	beforeEach(() => {
		recovery.isPending = false;
		recovery.isCheckingRecovery = false;
		recovery.retryRecovery.mockClear();
	});

	it("presents an unresolved request as pending instead of as a create failure", () => {
		renderPlanning(<PlanningSessionIndexPage />);

		expect(screen.getByRole("status", { name: "Session Request Pending" })).toBeVisible();
		expect(screen.getByText(/previous session request's outcome is still being confirmed/i)).toBeVisible();
		expect(screen.getByRole("button", { name: "Check Again" })).toBeVisible();
		expect(screen.getByRole("button", { name: "Start Planner Session" })).toBeDisabled();
		expect(screen.queryByText(/previous request may already have created/i)).not.toBeInTheDocument();
	});

	it("does not present the journaled first request as an earlier unresolved request", () => {
		recovery.isPending = true;
		renderPlanning(<PlanningSessionIndexPage />);

		expect(screen.queryByRole("status", { name: "Session Request Pending" })).not.toBeInTheDocument();
		expect(screen.getByRole("button", { name: "Starting..." })).toBeDisabled();
		expect(screen.queryByRole("button", { name: "Check Again" })).not.toBeInTheDocument();
	});

	it("does not label an outcome check as a new session start", () => {
		recovery.isPending = true;
		recovery.isCheckingRecovery = true;
		renderPlanning(<PlanningSessionIndexPage />);

		expect(screen.getByRole("button", { name: "Checking..." })).toBeDisabled();
		expect(screen.getByRole("button", { name: "Start Planner Session" })).toBeDisabled();
		expect(screen.queryByRole("button", { name: "Starting..." })).not.toBeInTheDocument();
	});
});
