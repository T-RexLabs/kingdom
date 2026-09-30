import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { GovernanceDecisionHistoryModal } from "./governance-decision-history-modal";

function formatLocalTimestamp(iso: string): string {
	const date = new Date(iso);
	const pad = (value: number) => value.toString().padStart(2, "0");
	return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}`;
}

afterEach(() => {
	vi.useRealTimers();
});

describe("governance decision history modal", () => {
	it("renders decisions in the supplied history order", () => {
		vi.useFakeTimers();
		vi.setSystemTime(new Date("2026-03-06T12:30:00.000Z"));

		render(
			<GovernanceDecisionHistoryModal
				open
				onClose={() => {}}
				items={[
					{
						decision_id: "decision-2",
						stage: "final",
						decision: "confirmed",
						feedback_markdown: "",
						created_at: "2026-03-06T12:00:00.000Z",
					},
					{
						decision_id: "decision-1",
						stage: "plan",
						decision: "needs_rework",
						feedback_markdown: "First guidance note.",
						created_at: "2026-03-06T10:00:00.000Z",
					},
				]}
			/>,
		);

		const dialog = screen.getByRole("dialog", { name: "Operator Decision History" });
		const decisionRows = within(dialog).getAllByText(/Requested Plan Rework|Confirmed Completion/);
		expect(decisionRows[0]).toHaveTextContent("Confirmed Completion");
		expect(decisionRows[1]).toHaveTextContent("Requested Plan Rework");
		expect(within(dialog).getByText(formatLocalTimestamp("2026-03-06T10:00:00.000Z"))).toBeInTheDocument();
		expect(within(dialog).getByText(formatLocalTimestamp("2026-03-06T12:00:00.000Z"))).toBeInTheDocument();
		expect(within(dialog).queryByText(/ago$/)).not.toBeInTheDocument();
	});
});
