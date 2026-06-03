import { QueryClient } from "@tanstack/react-query";
import { describe, expect, it } from "vitest";
import type { TimelineEvent } from "@/api/hooks-types";
import {
  MAX_LIVE_TIMELINE_EVENTS,
  appendLiveTimelineEvent,
  liveTimelineQueryKey,
  liveTimelineSyncQueryKey,
  mergeTimelineEvents,
  reconcileLiveTimelineEvents,
  settleLiveTimelineReconciliationAttempt,
  startLiveTimelineReconciliationAttempt,
} from "./run-live-timeline";

describe("run live timeline", () => {
	it("preserves authoritative run ordering when attempt sequence resets", () => {
		const merged = mergeTimelineEvents(
			[
				{ event_id: "evt-1", run_event_seq: 1, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:40:00Z", payload_json: {} },
				{ event_id: "evt-2", run_event_seq: 2, attempt_seq: 2, event_type: "attempt.progress", emitted_at: "2026-03-18T11:41:00Z", payload_json: {} },
			],
			[
				{ event_id: "evt-3", run_event_seq: 3, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:42:00Z", payload_json: {} },
				{ event_id: "evt-4", run_event_seq: 4, attempt_seq: 2, event_type: "attempt.succeeded", emitted_at: "2026-03-18T11:43:00Z", payload_json: {} },
			],
		);

		expect(merged.map((item) => item.run_event_seq)).toEqual([1, 2, 3, 4]);
		expect(merged.map((item) => item.attempt_seq)).toEqual([1, 2, 1, 2]);
	});

	it("restores chronological ordering while hidden-gap backfill is still in progress", () => {
		const merged = mergeTimelineEvents(
			[
				{ event_id: "evt-51", run_event_seq: 51, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:51:00Z", payload_json: {} },
			],
			[
				{ event_id: "evt-41", run_event_seq: 41, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:41:00Z", payload_json: {} },
				{ event_id: "evt-42", run_event_seq: 42, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:42:00Z", payload_json: {} },
				{ event_id: "evt-50", run_event_seq: 50, attempt_seq: 1, event_type: "attempt.succeeded", emitted_at: "2026-03-18T11:50:00Z", payload_json: {} },
			],
		);

		expect(merged.map((item) => item.run_event_seq)).toEqual([41, 42, 50, 51]);
	});

	it("appends live events in stream order and reconciles canonical coverage cleanly", () => {
		const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

		appendLiveTimelineEvent(queryClient, "run-1", {
			event_id: "evt-1",
			run_event_seq: 5,
			attempt_seq: 1,
			event_type: "attempt.started",
			emitted_at: "2026-03-18T11:45:00Z",
			payload_json: {},
		});
		appendLiveTimelineEvent(queryClient, "run-1", {
			event_id: "evt-2",
			run_event_seq: 6,
			attempt_seq: 2,
			event_type: "attempt.progress",
			emitted_at: "2026-03-18T11:46:00Z",
			payload_json: {},
		});
		appendLiveTimelineEvent(queryClient, "run-1", {
			event_id: "evt-3",
			run_event_seq: 7,
			attempt_seq: 3,
			event_type: "attempt.succeeded",
			emitted_at: "2026-03-18T11:47:00Z",
			payload_json: {},
		}, true);

		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
			{ event_id: "evt-1", run_event_seq: 5 },
			{ event_id: "evt-2", run_event_seq: 6 },
			{ event_id: "evt-3", run_event_seq: 7 },
		]);
		expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toEqual({
			dirtyRanges: [{ startSeq: 7, endSeq: 7 }],
			dirtyStartSeq: 7,
			dirtyEndSeq: 7,
			reconciliationAttempt: null,
			reconciliationRetryCount: 0,
			nextRetryAtMs: null,
		});

		reconcileLiveTimelineEvents(queryClient, "run-1", 6, 7);

		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toEqual([
			{ event_id: "evt-1", run_event_seq: 5, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:45:00Z", payload_json: {} },
		]);
		expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toEqual({
			dirtyStartSeq: null,
			dirtyEndSeq: null,
			dirtyRanges: [],
			reconciliationAttempt: null,
			reconciliationRetryCount: 0,
			nextRetryAtMs: null,
		});
	});

	it("ignores replayed older live events so the tail stays monotonic", () => {
		const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

		appendLiveTimelineEvent(queryClient, "run-1", {
			event_id: "evt-5",
			run_event_seq: 5,
			attempt_seq: 5,
			event_type: "attempt.progress",
			emitted_at: "2026-03-18T11:45:00Z",
			payload_json: {},
		});
		appendLiveTimelineEvent(queryClient, "run-1", {
			event_id: "evt-4",
			run_event_seq: 4,
			attempt_seq: 4,
			event_type: "attempt.started",
			emitted_at: "2026-03-18T11:44:00Z",
			payload_json: {},
		});

		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toEqual([
			{ event_id: "evt-5", run_event_seq: 5, attempt_seq: 5, event_type: "attempt.progress", emitted_at: "2026-03-18T11:45:00Z", payload_json: {} },
		]);
	});

	it("preserves the unresolved hidden tail after a partial canonical refetch", () => {
		const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
		queryClient.setQueryData(liveTimelineQueryKey("run-1"), [
			{ event_id: "evt-51", run_event_seq: 51, attempt_seq: 1, event_type: "attempt.started", emitted_at: "2026-03-18T11:51:00Z", payload_json: {} },
			{ event_id: "evt-52", run_event_seq: 52, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:52:00Z", payload_json: {} },
			{ event_id: "evt-53", run_event_seq: 53, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:53:00Z", payload_json: {} },
			{ event_id: "evt-54", run_event_seq: 54, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:54:00Z", payload_json: {} },
			{ event_id: "evt-55", run_event_seq: 55, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:55:00Z", payload_json: {} },
			{ event_id: "evt-56", run_event_seq: 56, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:56:00Z", payload_json: {} },
			{ event_id: "evt-57", run_event_seq: 57, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:57:00Z", payload_json: {} },
			{ event_id: "evt-58", run_event_seq: 58, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:58:00Z", payload_json: {} },
			{ event_id: "evt-59", run_event_seq: 59, attempt_seq: 1, event_type: "attempt.progress", emitted_at: "2026-03-18T11:59:00Z", payload_json: {} },
			{ event_id: "evt-60", run_event_seq: 60, attempt_seq: 1, event_type: "attempt.succeeded", emitted_at: "2026-03-18T12:00:00Z", payload_json: {} },
		]);
		queryClient.setQueryData(liveTimelineSyncQueryKey("run-1"), {
			dirtyStartSeq: 51,
			dirtyEndSeq: 60,
			reconciliationAttempt: null,
			reconciliationRetryCount: 0,
			nextRetryAtMs: null,
		});

		reconcileLiveTimelineEvents(queryClient, "run-1", 1, 55);

		expect(queryClient.getQueryData(liveTimelineQueryKey("run-1"))).toMatchObject([
			{ event_id: "evt-56", run_event_seq: 56 },
			{ event_id: "evt-57", run_event_seq: 57 },
			{ event_id: "evt-58", run_event_seq: 58 },
			{ event_id: "evt-59", run_event_seq: 59 },
			{ event_id: "evt-60", run_event_seq: 60 },
		]);
		expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toMatchObject({
			dirtyStartSeq: 56,
			dirtyEndSeq: 60,
		});
	});

	it("preserves split hidden gaps when canonical coverage fills only the middle window", () => {
		const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
		queryClient.setQueryData(liveTimelineSyncQueryKey("run-1"), {
			dirtyStartSeq: 41,
			dirtyEndSeq: 105,
			dirtyRanges: [
				{ startSeq: 41, endSeq: 55 },
				{ startSeq: 96, endSeq: 105 },
			],
			reconciliationAttempt: null,
			reconciliationRetryCount: 0,
			nextRetryAtMs: null,
		});

		reconcileLiveTimelineEvents(queryClient, "run-1", 56, 95);

		expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toMatchObject({
			dirtyStartSeq: 41,
			dirtyEndSeq: 105,
			dirtyRanges: [
				{ startSeq: 41, endSeq: 55 },
				{ startSeq: 96, endSeq: 105 },
			],
		});
	});

	it("bounds the live tail and records trimmed ranges for canonical catch-up", () => {
		const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });

		for (let seq = 1; seq <= MAX_LIVE_TIMELINE_EVENTS + 1; seq += 1) {
			appendLiveTimelineEvent(queryClient, "run-1", {
				event_id: `evt-${seq}`,
				run_event_seq: seq,
				attempt_seq: seq,
				event_type: "attempt.progress",
				emitted_at: `2026-03-18T11:${String(seq).padStart(2, "0")}:00Z`,
				payload_json: {},
			});
		}

		const liveItems = queryClient.getQueryData<TimelineEvent[]>(liveTimelineQueryKey("run-1")) ?? [];
		expect(liveItems).toHaveLength(MAX_LIVE_TIMELINE_EVENTS);
		expect(liveItems[0]?.run_event_seq).toBe(2);
		expect(liveItems[liveItems.length - 1]?.run_event_seq).toBe(MAX_LIVE_TIMELINE_EVENTS + 1);
		expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toEqual({
			dirtyStartSeq: 1,
			dirtyEndSeq: 1,
			dirtyRanges: [{ startSeq: 1, endSeq: 1 }],
			reconciliationAttempt: null,
			reconciliationRetryCount: 0,
			nextRetryAtMs: null,
		});
	});

	it("backs off reconciliation retries when a hidden-gap request makes no progress", () => {
		const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
		queryClient.setQueryData(liveTimelineSyncQueryKey("run-1"), {
			dirtyStartSeq: 51,
			dirtyEndSeq: 60,
		});

		expect(startLiveTimelineReconciliationAttempt(queryClient, "run-1", {
			kind: "refetch-tail",
			dirtyStartSeq: 51,
			dirtyEndSeq: 60,
			baseOldestSeq: 1,
			baseLatestSeq: 50,
		})).toBe(true);

		settleLiveTimelineReconciliationAttempt(
			queryClient,
			"run-1",
			true,
			{ oldestSeq: 1, latestSeq: 50 },
			1_000,
		);

		expect(queryClient.getQueryData(liveTimelineSyncQueryKey("run-1"))).toEqual({
			dirtyStartSeq: 51,
			dirtyEndSeq: 60,
			dirtyRanges: [{ startSeq: 51, endSeq: 60 }],
			reconciliationAttempt: null,
			reconciliationRetryCount: 1,
			nextRetryAtMs: 2_000,
		});
	});
});
