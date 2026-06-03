import { describe, expect, it, vi } from "vitest";
import {
  defaultSSEBackoff,
  subscribeSSE,
  type ParsedSSEEvent,
  type SSEConnectionState,
} from "./sse-stream";

/**
 * Build a Response whose body emits the supplied SSE chunks. Helpful for
 * exercising the parser across split boundaries (chunk that ends mid-event,
 * etc.) without needing a network.
 */
function streamResponse(chunks: string[]): Response {
  const encoder = new TextEncoder();
  const stream = new ReadableStream<Uint8Array>({
    async start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(encoder.encode(chunk));
      }
      controller.close();
    },
  });
  return new Response(stream, { status: 200, headers: { "Content-Type": "text/event-stream" } });
}

describe("subscribeSSE", () => {
  it("yields events split across read() boundaries", async () => {
    const events: ParsedSSEEvent[] = [];
    const states: SSEConnectionState[] = [];
    const controller = new AbortController();

    // Splitting the second event mid-data field exercises the streaming
    // parser's buffer-across-chunks path.
    const fetcher = vi.fn(async () =>
      streamResponse([
        "id: op:1\nevent: text_delta\ndata: {\"text\":\"hello \"}\n\n",
        "id: op:2\nevent: text_delta\nda",
        "ta: {\"text\":\"world\"}\n\n",
        "id: op:3\nevent: done\ndata: {}\n\n",
      ]),
    );

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: (event) => { events.push(event); return true; },
      onConnectionChange: (state) => states.push(state),
      isTerminalEventType: (type) => type === "done",
      signal: controller.signal,
      fetcher,
    });

    expect(events.map((e) => e.id)).toEqual(["op:1", "op:2", "op:3"]);
    expect(events.map((e) => e.type)).toEqual(["text_delta", "text_delta", "done"]);
    expect(events[1].data).toBe('{"text":"world"}');

    expect(states[0]).toEqual({ state: "connecting", attempt: 1 });
    expect(states[1]).toEqual({ state: "open", attempt: 1 });
  });

  it("retries with exponential backoff and emits 'failed' once attempts are exhausted", async () => {
    const sleep = vi.fn(async () => undefined);
    const fetcher = vi.fn(async () => {
      throw new Error("network down");
    });
    const states: SSEConnectionState[] = [];

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: () => true,
      onConnectionChange: (state) => states.push(state),
      isTerminalEventType: () => false,
      signal: new AbortController().signal,
      fetcher,
      sleep,
      backoff: { ...defaultSSEBackoff, maxAttempts: 3 },
    });

    // Two retries between the three attempts, plus a final "failed" state.
    const transitions = states.map((s) => s.state);
    expect(transitions).toEqual(["connecting", "retrying", "connecting", "retrying", "connecting", "failed"]);
    expect(fetcher).toHaveBeenCalledTimes(3);
    expect(sleep).toHaveBeenCalledTimes(2);
  });

  it("treats 404 as a fatal status and does not retry", async () => {
    const sleep = vi.fn();
    const fetcher = vi.fn(async () => new Response(null, { status: 404 }));
    const states: SSEConnectionState[] = [];

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: () => true,
      onConnectionChange: (state) => states.push(state),
      isTerminalEventType: () => false,
      signal: new AbortController().signal,
      fetcher,
      sleep,
    });

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(sleep).not.toHaveBeenCalled();
    expect(states[states.length - 1].state).toBe("failed");
  });

  it("parses CRLF-terminated streams (proxies often normalize to \\r\\n)", async () => {
    const events: ParsedSSEEvent[] = [];
    const fetcher = vi.fn(async () =>
      streamResponse([
        // CRLF for both line and record separators — was silently dropped
        // by the prior parser that only searched for "\n\n".
        "id: op:1\r\nevent: text_delta\r\ndata: {\"text\":\"a\"}\r\n\r\n",
        "id: op:2\r\nevent: done\r\ndata: {}\r\n\r\n",
      ]),
    );

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: (event) => { events.push(event); return true; },
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: new AbortController().signal,
      fetcher,
    });

    expect(events.map((e) => e.id)).toEqual(["op:1", "op:2"]);
    expect(events[0].data).toBe('{"text":"a"}');
    expect(events[1].type).toBe("done");
  });

  it("handles a CRLF that straddles a chunk boundary", async () => {
    const events: ParsedSSEEvent[] = [];
    // The first chunk ends with \r, the second begins with \n. The parser
    // must not treat that as two empty-line separators; it is a single
    // CRLF that completes the blank line ending event #1.
    const fetcher = vi.fn(async () =>
      streamResponse([
        "id: op:1\r\nevent: text_delta\r\ndata: {\"text\":\"x\"}\r\n\r",
        "\nid: op:2\r\nevent: done\r\ndata: {}\r\n\r\n",
      ]),
    );

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: (event) => { events.push(event); return true; },
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: new AbortController().signal,
      fetcher,
    });

    expect(events.map((e) => e.id)).toEqual(["op:1", "op:2"]);
  });

  it("flushes a final event at EOF without a trailing blank line", async () => {
    const events: ParsedSSEEvent[] = [];
    const fetcher = vi.fn(async () =>
      streamResponse([
        // Two events; the second one closes the stream with no trailing
        // "\n\n". A strict-parser would lose it and the planner UI would
        // hang waiting for a terminal that never arrives.
        "id: op:1\nevent: text_delta\ndata: {\"text\":\"a\"}\n\n",
        "id: op:2\nevent: done\ndata: {}",
      ]),
    );

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: (event) => { events.push(event); return true; },
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: new AbortController().signal,
      fetcher,
    });

    expect(events.map((e) => e.id)).toEqual(["op:1", "op:2"]);
    expect(events[1].type).toBe("done");
  });

  it("flushes a CRLF-terminated EOF event with no trailing blank line", async () => {
    const events: ParsedSSEEvent[] = [];
    const fetcher = vi.fn(async () =>
      streamResponse([
        "id: op:1\r\nevent: done\r\ndata: {}",
      ]),
    );

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: () => ({}),
      onEvent: (event) => { events.push(event); return true; },
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: new AbortController().signal,
      fetcher,
    });

    expect(events.map((e) => e.id)).toEqual(["op:1"]);
  });

  it("forwards the latest event id on retry so the server can resume", async () => {
    let attempt = 0;
    const seenLastEventIds: Array<string | null> = [];
    const fetcher = vi.fn(async () => {
      attempt += 1;
      if (attempt === 1) {
        return streamResponse(["id: op:7\nevent: text_delta\ndata: {}\n\n"]);
      }
      // Second attempt closes immediately so the loop exits via a max-attempt failure.
      return streamResponse(["id: op:9\nevent: done\ndata: {}\n\n"]);
    });

    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: (lastEventId) => {
        seenLastEventIds.push(lastEventId);
        return {};
      },
      onEvent: () => true,
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: new AbortController().signal,
      fetcher,
      sleep: async () => {},
    });

    expect(seenLastEventIds[0]).toBeNull();
    expect(seenLastEventIds[1]).toBe("op:7");
  });

  it("does not advance Last-Event-ID when the consumer rejects an event (apply failure)", async () => {
    const seenLastEventIds: Array<string | null> = [];
    let attempt = 0;
    const fetcher = vi.fn(async () => {
      attempt += 1;
      if (attempt === 1) {
        // Two events on the first attempt: op:7 applied, op:8 rejected.
        return streamResponse([
          "id: op:7\nevent: text_delta\ndata: {}\n\n",
          "id: op:8\nevent: text_delta\ndata: {}\n\n",
        ]);
      }
      // Second attempt closes immediately by yielding terminal.
      return streamResponse(["id: op:9\nevent: done\ndata: {}\n\n"]);
    });

    let observed = 0;
    await subscribeSSE({
      url: "https://example.invalid/events",
      buildHeaders: (lastEventId) => {
        seenLastEventIds.push(lastEventId);
        return {};
      },
      onEvent: (event) => {
        observed += 1;
        if (event.id === "op:8") return false; // simulate apply failure
        return true;
      },
      onConnectionChange: () => {},
      isTerminalEventType: (type) => type === "done",
      signal: new AbortController().signal,
      fetcher,
      sleep: async () => {},
    });

    // First connect: nothing applied yet → null. Second connect: op:7
    // applied (truthy) so lastEventId advanced. Crucially NOT op:8.
    expect(seenLastEventIds[0]).toBeNull();
    expect(seenLastEventIds[1]).toBe("op:7");
    expect(observed).toBeGreaterThanOrEqual(2);
  });
});
