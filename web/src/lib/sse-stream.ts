/**
 * Long-lived SSE subscription with explicit connection state and exponential
 * backoff. Replaces the silent `while(!aborted) { catch{} sleep 500ms }` loop
 * that masked outages and froze the planner UI.
 *
 * Callers receive a stream of typed events (one per `data:` payload) plus
 * granular `connection` state changes so the UI can show "connecting",
 * "retrying in 2.4s", or surface a hard "disconnected" banner once retries
 * are exhausted. The function returns when the consumer has seen a terminal
 * event or the backoff cap is hit; the caller is then responsible for
 * deciding whether to reset the abort signal and resubscribe.
 */

export type SSEConnectionState =
  | { state: "idle" }
  | { state: "connecting"; attempt: number }
  | { state: "open"; attempt: number }
  | { state: "retrying"; attempt: number; nextDelayMs: number; lastError: string }
  | { state: "failed"; attempt: number; lastError: string };

export interface SSEBackoffConfig {
  baseMs: number;
  capMs: number;
  maxAttempts: number;
}

export const defaultSSEBackoff: SSEBackoffConfig = {
  baseMs: 500,
  capMs: 30_000,
  maxAttempts: 6,
};

export interface ParsedSSEEvent {
  /** RFC-8895 `id:` value, blank if absent. */
  id: string;
  /** RFC-8895 `event:` value, defaults to "message". */
  type: string;
  /** Joined `data:` lines, with the leading `data: ` stripped. */
  data: string;
}

export interface SubscribeSSEArgs {
  /** Final URL to GET (caller is responsible for query string composition). */
  url: string;
  /** Per-attempt: `lastEventId` is the most recent successfully-applied `id:`. */
  buildHeaders: (lastEventId: string | null) => HeadersInit;
  /**
   * Called for every parsed event. Returns whether the consumer
   * successfully applied the event:
   *   - true  → safe to advance lastEventId. The next reconnect resumes
   *             after this event.
   *   - false → keep lastEventId at its prior value. The next reconnect
   *             will replay this event so a transient apply failure (JSON
   *             parse, cache write, etc.) cannot silently skip data.
   *
   * Throwing is treated as `false` and aborts the current attempt with a
   * retryable error; the rejected event is replayed.
   */
  onEvent: (event: ParsedSSEEvent) => boolean | Promise<boolean>;
  /** Called whenever the connection state changes. */
  onConnectionChange: (state: SSEConnectionState) => void;
  /** Returns true if `event.type` should end the subscription cleanly. */
  isTerminalEventType: (type: string) => boolean;
  /** Aborting unblocks any in-flight read and ends retries immediately. */
  signal: AbortSignal;
  /** Override default backoff (testing). */
  backoff?: Partial<SSEBackoffConfig>;
  /** Override fetch (testing). */
  fetcher?: typeof fetch;
  /** Override sleeper (testing). */
  sleep?: (ms: number, signal: AbortSignal) => Promise<void>;
}

export async function subscribeSSE(args: SubscribeSSEArgs): Promise<void> {
  const cfg: SSEBackoffConfig = { ...defaultSSEBackoff, ...args.backoff };
  const fetcher = args.fetcher ?? fetch;
  const sleep = args.sleep ?? defaultSleep;
  let attempt = 0;
  let lastEventId: string | null = null;

  while (!args.signal.aborted) {
    attempt += 1;
    args.onConnectionChange({ state: "connecting", attempt });

    let response: Response;
    try {
      response = await fetcher(args.url, {
        method: "GET",
        headers: args.buildHeaders(lastEventId),
        signal: args.signal,
        cache: "no-store",
      });
    } catch (err) {
      if (args.signal.aborted) return;
      const message = errorMessage(err);
      if (!shouldRetry(attempt, cfg)) {
        args.onConnectionChange({ state: "failed", attempt, lastError: message });
        return;
      }
      const nextDelayMs = backoffDelayMs(attempt, cfg);
      args.onConnectionChange({ state: "retrying", attempt, nextDelayMs, lastError: message });
      await sleep(nextDelayMs, args.signal);
      continue;
    }

    if (!response.ok || !response.body) {
      const message = `HTTP ${response.status}`;
      // 404/410 means the resource is gone — no point retrying.
      const fatal = response.status === 404 || response.status === 410;
      if (fatal || !shouldRetry(attempt, cfg)) {
        args.onConnectionChange({ state: "failed", attempt, lastError: message });
        return;
      }
      const nextDelayMs = backoffDelayMs(attempt, cfg);
      args.onConnectionChange({ state: "retrying", attempt, nextDelayMs, lastError: message });
      await sleep(nextDelayMs, args.signal);
      continue;
    }

    args.onConnectionChange({ state: "open", attempt });

    let terminal = false;
    let streamError: string | null = null;
    try {
      for await (const event of parseEventStream(response.body, args.signal)) {
        // Apply-then-advance: only mutate lastEventId after onEvent returns
        // truthy. A consumer that fails to apply (e.g. JSON parse error,
        // truncated payload) keeps lastEventId at its prior value so the
        // next reconnect's Last-Event-ID header replays the same event.
        // Without this, a single bad payload silently skips a real event.
        let applied = false;
        try {
          applied = (await args.onEvent(event)) === true;
        } catch (err) {
          if (args.signal.aborted) return;
          streamError = errorMessage(err);
          break;
        }
        if (applied && event.id) {
          lastEventId = event.id;
        }
        if (applied && args.isTerminalEventType(event.type)) {
          terminal = true;
          break;
        }
        if (!applied) {
          // Treat apply-failure as a retryable stream error so the next
          // attempt redelivers the same event. Without breaking out, we
          // would yield more events past one we never accepted.
          streamError = `consumer rejected event ${event.id || event.type}`;
          break;
        }
      }
    } catch (err) {
      if (args.signal.aborted) return;
      streamError = errorMessage(err);
    }

    if (terminal || args.signal.aborted) return;

    const message = streamError ?? "stream closed";
    if (!shouldRetry(attempt, cfg)) {
      args.onConnectionChange({ state: "failed", attempt, lastError: message });
      return;
    }
    const nextDelayMs = backoffDelayMs(attempt, cfg);
    args.onConnectionChange({ state: "retrying", attempt, nextDelayMs, lastError: message });
    await sleep(nextDelayMs, args.signal);
  }
}

function shouldRetry(attempt: number, cfg: SSEBackoffConfig): boolean {
  return attempt < cfg.maxAttempts;
}

/**
 * Full-jitter exponential backoff: each retry waits a random fraction of the
 * exponentially-increasing cap. AWS architecture blog has the canonical
 * argument for full-jitter over equal/decorrelated jitter — it minimises
 * thundering herd from synchronised clients.
 */
function backoffDelayMs(attempt: number, cfg: SSEBackoffConfig): number {
  const exponential = Math.min(cfg.capMs, cfg.baseMs * Math.pow(2, attempt - 1));
  return Math.floor(Math.random() * exponential);
}

function defaultSleep(ms: number, signal: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve();
    const timer = window.setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, ms);
    const onAbort = () => {
      window.clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      resolve();
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

function errorMessage(err: unknown): string {
  if (err instanceof Error) return err.message;
  return String(err);
}

/**
 * Parse a ReadableStream of an SSE response body into discrete events. Each
 * yielded event corresponds to one blank-line-terminated record per the SSE
 * spec; data lines are joined with newlines.
 */
async function* parseEventStream(
  body: ReadableStream<Uint8Array>,
  signal: AbortSignal,
): AsyncGenerator<ParsedSSEEvent> {
  const reader = body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  // Per the HTML5 SSE spec, lines (and therefore record-separating blank
  // lines) may be terminated by \r\n, \n, or \r. Pre-normalize each
  // chunk to LF and hold back a trailing \r across chunk boundaries —
  // when the next chunk's leading byte arrives we know whether that \r
  // was a lone-CR terminator or the first half of a CRLF.
  let pendingCR = false;
  try {
    while (!signal.aborted) {
      const { value, done } = await reader.read();
      if (done) break;
      let chunk = decoder.decode(value, { stream: true });

      if (pendingCR) {
        // The \r we held back from the previous chunk always becomes a
        // single \n in the normalized stream. If the new chunk starts
        // with \n, that byte is the second half of the original CRLF and
        // we drop it; otherwise the held \r was a lone-CR terminator on
        // its own.
        buffer += "\n";
        if (chunk.startsWith("\n")) {
          chunk = chunk.slice(1);
        }
        pendingCR = false;
      }

      if (chunk.endsWith("\r")) {
        pendingCR = true;
        chunk = chunk.slice(0, -1);
      }

      // CRLF → LF first, then any remaining lone CR → LF. Order matters:
      // doing the lone-CR replace first would corrupt CRLF pairs.
      buffer += chunk.replace(/\r\n/g, "\n").replace(/\r/g, "\n");

      let separator = buffer.indexOf("\n\n");
      while (separator >= 0) {
        const raw = buffer.slice(0, separator);
        buffer = buffer.slice(separator + 2);
        const event = parseEventBlock(raw);
        if (event) yield event;
        separator = buffer.indexOf("\n\n");
      }
    }
    if (pendingCR) {
      // Stream ended with a held-back lone \r — emit it as the trailing
      // newline so any final blank line still terminates correctly.
      buffer += "\n";
    }
    // EOF flush: if the stream closed without a trailing blank line but
    // the buffer still contains a complete-looking event block, emit it.
    // Servers and proxies in practice often omit the final "\n\n" and the
    // user-visible cost of dropping the last event (e.g. the terminal
    // "done") is severe — the planner UI hangs on a never-arriving
    // terminal. parseEventBlock returns null when there are no data lines,
    // so partial input is safely skipped.
    let separator = buffer.indexOf("\n\n");
    while (separator >= 0) {
      const raw = buffer.slice(0, separator);
      buffer = buffer.slice(separator + 2);
      const event = parseEventBlock(raw);
      if (event) yield event;
      separator = buffer.indexOf("\n\n");
    }
    if (buffer.length > 0) {
      const event = parseEventBlock(buffer);
      if (event) yield event;
    }
  } finally {
    try { reader.releaseLock(); } catch { /* already released */ }
  }
}

function parseEventBlock(raw: string): ParsedSSEEvent | null {
  let id = "";
  let type = "message";
  const dataLines: string[] = [];
  for (const rawLine of raw.split("\n")) {
    const line = rawLine.startsWith("\r") ? rawLine.slice(1) : rawLine;
    if (line === "" || line.startsWith(":")) continue;
    const colonIdx = line.indexOf(":");
    const field = colonIdx === -1 ? line : line.slice(0, colonIdx);
    const value = colonIdx === -1 ? "" : line.slice(colonIdx + 1).replace(/^ /, "");
    switch (field) {
      case "id":
        id = value;
        break;
      case "event":
        type = value;
        break;
      case "data":
        dataLines.push(value);
        break;
    }
  }
  if (dataLines.length === 0) return null;
  return { id, type, data: dataLines.join("\n") };
}
