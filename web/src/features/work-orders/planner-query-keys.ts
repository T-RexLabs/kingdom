/**
 * Single source of truth for all planning-related TanStack Query keys.
 *
 * Every planning query and cache mutation must use these builders
 * instead of hand-written arrays. This prevents prefix collisions
 * (e.g. setQueriesData matching detail keys when targeting the list)
 * and makes the key namespace discoverable in one place.
 */

export const PLANNER_LIST_LIMIT = 100;
export const PLANNER_MESSAGES_PAGE_SIZE = 50;

/**
 * Sentinel keys returned by the optional-id query helpers below when the
 * caller passes `undefined`. Using a clearly-marked placeholder rather than
 * the empty string keeps the cache transparent in the devtools and ensures
 * a real `""` id (which would never come from a router param) cannot collide
 * with the disabled-state entry.
 *
 * Each resource gets its own pending sentinel so disabled queries from
 * different resources can never alias the same cache slot. A coarse
 * shared sentinel would let an in-flight `detail` cache entry leak into a
 * disabled `messages` query (or vice-versa) when devtools, predicate
 * matchers, or future cache helpers touch the namespace.
 */
const PLANNER_PENDING_KEY = "__pending__";

export const plannerKeys = {
  /** Prefix shared by every planning-session query. Safe for broad invalidation. */
  all: () => ["planning-sessions"] as const,

  /**
   * Stable, per-resource sentinels for queries whose id has not resolved
   * yet (e.g. the page is mid-route-transition). Pair with `enabled` or
   * `skipToken` so no fetch is attempted. Constants are exported via the
   * builder shape for tests and devtools clarity.
   */
  pendingDetail: () => ["planning-sessions", PLANNER_PENDING_KEY, "detail"] as const,
  pendingMessages: () => ["planning-sessions", PLANNER_PENDING_KEY, "messages"] as const,
  pendingPlan: () => ["planning-sessions", PLANNER_PENDING_KEY, "plan"] as const,
  pendingVariants: () => ["planning-sessions", PLANNER_PENDING_KEY, "variants"] as const,
  pendingOperation: () => ["planning-operations", PLANNER_PENDING_KEY, "detail"] as const,

  /** Exact key for the sessions list query. */
  list: (limit = PLANNER_LIST_LIMIT, search = "") => search.trim()
    ? ["planning-sessions", "list", limit, { q: search.trim() }] as const
    : ["planning-sessions", "list", limit] as const,

  /**
   * Prefix shared by every limit-keyed list cache slice. Use this with
   * `setQueriesData` / `invalidateQueries` when an action affects every
   * cached list view, regardless of the page-size limit it was opened
   * with. `plannerKeys.list(limit)` is the *full* key for one slice;
   * `plannerKeys.listPrefix()` is the prefix that matches all of them.
   */
  listPrefix: () => ["planning-sessions", "list"] as const,

  /** Exact key for a single session detail. */
  detail: (sessionId: string) => ["planning-sessions", sessionId] as const,

  /** Exact key for a session's paginated messages. */
  messages: (sessionId: string, limit = PLANNER_MESSAGES_PAGE_SIZE) =>
    ["planning-sessions", sessionId, "messages", limit] as const,

  /** Exact key for a session's draft/plan. */
  plan: (sessionId: string) => ["planning-sessions", sessionId, "plan"] as const,

  /** Exact key for an operation detail. */
  operation: (operationId: string) => ["planning-operations", operationId] as const,

  /** Exact key for an operation's derived activity history. */
  activity: (operationId: string) => ["planning-operations", operationId, "activity"] as const,

  /** Exact key for a message's variants. */
  variants: (sessionId: string, messageId: string) =>
    ["planning-sessions", sessionId, "variants", messageId] as const,
} as const;
