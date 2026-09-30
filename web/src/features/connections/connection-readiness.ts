import type { components } from "@/api/schema";

type Connection = components["schemas"]["Connection"];

/**
 * Whether Kingdom can actually read through this connection right now.
 *
 * Three independent facts have to line up, and each can regress on its own:
 * the operator has not disabled it, its last health check passed, and its
 * credential is still on this host. A connection that fails any of them is
 * listed on the Connections page — where it can be repaired — but must never
 * be offered as an import source, because every request through it would fail.
 */
export function isConnectionUsable(connection: Connection): boolean {
  return (
    connection.lifecycle === "enabled" &&
    connection.health_class === "healthy" &&
    connection.credential_state === "present"
  );
}
