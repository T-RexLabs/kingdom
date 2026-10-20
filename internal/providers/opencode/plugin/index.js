// Kingdom's bridge into a managed OpenCode server. It owns no policy: Kingdom
// decides every admission, and this plugin only forwards exact identities and
// refuses to run a native operation Kingdom has not granted.
import crypto from "node:crypto"
import fs from "node:fs/promises"
import path from "node:path"

// The server evaluates this module once per location, but tool processes
// inherit the server environment, so the credential is read once, removed from
// the environment, and kept for the later locations.
const CONFIGURATION = Symbol.for("kingdom.runtime.configuration")
const NAMES = ["KINGDOM_AUTHORITY_URL", "KINGDOM_AUTHORITY_TOKEN", "KINGDOM_INSTANCE_ID", "KINGDOM_OUTBOX_DIR"]
globalThis[CONFIGURATION] ??= (() => {
  const values = NAMES.map((name) => process.env[name])
  for (const name of NAMES) delete process.env[name]
  return values
})()
const [authority, token, instanceID, outbox] = globalThis[CONFIGURATION]

const NATIVE_TOOLS = ["write", "edit", "patch", "read", "glob", "grep"]
const RECEIPT_ATTEMPTS = 3
const RECEIPT_RETRY_MS = 40
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function call(route, body, signal = AbortSignal.timeout(20_000)) {
  const response = await fetch(authority + route, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: "Bearer " + token },
    body: JSON.stringify(body),
    signal,
  })
  const data = await response.json()
  if (!response.ok) throw new Error(data.error || "Kingdom rejected the request")
  return data
}

async function syncDirectory() {
  const handle = await fs.open(outbox, "r")
  try {
    await handle.sync()
  } finally {
    await handle.close()
  }
}

// A receipt is durable before delivery is attempted, so Kingdom can recover it
// even if this process or the authority is unavailable.
async function persistReceipt(receipt) {
  const target = path.join(outbox, receipt.grantID + ".json")
  const temporary = target + "." + crypto.randomUUID() + ".tmp"
  const handle = await fs.open(temporary, "wx", 0o600)
  try {
    await handle.writeFile(JSON.stringify(receipt))
    await handle.sync()
  } finally {
    await handle.close()
  }
  await fs.rename(temporary, target)
  await syncDirectory()
  return target
}

async function deliverReceipt(receipt, target) {
  for (let attempt = 1; ; attempt++) {
    try {
      const result = await call("/v1/native/finish", receipt)
      if (!result.acknowledged) throw new Error("Receipt was not acknowledged")
      await fs.rm(target, { force: true })
      await syncDirectory()
      return
    } catch (error) {
      if (attempt === RECEIPT_ATTEMPTS) throw error
      await sleep(RECEIPT_RETRY_MS)
    }
  }
}

const objectSchema = (properties, required) => ({ type: "object", properties, required, additionalProperties: false })
const profileSchema = objectSchema({ profileID: { type: "string" } }, ["profileID"])

export default {
  id: "kingdom.runtime",
  async setup(ctx) {
    if (!authority || !token || !instanceID || !outbox) throw new Error("Kingdom runtime configuration is missing")
    const location = ctx.location.directory
    const generation = crypto.randomUUID()
    // Profiles are never redefined: the server resolves an agent by ID at every
    // step, so a changed definition would alter a request that is running.
    const active = new Map()
    const retired = new Set()
    let closed = false

    const refuseRetired = (context) => {
      if (closed || retired.has(context.agent)) throw new Error("Kingdom profile is no longer active")
    }
    // Only a registered request profile may run a tool, so nothing runs without
    // Kingdom's admission.
    const identityFor = (context) => {
      refuseRetired(context)
      const profile = active.get(context.agent)
      if (!profile) throw new Error("Only Kingdom request profiles may run tools")
      return { instanceID, generation, location, sessionID: context.sessionID, profileID: context.agent, requestID: profile.requestID }
    }

    const rpc = await ctx.rpc.register(
      {
        id: "kingdom",
        methods: {
          register: {
            input: objectSchema(
              {
                profileID: { type: "string" },
                requestID: { type: "string" },
                system: { type: "string" },
                actions: { type: "array", items: { type: "string" } },
              },
              ["profileID", "requestID", "system", "actions"],
            ),
            output: objectSchema({ instanceID: { type: "string" }, generation: { type: "string" }, location: { type: "string" } }, ["instanceID", "generation", "location"]),
          },
          retire: { input: profileSchema, output: objectSchema({ retired: { type: "boolean" } }, ["retired"]) },
        },
        events: {},
      },
      {
        register: async ({ profileID, requestID, system, actions }) => {
          if (closed || active.has(profileID) || retired.has(profileID)) throw new Error("Kingdom profile identity is already used")
          const registration = await ctx.agent.transform((editor) =>
            editor.update(profileID, (agent) => {
              agent.mode = "primary"
              agent.system = system
              agent.description = "Kingdom request " + requestID
              agent.permissions = [
                { action: "*", resource: "*", effect: "deny" },
                ...actions.map((action) => ({ action, resource: "*", effect: "allow" })),
              ]
            }),
          )
          active.set(profileID, { requestID, registration })
          return { instanceID, generation, location }
        },
        retire: async ({ profileID }) => {
          const profile = active.get(profileID)
          if (!profile) return { retired: false }
          // Retire first: an admission racing the registry removal is refused.
          active.delete(profileID)
          retired.add(profileID)
          await profile.registration.dispose()
          return { retired: true }
        },
      },
    )

    const tools = await ctx.tool.transform((editor) => {
      for (const name of NATIVE_TOOLS) {
        editor.update(name, (tool) => {
          const nativeExecute = tool.execute
          tool.execute = async (input, context) => {
            const identity = identityFor(context)
            const grant = await call("/v1/native/admit", { identity, messageID: context.messageID, callID: context.id, tool: name, input })
            if (!grant.execute) throw new Error("Native operation was already admitted; refusing to run it again")
            let nativeStarted = false
            let outcome = "aborted"
            try {
              refuseRetired(context)
              nativeStarted = true
              try {
                const result = await nativeExecute(input, context)
                outcome = "completed"
                return result
              } catch (error) {
                outcome = "failed"
                throw error
              }
            } finally {
              const receipt = { identity, grantID: grant.grantID, identityDigest: grant.identityDigest, outcome, nativeStarted }
              await deliverReceipt(receipt, await persistReceipt(receipt))
            }
          }
        })
      }
      editor.update("shell", (tool) => {
        tool.execute = async (input, context) => {
          const identity = identityFor(context)
          if (input.background) throw new Error("Managed background commands are not supported")
          if (input.workdir && path.resolve(location, input.workdir) !== location) {
            throw new Error("A managed command must run in its workspace root " + location + ". Omit workdir or set it to that directory.")
          }
          await ctx.permission.assert({
            sessionID: context.sessionID,
            agent: context.agent,
            source: { type: "tool", messageID: context.messageID, id: context.id },
            action: "shell",
            resources: [input.command],
            save: [],
          })
          const cancel = () => void call("/v1/command/cancel", { identity }).catch(() => {})
          context.signal.addEventListener("abort", cancel, { once: true })
          try {
            const result = await call(
              "/v1/command/invoke",
              { identity, messageID: context.messageID, callID: context.id, command: input.command, cwd: location, timeout: input.timeout ?? 120_000, toolInput: input },
              context.signal,
            )
            return {
              output: { output: result.output, exit: result.exit, truncated: result.truncated, status: "completed" },
              content: result.output,
              metadata: { guardID: result.guardID, exit: result.exit, quiescent: result.quiescent, truncated: result.truncated },
            }
          } finally {
            context.signal.removeEventListener("abort", cancel)
          }
        }
      })
    })

    return async () => {
      closed = true
      await tools.dispose()
      for (const profile of active.values()) await profile.registration.dispose()
      await rpc.dispose()
    }
  },
}
