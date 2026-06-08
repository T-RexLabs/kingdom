import { randomUUID } from "node:crypto"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"

import { tool } from "@opencode-ai/plugin"

const defaultProtocolVersion = process.env.KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION ?? "v1"
const defaultAttemptID = process.env.KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID ?? ""
const defaultInvocationID = process.env.KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID ?? ""
const defaultCompletionCapability = process.env.KINGDOM_COMPLETE_ATTEMPT_CAPABILITY ?? ""
const receiptDir = process.env.KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR ?? ""

export default tool({
  description: "Finalize the current Kingdom attempt for the active invocation. Call exactly once, only after your final response is complete. Use the prefilled `attempt_id`, `invocation_id`, and `completion_capability` exactly as provided for this invocation.",
  args: {
    protocol_version: tool.schema.string().default(defaultProtocolVersion).describe("Completion protocol version. Use the default unless runtime explicitly instructs otherwise."),
    attempt_id: tool.schema.string().default(defaultAttemptID).describe("Prefilled current attempt id. Do not change it unless runtime explicitly instructs you to do so."),
    invocation_id: tool.schema.string().default(defaultInvocationID).describe("Prefilled current invocation id. It must match this active invocation."),
    completion_capability: tool.schema.string().default(defaultCompletionCapability).describe("Prefilled single-use capability for this invocation only. Never reuse or copy a capability from another node, attempt, retry, or prior message."),
    final_text: tool.schema.string().describe("The full final canonical response text for this attempt."),
  },
  async execute(args) {
    if (!receiptDir) {
      throw new Error("KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR is not configured")
    }

    const payload = {
      protocol_version: args.protocol_version,
      attempt_id: args.attempt_id,
      invocation_id: args.invocation_id,
      completion_capability: args.completion_capability,
      final_text: args.final_text,
      structured_payload: {},
    }

    await mkdir(receiptDir, { recursive: true })
    const receiptPath = path.join(receiptDir, `receipt-${Date.now()}-${randomUUID()}.json`)
    await writeFile(receiptPath, JSON.stringify(payload, null, 2), "utf8")

    return `Kingdom completion receipt recorded for invocation ${args.invocation_id}.`
  },
})
