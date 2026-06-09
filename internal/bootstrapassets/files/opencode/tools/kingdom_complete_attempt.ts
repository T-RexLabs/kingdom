import { randomUUID } from "node:crypto"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"

import { tool } from "@opencode-ai/plugin"

export default tool({
  description: "Finalize the current Kingdom attempt for the active invocation. Call exactly once, only after your final response is complete. Provide the full final canonical response in `final_text`.",
  args: {
    final_text: tool.schema.string().describe("The full final canonical response text for this attempt."),
  },
  async execute(args) {
    const protocolVersion = process.env.KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION?.trim() ?? ""
    const attemptID = process.env.KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID?.trim() ?? ""
    const invocationID = process.env.KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID?.trim() ?? ""
    const completionCapability = process.env.KINGDOM_COMPLETE_ATTEMPT_CAPABILITY?.trim() ?? ""
    const receiptDir = process.env.KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR?.trim() ?? ""

    if (!protocolVersion) {
      throw new Error("KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION is not configured")
    }
    if (!attemptID) {
      throw new Error("KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID is not configured")
    }
    if (!invocationID) {
      throw new Error("KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID is not configured")
    }
    if (!completionCapability) {
      throw new Error("KINGDOM_COMPLETE_ATTEMPT_CAPABILITY is not configured")
    }
    if (!receiptDir) {
      throw new Error("KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR is not configured")
    }

    const payload = {
      protocol_version: protocolVersion,
      attempt_id: attemptID,
      invocation_id: invocationID,
      completion_capability: completionCapability,
      final_text: args.final_text,
      structured_payload: {},
    }

    await mkdir(receiptDir, { recursive: true })
    const receiptPath = path.join(receiptDir, `receipt-${Date.now()}-${randomUUID()}.json`)
    await writeFile(receiptPath, JSON.stringify(payload, null, 2), "utf8")

    return `Kingdom completion receipt recorded for invocation ${invocationID}.`
  },
})
