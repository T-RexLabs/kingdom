import { randomUUID } from "node:crypto"
import { mkdir, writeFile } from "node:fs/promises"
import path from "node:path"

import { tool } from "@opencode-ai/plugin"

const defaultProtocolVersion = process.env.KINGDOM_COMPLETE_ATTEMPT_PROTOCOL_VERSION ?? "v1"
const defaultAttemptID = process.env.KINGDOM_COMPLETE_ATTEMPT_ATTEMPT_ID ?? ""
const defaultInvocationID = process.env.KINGDOM_COMPLETE_ATTEMPT_INVOCATION_ID ?? ""
const defaultCompletionCapability = process.env.KINGDOM_COMPLETE_ATTEMPT_CAPABILITY ?? ""
const receiptDir = process.env.KINGDOM_COMPLETE_ATTEMPT_RECEIPT_DIR ?? ""

function buildStructuredPayload(verdict?: string) {
  if (!verdict) {
    return {}
  }
  return { verdict }
}

export default tool({
  description: "Request canonical Kingdom completion settlement for the active provider invocation.",
  args: {
    protocol_version: tool.schema.string().default(defaultProtocolVersion).describe("Completion protocol version."),
    attempt_id: tool.schema.string().default(defaultAttemptID).describe("Active Kingdom attempt id."),
    invocation_id: tool.schema.string().default(defaultInvocationID).describe("Active Kingdom provider invocation id."),
    completion_capability: tool.schema.string().default(defaultCompletionCapability).describe("Single-use completion capability for the active invocation."),
    final_text: tool.schema.string().describe("The final canonical response text for this attempt."),
    verdict: tool.schema.enum(["PASS", "REWORK"]).optional().describe("Required for review and validation roles."),
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
      structured_payload: buildStructuredPayload(args.verdict),
    }

    await mkdir(receiptDir, { recursive: true })
    const receiptPath = path.join(receiptDir, `receipt-${Date.now()}-${randomUUID()}.json`)
    await writeFile(receiptPath, JSON.stringify(payload, null, 2), "utf8")

    return `Kingdom completion receipt recorded for invocation ${args.invocation_id}.`
  },
})
