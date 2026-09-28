import { randomUUID } from "node:crypto"
import { link, open, unlink } from "node:fs/promises"
import path from "node:path"

import { tool } from "@opencode-ai/plugin"

const RECEIPT_WIRE_LIMIT_BYTES = 1024 * 1024

async function syncDirectory(directoryPath: string) {
  const directory = await open(directoryPath, process.platform === "win32" ? "r+" : "r")
  try {
    await directory.sync()
  } finally {
    await directory.close()
  }
}

export default tool({
  description: "Finalize the current Kingdom attempt for the active invocation. Call exactly once, only after your final response is complete. Provide the full final canonical response in `final_text`.",
  args: {
    final_text: tool.schema.string().describe("The full final canonical response text for this attempt."),
  },
  async execute(args) {
    const finalText = args.final_text.trim()
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
    if (!finalText) {
      throw new Error("final_text must not be empty")
    }

    const payload = {
      protocol_version: protocolVersion,
      attempt_id: attemptID,
      invocation_id: invocationID,
      completion_capability: completionCapability,
      final_text: finalText,
      structured_payload: {},
    }
    const serialized = JSON.stringify(payload)
    const wireBytes = Buffer.byteLength(serialized, "utf8")
    if (wireBytes > RECEIPT_WIRE_LIMIT_BYTES) {
      throw new Error(`completion receipt exceeds ${RECEIPT_WIRE_LIMIT_BYTES} byte limit`)
    }

    const receiptID = randomUUID()
    const temporaryPath = path.join(receiptDir, `.receipt-${receiptID}.tmp`)
    const receiptPath = path.join(receiptDir, `receipt-${receiptID}.json`)
    let temporaryExists = false

    try {
      const temporaryFile = await open(temporaryPath, "wx", 0o600)
      temporaryExists = true
      try {
        await temporaryFile.writeFile(serialized, "utf8")
        await temporaryFile.sync()
      } finally {
        await temporaryFile.close()
      }

      await link(temporaryPath, receiptPath)
      await unlink(temporaryPath)
      temporaryExists = false
      await syncDirectory(receiptDir)
    } catch (error) {
      if (temporaryExists) {
        try {
          await unlink(temporaryPath)
          temporaryExists = false
          await syncDirectory(receiptDir)
        } catch (cleanupError) {
          const publicationMessage = error instanceof Error ? error.message : String(error)
          const cleanupMessage = cleanupError instanceof Error ? cleanupError.message : String(cleanupError)
          throw new Error(`completion receipt publication failed: ${publicationMessage}; temporary receipt cleanup failed: ${cleanupMessage}`)
        }
      }
      throw error
    }

    return `Kingdom completion receipt recorded for invocation ${invocationID}.`
  },
})
