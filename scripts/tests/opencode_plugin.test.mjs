import { test } from "node:test"
import assert from "node:assert/strict"
import { mkdtemp, rm } from "node:fs/promises"
import os from "node:os"
import path from "node:path"

async function fixture(t, mode) {
  const outbox = await mkdtemp(path.join(os.tmpdir(), "kingdom-plugin-"))
  t.after(() => rm(outbox, { recursive: true, force: true }))
  delete globalThis[Symbol.for("kingdom.runtime.configuration")]
  Object.assign(process.env, { KINGDOM_AUTHORITY_URL: "https://authority.invalid", KINGDOM_AUTHORITY_TOKEN: "secret", KINGDOM_INSTANCE_ID: "instance", KINGDOM_OUTBOX_DIR: outbox })
  if (mode) process.env.KINGDOM_NATIVE_MODE = mode
  else delete process.env.KINGDOM_NATIVE_MODE
  const { default: plugin } = await import(`../../internal/providers/opencode/plugin/index.js?case=${Math.random()}`)
  assert.equal(process.env.KINGDOM_AUTHORITY_TOKEN, undefined)
  assert.equal(process.env.KINGDOM_NATIVE_MODE, undefined)
  let native = 0, methods
  const tools = new Map()
  const ctx = {
    location: { directory: "/workspace/project" },
    rpc: { register: async (_, impl) => { methods = impl; return { dispose: async () => {} } } },
    agent: { transform: async fn => { fn({ update: (_, cb) => cb({}) }); return { dispose: async () => {} } } },
    tool: { transform: async fn => { fn({ update: (name, cb) => { const tool = { execute: async () => { native++; return { content: "local" } } }; cb(tool); tools.set(name, tool) } }); return { dispose: async () => {} } } },
  }
  const dispose = await plugin.setup(ctx)
  t.after(dispose)
  await methods.register({ profileID: "profile", requestID: "request", system: "fixture", actions: ["read"] })
  const requests = []
  const original = globalThis.fetch
  t.after(() => { globalThis.fetch = original })
  let fail = false
  globalThis.fetch = async (url, options) => {
    requests.push({ route: new URL(url).pathname, body: JSON.parse(options.body) })
    const route = new URL(url).pathname
    const data = route === "/v1/native/invoke"
      ? fail ? { version: 1, tool: "read", ok: false, error: { message: "Confined operation refused" } } : { version: 1, tool: "read", ok: true, result: { content: "confined", output: {} } }
      : route === "/v1/native/admit" ? { execute: true, grantID: "native_" + "a".repeat(64), identityDigest: "digest" } : { acknowledged: true }
    return { ok: true, json: async () => data }
  }
  const context = { agent: "profile", sessionID: "session", messageID: "message", id: "call", signal: new AbortController().signal }
  return { invoke: () => tools.get("read").execute({ path: "file" }, context), requests, native: () => native, fail: () => { fail = true } }
}

test("hosted native tools use only the confined callback", async t => {
  const f = await fixture(t, "managed")
  assert.equal((await f.invoke()).content, "confined")
  f.fail()
  await assert.rejects(f.invoke(), /Confined operation refused/)
  assert.equal(f.native(), 0)
  assert.deepEqual(f.requests.map(r => r.route), ["/v1/native/invoke", "/v1/native/invoke"])
  assert.equal(f.requests[0].body.identity.requestID, "request")
})

test("default local native tools retain admission and durable receipt delivery", async t => {
  const f = await fixture(t)
  assert.equal((await f.invoke()).content, "local")
  assert.equal(f.native(), 1)
  assert.deepEqual(f.requests.map(r => r.route), ["/v1/native/admit", "/v1/native/finish"])
  assert.equal(f.requests[1].body.nativeStarted, true)
  assert.equal(f.requests[1].body.outcome, "completed")
})
