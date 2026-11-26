// This plugin is installed only in a private temporary login process. It must
// never be installed in a provider workload or exposed through a public API.
const object = (properties, required) => ({ type: "object", properties, required, additionalProperties: false })
export default {
  id: "kingdom.accounts",
  async setup(ctx) {
    const rpc = await ctx.rpc.register({
      id: "kingdom.accounts",
      methods: { resolve: {
        input: object({ provider: { type: "string" }, connectionID: { type: "string" } }, ["provider", "connectionID"]),
        output: { type: "object", additionalProperties: true },
      } }, events: {},
    }, {
      resolve: async ({ provider, connectionID }) => {
        if (provider !== "github-copilot") throw new Error("Unsupported account provider")
        const connection = await ctx.integration.connection.active(provider)
        if (!connection || connection.type !== "credential" || connection.id !== connectionID || connection.method !== "oauth") throw new Error("Account connection differs")
        const credential = await ctx.integration.connection.resolve(connection)
        if (!credential || credential.type !== "oauth" || credential.methodID !== "device") throw new Error("Account credential differs")
        return credential
      },
    })
    return { dispose: () => rpc.dispose() }
  },
}
