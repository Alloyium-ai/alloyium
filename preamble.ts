// Side-effect module — MUST be the first import in the entrypoint.
//
// ES module imports are hoisted and evaluated in source order BEFORE the
// importing module's body runs. So a console-reroute written at the top of
// webhook.ts would actually execute *after* nats.js / the MCP SDK have loaded.
// Putting it in a module that is imported first guarantees it runs before those
// dependencies evaluate — which matters because stdout is the MCP JSON-RPC pipe
// and nats.js calls console.log on some teardown/heartbeat paths (under Bun that
// goes to stdout and corrupts framing). The MCP transport writes via
// process.stdout.write directly, so rerouting console is safe.
console.log = (...a: unknown[]) => console.error(...a)
console.info = (...a: unknown[]) => console.error(...a)
console.debug = (...a: unknown[]) => console.error(...a)

// Never let one stray rejection/exception take down the bridge (which would
// silently stop delivering every NATS event until respawn).
process.on('unhandledRejection', (e) => console.error('[webhook] unhandledRejection', e))
process.on('uncaughtException', (e) => console.error('[webhook] uncaughtException', e))
