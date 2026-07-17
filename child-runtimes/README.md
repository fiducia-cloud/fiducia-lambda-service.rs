# Child runtimes

These newline-delimited JSON runners execute function definitions in isolated
child processes. `js-function-runner.mjs` handles the standard Node runtime;
`browser-function-runner.mjs` handles the Playwright and Puppeteer runtimes.

The browser runner treats function code as untrusted: it blocks private and
local network targets unless the operator explicitly allowlists an owned test
host, rejects URL credentials, creates a new browser context per invocation,
and closes that context after use. It receives no database, authentication,
NATS, or telemetry secrets.

Run the contract checks from the repository root with `npm test`.
