# Runtime contract tests

`browser-runtimes.test.mjs` launches the packaged child runners and verifies
that Node, Playwright, and Puppeteer definitions pass compile-only validation
through their real JSONL protocol. It deliberately does not open a browser or
contact a network endpoint, so it is safe for local and CI validation.

Run these checks with `npm test` from the repository root.
