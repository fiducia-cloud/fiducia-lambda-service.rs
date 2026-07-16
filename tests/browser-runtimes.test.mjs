import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { test } from 'node:test';

function invokeRunner(file, payload) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, [file], {
      cwd: process.cwd(),
      env: {
        PATH: process.env.PATH,
        HOME: process.env.HOME,
        PLAYWRIGHT_BROWSERS_PATH: process.env.PLAYWRIGHT_BROWSERS_PATH,
        PUPPETEER_EXECUTABLE_PATH: process.env.PUPPETEER_EXECUTABLE_PATH,
      },
      stdio: ['pipe', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');
    child.stdout.on('data', (chunk) => {
      stdout += chunk;
    });
    child.stderr.on('data', (chunk) => {
      stderr += chunk;
    });
    child.on('error', reject);
    child.on('close', (code) => {
      if (code !== 0) {
        reject(new Error(`runner exited ${code}: ${stderr}`));
        return;
      }
      const line = stdout.trim().split('\n').at(-1);
      resolve(JSON.parse(line));
    });
    child.stdin.end(`${JSON.stringify(payload)}\n`);
  });
}

function checkPayload(runtime) {
  return {
    slug: 'browser-check',
    definition: {
      slug: 'browser-check',
      runtime,
      functionBody: 'return { ok: true };',
    },
    request: {},
    checkOnly: true,
  };
}

test('standard Node runner is packaged and compiles function bodies', async () => {
  const result = await invokeRunner('child-runtimes/js-function-runner.mjs', checkPayload('nodejs'));
  assert.equal(result.ok, true);
  assert.equal(result.check.runtime, 'nodejs');
});

for (const runtime of ['playwright', 'puppeteer']) {
  test(`${runtime} is a first-class compile-checked runtime`, async () => {
    const result = await invokeRunner(
      'child-runtimes/browser-function-runner.mjs',
      checkPayload(runtime),
    );
    assert.equal(result.ok, true);
    assert.equal(result.check.runtime, runtime);
    assert.equal(result.check.slug, 'browser-check');
  });
}

test('browser runner rejects a non-browser runtime', async () => {
  const result = await invokeRunner(
    'child-runtimes/browser-function-runner.mjs',
    checkPayload('nodejs'),
  );
  assert.equal(result.ok, false);
  assert.match(result.error, /requires runtime playwright or puppeteer/);
});
