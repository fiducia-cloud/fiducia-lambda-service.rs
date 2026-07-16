import { createHash } from 'node:crypto';
import { isIP } from 'node:net';
import { env, stderr, stdin, stdout } from 'node:process';
import { createContext, Script } from 'node:vm';
import { chromium as playwrightChromium } from 'playwright';
import puppeteer from 'puppeteer-core';

const maxCompiledFunctions = positiveInt(env.LAMBDA_FUNCTION_CACHE_MAX, 128);
const maxFunctionBodyBytes = positiveInt(env.LAMBDA_FUNCTION_BODY_MAX_BYTES, 262_144);
const maxInputLineBytes = positiveInt(env.LAMBDA_CHILD_INPUT_MAX_BYTES, 6_291_456);
const maxResultBytes = positiveInt(env.LAMBDA_RESULT_MAX_BYTES, 1_048_576);
const allowPrivateNetworks = env.LAMBDA_BROWSER_ALLOW_PRIVATE_NETWORKS === 'true';
const allowedHosts = new Set(
  String(env.LAMBDA_BROWSER_ALLOWED_HOSTS || '')
    .split(',')
    .map((host) => host.trim().toLowerCase())
    .filter(Boolean),
);

const compiledFunctions = new Map();
const browserPromises = new Map();
let inputBuffer = '';

function positiveInt(value, fallback) {
  const parsed = Number.parseInt(String(value || ''), 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
}

const safeConsole = Object.freeze(
  Object.fromEntries(
    ['debug', 'error', 'info', 'log', 'warn'].map((level) => [
      level,
      (...args) => {
        const rendered = args
          .map((arg) => (typeof arg === 'string' ? arg : JSON.stringify(arg)))
          .join(' ');
        stderr.write(`[lambda-browser:${level}] ${rendered}\n`);
      },
    ]),
  ),
);

function hashBody(runtime, body) {
  return createHash('sha256').update(runtime).update('\0').update(body).digest('hex');
}

function compileFunction(runtime, functionBody) {
  const cacheKey = hashBody(runtime, functionBody);
  const cached = compiledFunctions.get(cacheKey);
  if (cached) {
    return cached;
  }
  const script = new Script(
    `"use strict"; (async (request, context, page, browser, console) => {\n${functionBody}\n})(request, context, page, browser, console);`,
    { filename: `lambda-browser-${cacheKey.slice(0, 12)}.mjs` },
  );
  compiledFunctions.set(cacheKey, script);
  while (compiledFunctions.size > maxCompiledFunctions) {
    compiledFunctions.delete(compiledFunctions.keys().next().value);
  }
  return script;
}

function assertSlug(slug) {
  const normalized = String(slug || '').trim().toLowerCase();
  if (!/^[a-z0-9][a-z0-9-]{1,118}[a-z0-9]$/.test(normalized)) {
    throw new Error('valid lambda slug is required');
  }
  return normalized;
}

function resolveDefinition(envelope) {
  const definition = envelope.definition || (envelope.functionBody ? envelope : null);
  if (!definition || typeof definition !== 'object') {
    throw new Error('lambda definition with functionBody is required');
  }
  definition.slug = assertSlug(definition.slug || envelope.slug);
  if (definition.status === 'paused' || definition.status === 'archived') {
    throw new Error(`lambda function is ${definition.status}`);
  }
  if (definition.runtime !== 'playwright' && definition.runtime !== 'puppeteer') {
    throw new Error('browser runner requires runtime playwright or puppeteer');
  }
  return definition;
}

function assertAllowedUrl(rawUrl) {
  let url;
  try {
    url = new URL(rawUrl);
  } catch {
    throw new Error('browser request URL is invalid');
  }
  if (url.protocol !== 'http:' && url.protocol !== 'https:') {
    throw new Error(`browser request scheme ${url.protocol} is not allowed`);
  }
  if (url.username || url.password) {
    throw new Error('browser request URL credentials are not allowed');
  }
  const host = url.hostname.toLowerCase().replace(/^\[/, '').replace(/\]$/, '');
  if (allowedHosts.has(host) || allowPrivateNetworks) {
    return;
  }
  if (
    host === 'localhost' ||
    host.endsWith('.localhost') ||
    host.endsWith('.local') ||
    host.endsWith('.internal') ||
    isPrivateIp(host)
  ) {
    throw new Error(`browser request target ${host} is private or local`);
  }
}

function isPrivateIp(host) {
  const kind = isIP(host);
  if (kind === 4) {
    const octets = host.split('.').map(Number);
    const [a, b] = octets;
    return (
      a === 0 ||
      a === 10 ||
      a === 127 ||
      (a === 100 && b >= 64 && b <= 127) ||
      (a === 169 && b === 254) ||
      (a === 172 && b >= 16 && b <= 31) ||
      (a === 192 && b === 168) ||
      a >= 224
    );
  }
  if (kind === 6) {
    const normalized = host.toLowerCase();
    return (
      normalized === '::' ||
      normalized === '::1' ||
      normalized.startsWith('fc') ||
      normalized.startsWith('fd') ||
      /^fe[89ab]/.test(normalized) ||
      normalized.startsWith('::ffff:')
    );
  }
  return false;
}

async function getBrowser(engine) {
  let pending = browserPromises.get(engine);
  if (!pending) {
    pending = engine === 'playwright'
      ? playwrightChromium.launch({
          headless: true,
          args: ['--disable-dev-shm-usage', '--no-sandbox'],
        })
      : puppeteer.launch({
          headless: true,
          executablePath: env.PUPPETEER_EXECUTABLE_PATH || playwrightChromium.executablePath(),
          args: ['--disable-dev-shm-usage', '--no-sandbox'],
        });
    browserPromises.set(engine, pending);
    pending.catch(() => browserPromises.delete(engine));
  }
  return await pending;
}

async function createSession(engine) {
  const browser = await getBrowser(engine);
  if (engine === 'playwright') {
    const context = await browser.newContext();
    const page = await context.newPage();
    await page.route('**/*', async (route) => {
      try {
        assertAllowedUrl(route.request().url());
        await route.continue();
      } catch (error) {
        safeConsole.warn(error instanceof Error ? error.message : String(error));
        await route.abort('blockedbyclient');
      }
    });
    return { page, close: () => context.close() };
  }

  const context = await browser.createBrowserContext();
  const page = await context.newPage();
  await page.setRequestInterception(true);
  page.on('request', (request) => {
    try {
      assertAllowedUrl(request.url());
      void request.continue();
    } catch (error) {
      safeConsole.warn(error instanceof Error ? error.message : String(error));
      void request.abort('blockedbyclient');
    }
  });
  return { page, close: () => context.close() };
}

async function invoke(line) {
  const envelope = JSON.parse(line);
  const definition = resolveDefinition(envelope);
  const functionBody = String(definition.functionBody || '');
  if (!functionBody.trim()) {
    throw new Error('functionBody is required');
  }
  if (Buffer.byteLength(functionBody, 'utf8') > maxFunctionBodyBytes) {
    throw new Error('functionBody exceeds configured byte limit');
  }

  const runtime = definition.runtime;
  const script = compileFunction(runtime, functionBody);
  if (envelope.checkOnly === true || envelope.mode === 'check') {
    return {
      ok: true,
      check: { runtime, slug: definition.slug },
      cachedFunctions: compiledFunctions.size,
    };
  }

  const session = await createSession(runtime);
  const browser = Object.freeze({
    engine: runtime,
    privateNetworksAllowed: allowPrivateNetworks,
    allowedHosts: Object.freeze([...allowedHosts]),
  });
  const context = Object.freeze({
    id: definition.id,
    invocationId: envelope.invocationId,
    slug: definition.slug,
    browser,
    meta: Object.freeze({
      runtime,
      labels: definition.labels,
      metaData: definition.metaData,
      ...(envelope.meta || {}),
    }),
  });
  try {
    const sandbox = createContext(
      {
        request: envelope.request ?? {},
        context,
        page: session.page,
        browser,
        console: safeConsole,
      },
      {
        name: `lambda-browser:${definition.slug}`,
        codeGeneration: { strings: false, wasm: false },
      },
    );
    const result = await script.runInContext(sandbox);
    return {
      ok: true,
      result: result ?? null,
      invocationId: context.invocationId,
      cachedFunctions: compiledFunctions.size,
    };
  } finally {
    await session.close().catch(() => undefined);
  }
}

async function handleLine(line) {
  try {
    writeResult(await invoke(line));
  } catch (error) {
    writeResult({ ok: false, error: error instanceof Error ? error.message : String(error) });
  }
}

const pendingInvocations = new Set();
let shutdownPromise;

function trackInvocation(line) {
  const pending = handleLine(line);
  pendingInvocations.add(pending);
  void pending.finally(() => pendingInvocations.delete(pending));
}

function writeResult(result) {
  let encoded = JSON.stringify(result);
  if (Buffer.byteLength(encoded, 'utf8') > maxResultBytes) {
    encoded = JSON.stringify({ ok: false, error: 'lambda result exceeds configured byte limit' });
  }
  stdout.write(`${encoded}\n`);
}

async function shutdown() {
  shutdownPromise ??= (async () => {
    await Promise.allSettled([...pendingInvocations]);
    const browsers = await Promise.allSettled([...browserPromises.values()]);
    await Promise.allSettled(
      browsers
        .filter((result) => result.status === 'fulfilled')
        .map((result) => result.value.close()),
    );
  })();
  await shutdownPromise;
}

stdin.setEncoding('utf8');
stdin.on('data', (chunk) => {
  inputBuffer += chunk;
  if (Buffer.byteLength(inputBuffer, 'utf8') > maxInputLineBytes) {
    inputBuffer = '';
    writeResult({ ok: false, error: 'lambda input exceeds configured byte limit' });
    return;
  }
  let newlineIndex = inputBuffer.indexOf('\n');
  while (newlineIndex >= 0) {
    const line = inputBuffer.slice(0, newlineIndex).trim();
    inputBuffer = inputBuffer.slice(newlineIndex + 1);
    if (line) {
      trackInvocation(line);
    }
    newlineIndex = inputBuffer.indexOf('\n');
  }
});
stdin.on('end', () => void shutdown());
process.on('SIGTERM', () => void shutdown().finally(() => process.exit(0)));
process.on('SIGINT', () => void shutdown().finally(() => process.exit(0)));
