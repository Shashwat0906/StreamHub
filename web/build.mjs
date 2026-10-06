// Build script: bundles src/ with esbuild and compiles Tailwind CSS v4 with
// its JavaScript API, writing to ../internal/dashboard/ui/dist (embedded
// into the Go binary).
//
// Why not Vite / the Tailwind CLI? Neither needs to be installed: esbuild
// and the tailwindcss package are enough, and this script also runs in
// offline environments where packages come from a shared store (set
// STREAMHUB_NODE_MODULES, default /opt/npm-tools/node_modules) instead of
// ./node_modules.
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const here = path.dirname(fileURLToPath(import.meta.url));
const local = path.join(here, 'node_modules');
const shared = process.env.STREAMHUB_NODE_MODULES || '/opt/npm-tools/node_modules';
const modulesDir = fs.existsSync(path.join(local, 'react')) ? local : shared;
const require = createRequire(path.join(modulesDir, 'noop.js'));
const esbuild = require('esbuild');
const tailwind = require('tailwindcss');

const outDir = path.resolve(here, '../internal/dashboard/ui/dist');
const watch = process.argv.includes('--watch');

// ---------------------------------------------------------------- CSS

const twBase = path.dirname(require.resolve('tailwindcss/package.json'));

function sourceFiles(dir) {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((e) => {
    const p = path.join(dir, e.name);
    if (e.isDirectory()) return sourceFiles(p);
    return /\.(tsx?|html)$/.test(e.name) ? [p] : [];
  });
}

// Tailwind v4 normally scans sources with a native scanner; we tokenise the
// sources ourselves and let Tailwind discard anything that is not a class.
function candidates() {
  const set = new Set();
  for (const f of [...sourceFiles(path.join(here, 'src')), path.join(here, 'index.html')]) {
    for (const tok of fs.readFileSync(f, 'utf8').split(/[^A-Za-z0-9_\-:/.\[\]#%()!@*&>+~=,'"]+/)) {
      for (const t of tok.split(/["'`]/)) if (t) set.add(t);
    }
  }
  return [...set];
}

async function buildCSS() {
  const input = fs.readFileSync(path.join(here, 'src/styles.css'), 'utf8');
  const { build } = await tailwind.compile(input, {
    base: path.join(here, 'src'),
    loadStylesheet: async (id, base) => {
      let file;
      if (id === 'tailwindcss') file = path.join(twBase, 'index.css');
      else if (id.startsWith('tailwindcss/')) file = path.join(twBase, id.slice('tailwindcss/'.length));
      else file = path.resolve(base, id);
      return { path: file, base: path.dirname(file), content: fs.readFileSync(file, 'utf8') };
    },
    loadModule: async (id) => { throw new Error(`tailwind plugins are not supported here: ${id}`); },
  });
  let css = build(candidates());
  const min = await esbuild.transform(css, { loader: 'css', minify: true });
  fs.writeFileSync(path.join(outDir, 'assets/app.css'), min.code);
  return min.code.length;
}

// ---------------------------------------------------------------- JS

const jsOptions = {
  entryPoints: [path.join(here, 'src/main.tsx')],
  bundle: true,
  outfile: path.join(outDir, 'assets/app.js'),
  format: 'esm',
  target: 'es2020',
  jsx: 'automatic',
  minify: !watch,
  sourcemap: watch ? 'inline' : false,
  nodePaths: [modulesDir],
  define: { 'process.env.NODE_ENV': JSON.stringify(watch ? 'development' : 'production') },
  logLevel: 'warning',
};

function writeHTML() {
  const html = fs.readFileSync(path.join(here, 'index.html'), 'utf8');
  fs.writeFileSync(path.join(outDir, 'index.html'), html);
}

fs.rmSync(outDir, { recursive: true, force: true });
fs.mkdirSync(path.join(outDir, 'assets'), { recursive: true });
writeHTML();
if (watch) {
  const ctx = await esbuild.context({
    ...jsOptions,
    plugins: [{ name: 'css', setup(b) { b.onEnd(async () => { await buildCSS(); console.log('rebuilt', new Date().toLocaleTimeString()); }); } }],
  });
  await ctx.watch();
  console.log(`watching src/ (modules from ${modulesDir})`);
} else {
  const t0 = Date.now();
  await esbuild.build(jsOptions);
  const cssSize = await buildCSS();
  const jsSize = fs.statSync(jsOptions.outfile).size;
  console.log(`built in ${Date.now() - t0} ms: app.js ${(jsSize / 1024).toFixed(0)} KiB, app.css ${(cssSize / 1024).toFixed(0)} KiB (modules from ${modulesDir})`);
}
