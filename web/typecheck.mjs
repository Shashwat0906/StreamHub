// Runs the TypeScript compiler. If @types/react is installed (normal npm
// install) the real React types are used; otherwise (offline store without
// @types/react) tsconfig.offline.json adds minimal local type shims.
import { createRequire } from 'node:module';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';

const here = path.dirname(fileURLToPath(import.meta.url));
const local = path.join(here, 'node_modules');
const shared = process.env.STREAMHUB_NODE_MODULES || '/opt/npm-tools/node_modules';
const modulesDir = fs.existsSync(path.join(local, 'typescript')) ? local : shared;
const require = createRequire(path.join(modulesDir, 'noop.js'));
const tsc = path.join(path.dirname(require.resolve('typescript/package.json')), 'bin/tsc');
const realTypes = fs.existsSync(path.join(modulesDir, '@types/react'));
const project = realTypes ? 'tsconfig.json' : 'tsconfig.offline.json';
console.log(`typecheck: ${project} (modules from ${modulesDir})`);
const r = spawnSync(process.execPath, [tsc, '--noEmit', '-p', project], {
  cwd: here, stdio: 'inherit',
  env: { ...process.env, NODE_PATH: modulesDir },
});
process.exit(r.status ?? 1);
