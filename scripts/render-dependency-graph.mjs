// Renders the dependency-cruiser module graph to SVG without needing a
// system Graphviz install (@hpcc-js/wasm-graphviz ships Graphviz as WASM).
import { execFileSync } from 'node:child_process';
import { writeFileSync } from 'node:fs';

import { Graphviz } from '@hpcc-js/wasm-graphviz';

const dot = execFileSync(
  'npx',
  ['depcruise', '--config', '.dependency-cruiser.cjs', '--output-type', 'dot', 'src'],
  { encoding: 'utf8', maxBuffer: 1024 * 1024 * 10 },
);

const graphviz = await Graphviz.load();
const svg = graphviz.layout(dot, 'svg', 'dot');

writeFileSync('docs/dependency-graph.svg', svg);
console.log('wrote docs/dependency-graph.svg');
