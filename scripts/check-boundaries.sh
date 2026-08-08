#!/usr/bin/env bash
# Milestone 17: the same check `npm run boundaries` runs, plus a
# regenerated dependency graph — one command that both enforces and
# visualizes the module-boundary rule ahead of the Milestone 18 extraction.
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> Checking module boundaries (dependency-cruiser)"
npx depcruise --config .dependency-cruiser.cjs src

echo "==> Rendering dependency graph to docs/dependency-graph.svg"
node scripts/render-dependency-graph.mjs
