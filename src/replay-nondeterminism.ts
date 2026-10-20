import { readFileSync } from 'node:fs';
import { DeterminismViolationError } from '@temporalio/workflow';
import { Worker } from '@temporalio/worker';

// Replays a captured OrderWorkflow history against the current workflow code and asserts that it produces a
// nondeterminism error. Exits 0 only if the error actually happens.
//   npm run replay:nondeterminism  histories/queried-order.json, a queried v2 run: the module-level task counter (US-007)
//   npm run replay:v1-history      histories/v1-order.json, an unqueried v1 run: the unversioned fraud check (US-018)
async function run(): Promise<void> {
  const historyPath = process.argv[2] ?? 'histories/queried-order.json';
  const history = JSON.parse(readFileSync(historyPath, 'utf8'));
  const workflowsPath = require.resolve('./workflows');

  try {
    await Worker.runReplayHistory(
      { workflowsPath, interceptors: { workflowModules: [workflowsPath] } },
      history,
      'replay-nondeterminism',
    );
  } catch (err) {
    if (err instanceof DeterminismViolationError) {
      console.log(`Got the expected nondeterminism error:\n${err.message}`);
      return;
    }
    throw err;
  }
  console.error('Replay succeeded, but a nondeterminism error was expected');
  process.exit(1);
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
