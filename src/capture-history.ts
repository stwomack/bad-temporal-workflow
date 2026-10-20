import { writeFileSync } from 'node:fs';
import { Client, Connection } from '@temporalio/client';
import { historyToJSON } from '@temporalio/common/lib/proto-utils';
import type { Order } from './activities';
import { TASK_QUEUE, connectionOptions, loadConfig } from './config';
import { OrderWorkflow, orderStatusQuery } from './workflows';

// Starts an OrderWorkflow, queries it during its 5 second hold so the module-level task counter runs ahead of history,
// waits until it has moved past chargeCard, and saves the history to the path given as the first argument.
// Pass --no-query to skip the queries. To recapture histories/v1-order.json, set ORDER_WORKFLOW_VERSION to 1 in
// workflows.ts, rebuild, restart the worker, run `npm run capture-history -- histories/v1-order.json --no-query`,
// then set it back to 2.
// Requires `npm run worker` to be running against the same namespace.
async function run(): Promise<void> {
  const args = process.argv.slice(2);
  const outPath = args.find((arg) => !arg.startsWith('--')) ?? 'histories/queried-order.json';
  const queryCount = args.includes('--no-query') ? 0 : 3;
  const config = loadConfig();
  const connection = await Connection.connect(connectionOptions(config));
  try {
    const client = new Client({ connection, namespace: config.namespace });
    const order: Order = {
      orderId: `order-capture-${Date.now()}`,
      customerId: 'customer-42',
      items: [{ sku: 'CURSED-MUG', quantity: 1 }],
      amountCents: 1999,
    };
    const handle = await client.workflow.start(OrderWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: order.orderId,
      args: [order],
    });

    await new Promise((resolve) => setTimeout(resolve, 1500));
    for (let i = 0; i < queryCount; i++) {
      console.log('orderStatus', await handle.query(orderStatusQuery));
    }

    // The hold is 5 seconds; give the run time to schedule notifyWarehouse and chargeCard.
    await new Promise((resolve) => setTimeout(resolve, 8000));
    const history = await handle.fetchHistory();
    writeFileSync(outPath, historyToJSON(history));
    console.log(`Saved history for ${handle.workflowId} to ${outPath}`);
  } finally {
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
