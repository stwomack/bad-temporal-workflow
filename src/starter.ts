import { Client, Connection } from '@temporalio/client';
import { DISCONTINUED_SKU, type Order } from './activities';
import { TASK_QUEUE, connectionOptions, loadConfig } from './config';
import { OrderWorkflow } from './workflows';

const sampleOrder: Order = {
  orderId: `order-${Date.now()}`,
  customerId: 'customer-42',
  items: [
    { sku: 'CURSED-MUG', quantity: 2 },
    { sku: 'HAUNTED-TEE', quantity: 1 },
  ],
  amountCents: 4999,
};

// `npm run start-order -- --invalid` sends an order that validateOrder rejects, to watch it retry forever.
const invalidOrder: Order = { ...sampleOrder, items: [], amountCents: 0 };
// `npm run start-order -- --discontinued` makes applyCatalogPricing fail so the saga compensations run (and fail).
const discontinuedOrder: Order = { ...sampleOrder, items: [{ sku: DISCONTINUED_SKU, quantity: 1 }] };
const order = process.argv.includes('--invalid')
  ? invalidOrder
  : process.argv.includes('--discontinued')
    ? discontinuedOrder
    : sampleOrder;

async function run(): Promise<void> {
  const config = loadConfig();
  const connection = await Connection.connect(connectionOptions(config));
  try {
    const client = new Client({ connection, namespace: config.namespace });
    // BAD IDEA: no workflowExecutionTimeout or workflowRunTimeout. Combined with the infinite retries and the endless
    // polling loop in the workflow, a stuck order stays open forever (retention only starts once a run closes), quietly
    // burning actions and worker capacity until someone notices and terminates it by hand. Set an execution timeout
    // that reflects the business deadline for an order.
    const handle = await client.workflow.start(OrderWorkflow, {
      taskQueue: TASK_QUEUE,
      workflowId: order.orderId,
      args: [order],
    });
    console.log(`Started ${OrderWorkflow.name} ${handle.workflowId} (run ${handle.firstExecutionRunId})`);
  } finally {
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
