import { Client, Connection } from '@temporalio/client';
import { connectionOptions, loadConfig } from './config';
import { cancelOrderSignal } from './workflows';

// Sends cancelOrder to a running OrderWorkflow. It succeeds and does nothing, because the workflow has no handler.
async function run(): Promise<void> {
  const [workflowId, reason = 'customer changed their mind'] = process.argv.slice(2);
  if (!workflowId) {
    throw new Error('Usage: npm run cancel-order -- <workflowId> [reason]');
  }
  const config = loadConfig();
  const connection = await Connection.connect(connectionOptions(config));
  try {
    const client = new Client({ connection, namespace: config.namespace });
    await client.workflow.getHandle(workflowId).signal(cancelOrderSignal, reason);
    console.log(`Sent cancelOrder to ${workflowId}: "${reason}"`);
  } finally {
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
