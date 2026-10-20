import { NativeConnection, Worker } from '@temporalio/worker';
import * as activities from './activities';
import { TASK_QUEUE, connectionOptions, loadConfig } from './config';

async function run(): Promise<void> {
  const config = loadConfig();
  const connection = await NativeConnection.connect(connectionOptions(config));
  try {
    const worker = await Worker.create({
      connection,
      namespace: config.namespace,
      taskQueue: TASK_QUEUE,
      workflowsPath: require.resolve('./workflows'),
      interceptors: { workflowModules: [require.resolve('./workflows')] },
      activities,
    });
    console.log(`Worker polling task queue ${TASK_QUEUE} on ${config.namespace}`);
    await worker.run();
  } finally {
    await connection.close();
  }
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
