import { defineQuery, defineSignal, executeChild, proxyActivities, setHandler, sleep, workflowInfo } from '@temporalio/workflow';
import type { WorkflowInterceptorsFactory } from '@temporalio/workflow';
import type * as activities from './activities';
import type { Order } from './activities';

const {
  notifyWarehouse,
  loadProductCatalog,
  applyCatalogPricing,
  checkOrderStatus,
  sendCustomerEmail,
  runFraudCheck,
} = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
});

const { chargeCard } = proxyActivities<typeof activities>({
  // BAD IDEA: a 1s timeout on a payment call that can legitimately take longer, plus fast, flat, uncapped retries.
  // A timeout does not mean the charge failed, only that the response was late, so each retry of this non-idempotent
  // activity can charge the card again. Give payment calls generous timeouts and make them idempotent before retrying.
  startToCloseTimeout: '1 second',
  retry: {
    initialInterval: '100 milliseconds',
    backoffCoefficient: 1,
    maximumAttempts: Number.POSITIVE_INFINITY,
  },
});

const { refundCard, releaseInventory } = proxyActivities<typeof activities>({
  startToCloseTimeout: '30 seconds',
  // BAD IDEA: compensations get exactly one attempt. They are the steps that most need to succeed (they undo money and
  // stock movements), so they should retry generously, be idempotent, and alert a human if they still cannot finish.
  retry: { maximumAttempts: 1 },
});

const { validateOrder } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  // BAD IDEA: no non-retryable error types and no attempt cap. validateOrder throws plain Errors for input that will
  // never become valid, and every one of them is retried, forever, with the order stuck in limbo. Throw
  // ApplicationFailure.nonRetryable (or list the error type here) so bad input fails the workflow immediately.
  retry: {
    nonRetryableErrorTypes: [],
  },
});

const { reserveInventory } = proxyActivities<typeof activities>({
  // BAD IDEA: scheduleToCloseTimeout (5s) is smaller than startToCloseTimeout (30s). The overall budget runs out before even one full attempt could, so no retry ever fits; the server silently clamps start-to-close down to 5s, hiding the mistake. scheduleToClose should cover several attempts plus backoff.
  startToCloseTimeout: '30 seconds',
  scheduleToCloseTimeout: '5 seconds',
});

const { shipOrder } = proxyActivities<typeof activities>({
  // BAD IDEA: startToCloseTimeout (2s) is shorter than shipOrder's real runtime (~10s), so it can never succeed; size timeouts to the slowest legitimate run.
  startToCloseTimeout: '2 seconds',
  // BAD IDEA: unlimited attempts, backoffCoefficient 1 means no backoff, and a 100ms interval means it hammers the downstream service ~10x/second forever; cap attempts and back off exponentially.
  // The server spells "unlimited" as maximumAttempts 0, but the TS SDK throws a ValueError on 0 and expects Infinity, which it sends to the server as 0.
  retry: {
    maximumAttempts: Number.POSITIVE_INFINITY,
    backoffCoefficient: 1,
    initialInterval: '100 milliseconds',
  },
});

const { generateInvoicePdf } = proxyActivities<typeof activities>({
  startToCloseTimeout: '1 minute',
  // BAD IDEA: heartbeatTimeout (3s) is a tenth of generateInvoicePdf's ~30s runtime and the activity never heartbeats, so it always fails with a heartbeat timeout.
  heartbeatTimeout: '3 seconds',
});

// BAD IDEA: mutable module-level state that counts every workflow task (activation) this sandbox has processed, including
// query-only tasks. Queries never appear in history, so a run that was queried sees a higher count than its replay does.
// When the worker evicts the run from cache (or you run Worker.runReplayHistory), the count comes out lower, the
// notifyWarehouse branch below flips, and replay dies with a TMPRL1100 nondeterminism error. Workflow decisions must be
// derived only from workflow inputs and history (activity results, signals, updates), never from module state.
let workflowTasksSeen = 0;

export const interceptors: WorkflowInterceptorsFactory = () => ({
  internals: [
    {
      activate(input, next) {
        workflowTasksSeen++;
        return next(input);
      },
    },
  ],
});

/**
 * Asks a running OrderWorkflow to stop processing the order. Payload is a human-readable reason.
 * Send it with `npm run cancel-order -- <workflowId> [reason]`.
 */
// BAD IDEA: the signal is defined, exported and documented, but OrderWorkflow never calls setHandler for it. The server
// accepts every cancelOrder signal and records it in history, the client gets a success response, and the SDK just
// buffers it waiting for a handler that never arrives. The order keeps charging, shipping and polling, and when the run
// eventually ends the buffered signals are dropped. Register the handler at the top of the workflow function.
export const cancelOrderSignal = defineSignal<[string]>('cancelOrder');

export const orderStatusQuery = defineQuery<{ workflowTasksSeen: number }>('orderStatus');

// Tasks an unqueried run has processed by the time it reaches the warehouse check: start, validateOrder result, timer.
const EXPECTED_TASKS_BEFORE_CHARGE = 3;

// BAD IDEA: network I/O and environment reads inside workflow code. Neither is recorded in history, so even if they
// worked, a replay on another machine or a day later would see a different response or a different env and take a
// different branch. In practice the sandbox makes it worse: there is no `fetch` and no `process` in the workflow
// isolate, so the first line throws a ReferenceError. The TS SDK treats that as a workflow task failure (not a workflow
// failure), so the run sits there retrying the workflow task forever until someone deploys a fix. Do this in an
// activity and pass the result in.
async function pickCarrier(order: Order): Promise<string> {
  const response = await fetch(`https://rates.example.com/carriers?order=${order.orderId}`);
  const rates = (await response.json()) as { cheapest: string };
  if (process.env.FORCE_OVERNIGHT === 'true') {
    return 'overnight-express';
  }
  return rates.cheapest;
}

// BAD IDEA: an infinite polling loop with no exit condition and no continueAsNew. Each iteration appends 2 timer events,
// 3 activity events and 2 workflow tasks (3 events each), about 11 events every 2 seconds. History grows
// until it trips the 10,240 event warning and then the 51,200 event / 50 MiB hard limit, at which point the server
// terminates the run. Replaying it on cache eviction gets slower every hour. Loop for a bounded number of iterations,
// then continueAsNew, and stop once the order reaches a terminal status.
async function pollOrderStatus(order: Order, trackingNumber: string): Promise<never> {
  for (;;) {
    await sleep('2 seconds');
    await checkOrderStatus(order.orderId, trackingNumber);
  }
}

// v1 charged the card right after validation (and the optional warehouse notice). v2 adds a fraud screen before the
// charge. Bumping this constant and redeploying the worker is how v2 "ships".
const ORDER_WORKFLOW_VERSION: number = 2;

const CUSTOMERS_TO_NOTIFY = 10_000;

export async function CustomerNotificationWorkflow(customerId: string, orderId: string): Promise<void> {
  await sendCustomerEmail(customerId, orderId);
}

// BAD IDEA: 10,000 child workflows started in a single Promise.all with no batching. All 10,000 StartChildWorkflow
// commands go out in one workflow task, blowing past the per-execution pending child limit of 2,000, so the server
// fails the task with PendingChildWorkflowsLimitExceeded and the parent retries it forever without starting a single
// child. Even with the limit raised, each child adds 3 events to the parent (30,000+ here, most of the way to the
// 51,200 event cap) and the burst can rate-limit the whole namespace. Start children in small batches, or fan out
// through intermediate child workflows that each continueAsNew.
async function notifyCustomers(order: Order): Promise<void> {
  const parentId = workflowInfo().workflowId;
  await Promise.all(
    Array.from({ length: CUSTOMERS_TO_NOTIFY }, (_, i) =>
      executeChild(CustomerNotificationWorkflow, {
        workflowId: `${parentId}-notify-${i}`,
        args: [`customer-${i}`, order.orderId],
      }),
    ),
  );
}

export async function OrderWorkflow(order: Order): Promise<string> {
  setHandler(orderStatusQuery, () => ({ workflowTasksSeen }));

  await validateOrder(order);
  // Gives callers a window to query the run before the warehouse decision.
  await sleep('5 seconds');

  // BAD IDEA: branching on module-level state. Any query that landed during the sleep pushes the count past the
  // threshold, so the original run schedules notifyWarehouse and the replay (which never sees the queries) does not.
  if (workflowTasksSeen > EXPECTED_TASKS_BEFORE_CHARGE) {
    await notifyWarehouse(order);
  }
  const compensations: Array<() => Promise<void>> = [];
  try {
    // BAD IDEA: a v2 change that inserts a new activity ahead of chargeCard with no patched()/deprecatePatch() and no
    // worker versioning. Every run that started on v1 and is still open has ActivityTaskScheduled(chargeCard) where v2
    // now issues runFraudCheck, so the moment the v2 worker replays it (cache eviction, restart, deploy) it fails with
    // TMPRL1100 and stays stuck. Wrap it as `if (patched('fraud-check')) await runFraudCheck(order);` or route new runs
    // to a new Worker Deployment Version. `npm run replay:v1-history` shows the failure against a captured v1 history.
    if (ORDER_WORKFLOW_VERSION >= 2) {
      await runFraudCheck(order);
    }
    await chargeCard(order);
    compensations.unshift(() => refundCard(order));

    try {
      await reserveInventory(order);
    } catch {
      // BAD IDEA: swallowing the reserveInventory failure with no logging, no rethrow and no compensation. The order
      // carries on as if stock was reserved, the customer stays charged and gets "shipped" an item that may not exist,
      // and nothing in the logs or the workflow result says anything went wrong. Let it fail, or handle it explicitly.
    }
    compensations.unshift(() => releaseInventory(order));

    // BAD IDEA: the ~1.9 MiB catalog comes back as an activity result and goes straight back out as an activity
    // argument, so history stores it twice (ActivityTaskCompleted and ActivityTaskScheduled), close to 4 MiB for one
    // order. That inflates every replay and creeps toward the 4 MiB gRPC message and 50 MiB history limits. Pass a
    // blob reference.
    const catalog = await loadProductCatalog(order);
    await applyCatalogPricing(order, catalog);
  } catch (err) {
    // BAD IDEA: compensations run back to back with no guard. releaseInventory runs first and always throws, so the
    // loop aborts, refundCard never runs, and the customer keeps every charge. The compensation's ActivityFailure also
    // replaces the original error, so the workflow failure points at the inventory service instead of the real cause.
    // Wrap each compensation in its own try/catch, keep going, retry them durably, and rethrow the original error.
    for (const compensate of compensations) {
      await compensate();
    }
    throw err;
  }

  await pickCarrier(order);
  await generateInvoicePdf(order);
  const trackingNumber = await shipOrder(order);
  await notifyCustomers(order);
  await pollOrderStatus(order, trackingNumber);
  return trackingNumber;
}
