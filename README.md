# cursed-order-workflow

An order-processing Temporal workflow in TypeScript, built almost entirely out of anti-patterns. Every mistake is deliberate and marked in the source with a `// BAD IDEA:` comment that explains what goes wrong. Use it for teaching, code-review practice, or to watch Temporal Cloud react to bad configuration. Do not copy any of it into a real system.

The workflow validates an order, charges a card, reserves inventory, prices it against a catalog, picks a carrier, generates an invoice, ships, notifies customers, and polls shipment status. Each step is broken in its own way.

## Requirements

- Node.js 20 or later
- A Temporal Cloud namespace, plus either an API key or an mTLS client certificate for it

## Connecting to Temporal Cloud

All connection settings come from environment variables.

| Variable | Required | Example |
| --- | --- | --- |
| `TEMPORAL_ADDRESS` | yes | `my-ns.a1b2c.tmprl.cloud:7233` (mTLS) or `us-east-1.aws.api.temporal.io:7233` (API key) |
| `TEMPORAL_NAMESPACE` | yes | `my-ns.a1b2c` |
| `TEMPORAL_API_KEY` | one auth method | an API key with access to the namespace |
| `TEMPORAL_TLS_CERT` | one auth method | path to the client certificate PEM |
| `TEMPORAL_TLS_KEY` | one auth method | path to the client private key PEM |

If `TEMPORAL_API_KEY` is set, it is used. Otherwise both `TEMPORAL_TLS_CERT` and `TEMPORAL_TLS_KEY` must be set. Use the endpoint the Temporal Cloud UI shows for your chosen auth method.

```sh
export TEMPORAL_ADDRESS=us-east-1.aws.api.temporal.io:7233
export TEMPORAL_NAMESPACE=my-ns.a1b2c
export TEMPORAL_API_KEY=...
```

## Running it

```sh
npm install
npm run build

# terminal 1: worker polling the cursed-orders task queue
npm run worker

# terminal 2: start an order
npm run start-order
npm run start-order -- --invalid        # empty cart, watch validateOrder retry forever
npm run start-order -- --discontinued   # trigger the saga and its failing compensations

# send the cancelOrder signal that nothing listens for
npm run cancel-order -- <workflowId> "changed my mind"
```

What a default order does on Cloud: validation passes, the run waits 5 seconds, `chargeCard` books three charges because of its timeout and retries, the inventory result is ignored, the 1.88 MiB catalog travels through two activities, and then `pickCarrier` throws `ReferenceError: fetch is not defined`. The workflow task fails and retries indefinitely, so the run stays open with no timeout to end it. Everything after that point (invoice, shipping, the 10,000 children, the polling loop) is present in the code but only reachable once you comment out earlier steps.

## Replay demos

Two committed histories show nondeterminism errors produced by the code as written. Each script exits 0 only when replay actually fails with `DeterminismViolationError`.

```sh
npm run replay:nondeterminism   # histories/queried-order.json: module-level state (TMPRL1100, notifyWarehouse vs runFraudCheck)
npm run replay:v1-history       # histories/v1-order.json: unversioned v2 change (TMPRL1100, chargeCard vs runFraudCheck)
```

To recapture them against your own namespace with the worker running:

```sh
npm run capture-history -- histories/queried-order.json
# for the v1 history, set ORDER_WORKFLOW_VERSION to 1 in src/workflows.ts, rebuild, restart the worker, then:
npm run capture-history -- histories/v1-order.json --no-query
# and set ORDER_WORKFLOW_VERSION back to 2
```

## Catalog of bad ideas

Locations are given as file and symbol so they survive edits. `grep -n "BAD IDEA" src/*.ts` lists every marker with its line number.

| # | Anti-pattern | Where it lives | What goes wrong | The fix |
| --- | --- | --- | --- | --- |
| US-003 | Start-to-close timeout shorter than the activity | `shipOrder` in `src/activities.ts` (sleeps 10s), its proxy in `src/workflows.ts` (2s timeout) | Every attempt times out and its work is thrown away | Size `startToCloseTimeout` to the slowest legitimate run, and heartbeat long work |
| US-004 | Unlimited retries with no backoff | `shipOrder` proxy `retry` in `src/workflows.ts` | Hammers the downstream service about 10 times a second, forever | Cap `maximumAttempts`, use a `backoffCoefficient` of 2 or more, set a sane `maximumInterval` |
| US-005 | Schedule-to-close shorter than start-to-close | `reserveInventory` proxy in `src/workflows.ts` (5s vs 30s) | No retry can fit; the server silently clamps start-to-close, hiding the mistake | Make `scheduleToCloseTimeout` cover several attempts plus backoff |
| US-006 | Tight heartbeat timeout on a non-heartbeating activity | `generateInvoicePdf` in `src/activities.ts`, its proxy in `src/workflows.ts` | 30s activity, 3s heartbeat timeout, zero heartbeats: every attempt fails | Call `Context.current().heartbeat()` well inside the heartbeat window |
| US-007 | Non-determinism via mutable module-level state | `workflowTasksSeen`, `interceptors` and the `notifyWarehouse` branch in `OrderWorkflow`, `src/workflows.ts` | Queries bump the counter but never reach history, so a queried run replays down a different branch and fails with TMPRL1100 | Decide only from workflow inputs and history (activity results, signals, updates); keep query handlers read-only |
| US-008 | Network and environment access in workflow code | `pickCarrier` in `src/workflows.ts` | `fetch` and `process` do not exist in the sandbox; the ReferenceError fails the workflow task forever. Even if they existed, results would differ on replay | Move I/O and env reads into an activity and pass the result in |
| US-009 | Huge payload between activities | `loadProductCatalog` and `applyCatalogPricing` in `src/activities.ts`, called from `OrderWorkflow` | 1.88 MiB blob sits just under the 2 MiB payload limit and is stored twice in history | Store the blob externally (S3, a database) and pass a reference |
| US-010 | Unbounded history, no continue-as-new | `pollOrderStatus` in `src/workflows.ts` | About 11 events every 2 seconds until the server terminates the run at the history limit | Bound the loop, `continueAsNew` periodically, stop on a terminal status |
| US-011 | Massive unthrottled child fan-out | `notifyCustomers` in `src/workflows.ts` | 10,000 children in one `Promise.all` exceed the 2,000 pending-child limit; the workflow task fails with `PendingChildWorkflowsLimitExceeded` and no child starts | Start children in small batches, or fan out through intermediate children that `continueAsNew` |
| US-012 | Validation errors are retryable | `validateOrder` in `src/activities.ts`, its proxy in `src/workflows.ts` | Plain `Error` on permanently bad input, empty `nonRetryableErrorTypes`, unlimited attempts: retries forever | Throw `ApplicationFailure.nonRetryable` for invalid input, or list the type in `nonRetryableErrorTypes` |
| US-013 | Swallowed error | empty `catch` around `reserveInventory` in `OrderWorkflow` | The order proceeds as if stock was reserved, with no log or trace | Let it fail, or handle it explicitly and record what happened |
| US-014 | Non-idempotent payment with aggressive retries | `chargeCard` and `ledger` in `src/activities.ts`, `chargeCard` proxy in `src/workflows.ts` | Slow gateway plus a 1s timeout and flat 100ms retries: one order is charged three times | Send an idempotency key (workflow ID plus activity ID) and give payment calls generous timeouts |
| US-015 | No workflow-level timeout | `client.workflow.start` in `src/starter.ts` | A stuck run stays open forever; retention only applies after a run closes | Set `workflowExecutionTimeout` to the business deadline |
| US-016 | Signal handler never registered | `cancelOrderSignal` in `src/workflows.ts`, sent by `src/cancel-order.ts` | The signal is accepted and recorded, then ignored; the order keeps going | Call `setHandler(cancelOrderSignal, ...)` at the top of the workflow |
| US-017 | Saga compensations that also fail | `compensations` loop in `OrderWorkflow`, `refundCard` and `releaseInventory` in `src/activities.ts` and their proxy | `releaseInventory` throws first, `refundCard` never runs, and the compensation error replaces the original failure | Guard each compensation, keep going, retry them durably, rethrow the original error |
| US-018 | Unversioned change that breaks replay | `ORDER_WORKFLOW_VERSION` and `runFraudCheck` in `OrderWorkflow`, `src/workflows.ts` | Open v1 runs replayed by the v2 worker fail with TMPRL1100 and stay stuck | `if (patched('fraud-check')) await runFraudCheck(order);`, or Worker Versioning |

## Notes

- `maximumAttempts: 0` is how the server spells unlimited retries, but the TypeScript SDK rejects `0` and expects `Number.POSITIVE_INFINITY`, which it sends to the server as `0`. The code uses `Infinity` for that reason.
- `protobufjs` is a direct dependency on purpose. Without it npm hoists grpc's protobufjs 7 and gives each `@temporalio` package a private copy of protobufjs 8, which breaks `historyToJSON` in `capture-history` with `type must be a Type`.
- Any replay of these histories must register `src/workflows.ts` as a workflow interceptor module, as the worker and replay script do. Without the interceptor the module-level counter never moves.
