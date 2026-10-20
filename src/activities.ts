import { ApplicationFailure, Context } from '@temporalio/activity';

export interface Order {
  orderId: string;
  customerId: string;
  items: { sku: string; quantity: number }[];
  amountCents: number;
}

export async function validateOrder(order: Order): Promise<void> {
  console.log(`validateOrder ${order.orderId}`);
  // BAD IDEA: permanently invalid input reported with plain Errors, which Temporal treats as retryable. Retrying will not
  // make an empty cart or a negative total valid; use ApplicationFailure.nonRetryable for validation failures.
  if (order.items.length === 0) {
    throw new Error(`Order ${order.orderId} has no items`);
  }
  if (order.items.some((item) => item.quantity <= 0)) {
    throw new Error(`Order ${order.orderId} has a line item with quantity <= 0`);
  }
  if (order.amountCents <= 0) {
    throw new Error(`Order ${order.orderId} has a non-positive total`);
  }
}

export interface LedgerEntry {
  chargeId: string;
  orderId: string;
  amountCents: number;
  attempt: number;
}

// Stands in for the payment provider's ledger. Lives as long as the worker process.
export const ledger: LedgerEntry[] = [];

// The fake gateway is slower than chargeCard's 1s startToCloseTimeout on the first two attempts.
const SLOW_GATEWAY_ATTEMPTS = 2;
const SLOW_GATEWAY_MS = 1_500;

export async function chargeCard(order: Order): Promise<string> {
  const { attempt } = Context.current().info;
  // BAD IDEA: no idempotency key. Every execution books a brand new charge (fresh chargeId, nothing keyed on the order
  // or the workflow/activity ID), so each timed-out attempt that already reached the gateway leaves a real charge
  // behind and the retry charges again. With the slow gateway below and a 1s timeout, one order is charged three times.
  // Send an idempotency key derived from workflowId + activityId so the provider dedupes retries.
  const chargeId = `charge-${order.orderId}-${ledger.length + 1}`;
  ledger.push({ chargeId, orderId: order.orderId, amountCents: order.amountCents, attempt });
  const chargesForOrder = ledger.filter((entry) => entry.orderId === order.orderId).length;
  console.log(`chargeCard ${order.orderId} attempt ${attempt}: booked ${chargeId}, ${chargesForOrder} charge(s) for this order`);

  if (attempt <= SLOW_GATEWAY_ATTEMPTS) {
    // The charge is already booked; the response just arrives after the workflow has stopped waiting.
    await new Promise((resolve) => setTimeout(resolve, SLOW_GATEWAY_MS));
  }
  return chargeId;
}

export async function reserveInventory(order: Order): Promise<void> {
  console.log(`reserveInventory ${order.orderId}`);
}

export async function shipOrder(order: Order): Promise<string> {
  console.log(`shipOrder ${order.orderId}`);
  // BAD IDEA: this takes ~10s but the workflow gives it a 2s startToCloseTimeout, so every attempt times out and the work is thrown away.
  await new Promise((resolve) => setTimeout(resolve, 10_000));
  return `tracking-${order.orderId}`;
}

export async function generateInvoicePdf(order: Order): Promise<string> {
  console.log(`generateInvoicePdf ${order.orderId}`);
  // BAD IDEA: ~30s of work with a 3s heartbeatTimeout and not a single heartbeat() call, so the server declares the worker dead after 3s on every attempt. Long activities must call Context.current().heartbeat() well inside the heartbeat window.
  await new Promise((resolve) => setTimeout(resolve, 30_000));
  return `invoice-${order.orderId}.pdf`;
}

export async function notifyWarehouse(order: Order): Promise<void> {
  console.log(`notifyWarehouse ${order.orderId}`);
}

// About 1.41 MiB of raw bytes, which base64-encodes to about 1.88 MiB, just under the 2 MiB payload limit.
const CATALOG_RAW_BYTES = 1_480_000;

export async function loadProductCatalog(order: Order): Promise<string> {
  console.log(`loadProductCatalog ${order.orderId}`);
  // BAD IDEA: returning a ~1.9 MiB base64 blob as an activity result. Temporal warns at 512 KiB and rejects payloads at
  // 2 MiB, so this is one catalog update away from a BlobSizeLimitError. Every byte is also persisted in workflow history
  // and re-downloaded on every replay. Store the blob in S3 or a database and pass a reference instead.
  const fakeCatalog = Buffer.alloc(CATALOG_RAW_BYTES);
  for (let i = 0; i < fakeCatalog.length; i++) {
    fakeCatalog[i] = (i * 31 + 7) % 256;
  }
  return fakeCatalog.toString('base64');
}

export const DISCONTINUED_SKU = 'DISCONTINUED-ITEM';

export async function applyCatalogPricing(order: Order, catalogBase64: string): Promise<number> {
  console.log(`applyCatalogPricing ${order.orderId} with a ${catalogBase64.length} byte catalog`);
  if (order.items.some((item) => item.sku === DISCONTINUED_SKU)) {
    throw ApplicationFailure.nonRetryable(`${DISCONTINUED_SKU} is no longer in the catalog`, 'DiscontinuedItem');
  }
  return order.amountCents;
}

export async function refundCard(order: Order): Promise<void> {
  console.log(`refundCard ${order.orderId}`);
  // BAD IDEA: a compensation that fails, run with a single attempt and no handling. The charge stays on the card.
  throw new Error(`Payment provider rejected refund for ${order.orderId}: charge already settled`);
}

export async function releaseInventory(order: Order): Promise<void> {
  console.log(`releaseInventory ${order.orderId}`);
  // BAD IDEA: a compensation that fails, run with a single attempt and no handling. The stock stays reserved.
  throw new Error(`Inventory service unavailable, could not release stock for ${order.orderId}`);
}

export async function checkOrderStatus(orderId: string, trackingNumber: string): Promise<string> {
  console.log(`checkOrderStatus ${orderId} ${trackingNumber}`);
  return 'in-transit';
}

export async function sendCustomerEmail(customerId: string, orderId: string): Promise<void> {
  console.log(`sendCustomerEmail ${customerId} about ${orderId}`);
}

export async function runFraudCheck(order: Order): Promise<void> {
  console.log(`runFraudCheck ${order.orderId}`);
}
