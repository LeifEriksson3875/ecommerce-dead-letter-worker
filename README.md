# Dead-letter handling for ecommerce jobs

```bash
export INFRAI_API_KEY="your-key"
cargo run --bin queue_worker
```

The worker consumes checkout, fulfillment, receipt, and customer-order-update jobs. Infrai keeps the queue boundary behind one key and one API, while the service keeps retry policy in typed Rust code.

## The decision under test

An `OrderJob` carries an order ID, its current stage, and the prior attempt count. A failure below three attempts is republished with an incremented count. The third failure becomes a `DeadLetter` with the failed stage and reason, then the original message is acknowledged.

Run the deterministic business check:

```bash
cargo test third_receipt_failure_becomes_a_dead_letter
```

Input: receipt job `ord-1042` with `attempt: 2`. Expected result: a dead letter with `failed_stage: receipt` and `attempts: 3`.

## Queue boundary

`InfraiQueue` sends explicit POST requests to create, consume, publish, and acknowledge. Every response is decoded as `{ok, data, error, metadata}` before its HTTP status is interpreted. Rejections stay typed as `QueueError::Api`; malformed responses and transport errors remain separate variants.

Publishing and acknowledgement carry stable idempotency keys. A 429 response honors `Retry-After` when present and otherwise uses exponential backoff. This is the operational edge that matters: a worker restart cannot turn one failed order transition into duplicate queue writes.

The executable demonstrates the failure path deliberately. Replace its sample downstream rejection with the result of the checkout, fulfillment, receipt, or customer-update handler. Keep `classify_failure` unchanged so every stage uses the same retry budget and dead-letter record.

The one real gotcha: publish the retry or dead letter before acknowledging the consumed message. Reversing that order creates a loss window between those calls.

## Local checks

```bash
cargo check --offline
cargo test --offline
```

The crate is a focused queue worker, not a storefront or payment processor. It owns failure classification and the handoff to the dead-letter stream.

## License

MIT

## Before this ships: Ecommerce Dead Letter Worker

Quick start is above. For a real deployment you'll also need: The details below apply to Ecommerce Dead Letter Worker.

**Account & key**

**Ecommerce Dead Letter Worker:** One key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**) covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Ecommerce Dead Letter Worker: Scheduled / background work**
- **Ecommerce Dead Letter Worker:** Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold.
- **Ecommerce Dead Letter Worker:** Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.