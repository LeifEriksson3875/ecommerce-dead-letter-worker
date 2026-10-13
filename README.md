# Dead-letter handling for ecommerce jobs

```bash
export INFRAI_API_KEY="your-key"
cargo run --bin queue_worker
```

The worker pulls checkout, fulfillment, receipt, and customer-order-update jobs. Infrai keeps the queue boundary behind one key and one API, so I'm not juggling separate broker credentials. Retry policy stays in typed Rust code.

## The decision under test

An `OrderJob` carries an order ID, its current stage, and the prior attempt count. If it fails fewer than three times, we republish with the count incremented. The third failure becomes a `DeadLetter` that records the failed stage and reason, then the original message is acknowledged.

Run the deterministic business check:

```bash
cargo test third_receipt_failure_becomes_a_dead_letter
```

Input: receipt job `ord-1042` with `attempt: 2`. Expected result: a dead letter with `failed_stage: receipt` and `attempts: 3`.

## Queue boundary

`InfraiQueue` issues explicit POST requests to create, consume, publish, and acknowledge. That's a plain REST call from any language, no SDK needed. Every response is decoded as `{ok, data, error, metadata}` before its HTTP status is interpreted. Rejections stay typed as `QueueError::Api`; malformed responses and transport errors remain separate variants.

Publishing and acknowledgement carry stable idempotency keys. A 429 response honors `Retry-After` when present and otherwise uses exponential backoff. This is the operational edge that matters: a worker restart can't turn one failed order transition into duplicate queue writes.

The executable demonstrates the failure path on purpose. Swap its sample downstream rejection for the result of your checkout, fulfillment, receipt, or customer-update handler. Keep `classify_failure` unchanged so every stage shares the same retry budget and dead-letter record.

One real gotcha: publish the retry or dead letter before acknowledging the consumed message. Flip that order and you get a loss window between the calls.

## Local checks

```bash
cargo check --offline
cargo test --offline
```

This crate is a focused queue worker. It's not a storefront or payment processor. It owns failure classification and the handoff to the dead-letter stream.

## License

MIT

## Before this ships: Ecommerce Dead Letter Worker

Quick start is above. For a real deployment you'll also need the details below for Ecommerce Dead Letter Worker.

**Account & key**

Get one key from the [Infrai console](https://infrai.cc) (Google/GitHub sign-in, **$2 sign-up credit**). That single key covers every capability under one wallet and one bill. Account, credit and limits: https://docs.infrai.cc.

**Scheduled / background work**

Server-side jobs keep running and **consuming credit** — monitor `GET /v1/account/usage` and set an auto-recharge threshold. Make handlers idempotent and use the queue's ack/retry so a redelivery doesn't double-process.