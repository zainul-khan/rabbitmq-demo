# RabbitMQ Demo

A small Node.js project that demonstrates core RabbitMQ patterns: direct and fanout exchanges, multiple consumers, and a retry + dead-letter queue setup.

## Architecture

```
                 ┌─ order.created ──► orders.payment ──► payment-worker (retry ×3 → DLQ)
POST /orders ────┤                ┌─► orders.email   ──► email-worker
                 └─ order.cancelled┤
POST /orders/:id/cancel ───────────┴─► orders.cancellation ──► cancellation-worker

POST /announcements ──► announcements.exchange (fanout) ─┬─► announcements.email ──► announcement-email-worker
                                                          └─► announcements.slack ──► announcement-slack-worker
```

| Service | Role |
| --- | --- |
| `api-server` | Express API (port 3000) that publishes events |
| `payment-worker` | Consumes `order.created`; retries after a 5s delay, up to 3 times, then routes to a dead-letter queue (payment failure is simulated) |
| `email-worker` | Consumes both `order.created` and `order.cancelled` |
| `cancellation-worker` | Consumes `order.cancelled` |
| `announcement-email-worker` / `announcement-slack-worker` | Each get a copy of every announcement (fanout) |

Exchanges: `orders.exchange` (direct), `orders.fanout` (fanout), `announcements.exchange` (fanout).

## Getting started

Requires Node.js and Docker.

```bash
docker compose up -d        # RabbitMQ; management UI at http://localhost:15672 (admin / admin)
```

In a separate terminal for each service:

```bash
cd api-server && npm install && npm start
cd payment-worker && npm install && npm start
# ...and likewise for the other workers
```

## Try it

```bash
curl -X POST localhost:3000/orders -H "Content-Type: application/json" \
  -d '{"userId":"u1","productId":"p1","amount":49.99}'

curl -X POST localhost:3000/orders/ORD-123/cancel

curl -X POST localhost:3000/announcements -H "Content-Type: application/json" \
  -d '{"title":"Hello","message":"Fanout to all subscribers"}'
```

Watch the worker terminals and the management UI to see messages flow through the queues.

## Notes

See [understanding.md](understanding.md) for a longer walkthrough of the RabbitMQ concepts used here. Credentials are hardcoded for local demo use only.
