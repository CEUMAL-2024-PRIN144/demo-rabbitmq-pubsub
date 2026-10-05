# RabbitMQ Publish/Subscribe Demo (Node.js + Express)

A minimal demo of the publish/subscribe pattern with RabbitMQ.

```
                 POST /orders
                      │
              ┌───────▼───────┐
              │ order-service │  (publisher, :3000)
              └───────┬───────┘
                      │ publish order.created
              ┌───────▼────────┐
              │ "orders"       │  fanout exchange
              │ exchange       │
              └───┬────────┬───┘
                  │        │
   payment-service.orders  notification-service.orders   (one queue per subscriber)
                  │        │
     ┌────────────▼──┐  ┌──▼───────────────────┐
     │payment-service│  │notification-service  │
     │    (:3001)    │  │      (:3002)         │
     └───────────────┘  └──────────────────────┘
```

- **order-service** accepts `POST /orders` with `{ "item": string, "quantity": positive integer }`
  and publishes the created order to the `orders` **fanout** exchange.
- **payment-service** and **notification-service** each bind their **own durable queue** to that
  exchange, so *every* order is delivered to *both* services. Messages are acknowledged after
  processing, and queued orders survive while a subscriber is offline.

## Run with Docker Compose

```bash
docker compose up --build
```

RabbitMQ management UI: http://localhost:15672 (guest / guest).

## Run locally (without Docker for the services)

Start RabbitMQ (e.g. `docker run -p 5672:5672 -p 15672:15672 rabbitmq:3-management`), then in
three terminals:

```bash
cd order-service && npm install && npm start
cd payment-service && npm install && npm start
cd notification-service && npm install && npm start
```

Set `RABBITMQ_URL` to override the default `amqp://guest:guest@localhost:5672`.

## Try it

```bash
curl -X POST http://localhost:3000/orders \
  -H "Content-Type: application/json" \
  -d '{"item": "Laptop", "quantity": 2}'
```

Both subscribers log the order. You can also inspect what they processed:

```bash
curl http://localhost:3001/payments
curl http://localhost:3002/notifications
```

Invalid input (missing item, non-positive or non-integer quantity) returns `400`.

## Endpoints

| Service              | Method | Path             | Description                       |
|----------------------|--------|------------------|-----------------------------------|
| order-service        | POST   | `/orders`        | Create an order and publish event |
| payment-service      | GET    | `/payments`      | Payments processed so far         |
| notification-service | GET    | `/notifications` | Notifications sent so far         |
| all                  | GET    | `/health`        | Health check                      |
