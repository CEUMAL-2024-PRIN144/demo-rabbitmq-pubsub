const express = require('express');
const amqp = require('amqplib');

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = 'orders';
const QUEUE = 'payment-service.orders';
const DLX = 'orders.dlx';
const DLQ = 'payment-service.orders.dlq';
const DLQ_ROUTING_KEY = 'payment-service';
// Orders with this item always fail, to demonstrate dead-lettering.
const FAIL_TEST_ITEM = 'FAIL_TEST';

// In-memory record of processed payments, exposed over HTTP for the demo.
const payments = [];

async function connectRabbitMQ(retries = 10, delayMs = 3000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      connection.on('close', () => {
        console.error('[payment-service] RabbitMQ connection closed, exiting');
        process.exit(1);
      });
      connection.on('error', (err) => {
        console.error(`[payment-service] RabbitMQ error: ${err.message}`);
        // 406 here means the main queue was created by an older version without
        // dead-letter arguments, and RabbitMQ won't change a queue's arguments.
        if (err.code === 406) {
          console.error(
            `[payment-service] Delete queue "${QUEUE}" (e.g. in the management UI) and restart this service.`
          );
        }
      });
      console.log('[payment-service] Connected to RabbitMQ');
      return connection.createChannel();
    } catch (err) {
      console.log(`[payment-service] RabbitMQ not ready (attempt ${attempt}/${retries}): ${err.message}`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw new Error('Could not connect to RabbitMQ');
}

let channel;

async function subscribe() {
  channel = await connectRabbitMQ();

  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });

  // Dead letter setup: messages rejected from the main queue are re-routed by
  // RabbitMQ to the DLX, which delivers them to the DLQ for later inspection.
  await channel.assertExchange(DLX, 'direct', { durable: true });
  await channel.assertQueue(DLQ, { durable: true });
  await channel.bindQueue(DLQ, DLX, DLQ_ROUTING_KEY);

  // Each subscriber owns a durable queue bound to the fanout exchange,
  // so it receives every order even if it was offline when it was published.
  await channel.assertQueue(QUEUE, {
    durable: true,
    deadLetterExchange: DLX,
    deadLetterRoutingKey: DLQ_ROUTING_KEY,
  });
  await channel.bindQueue(QUEUE, EXCHANGE, '');
  await channel.prefetch(1);

  console.log(`[payment-service] Waiting for orders on queue "${QUEUE}"`);

  channel.consume(QUEUE, (msg) => {
    if (!msg) return;
    try {
      const order = JSON.parse(msg.content.toString());
      if (order.item === FAIL_TEST_ITEM) {
        throw new Error(`Payment failed for order ${order.id} (item "${FAIL_TEST_ITEM}")`);
      }
      const payment = {
        orderId: order.id,
        status: 'PAID',
        processedAt: new Date().toISOString(),
      };
      payments.push(payment);
      console.log(`[payment-service] Processed payment for order ${order.id} (${order.quantity} x ${order.item})`);
      channel.ack(msg);
    } catch (err) {
      // Reject without requeueing so RabbitMQ dead-letters the message to the DLQ.
      console.error(`[payment-service] ${err.message} -> sent to DLQ "${DLQ}"`);
      channel.nack(msg, false, false);
    }
  });
}

const app = express();
app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.get('/payments', (req, res) => res.json(payments));

// Report how many failed orders are waiting in the DLQ (without consuming them).
app.get('/payments/dlq', async (req, res) => {
  try {
    const { messageCount } = await channel.checkQueue(DLQ);
    res.json({ queue: DLQ, messageCount });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

subscribe()
  .then(() => app.listen(PORT, () => console.log(`[payment-service] Listening on port ${PORT}`)))
  .catch((err) => {
    console.error(`[payment-service] ${err.message}`);
    process.exit(1);
  });
