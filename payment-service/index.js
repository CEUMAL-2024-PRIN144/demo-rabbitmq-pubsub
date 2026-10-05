const express = require('express');
const amqp = require('amqplib');

const PORT = process.env.PORT || 3001;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = 'orders';
const QUEUE = 'payment-service.orders';

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
      console.log('[payment-service] Connected to RabbitMQ');
      return connection.createChannel();
    } catch (err) {
      console.log(`[payment-service] RabbitMQ not ready (attempt ${attempt}/${retries}): ${err.message}`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw new Error('Could not connect to RabbitMQ');
}

async function subscribe() {
  const channel = await connectRabbitMQ();

  await channel.assertExchange(EXCHANGE, 'fanout', { durable: true });
  // Each subscriber owns a durable queue bound to the fanout exchange,
  // so it receives every order even if it was offline when it was published.
  await channel.assertQueue(QUEUE, { durable: true });
  await channel.bindQueue(QUEUE, EXCHANGE, '');
  await channel.prefetch(1);

  console.log(`[payment-service] Waiting for orders on queue "${QUEUE}"`);

  channel.consume(QUEUE, (msg) => {
    if (!msg) return;
    try {
      const order = JSON.parse(msg.content.toString());
      const payment = {
        orderId: order.id,
        status: 'PAID',
        processedAt: new Date().toISOString(),
      };
      payments.push(payment);
      console.log(`[payment-service] Processed payment for order ${order.id} (${order.quantity} x ${order.item})`);
      channel.ack(msg);
    } catch (err) {
      console.error(`[payment-service] Failed to process message: ${err.message}`);
      channel.nack(msg, false, false);
    }
  });
}

const app = express();
app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.get('/payments', (req, res) => res.json(payments));

subscribe()
  .then(() => app.listen(PORT, () => console.log(`[payment-service] Listening on port ${PORT}`)))
  .catch((err) => {
    console.error(`[payment-service] ${err.message}`);
    process.exit(1);
  });
