const express = require('express');
const amqp = require('amqplib');

const PORT = process.env.PORT || 3002;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = 'orders';
const QUEUE = 'notification-service.orders';

// In-memory record of sent notifications, exposed over HTTP for the demo.
const notifications = [];

async function connectRabbitMQ(retries = 10, delayMs = 3000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      connection.on('close', () => {
        console.error('[notification-service] RabbitMQ connection closed, exiting');
        process.exit(1);
      });
      console.log('[notification-service] Connected to RabbitMQ');
      return connection.createChannel();
    } catch (err) {
      console.log(`[notification-service] RabbitMQ not ready (attempt ${attempt}/${retries}): ${err.message}`);
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

  console.log(`[notification-service] Waiting for orders on queue "${QUEUE}"`);

  channel.consume(QUEUE, (msg) => {
    if (!msg) return;
    try {
      const order = JSON.parse(msg.content.toString());
      const notification = {
        orderId: order.id,
        message: `Your order of ${order.quantity} x ${order.item} has been received.`,
        sentAt: new Date().toISOString(),
      };
      notifications.push(notification);
      console.log(`[notification-service] Sent notification: ${notification.message} (order ${order.id})`);
      channel.ack(msg);
    } catch (err) {
      console.error(`[notification-service] Failed to process message: ${err.message}`);
      channel.nack(msg, false, false);
    }
  });
}

const app = express();
app.get('/health', (req, res) => res.json({ status: 'ok' }));
app.get('/notifications', (req, res) => res.json(notifications));

subscribe()
  .then(() => app.listen(PORT, () => console.log(`[notification-service] Listening on port ${PORT}`)))
  .catch((err) => {
    console.error(`[notification-service] ${err.message}`);
    process.exit(1);
  });
