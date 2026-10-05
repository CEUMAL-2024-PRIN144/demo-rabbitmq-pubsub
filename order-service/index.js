const express = require('express');
const amqp = require('amqplib');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://guest:guest@localhost:5672';
const EXCHANGE = 'orders';

let channel;

// RabbitMQ can take a few seconds to boot, so retry until it is reachable.
async function connectRabbitMQ(retries = 10, delayMs = 3000) {
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      const connection = await amqp.connect(RABBITMQ_URL);
      const ch = await connection.createChannel();

      // A fanout exchange broadcasts every message to all bound queues,
      // so each subscriber service gets its own copy of every order.
      await ch.assertExchange(EXCHANGE, 'fanout', { durable: true });

      connection.on('close', () => {
        console.error('[order-service] RabbitMQ connection closed, exiting');
        process.exit(1);
      });

      console.log('[order-service] Connected to RabbitMQ');
      return ch;
    } catch (err) {
      console.log(`[order-service] RabbitMQ not ready (attempt ${attempt}/${retries}): ${err.message}`);
      await new Promise((resolve) => setTimeout(resolve, delayMs));
    }
  }
  throw new Error('Could not connect to RabbitMQ');
}

const app = express();
app.use(express.json());

app.get('/health', (req, res) => res.json({ status: 'ok' }));

app.post('/orders', (req, res) => {
  const { item, quantity } = req.body || {};

  if (typeof item !== 'string' || item.trim() === '') {
    return res.status(400).json({ error: '"item" is required and must be a non-empty string' });
  }
  if (!Number.isInteger(quantity) || quantity <= 0) {
    return res.status(400).json({ error: '"quantity" is required and must be a positive integer' });
  }

  const order = {
    id: crypto.randomUUID(),
    item: item.trim(),
    quantity,
    createdAt: new Date().toISOString(),
  };

  channel.publish(EXCHANGE, '', Buffer.from(JSON.stringify(order)), {
    persistent: true,
    contentType: 'application/json',
    type: 'order.created',
  });

  console.log(`[order-service] Published order.created: ${JSON.stringify(order)}`);
  res.status(201).json(order);
});

connectRabbitMQ()
  .then((ch) => {
    channel = ch;
    app.listen(PORT, () => console.log(`[order-service] Listening on port ${PORT}`));
  })
  .catch((err) => {
    console.error(`[order-service] ${err.message}`);
    process.exit(1);
  });
