require("dotenv").config();
const express = require("express");
const cors = require("cors");
const amqplib = require("amqplib");

const app = express();
app.use(cors());
app.use(express.json());

const PORT = parseInt(process.env.PORT || "4000", 10);
const QUEUE = process.env.QUEUE_NAME || "order_queue";

const orders = []; // in-memory store (fine for midterm)

function getAmqpUrl() {
  const host = process.env.RABBITMQ_HOST || "localhost";
  const port = process.env.RABBITMQ_PORT || "5672";
  const user = process.env.RABBITMQ_USER || "guest";
  const pass = process.env.RABBITMQ_PASS || "guest";
  return `amqp://${user}:${pass}@${host}:${port}`;
}

let consumerStatus = { connected: false, lastMessageAt: null, error: null };

async function startConsumer() {
  try {
    const url = getAmqpUrl();
    const conn = await amqplib.connect(url);
    const ch = await conn.createChannel();

    await ch.assertQueue(QUEUE, { durable: false });
    // good practice: one message at a time
    await ch.prefetch(1);

    consumerStatus.connected = true;
    consumerStatus.error = null;

    console.log(`Connected to RabbitMQ. Consuming from queue: ${QUEUE}`);

    ch.consume(
      QUEUE,
      (msg) => {
        if (!msg) return;
        try {
          const content = msg.content.toString();
          const order = JSON.parse(content);

          // Basic validation
          if (
            order?.product?.name &&
            typeof order.quantity === "number" &&
            typeof order.totalPrice === "number"
          ) {
            orders.push({
              ...order,
              receivedAt: new Date().toISOString(),
            });
          }

          consumerStatus.lastMessageAt = new Date().toISOString();
          ch.ack(msg);
        } catch (e) {
          // If message is bad JSON, ack it to avoid blocking the queue
          console.error("Failed to process message:", e);
          consumerStatus.error = e.message;
          consumerStatus.lastMessageAt = new Date().toISOString();
          ch.ack(msg);
        }
      },
      { noAck: false }
    );

    // handle shutdown nicely
    process.on("SIGINT", async () => {
      await ch.close();
      await conn.close();
      process.exit(0);
    });
  } catch (err) {
    consumerStatus.connected = false;
    consumerStatus.error = err.message;
    console.error("RabbitMQ consumer error:", err.message);
    // retry after 3 seconds
    setTimeout(startConsumer, 3000);
  }
}

/** API endpoints */
app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    service: "order-analytics-service",
    connectedToRabbitMQ: consumerStatus.connected,
    queue: QUEUE,
    totalOrdersStored: orders.length,
    lastMessageAt: consumerStatus.lastMessageAt,
    error: consumerStatus.error,
  });
});

app.get("/orders", (req, res) => {
  res.json(orders);
});

app.get("/analytics/summary", (req, res) => {
  const totalOrders = orders.length;
  const totalRevenue = orders.reduce((sum, o) => sum + (o.totalPrice || 0), 0);
  const averageOrderValue = totalOrders === 0 ? 0 : totalRevenue / totalOrders;

  res.json({
    totalOrders,
    totalRevenue: Number(totalRevenue.toFixed(2)),
    averageOrderValue: Number(averageOrderValue.toFixed(2)),
  });
});

app.get("/analytics/products", (req, res) => {
  const map = new Map();

  for (const o of orders) {
    const name = o.product?.name || "Unknown";
    const entry = map.get(name) || { productName: name, orderCount: 0, totalRevenue: 0 };
    entry.orderCount += 1;
    entry.totalRevenue += o.totalPrice || 0;
    map.set(name, entry);
  }

  const result = Array.from(map.values()).map((x) => ({
    ...x,
    totalRevenue: Number(x.totalRevenue.toFixed(2)),
  }));

  res.json(result);
});

app.get("/analytics/top-products", (req, res) => {
  const qtyMap = new Map();

  for (const o of orders) {
    const name = o.product?.name || "Unknown";
    const qty = o.quantity || 0;
    qtyMap.set(name, (qtyMap.get(name) || 0) + qty);
  }

  const top = Array.from(qtyMap.entries())
    .map(([productName, totalQuantity]) => ({ productName, totalQuantity }))
    .sort((a, b) => b.totalQuantity - a.totalQuantity)
    .slice(0, 3);

  res.json(top);
});

app.listen(PORT, () => {
  console.log(`order-analytics-service running on http://localhost:${PORT}`);
  startConsumer();
});