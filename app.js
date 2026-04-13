require("dotenv").config();
const express = require("express");
const mysql = require("mysql2/promise");

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);

// MySQL pool
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: Number(process.env.DB_PORT || 3306),
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

// Schema SQL (run once in MySQL client)
const SCHEMA_SQL = `
CREATE DATABASE IF NOT EXISTS tiny_orders;
USE tiny_orders;

CREATE TABLE IF NOT EXISTS products (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL UNIQUE,
  stock INT NOT NULL,
  price DECIMAL(12,2) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT chk_products_stock CHECK (stock >= 0)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS orders (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  customer_email VARCHAR(255) NOT NULL,
  status ENUM('PLACED','REJECTED') NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS order_items (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_id BIGINT NOT NULL,
  product_id BIGINT NOT NULL,
  qty INT NOT NULL,
  unit_price DECIMAL(12,2) NOT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT fk_order_items_order
    FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,

  CONSTRAINT fk_order_items_product
    FOREIGN KEY (product_id) REFERENCES products(id),

  CONSTRAINT chk_order_items_qty CHECK (qty > 0)
) ENGINE=InnoDB;

CREATE INDEX idx_order_items_order_id ON order_items(order_id);
CREATE INDEX idx_order_items_product_id ON order_items(product_id);
`;

// Helpers
function isISODate(s) {
  return typeof s === "string" && /^\d{4}-\d{2}-\d{2}$/.test(s);
}

function isValidEmail(email) {
  return typeof email === "string" && email.includes("@") && email.length <= 255;
}

function normalizeItems(items) {
  if (!Array.isArray(items) || items.length === 0) return null;

  const tmp = [];
  for (const it of items) {
    const product_id = Number(it.product_id);
    const qty = Number(it.qty);
    if (!Number.isInteger(product_id) || product_id <= 0) return null;
    if (!Number.isInteger(qty) || qty <= 0) return null;
    tmp.push({ product_id, qty });
  }

  // Merge duplicate product_ids
  const map = new Map();
  for (const it of tmp) map.set(it.product_id, (map.get(it.product_id) || 0) + it.qty);

  // Sort by product_id
  return Array.from(map.entries())
    .map(([product_id, qty]) => ({ product_id, qty }))
    .sort((a, b) => a.product_id - b.product_id);
}

async function startTx(conn) {
  // Use explicit statements
  await conn.query("START TRANSACTION");
}
async function commitTx(conn) {
  await conn.query("COMMIT");
}
async function rollbackTx(conn) {
  await conn.query("ROLLBACK");
}

// Create REJECTED order (no stock changes) Runs its own transaction
async function createRejectedOrder(conn, customer_email, normItems) {
  let localConn = conn;
  let shouldRelease = false;

  if (!localConn) {
    localConn = await pool.getConnection();
    shouldRelease = true;
  }

  await startTx(localConn);
  try {
    const [orderRes] = await localConn.query(
      `INSERT INTO orders (customer_email, status) VALUES (?, 'REJECTED')`,
      [customer_email]
    );
    const orderId = orderRes.insertId;

    // Get current price for known products, else 0
    const ids = normItems.map(x => x.product_id);
    const placeholders = ids.map(() => "?").join(",");
    const [products] = await localConn.query(
      `SELECT id, price FROM products WHERE id IN (${placeholders})`,
      ids
    );
    const pMap = new Map(products.map(p => [Number(p.id), p]));

    for (const it of normItems) {
      const unitPrice = pMap.get(it.product_id)?.price ?? 0;
      await localConn.query(
        `INSERT INTO order_items (order_id, product_id, qty, unit_price)
         VALUES (?, ?, ?, ?)`,
        [orderId, it.product_id, it.qty, unitPrice]
      );
    }

    await commitTx(localConn);
    return orderId;
  } catch (e) {
    await rollbackTx(localConn);
    throw e;
  } finally {
    if (shouldRelease) localConn.release();
  }
}

// Routes

app.get("/health", (_req, res) => res.json({ ok: true }));

// GET /schema
app.get("/schema", (_req, res) => {
  res.type("text/plain").send(SCHEMA_SQL);
});

// POST /seed
// Creates 20 products (upsert by unique name)
app.post("/seed", async (_req, res, next) => {
  let conn;
  try {
    conn = await pool.getConnection();
    await startTx(conn);

    const base = [
      { name: "Keyboard", stock: 5, price: 49.99 },
      { name: "Mouse", stock: 10, price: 19.99 },
      { name: "Monitor", stock: 2, price: 199.99 }
    ];

    for (let i = base.length; i < 20; i++) {
      base.push({
        name: `Product-${i + 1}`,
        stock: 3 + (i % 8),
        price: Number((10 + i * 2.15).toFixed(2))
      });
    }

    for (const p of base) {
      await conn.query(
        `
        INSERT INTO products (name, stock, price)
        VALUES (?, ?, ?)
        ON DUPLICATE KEY UPDATE stock = VALUES(stock), price = VALUES(price)
        `,
        [p.name, p.stock, p.price]
      );
    }

    await commitTx(conn);
    res.json({ ok: true, inserted_or_updated: base.length });
  } catch (e) {
    if (conn) await rollbackTx(conn);
    next(e);
  } finally {
    if (conn) conn.release();
  }
});

// GET /products
// Returns id, name, stock, price
app.get("/products", async (_req, res, next) => {
  try {
    const [rows] = await pool.query(
      `SELECT id, name, stock, price FROM products ORDER BY id ASC`
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

app.post("/orders", async (req, res, next) => {
  const { customer_email, items } = req.body || {};

  if (!isValidEmail(customer_email)) {
    return res.status(400).json({ error: "Invalid customer_email" });
  }

  const normItems = normalizeItems(items);
  if (!normItems) {
    return res.status(400).json({ error: "Invalid items (must be non-empty with qty > 0)" });
  }

  let conn;
  try {
    conn = await pool.getConnection();
    await startTx(conn);

    const ids = normItems.map(x => x.product_id);
    const placeholders = ids.map(() => "?").join(",");

    // Lock rows for concurrency
    const [products] = await conn.query(
      `
      SELECT id, stock, price, name
      FROM products
      WHERE id IN (${placeholders})
      FOR UPDATE
      `,
      ids
    );

    if (products.length !== ids.length) {
      // Unknown product leads to rejection
      await rollbackTx(conn);
      const missing = ids.filter(id => !products.find(p => Number(p.id) === Number(id)));
      const rejId = await createRejectedOrder(null, customer_email, normItems);
      return res.status(409).json({
        order_id: rejId,
        status: "REJECTED",
        reason: `Unknown product_id(s): ${missing.join(",")}`
      });
    }

    const pMap = new Map(products.map(p => [Number(p.id), p]));

    // Validate stock while rows are locked
    const insufficient = [];
    for (const it of normItems) {
      const p = pMap.get(it.product_id);
      if (!p || Number(p.stock) < it.qty) {
        insufficient.push({
          product_id: it.product_id,
          requested: it.qty,
          available: p ? Number(p.stock) : 0
        });
      }
    }

    if (insufficient.length > 0) {
      await rollbackTx(conn);
      const rejId = await createRejectedOrder(null, customer_email, normItems);
      return res.status(409).json({
        order_id: rejId,
        status: "REJECTED",
        reason: "Insufficient stock",
        details: insufficient
      });
    }

    // Insert PLACED order
    const [orderRes] = await conn.query(
      `INSERT INTO orders (customer_email, status) VALUES (?, 'PLACED')`,
      [customer_email]
    );
    const orderId = orderRes.insertId;

    // Insert order items with unit_price from product.price
    for (const it of normItems) {
      const p = pMap.get(it.product_id);
      await conn.query(
        `INSERT INTO order_items (order_id, product_id, qty, unit_price)
         VALUES (?, ?, ?, ?)`,
        [orderId, it.product_id, it.qty, p.price]
      );
    }

    // Atomic decrement stock per item
    for (const it of normItems) {
      const [upd] = await conn.query(
        `UPDATE products
         SET stock = stock - ?
         WHERE id = ? AND stock >= ?`,
        [it.qty, it.product_id, it.qty]
      );

      if (upd.affectedRows !== 1) {
        // Someone else consumed stock hence rollback to prevent oversell
        throw Object.assign(new Error("INSUFFICIENT_STOCK_DURING_UPDATE"), {
          code: "INSUFFICIENT_STOCK_DURING_UPDATE"
        });
      }
    }

    await commitTx(conn);
    return res.status(201).json({ order_id: orderId, status: "PLACED" });
  } catch (e) {
    if (conn) {
      try { await rollbackTx(conn); } catch { /* ignore */ }
    }

    if (e && e.code === "INSUFFICIENT_STOCK_DURING_UPDATE") {
      try {
        const rejId = await createRejectedOrder(null, customer_email, normItems);
        return res.status(409).json({
          order_id: rejId,
          status: "REJECTED",
          reason: "Insufficient stock"
        });
      } catch (inner) {
        return next(inner);
      }
    }

    next(e);
  } finally {
    if (conn) conn.release();
  }
});

  // GET /reports/orders?from=YYYY-MM-DD&to=YYYY-MM-DD
  // Returns:
  // - order_id, customer_email, status
  // - total_amount = SUM(qty * unit_price)
  // - item_count = SUM(qty)
  // - created_at
app.get("/reports/orders", async (req, res, next) => {
  const { from, to } = req.query;

  if (!isISODate(from) || !isISODate(to)) {
    return res.status(400).json({ error: "from/to must be YYYY-MM-DD" });
  }

  try {
    const [rows] = await pool.query(
      `
      SELECT
        o.id AS order_id,
        o.customer_email,
        o.status,
        COALESCE(SUM(oi.qty * oi.unit_price), 0) AS total_amount,
        COALESCE(SUM(oi.qty), 0) AS item_count,
        o.created_at
      FROM orders o
      LEFT JOIN order_items oi ON oi.order_id = o.id
      WHERE o.created_at >= STR_TO_DATE(?, '%Y-%m-%d')
        AND o.created_at <  DATE_ADD(STR_TO_DATE(?, '%Y-%m-%d'), INTERVAL 1 DAY)
      GROUP BY o.id
      ORDER BY o.created_at DESC
      `,
      [from, to]
    );

    res.json(rows);
  } catch (e) {
    next(e);
  }
});


 // GET /reports/top-products
 // Returns Top 3 products by sold quantity in last 7 days (PLACED only)
 //- product_id, name, sold_qty
app.get("/reports/top-products", async (_req, res, next) => {
  try {
    const [rows] = await pool.query(
      `
      SELECT
        p.id AS product_id,
        p.name,
        SUM(oi.qty) AS sold_qty
      FROM order_items oi
      JOIN orders o   ON o.id = oi.order_id
      JOIN products p ON p.id = oi.product_id
      WHERE o.status = 'PLACED'
        AND o.created_at >= NOW() - INTERVAL 7 DAY
      GROUP BY p.id, p.name
      ORDER BY sold_qty DESC
      LIMIT 3
      `
    );
    res.json(rows);
  } catch (e) {
    next(e);
  }
});

// Error handler
app.use((err, _req, res, _next) => {
  console.error(err);
  res.status(500).json({ error: "Internal Server Error" });
});

app.listen(PORT, () => {
  console.log(`Server running on :${PORT}`);
  console.log(`Tip: GET /schema to view schema SQL`);
});