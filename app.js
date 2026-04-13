require("dotenv").config();
const express = require("express");
const mysql = require("mysql2/promise");

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 3000);

// ----- MySQL pool -----
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

// ----- Schema SQL (run once in MySQL client) -----
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

// ----- Helpers -----
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

  // Sort by product_id to reduce deadlock chance
  return Array.from(map.entries())
    .map(([product_id, qty]) => ({ product_id, qty }))
    .sort((a, b) => a.product_id - b.product_id);
}

async function startTx(conn) {
  // Use explicit statements for wide compatibility
  await conn.query("START TRANSACTION");
}
async function commitTx(conn) {
  await conn.query("COMMIT");
}
async function rollbackTx(conn) {
  await conn.query("ROLLBACK");
}
