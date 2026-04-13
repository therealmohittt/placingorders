# placingorders
multi tier code using node.js, express.js and mySQL with the required schema, endpoints, concurrency safe ordering with no oversell using transaction, locking and conditional update plus the reporting endpoint and the Top 3 Sold Products with SQL Query.
// MySQL pool
// Schema SQL (run once in MySQL client)
// Helpers
// Sort by product_id
// Use explicit statements
// Create REJECTED order (no stock changes) Runs its own transaction
// Get current price for known products, else 0
// Routes
// GET /schema
// POST /seed // Creates 20 products (upsert by unique name)
// GET /products // Returns id, name, stock, price