// server.js
const express = require("express");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const winston = require("winston");
const bodyParser = require("body-parser");
const cors = require("cors");

// Load environment variables
dotenv.config();

// The "default" table name (can be overridden per‐request)
const DEFAULT_TABLE = process.env.RRC_CLIENTS_TABLE || "rrc_clients";

// Simple validator to allow only letters, numbers and underscores
function isValidIdentifier(name) {
  return /^[a-zA-Z0-9_]+$/.test(name);
}

// Configure logger
const logger = winston.createLogger({
  level: "info",
  format: winston.format.combine(
    winston.format.timestamp(),
    winston.format.printf(
      ({ timestamp, level, message }) => `${timestamp} ${level}: ${message}`
    )
  ),
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({
      filename: `./logs/server-${new Date().toISOString().slice(0, 10)}.log`,
    }),
  ],
});

// Initialize Express
const app = express();
app.use(cors());
app.use(bodyParser.json({ limit: "50mb" }));

// PostgreSQL pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  max: 20, // Increase connection pool size
  idleTimeoutMillis: 30000, // 30 seconds
  connectionTimeoutMillis: 2000, // 2 seconds
});

// Test DB connection on startup
pool.query("SELECT NOW()", (err) => {
  if (err) logger.error(`DB connection failed: ${err.message}`);
  else logger.info("DB connection successful");
});

// Sync endpoint: clear existing data and add all new data
app.post("/api/sync", async (req, res) => {
  const { data, apiKey, tableName } = req.body;

  // 1) Authenticate
  if (apiKey !== process.env.API_KEY) {
    logger.warn(`Bad API key from ${req.ip}`);
    return res.status(401).json({ success: false, message: "Invalid API key" });
  }

  // 2) Validate payload
  if (!Array.isArray(data)) {
    logger.warn("Sync request missing data array");
    return res
      .status(400)
      .json({ success: false, message: "Invalid data format" });
  }

  // 3) Determine table to write into
  const targetTable =
    tableName && isValidIdentifier(tableName) ? tableName : DEFAULT_TABLE;

  logger.info(
    `Sync received ${data.length} records for table "${targetTable}"`
  );

  const client = await pool.connect();
  try {
    // Start a transaction
    await client.query("BEGIN");

    // 4) Clear existing data
    await client.query(`DELETE FROM ${targetTable}`);
    logger.info(`Cleared existing data from table "${targetTable}"`);

    // 5) Insert all new data in batches
    const chunkSize = 500;
    let inserted = 0;

    for (let i = 0; i < data.length; i += chunkSize) {
      const chunk = data.slice(i, i + chunkSize);

      const valuePlaceholders = [];
      const values = [];

      chunk.forEach((row, idx) => {
        const base = idx * 4;
        valuePlaceholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`
        );
        // Sanitize input: trim whitespace, handle potential undefined
        values.push(
          (row.code || "").toString().trim(),
          (row.name || "").toString().trim(),
          (row.address || "").toString().trim(),
          (row.branch || "").toString().trim()
        );
      });

      const sql = `
        INSERT INTO ${targetTable} (code, name, address, branch)
        VALUES ${valuePlaceholders.join(", ")}
      `;
      await client.query(sql, values);
      inserted += chunk.length;
    }

    // Commit the transaction
    await client.query("COMMIT");

    logger.info(`Sync complete: inserted ${inserted} rows into ${targetTable}`);

    res.json({ success: true, insertedCount: inserted });
  } catch (err) {
    // Rollback the transaction in case of error
    await client.query("ROLLBACK");

    logger.error(`Sync failed: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  } finally {
    client.release();
  }
});

// Health check
app.get("/health", (_req, res) => res.json({ status: "ok" }));

// Error handling middleware
app.use((err, req, res, next) => {
  logger.error(`Unhandled error: ${err.message}`);
  res.status(500).json({ success: false, message: "Internal server error" });
});

// Start server
const PORT = process.env.PORT || 5015;
app.listen(PORT, () => {
  logger.info(`Server listening on port ${PORT}`);
  console.log(`Server running on port ${PORT}`);
});

// Graceful shutdown
process.on("SIGINT", async () => {
  logger.info("Shutting down gracefully");
  await pool.end();
  process.exit(0);
});
