// server.js
const express = require("express");
const { Pool } = require("pg");
const dotenv = require("dotenv");
const winston = require("winston");
const bodyParser = require("body-parser");
const cors = require("cors");

// Load environment variables
dotenv.config();

// The “default” table name (can be overridden per‐request)
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
const pool = new Pool({ connectionString: process.env.DATABASE_URL });

// Test DB connection on startup
pool.query("SELECT NOW()", (err) => {
  if (err) logger.error(`DB connection failed: ${err.message}`);
  else logger.info("DB connection successful");
});

// Sync endpoint: clear old rows, then bulk-insert new ones
app.post("/api/sync", async (req, res) => {
  const { data, apiKey, tableName, truncateFirst = false } = req.body;

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
    await client.query("BEGIN");

    // 4) clear out old data
    if (truncateFirst) {
      logger.info(`Truncating table "${targetTable}" as requested`);
      await client.query(`TRUNCATE ${targetTable}`);
    }

    // 5) insert in batches
    const chunkSize = 500;
    let inserted = 0;

    for (let i = 0; i < data.length; i += chunkSize) {
      const chunk = data.slice(i, i + chunkSize);

      // assume each row has exactly these four fields:
      // code, name, address, branch
      const valuePlaceholders = [];
      const values = [];

      chunk.forEach((row, idx) => {
        const base = idx * 4;
        valuePlaceholders.push(
          `($${base + 1}, $${base + 2}, $${base + 3}, $${base + 4})`
        );
        values.push(row.code, row.name, row.address, row.branch);
      });

      const sql = `
        INSERT INTO ${targetTable} (code, name, address, branch)
        VALUES ${valuePlaceholders.join(", ")}
      `;
      await client.query(sql, values);
      inserted += chunk.length;
    }

    await client.query("COMMIT");
    logger.info(`Sync complete: inserted ${inserted} rows into ${targetTable}`);

    res.json({ success: true, insertedCount: inserted });
  } catch (err) {
    await client.query("ROLLBACK");
    logger.error(`Sync failed: ${err.message}`);
    res.status(500).json({ success: false, message: err.message });
  } finally {
    client.release();
  }
});

// Health check
app.get("/health", (_req, res) => res.json({ status: "ok" }));

// Start server
const PORT = process.env.PORT || 5015;
app.listen(PORT, () => {
  logger.info(`Server listening on port ${PORT}`);
  console.log(`Server running on port ${PORT}`);
});
