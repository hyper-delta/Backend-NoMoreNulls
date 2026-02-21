const express = require("express");
const router = express.Router();
const { createConnection } = require("../db/connection");
const axios = require("axios");

// 🔗 Python AI service (Render or local fallback)
const PYTHON_SERVICE_URL =
  process.env.PYTHON_SERVICE_URL || "http://127.0.0.1:8000";

console.log("🔥 PYTHON_SERVICE_URL =", PYTHON_SERVICE_URL);

router.post("/extract", async (req, res) => {
  console.log("📩 /api/metadata/extract called");

  const config = req.body;

  try {
    // ==========================
    // ✅ DATABASE CONNECTION
    // ==========================
    const connection = await createConnection(config);
    console.log("✅ Database connected");

    // ==========================
    // ✅ FETCH TABLES
    // ==========================
    const [tables] = await connection.execute(
      `
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = ?
      `,
      [config.database],
    );

    console.log(`📊 Tables found: ${tables.length}`);

    const metadata = [];

    // ==========================
    // 🔁 LOOP TABLES
    // ==========================
    for (let table of tables) {
      const tableName = table.TABLE_NAME || table.table_name;
      console.log(`\n📄 Processing table: ${tableName}`);

      // --------------------------
      // COLUMNS
      // --------------------------
      const [columns] = await connection.execute(
        `
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = ?
        AND table_name = ?
        `,
        [config.database, tableName],
      );

      // --------------------------
      // PRIMARY KEYS
      // --------------------------
      const [primaryKeys] = await connection.execute(
        `
        SELECT column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = ?
        AND table_name = ?
        AND constraint_name = 'PRIMARY'
        `,
        [config.database, tableName],
      );

      // --------------------------
      // FOREIGN KEYS
      // --------------------------
      const [foreignKeys] = await connection.execute(
        `
        SELECT
          column_name,
          referenced_table_name,
          referenced_column_name
        FROM information_schema.key_column_usage
        WHERE table_schema = ?
        AND table_name = ?
        AND referenced_table_name IS NOT NULL
        `,
        [config.database, tableName],
      );

      // --------------------------
      // ENRICH COLUMNS
      // --------------------------
      const enrichedColumns = columns.map((col) => {
        const isPK = primaryKeys.some(
          (pk) => pk.column_name === col.column_name,
        );
        const isFK = foreignKeys.some(
          (fk) => fk.column_name === col.column_name,
        );

        return {
          name: col.column_name,
          type: col.data_type,
          isPrimaryKey: isPK,
          isForeignKey: isFK,
        };
      });

      const relationships = foreignKeys.map((fk) => ({
        column: fk.column_name,
        references: `${fk.referenced_table_name}.${fk.referenced_column_name}`,
      }));

      // ==========================
      // 🤖 AI BUSINESS SUMMARY
      // ==========================
      console.log(`🤖 Calling AI summary for table: ${tableName}`);
      console.log("➡️ URL:", `${PYTHON_SERVICE_URL}/generate-summary`);

      const aiResponse = await axios.post(
        `${PYTHON_SERVICE_URL}/generate-summary`,
        {
          tableName,
          columns: enrichedColumns,
        },
      );

      console.log("✅ AI summary received");

      // ==========================
      // 📊 FETCH TABLE ROWS
      // ==========================
      const [rows] = await connection.execute(
        `SELECT * FROM \`${tableName}\` LIMIT 1000`,
      );

      // ==========================
      // 📊 DATA QUALITY ANALYSIS
      // ==========================
      console.log(`📊 Analyzing data quality for ${tableName}`);
      console.log("➡️ URL:", `${PYTHON_SERVICE_URL}/analyze-data`);

      const qualityResponse = await axios.post(
        `${PYTHON_SERVICE_URL}/analyze-data`,
        {
          tableName,
          rows,
        },
      );

      console.log("✅ Data quality received");

      // ==========================
      // 📦 PUSH FINAL TABLE METADATA
      // ==========================
      metadata.push({
        tableName,
        businessSummary: aiResponse.data.businessSummary,
        columns: enrichedColumns,
        relationships,
        dataQuality: qualityResponse.data.metrics,
        freshness: qualityResponse.data.freshness,
        risks: qualityResponse.data.risks,
      });
    }

    console.log("🚀 Metadata extraction completed");
    res.json(metadata);
  } catch (error) {
    console.error("❌ ERROR:", error.message);
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
