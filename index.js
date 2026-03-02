import express from "express";
import { engine } from "express-handlebars";
import { Pool } from "pg";
import dotenv from "dotenv";
import multer from "multer";
import stream from "node:stream";
import { parse } from "csv-parse";
dotenv.config();

const app = express();
app.use(express.urlencoded({ extended: true }));

const pool = new Pool({
  connectionString: process.env.DB_URL,
});

app.engine("hbs", engine({ extname: ".hbs" }));
app.set("view engine", "hbs");

const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (req, file, cb) => {
    if (file.mimetype === "text/csv" || file.originalname.endsWith(".csv"))
      cb(null, true);
    else cb(new Error("Strictly .csv files only!"));
  },
});

const statusTracker = {};

app.get("/", async (req, res) => {
  const dbResult = await pool.query(`
    SELECT table_name FROM information_schema.tables
    WHERE table_schema = 'public'
    ORDER BY table_name
  `);
  const tables = await Promise.all(
    dbResult.rows.map(async (row) => {
      const countRes = await pool.query(
        `SELECT COUNT(*) FROM "${row.table_name}"`,
      );
      return {
        name: row.table_name,
        count: Number(countRes.rows[0].count),
        status: statusTracker[row.table_name] || "Ready",
      };
    }),
  );
  res.render("index", {
    files: [],
    tables,
  });
});

app.post("/upload", upload.single("csvFile"), async (req, res) => {
  if (!req.file) return res.status(400).send("No file uploaded.");
  const originalName = req.file.originalname;
  if (!originalName || !originalName.toLowerCase().endsWith(".csv"))
    return res.status(400).send("Only .csv allowed.");

  const tableName = originalName
    .replace(/\.[^/.]+$/, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "_");
  statusTracker[tableName] = "Parsing...";

  const parser = parse({
    columns: true,
    skip_empty_lines: true,
    relax_column_count: true,
  });

  const readable = stream.Readable.from(req.file.buffer);

  const BATCH_SIZE = 1000;
  let batchRows = [];
  let headers = null;
  let isTableCreated = false;

  try {
    for await (const chunk of readable) {
      parser.write(chunk);
    }
    parser.end();

    for await (const record of parser) {
      if (!isTableCreated) {
        headers = Object.keys(record);
        const columnsDef = headers.map((col) => `"${col}" TEXT`).join(",");
        await pool.query(
          `CREATE TABLE IF NOT EXISTS "${tableName}" (${columnsDef})`,
        );
        isTableCreated = true;
      }

      const normalized = headers.map((h) =>
        record[h] === "" ? null : record[h],
      );
      batchRows.push(normalized);

      if (batchRows.length >= BATCH_SIZE) {
        await insertBatch(pool, tableName, headers, batchRows);
        batchRows = [];
      }
    }

    if (batchRows.length > 0)
      await insertBatch(pool, tableName, headers, batchRows);
    statusTracker[tableName] = "Complete!";
    res.redirect("/");
  } catch (err) {
    console.error(`Error parsing ${fileName}:`, err);
    statusTracker[tableName] = "Error during parsing";
    res.status(500).send("Error parsing CSV: " + err.message);
  }
});

async function insertBatch(pool, tableName, headers, rows) {
  if (!rows.length) return;
  const columnsStr = headers.map((h) => `"${h}"`).join(",");
  let paramIndex = 1;
  const valuesPlaceholders = rows
    .map((row) => {
      const placeholders = row.map(() => `$${paramIndex++}`);
      return `(${placeholders.join(",")})`;
    })
    .join(",");
  const flatValues = rows.flat();
  const sql = `INSERT INTO "${tableName}" (${columnsStr}) VALUES ${valuesPlaceholders}`;
  await pool.query(sql, flatValues);
}

app.post(
  "/delete",
  express.urlencoded({ extended: true }),
  async (req, res) => {
    const tableName = req.body.tableName;
    if (!tableName) return res.redirect("/");
    await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
    delete statusTracker[tableName];
    res.redirect("/");
  },
);

app.get("/table/:name", async (req, res) => {
  const tableName = req.params.name;
  const search = req.query.search || "";
  const limit = 100;

  let page = 1;
  if (req.query.page) {
    const parsed = parseInt(req.query.page, 10);
    if (!isNaN(parsed) && parsed > 0) {
      page = parsed;
    }
  }

  try {
    const columnsResult = await pool.query(
      `SELECT column_name FROM information_schema.columns 
       WHERE table_name = $1`,
      [tableName],
    );
    const columns = columnsResult.rows.map((r) => r.column_name);

    let whereClause = "";
    let searchParams = [];
    if (search) {
      const searchConditions = columns
        .map((col, idx) => `"${col}"::TEXT ILIKE $${idx + 1}`)
        .join(" OR ");
      whereClause = ` WHERE ${searchConditions}`;
      searchParams = columns.map(() => `%${search}%`);
    }

    const countQuery = `SELECT COUNT(*) FROM "${tableName}"${whereClause}`;
    const countResult = await pool.query(countQuery, searchParams);
    const totalCount = Number(countResult.rows[0].count);
    const totalPages = Math.max(1, Math.ceil(totalCount / limit));

    if (page > totalPages) {
      page = totalPages;
    }

    const offset = (page - 1) * limit;

    const dataQuery = `SELECT * FROM "${tableName}"${whereClause} ORDER BY ctid LIMIT $${searchParams.length + 1} OFFSET $${searchParams.length + 2}`;
    const dataParams = [...searchParams, limit, offset];
    const result = await pool.query(dataQuery, dataParams);

    res.render("table", {
      tableName,
      columns,
      rows: result.rows,
      page,
      search,
      nextPage: page < totalPages ? page + 1 : null,
      prevPage: page > 1 ? page - 1 : null,
      totalCount,
      totalPages,
    });
  } catch (err) {
    res.status(500).send("Could not open table: " + err.message);
  }
});

app.listen(3000, () => console.log("Server: http://localhost:3000"));
