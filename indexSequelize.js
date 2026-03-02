import express from "express";
import { engine } from "express-handlebars";
import { Sequelize, DataTypes, Op } from "sequelize";
import dotenv from "dotenv";
import multer from "multer";
import stream from "node:stream";
import { parse } from "csv-parse";

dotenv.config();

const app = express();
app.use(express.urlencoded({ extended: true }));

const sequelize = new Sequelize(process.env.DB_URL, {
  dialect: "postgres",
  logging: false,
  define: {
    timestamps: false,
    freezeTableName: true,
  },
});

const statusTracker = {};

app.engine("hbs", engine({ extname: ".hbs" }));
app.set("view engine", "hbs");

const upload = multer({
  storage: multer.memoryStorage(),
  fileFilter: (req, file, cb) => {
    if (
      file.mimetype === "text/csv" ||
      file.originalname?.toLowerCase().endsWith(".csv")
    ) {
      cb(null, true);
    } else {
      cb(new Error("Only .csv files allowed!"));
    }
  },
});

app.get("/", async (req, res) => {
  try {
    const [tables] = await sequelize.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public' 
      ORDER BY table_name
    `);

    const tableInfo = await Promise.all(
      tables.map(async (t) => {
        const tableName = t.table_name;
        const model = sequelize.models[tableName];

        let count = 0;
        if (model) {
          count = await model.count();
        } else {
          const [[{ count: c }]] = await sequelize.query(
            `SELECT COUNT(*) FROM "${tableName}"`,
          );
          count = Number(c);
        }

        return {
          name: tableName,
          count,
          status: statusTracker[tableName] || "Ready",
        };
      }),
    );

    res.render("index", {
      files: [],
      tables: tableInfo,
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Cannot load tables list");
  }
});

app.post("/upload", upload.single("csvFile"), async (req, res) => {
  if (!req.file) {
    return res.status(400).send("No file uploaded.");
  }

  const originalName = req.file.originalname;
  if (!originalName.toLowerCase().endsWith(".csv")) {
    return res.status(400).send("Only .csv files allowed.");
  }

  const tableName = originalName
    .replace(/\.[^/.]+$/, "")
    .toLowerCase()
    .replace(/[^a-z0-9_]/g, "_");

  statusTracker[tableName] = "Parsing...";

  try {
    const parser = parse({
      columns: true,
      skip_empty_lines: true,
      relax_column_count: true,
    });

    const readable = stream.Readable.from(req.file.buffer);

    const rows = [];
    let headers = null;

    parser.on("readable", () => {
      let record;
      while ((record = parser.read()) !== null) {
        if (!headers) {
          headers = Object.keys(record);
        }
        rows.push(record);
      }
    });

    await new Promise((resolve, reject) => {
      parser.on("end", resolve);
      parser.on("error", reject);
      readable.pipe(parser);
    });

    if (rows.length === 0 || !headers) {
      throw new Error("CSV file is empty or has no headers");
    }

    const fields = {};
    headers.forEach((col) => {
      fields[col] = {
        type: DataTypes.TEXT,
        allowNull: true,
      };
    });
    const idCol = headers.find((h) => h.toLowerCase() === "id");
    if (idCol) {
      fields[idCol].primaryKey = true;
    }

    let Model = sequelize.models[tableName];
    if (!Model) {
      Model = sequelize.define(tableName, fields, {
        tableName,
        timestamps: false,
      });
      await Model.sync({ force: false });
    }

    statusTracker[tableName] = "Inserting...";

    const data = rows.map((row) => {
      const cleaned = {};
      headers.forEach((h) => {
        cleaned[h] = row[h] === "" ? null : row[h];
      });
      return cleaned;
    });

    const BATCH_SIZE = 1000;
    for (let i = 0; i < data.length; i += BATCH_SIZE) {
      const batch = data.slice(i, i + BATCH_SIZE);
      await Model.bulkCreate(batch, {
        validate: false,
        individualHooks: false,
        ignoreDuplicates: false,
      });
    }

    statusTracker[tableName] = "Complete!";
    res.redirect("/");
  } catch (err) {
    console.error("Upload error:", err);
    statusTracker[tableName] = "Error";
    res.status(500).send("Error processing CSV: " + err.message);
  }
});

app.post("/delete", async (req, res) => {
  const { tableName } = req.body;
  if (!tableName) return res.redirect("/");

  try {
    const Model = sequelize.models[tableName];
    if (Model) {
      await Model.drop();
      delete sequelize.models[tableName];
    } else {
      await sequelize.query(`DROP TABLE IF EXISTS "${tableName}"`);
    }
    delete statusTracker[tableName];
    res.redirect("/");
  } catch (err) {
    console.error(err);
    res.status(500).send("Cannot delete table");
  }
});

app.get("/table/:name", async (req, res) => {
  const tableName = req.params.name;
  const search = req.query.search?.trim() || "";
  let page = parseInt(req.query.page) || 1;
  if (page < 1) page = 1;

  const limit = 100;
  const offset = (page - 1) * limit;

  try {
    let Model = sequelize.models[tableName];
    if (!Model) {
      const [columnsResult] = await sequelize.query(
        `
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_name = :table
      `,
        { replacements: { table: tableName } },
      );

      const columns = columnsResult.map((c) => c.column_name);

      if (columns.length === 0) {
        return res.status(404).send("Table not found");
      }

      const fields = {};
      columns.forEach((col) => {
        fields[col] = { type: DataTypes.TEXT, allowNull: true };
      });

      const idCol = columns.find((c) => c.toLowerCase() === "id");
      if (idCol) {
        fields[idCol].primaryKey = true;
      }

      Model = sequelize.define(tableName, fields, {
        tableName,
        timestamps: false,
      });
      await Model.sync({ force: false });
    }

    const where = search
      ? {
          [Op.or]: Object.keys(Model.rawAttributes).map((col) =>
            Sequelize.where(
              Sequelize.cast(Sequelize.col(col), "text"),
              Op.iLike,
              `%${search}%`,
            ),
          ),
        }
      : undefined;

    const { count, rows } = await Model.findAndCountAll({
      where,
      limit,
      offset,
      raw: true,
      nest: false,
    });

    const [columnsResult] = await sequelize.query(
      `
      SELECT column_name 
      FROM information_schema.columns 
      WHERE table_name = :table
    `,
      { replacements: { table: tableName } },
    );
    const columns = columnsResult.map((c) => c.column_name);

    const totalPages = Math.max(1, Math.ceil(count / limit));
    if (page > totalPages) page = totalPages;

    res.render("table", {
      tableName,
      columns,
      rows,
      page,
      search,
      totalCount: count,
      totalPages,
      nextPage: page < totalPages ? page + 1 : null,
      prevPage: page > 1 ? page - 1 : null,
    });
  } catch (err) {
    console.error(err);
    res.status(500).send("Cannot display table: " + err.message);
  }
});

sequelize
  .authenticate()
  .then(() => {
    console.log("Database connection OK");
    app.listen(3000, () => {
      console.log("Server running → http://localhost:3000");
    });
  })
  .catch((err) => {
    console.error("Cannot connect to database:", err);
  });
