import express from 'express';
import { engine } from 'express-handlebars';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Pool } from 'pg';
import csv from 'csv-parser';
import dotenv from 'dotenv';

dotenv.config();

const dirname = path.dirname(fileURLToPath(import.meta.url));
const app = express();

app.use(express.urlencoded({ extended: true }));

const pool = new Pool({
    connectionString: process.env.DB_URL
})

app.engine('hbs', engine({ extname: '.hbs' }));
app.set('view engine', 'hbs');

const csvFolder = path.join(dirname, 'csv')

app.get('/', async (req, res) => {
  try {
    const allFiles = fs.readdirSync(csvFolder);
    const csvFiles = allFiles.filter(file => path.extname(file).toLowerCase() === '.csv');

    const dbResult = await pool.query(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = 'public'
    `);

    const tablesWithCounts = await Promise.all(dbResult.rows.map(async (row) => {
      const countRes = await pool.query(`SELECT COUNT(*) FROM "${row.table_name}"`);
      return { 
        name: row.table_name, 
        count: countRes.rows[0].count 
      };
    }));

    res.render('index', { 
      files: csvFiles, 
      tables: tablesWithCounts 
    });
  } catch (err) {
    res.status(500).send(err.message);
  }
});

app.post('/parse', async (req, res) => {
  const { fileName } = req.body;

  if (!fileName || path.extname(fileName).toLowerCase() !== '.csv') {
    return res.status(400).send("Invalid file type. Only CSV allowed.");
  }

  const filePath = path.join(csvFolder, fileName);
  const tableName = path.parse(fileName).name.toLowerCase().replace(/[^a-z0-9]/g, '_');
  
  let isTableCreated = false;
  const stream = fs.createReadStream(filePath).pipe(csv());

  try {
    for await (const row of stream) {
      if (!isTableCreated) {
        const columns = Object.keys(row).map(column => `"${column}" TEXT`).join(', ');
        await pool.query(`CREATE TABLE IF NOT EXISTS "${tableName}" (${columns})`);
        isTableCreated = true;
      }

      const colNames = Object.keys(row).map(col => `"${col}"`).join(', ');
      const colValues = Object.values(row);
      const placeholders = colValues.map((_, i) => `$${i + 1}`).join(', ');

      await pool.query(`INSERT INTO "${tableName}" (${colNames}) VALUES (${placeholders})`, colValues);
    }
    res.redirect('/');
  } catch (err) {
    res.status(500).send("Parsing failed: " + err.message);
  }
});

app.get('/table/:name', async (req, res) => {
  const tableName = req.params.name;

  try {
    const result = await pool.query(`SELECT * FROM "${tableName}" LIMIT 100`);
    
    const columns = result.fields.map(field => field.name);
    
    res.render('table', { 
      tableName, 
      columns, 
      rows: result.rows 
    });
  } catch (err) {
    res.status(500).send("Could not open table: " + err.message);
  }
});

app.listen(3000, () => console.log('Server: http://localhost:3000'));