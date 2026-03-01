import express from 'express';
import { engine } from 'express-handlebars';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { Pool } from 'pg';
import csv from 'csv-parser';
import dotenv from 'dotenv';
import multer from 'multer';

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

const storage = multer.diskStorage({
  destination: csvFolder,
  filename: (req, file, cb) => cb(null, file.originalname)
});
const upload = multer({ 
  storage,
  fileFilter: (req, file, cb) => {
    if (file.mimetype === 'text/csv' || file.originalname.endsWith('.csv')) cb(null, true);
    else cb(new Error('Strictly .csv files only!'));
  }
});

const statusTracker = {};

app.get('/', async (req, res) => {
  const csvFiles = fs.readdirSync(csvFolder).filter(f => f.endsWith('.csv'));
  const dbResult = await pool.query(`
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public'
    `);
  
  const tables = await Promise.all(dbResult.rows.map(async (row) => {
    const countRes = await pool.query(`SELECT COUNT(*) FROM "${row.table_name}"`);
    return { 
      name: row.table_name, 
      count: countRes.rows[0].count,
      status: statusTracker[row.table_name] || 'Ready'
    };
  }));

  res.render('index', { 
    files: csvFiles,
    tables 
});
});

app.post('/upload', upload.single('csvFile'), (req, res) => {
  res.redirect('/');
});

app.post('/parse', async (req, res) => {
  const { fileName } = req.body;

  if (!fileName || path.extname(fileName).toLowerCase() !== '.csv') {
    return res.status(400).send("Invalid file type. Only CSV allowed.");
  }

  const filePath = path.join(csvFolder, fileName);
  const tableName = path.parse(fileName).name.toLowerCase().replace(/[^a-z0-9]/g, '_');
  
  res.redirect('/'); 
  
  statusTracker[tableName] = 'Parsing...';
  let isTableCreated = false;
  let batch = [];
  let columnsStr = '';
  let headers = []; 

  const stream = fs.createReadStream(filePath).pipe(csv());

  try {
    for await (const row of stream) {
      if (!isTableCreated) {
        headers = Object.keys(row); 
        const columns = headers.map(col => `"${col}" TEXT`).join(', ');
        columnsStr = headers.map(c => `"${c}"`).join(', ');
        await pool.query(`CREATE TABLE IF NOT EXISTS "${tableName}" (${columns})`);
        isTableCreated = true;
      }

      const normalizedRow = headers.map(header => row[header] === undefined ? null : row[header]);
      batch.push(normalizedRow);

      if (batch.length === 1000) {
        stream.pause();
        await insertBatch(tableName, columnsStr, batch);
        batch = [];
        stream.resume();
      }
    }

    if (batch.length > 0) await insertBatch(tableName, columnsStr, batch);
    
    statusTracker[tableName] = 'Complete!';
  } catch (err) {
    console.error(`Error parsing ${fileName}:`, err);
    statusTracker[tableName] = 'Error during parsing';
  }
});

async function insertBatch(tableName, columnsStr, rowsArray) {
  let valuesString = '';
  let flatValues = [];
  let paramIndex = 1;

  rowsArray.forEach((row, rowIndex) => {
    const rowParams = [];
    row.forEach(val => {
      rowParams.push(`$${paramIndex++}`);
      flatValues.push(val);
    });
    valuesString += `(${rowParams.join(',')})` + (rowIndex === rowsArray.length - 1 ? '' : ',');
  });

  await pool.query(`INSERT INTO "${tableName}" (${columnsStr}) VALUES ${valuesString}`, flatValues);
}



app.post('/delete', async (req, res) => {
  const { tableName } = req.body;
  await pool.query(`DROP TABLE IF EXISTS "${tableName}"`);
  delete statusTracker[tableName]; 
  res.redirect('/');
});

app.get('/table/:name', async (req, res) => {
  const tableName = req.params.name;
  const page = parseInt(req.query.page) || 1;
  const limit = 100; 
  const offset = (page - 1) * limit;
  const search = req.query.search || '';

  try {
    let whereClause = '';
    if (search) {
      whereClause = `WHERE "${tableName}"::text ILIKE '%${search}%'`;
    }

    const query = `SELECT * FROM "${tableName}" ${whereClause} LIMIT $1 OFFSET $2`;
    const result = await pool.query(query, [limit, offset]);
    
    const columns = result.fields.length > 0 ? result.fields.map(f => f.name) : [];

    res.render('table', { 
      tableName, 
      columns, 
      rows: result.rows, 
      page, 
      search,
      nextPage: page + 1, 
      prevPage: page > 1 ? page - 1 : null
    });
  } catch (err) {
    res.status(500).send("Could not open table: " + err.message);
  }
});

app.listen(3000, () => console.log('Server: http://localhost:3000'));