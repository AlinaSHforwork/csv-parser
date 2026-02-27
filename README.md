# CSV to PostgreSQL Stream Parser

### Node.js web application that scans a local folder for CSV files and streams them into a PostgreSQL database.

---

## Set up:
- Install Dependencies:

```bash
npm install
```

- Database Config:

```.env
DB_URL=
```
- Put your `.csv` files in csv folder

```
├── /csv           <-- Put your .csv files here
├── /views
│    ├── /layouts
│    │    └── main.hbs
│    ├── index.hbs
│    └── table.hbs
├── index.js
└── package.json
```

## Start server: 

```bash
npm start
```
### Server running on `[http://localhost:3000](http://localhost:3000)`