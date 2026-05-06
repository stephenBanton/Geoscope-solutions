require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

async function createDbIfMissing() {
  const admin = new Client({
    host: process.env.PG_HOST,
    port: parseInt(process.env.PG_PORT || '5432', 10),
    database: 'postgres',
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
  });

  await admin.connect();
  const check = await admin.query("SELECT 1 FROM pg_database WHERE datname = 'geoscope'");
  if (check.rowCount === 0) {
    await admin.query('CREATE DATABASE geoscope');
    console.log('DB_CREATED');
  } else {
    console.log('DB_EXISTS');
  }
  await admin.end();
}

async function applySchema() {
  const client = new Client({
    host: process.env.PG_HOST,
    port: parseInt(process.env.PG_PORT || '5432', 10),
    database: 'geoscope',
    user: process.env.PG_USER,
    password: process.env.PG_PASSWORD,
  });

  await client.connect();
  const schemaPath = path.join(__dirname, '..', 'schema.sql');
  const schema = fs.readFileSync(schemaPath, 'utf8');
  await client.query(schema);
  await client.end();
  console.log('SCHEMA_APPLIED');
}

(async () => {
  await createDbIfMissing();
  await applySchema();
})().catch((err) => {
  console.error('INIT_FAILED:', err.message);
  process.exit(1);
});
