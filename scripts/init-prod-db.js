#!/usr/bin/env node
// Creates environmental_sites table in the production Supabase DB
// Run: DATABASE_URL="..." node scripts/init-prod-db.js

const { Pool } = require('pg');

const connStr = process.env.DATABASE_URL ||
  'postgresql://postgres.imvcveoynxkceupggnnw:Mombasad3780%2A@aws-1-eu-west-1.pooler.supabase.com:5432/postgres';

const pool = new Pool({
  connectionString: connStr,
  ssl: { rejectUnauthorized: false },
  connectionTimeoutMillis: 15000,
});

const DDL = `
CREATE TABLE IF NOT EXISTS environmental_sites (
    id            BIGSERIAL  PRIMARY KEY,
    location      GEOMETRY(POINT, 4326) NOT NULL,
    database_name VARCHAR(200) NOT NULL,
    category      VARCHAR(50)  NOT NULL,
    class_code    VARCHAR(80),
    class_description VARCHAR(255),
    priority_tier VARCHAR(20) DEFAULT 'standard',
    priority_score INTEGER DEFAULT 0,
    source_org    VARCHAR(100),
    site_name     VARCHAR(500),
    address       TEXT,
    city          VARCHAR(200),
    state         CHAR(2),
    zip           VARCHAR(20),
    status        VARCHAR(100),
    registry_id   VARCHAR(100),
    source_id     VARCHAR(100),
    attributes    JSONB,
    imported_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS env_sites_location_gist ON environmental_sites USING GIST (location);
CREATE INDEX IF NOT EXISTS env_sites_category_idx  ON environmental_sites (category);
CREATE INDEX IF NOT EXISTS env_sites_db_name_idx   ON environmental_sites (database_name);
CREATE INDEX IF NOT EXISTS env_sites_state_idx     ON environmental_sites (state);
`;

async function main() {
  console.log('Connecting to production DB...');
  try {
    const client = await pool.connect();
    console.log('Connected OK');
    await client.query(DDL);
    console.log('environmental_sites table and indexes created (or already existed)');

    const res = await client.query("SELECT COUNT(*) FROM environmental_sites");
    console.log(`Row count: ${res.rows[0].count}`);
    client.release();
  } catch (e) {
    console.error('ERROR:', e.message);
  }
  await pool.end();
}

main();
