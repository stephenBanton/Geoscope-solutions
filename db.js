// =============================================================================
// PostgreSQL connection pool — used by all PostGIS queries in GeoScope
// =============================================================================
// Fill in PG_* variables in your .env file (see .env.example)
// =============================================================================

const { Pool } = require('pg');

const pool = new Pool({
  host:     process.env.PG_HOST     || 'localhost',
  port:     parseInt(process.env.PG_PORT || '5432', 10),
  database: process.env.PG_DATABASE || 'geoscope',
  user:     process.env.PG_USER     || 'postgres',
  password: String(process.env.PG_PASSWORD ?? ''),
  max:      10,            // max connections in pool
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 5000,
});

// Log a warning if the pool can't connect — non-fatal so the rest of the API
// keeps running even when PostgreSQL is not yet set up.
pool.on('error', (err) => {
  console.warn('[DB] Unexpected pool error:', err.message);
});

/**
 * Quick connectivity check — call this at server startup.
 * Returns true if we can reach PostgreSQL, false otherwise.
 */
async function pingDB() {
  try {
    const client = await pool.connect();
    await client.query('SELECT 1');
    client.release();
    console.log('[DB] PostgreSQL connected ✓');
    return true;
  } catch (err) {
    console.warn('[DB] PostgreSQL not available:', err.message);
    return false;
  }
}

module.exports = { pool, pingDB };
