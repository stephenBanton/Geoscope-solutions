// =============================================================================
// PostgreSQL connection pool — used by all PostGIS queries in GeoScope
// =============================================================================
// Fill in PG_* variables in your .env file (see .env.example)
// =============================================================================

const { Pool } = require('pg');

function cleanEnvValue(value) {
  return String(value ?? '').replace(/\r|\n/g, '').trim();
}

function buildPoolConfig() {
  const databaseUrl = cleanEnvValue(process.env.DATABASE_URL);

  if (databaseUrl) {
    const isLocalHost = /localhost|127\.0\.0\.1/i.test(databaseUrl);
    return {
      connectionString: databaseUrl,
      ssl: isLocalHost ? false : { rejectUnauthorized: false },
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    };
  }

  return {
    host: cleanEnvValue(process.env.PG_HOST) || 'localhost',
    port: parseInt(cleanEnvValue(process.env.PG_PORT) || '5432', 10),
    database: cleanEnvValue(process.env.PG_DATABASE) || 'geoscope',
    user: cleanEnvValue(process.env.PG_USER) || 'postgres',
    password: cleanEnvValue(process.env.PG_PASSWORD),
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  };
}

function buildDataPoolConfig() {
  const dataDatabaseUrl = cleanEnvValue(process.env.DATA_DATABASE_URL);

  if (dataDatabaseUrl) {
    const isLocalHost = /localhost|127\.0\.0\.1/i.test(dataDatabaseUrl);
    return {
      connectionString: dataDatabaseUrl,
      ssl: isLocalHost ? false : { rejectUnauthorized: false },
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 5000,
    };
  }

  // Fallback to the primary PG connection when dedicated data DB is not set.
  return {
    host: cleanEnvValue(process.env.DATA_PG_HOST) || cleanEnvValue(process.env.PG_HOST) || 'localhost',
    port: parseInt(cleanEnvValue(process.env.DATA_PG_PORT) || cleanEnvValue(process.env.PG_PORT) || '5432', 10),
    database: cleanEnvValue(process.env.DATA_PG_DATABASE) || cleanEnvValue(process.env.PG_DATABASE) || 'geoscope',
    user: cleanEnvValue(process.env.DATA_PG_USER) || cleanEnvValue(process.env.PG_USER) || 'postgres',
    password: cleanEnvValue(process.env.DATA_PG_PASSWORD) || cleanEnvValue(process.env.PG_PASSWORD),
    max: 10,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
  };
}

function isTransientDataPoolFailure(err) {
  if (!err) return false;
  const code = String(err.code || '').toUpperCase();
  if (['ENOTFOUND', 'ECONNREFUSED', 'ETIMEDOUT', 'EHOSTUNREACH'].includes(code)) {
    return true;
  }
  const message = String(err.message || '').toLowerCase();
  return message.includes('getaddrinfo') || message.includes('connection terminated');
}

const pool = new Pool(buildPoolConfig());
const dedicatedDataPool = new Pool(buildDataPoolConfig());

const dataPool = {
  async query(text, params) {
    try {
      return await dedicatedDataPool.query(text, params);
    } catch (err) {
      if (!isTransientDataPoolFailure(err)) {
        throw err;
      }
      console.warn('[DATA-DB] Falling back to primary DB pool:', err.message);
      return pool.query(text, params);
    }
  },

  async connect() {
    try {
      return await dedicatedDataPool.connect();
    } catch (err) {
      if (!isTransientDataPoolFailure(err)) {
        throw err;
      }
      console.warn('[DATA-DB] Falling back to primary DB connection:', err.message);
      return pool.connect();
    }
  },

  on(eventName, listener) {
    dedicatedDataPool.on(eventName, listener);
  },

  async end() {
    await dedicatedDataPool.end();
  }
};

// Log a warning if the pool can't connect — non-fatal so the rest of the API
// keeps running even when PostgreSQL is not yet set up.
pool.on('error', (err) => {
  console.warn('[DB] Unexpected pool error:', err.message);
});

dataPool.on('error', (err) => {
  console.warn('[DATA-DB] Unexpected pool error:', err.message);
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

async function pingDataDB() {
  try {
    const client = await dataPool.connect();
    await client.query('SELECT 1');
    client.release();
    console.log('[DATA-DB] Dataset PostgreSQL connected ✓');
    return true;
  } catch (err) {
    console.warn('[DATA-DB] Dataset PostgreSQL not available:', err.message);
    return false;
  }
}

module.exports = { pool, dataPool, pingDB, pingDataDB };
