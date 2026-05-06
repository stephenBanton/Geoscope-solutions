#!/usr/bin/env node
require('dotenv').config({
  path: require('path').join(__dirname, '../.env'),
  quiet: true,
});

const fs = require('fs');
const path = require('path');
const { Client } = require('pg');

const usersPath = path.join(__dirname, '../.data/users.json');
const databaseUrl = String(process.env.DATABASE_URL || '').trim();

if (!databaseUrl) {
  console.error('DATABASE_URL is required');
  process.exit(1);
}

if (!fs.existsSync(usersPath)) {
  console.error(`Users file not found: ${usersPath}`);
  process.exit(1);
}

const users = JSON.parse(fs.readFileSync(usersPath, 'utf8'));
const allowedRoles = new Set(['admin', 'analyst', 'gis', 'client']);

async function main() {
  const client = new Client({
    connectionString: databaseUrl,
    ssl: /localhost|127\.0\.0\.1/i.test(databaseUrl) ? false : { rejectUnauthorized: false },
  });

  await client.connect();

  await client.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      role VARCHAR(20) NOT NULL CHECK (role IN ('admin','analyst','gis','client')),
      name VARCHAR(200),
      company VARCHAR(200),
      email VARCHAR(320) NOT NULL UNIQUE,
      password_hash VARCHAR(200) NOT NULL,
      auto_provisioned BOOLEAN DEFAULT FALSE,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      updated_at TIMESTAMPTZ DEFAULT NOW()
    )
  `);

  let synced = 0;
  for (const user of users) {
    const email = String(user.email || '').trim().toLowerCase();
    if (!email) continue;

    const role = allowedRoles.has(String(user.role || '').toLowerCase())
      ? String(user.role).toLowerCase()
      : 'client';

    await client.query(
      `INSERT INTO users (name, email, password_hash, role, company, auto_provisioned, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7::timestamptz, NOW()), NOW())
       ON CONFLICT (email) DO UPDATE SET
         name = EXCLUDED.name,
         password_hash = EXCLUDED.password_hash,
         role = EXCLUDED.role,
         company = EXCLUDED.company,
         auto_provisioned = EXCLUDED.auto_provisioned,
         updated_at = NOW()`,
      [
        user.name || '',
        email,
        String(user.password || ''),
        role,
        user.company || '',
        Boolean(user.auto_provisioned),
        user.created_at || null,
      ]
    );

    synced += 1;
  }

  console.log(JSON.stringify({ synced }));
  await client.end();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
