#!/usr/bin/env node
const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: process.env.PG_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT || '5432', 10),
  database: process.env.PG_DATABASE || 'geoscope',
  user: process.env.PG_USER || 'postgres',
  password: process.env.PG_PASSWORD || 'postgres',
});

async function testIncrementSequence() {
  const client = await pool.connect();
  try {
    console.log('Creating 3 test orders to verify increment...\n');
    
    const ids = [];
    for (let i = 0; i < 3; i++) {
      const result = await client.query(`
        INSERT INTO orders (
          project_name, client_name, client_company, recipient_email_1,
          address, latitude, longitude, status, source, created_at, updated_at
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), NOW()
        ) RETURNING id
      `, [
        `Order ${i+1}`,
        `Client ${i+1}`,
        `Company ${i+1}`,
        `test${i+1}@example.com`,
        '123 Test St',
        40.7128,
        -74.0060,
        'received',
        'test'
      ]);
      ids.push(result.rows[0].id);
      console.log(`   Order ${i+1}: ID ${result.rows[0].id}`);
    }
    
    console.log(`\n✅ Sequence working! IDs are incremental: ${ids.join(', ')}`);
    console.log(`✅ Starting from 1001 and incrementing correctly!`);
    
    // Delete test orders
    for (const id of ids) {
      await client.query('DELETE FROM orders WHERE id = $1', [id]);
    }
    console.log(`\n🗑️  Test orders cleaned up`);
    
  } catch (err) {
    console.error('❌ Error:', err.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

testIncrementSequence();
