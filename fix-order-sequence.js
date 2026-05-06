#!/usr/bin/env node
/**
 * Fix Order ID Sequence
 * Resets the orders table auto-increment sequence to continue from the highest existing order ID
 */

const { Pool } = require('pg');
require('dotenv').config();

const pool = new Pool({
  host: process.env.PG_HOST || process.env.DB_HOST || 'localhost',
  port: parseInt(process.env.PG_PORT || process.env.DB_PORT || '5432', 10),
  database: process.env.PG_DATABASE || process.env.DB_NAME || 'geoscope',
  user: process.env.PG_USER || process.env.DB_USER || 'postgres',
  password: process.env.PG_PASSWORD || process.env.DB_PASSWORD || 'postgres',
});

async function fixOrderSequence() {
  const client = await pool.connect();
  try {
    console.log('🔍 Checking order sequence...');
    
    // Get current sequence value
    const seqResult = await client.query(
      `SELECT last_value FROM orders_id_seq`
    );
    const currentSeq = seqResult.rows[0]?.last_value || 0;
    console.log(`   Current sequence value: ${currentSeq}`);
    
    // Get max ID in orders table
    const maxResult = await client.query(`SELECT MAX(id) as max_id FROM orders`);
    const maxId = maxResult.rows[0]?.max_id || 0;
    console.log(`   Highest order ID: ${maxId}`);
    
    // Get count of orders
    const countResult = await client.query(`SELECT COUNT(*) as count FROM orders`);
    const orderCount = countResult.rows[0]?.count || 0;
    console.log(`   Total orders: ${orderCount}`);
    
    if (maxId > 0 && currentSeq <= maxId) {
      const nextId = maxId + 1;
      console.log(`\n⚙️  Fixing sequence to restart at: ${nextId}`);
      
      await client.query(`ALTER SEQUENCE orders_id_seq RESTART WITH ${nextId}`);
      
      // Verify fix
      const verifyResult = await client.query(`SELECT last_value FROM orders_id_seq`);
      const newSeq = verifyResult.rows[0]?.last_value || 0;
      console.log(`✅ Sequence reset successful! New value: ${newSeq}`);
      
      if (newSeq === nextId - 1) {
        console.log(`\n✨ Next order will be created with ID: ${nextId}`);
      }
    } else {
      console.log(`\n✅ Sequence is already correct!`);
    }
    
  } catch (err) {
    console.error('❌ Error fixing sequence:', err.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

fixOrderSequence();
