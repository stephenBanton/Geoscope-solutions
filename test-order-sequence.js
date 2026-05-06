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

async function testOrderCreation() {
  const client = await pool.connect();
  try {
    console.log('📋 Checking database state...\n');
    
    // Check if table exists
    const tableCheck = await client.query(`
      SELECT EXISTS(
        SELECT FROM information_schema.tables 
        WHERE table_name = 'orders'
      )
    `);
    console.log(`✅ Orders table exists: ${tableCheck.rows[0].exists}`);
    
    // Get table info
    const tableInfo = await client.query(`
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_name = 'orders' 
      ORDER BY ordinal_position 
      LIMIT 5
    `);
    console.log(`\n📊 Orders table columns:`);
    tableInfo.rows.forEach(row => {
      console.log(`   - ${row.column_name}: ${row.data_type}`);
    });
    
    // Check sequence
    const seqCheck = await client.query(`
      SELECT EXISTS(
        SELECT FROM pg_sequences 
        WHERE sequencename = 'orders_id_seq'
      )
    `);
    console.log(`\n✅ Sequence exists: ${seqCheck.rows[0].exists}`);
    
    const seqStatus = await client.query(`SELECT last_value FROM orders_id_seq`);
    console.log(`📊 Current sequence value: ${seqStatus.rows[0].last_value}`);
    
    // Count existing orders
    const countCheck = await client.query(`SELECT COUNT(*) as cnt FROM orders`);
    console.log(`\n📊 Total orders in table: ${countCheck.rows[0].cnt}`);
    
    // Test insert
    console.log(`\n🧪 Testing new order creation...\n`);
    const testInsert = await client.query(`
      INSERT INTO orders (
        project_name, client_name, client_company, recipient_email_1,
        address, latitude, longitude, status, source, created_at, updated_at
      ) VALUES (
        'Test Order', 'Test Client', 'Test Company', 'test@example.com',
        '123 Test St', 40.7128, -74.0060, 'received', 'test', NOW(), NOW()
      ) RETURNING id
    `);
    const newOrderId = testInsert.rows[0].id;
    console.log(`✨ New order created with ID: ${newOrderId}`);
    
    // Verify sequence updated
    const seqAfter = await client.query(`SELECT last_value FROM orders_id_seq`);
    console.log(`📊 Sequence after insert: ${seqAfter.rows[0].last_value}`);
    
    // Delete test order
    await client.query(`DELETE FROM orders WHERE id = $1`, [newOrderId]);
    console.log(`🗑️  Test order deleted`);
    
    console.log(`\n✅ Sequence is working! Next order will be ID ${newOrderId + 1}`);
    
  } catch (err) {
    console.error('❌ Error:', err.message);
    process.exit(1);
  } finally {
    client.release();
    await pool.end();
  }
}

testOrderCreation();
