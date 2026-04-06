const pg = require('pg');

async function checkSetup() {
  const client = new pg.Client({
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: '2030',
    database: 'geoscope'
  });

  try {
    await client.connect();
    console.log('✓ PostgreSQL connection successful\n');

    // Check PostGIS
    const postgisResult = await client.query('SELECT postgis_version()');
    console.log('✓ PostGIS version:', postgisResult.rows[0].postgis_version);

    // Check tables
    const tablesResult = await client.query(
      "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name"
    );
    const tables = tablesResult.rows.map(t => t.table_name);
    console.log('\n✓ Database tables (' + tables.length + '):', tables.join(', '));

    // Check for required tables
    const requiredTables = ['area_features', 'environmental_sites', 'database_catalog', 'users', 'orders'];
    const missingTables = requiredTables.filter(t => !tables.includes(t));

    if (missingTables.length > 0) {
      console.log('\n⚠ Missing tables:', missingTables.join(', '));
      console.log('   Run: node scripts/init-postgres.js');
    } else {
      console.log('\n✓ All required tables present');
    }

    // Check counts
    console.log('\nData counts:');
    for (const table of requiredTables) {
      if (tables.includes(table)) {
        const countResult = await client.query(`SELECT COUNT(*) as cnt FROM "${table}"`);
        console.log(`  - ${table}: ${countResult.rows[0].cnt} rows`);
      }
    }

    await client.end();
  } catch (error) {
    console.error('✗ Error:', error.message);
    process.exit(1);
  }
}

checkSetup();
