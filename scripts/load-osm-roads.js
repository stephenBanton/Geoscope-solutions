#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env') });

const { pool } = require('../db');

async function main() {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const sourceTable = await client.query(`
      SELECT table_name
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name IN ('planet_osm_line', 'osm_lines_raw')
      ORDER BY CASE table_name
        WHEN 'planet_osm_line' THEN 1
        WHEN 'osm_lines_raw' THEN 2
        ELSE 99
      END
      LIMIT 1
    `);

    if (!sourceTable.rows[0]?.table_name) {
      throw new Error('No OSM line source table found. Run import-osm-pbf.ps1 first.');
    }

    const table = sourceTable.rows[0].table_name;

    const sourceSql = table === 'planet_osm_line'
      ? `
        SELECT
          ('line-' || l.osm_id::text) AS osm_id,
          COALESCE(NULLIF(l.name, ''), 'Road') AS name,
          'road'::text AS type,
          ST_LineInterpolatePoint(ST_Transform(l.way, 4326), 0.5) AS midpt,
          jsonb_build_object('highway', l.highway) AS attrs
        FROM planet_osm_line l
        WHERE l.highway IS NOT NULL AND l.osm_id IS NOT NULL
      `
      : `
        SELECT
          ('line-' || l.osm_id::text) AS osm_id,
          COALESCE(NULLIF(l.name, ''), 'Road') AS name,
          'road'::text AS type,
          ST_LineInterpolatePoint(ST_Transform(l.geom, 4326), 0.5) AS midpt,
          jsonb_build_object('highway', l.highway, 'other_tags', l.other_tags) AS attrs
        FROM osm_lines_raw l
        WHERE l.highway IS NOT NULL AND l.osm_id IS NOT NULL
      `;

    const upsert = await client.query(`
      WITH src AS (
        ${sourceSql}
      )
      INSERT INTO area_features
        (osm_id, name, type, address, latitude, longitude, geom, attributes, updated_at)
      SELECT
        s.osm_id,
        s.name,
        s.type,
        NULL,
        ST_Y(s.midpt),
        ST_X(s.midpt),
        s.midpt,
        s.attrs,
        NOW()
      FROM src s
      ON CONFLICT (osm_id)
      DO UPDATE SET
        name = EXCLUDED.name,
        type = EXCLUDED.type,
        latitude = EXCLUDED.latitude,
        longitude = EXCLUDED.longitude,
        geom = EXCLUDED.geom,
        attributes = EXCLUDED.attributes,
        updated_at = NOW()
      RETURNING 1
    `);

    await client.query('COMMIT');
    console.log(`[OSM] Source table: ${table}`);
    console.log(`[OSM] Roads loaded into area_features: ${upsert.rowCount}`);
  } catch (err) {
    await client.query('ROLLBACK');
    console.error(`[OSM] Load failed: ${err.message}`);
    process.exitCode = 1;
  } finally {
    client.release();
    await pool.end();
  }
}

main();
