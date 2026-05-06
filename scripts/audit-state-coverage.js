#!/usr/bin/env node
require('dotenv').config({ path: require('path').join(__dirname, '../.env'), quiet: true });
const fs = require('fs');
const path = require('path');
const { Pool } = require('pg');

const pool = new Pool({
  host: process.env.PG_HOST,
  port: process.env.PG_PORT,
  database: process.env.PG_DATABASE,
  user: process.env.PG_USER,
  password: process.env.PG_PASSWORD,
  max: 1,
});

const SQL = `
WITH states AS (
  SELECT unnest(ARRAY[
    'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD',
    'MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC',
    'SD','TN','TX','UT','VT','VA','WA','WV','WI','WY'
  ]) AS st
),
db_rows AS (
  SELECT database_name, upper(state) AS st
  FROM environmental_sites
  WHERE state ~ '^[A-Za-z]{2}$'
),
db_list AS (
  SELECT database_name, count(*)::bigint AS records
  FROM db_rows
  GROUP BY database_name
),
db_state_50 AS (
  SELECT DISTINCT r.database_name, r.st
  FROM db_rows r
  INNER JOIN states s ON s.st = r.st
),
covered_counts AS (
  SELECT database_name, count(*)::int AS states_present
  FROM db_state_50
  GROUP BY database_name
),
missing_rollup AS (
  SELECT
    d.database_name,
    count(*) FILTER (WHERE ds.st IS NULL)::int AS states_missing,
    array_agg(s.st ORDER BY s.st) FILTER (WHERE ds.st IS NULL) AS missing_states
  FROM db_list d
  CROSS JOIN states s
  LEFT JOIN db_state_50 ds ON ds.database_name = d.database_name AND ds.st = s.st
  GROUP BY d.database_name
)
SELECT
  d.database_name,
  coalesce(c.states_present, 0) AS states_present,
  m.states_missing,
  d.records,
  m.missing_states
FROM db_list d
LEFT JOIN covered_counts c ON c.database_name = d.database_name
LEFT JOIN missing_rollup m ON m.database_name = d.database_name
ORDER BY states_present DESC, records DESC;
`;

function parseOutArg(argv) {
  const i = argv.indexOf('--out');
  if (i !== -1 && argv[i + 1]) return argv[i + 1];
  const eq = argv.find((a) => a.startsWith('--out='));
  if (eq) return eq.slice('--out='.length);
  return null;
}

(async function main() {
  try {
    const r = await pool.query(SQL);
    const payload = JSON.stringify(r.rows, null, 2);
    const outPathArg = parseOutArg(process.argv.slice(2));

    if (outPathArg) {
      const resolved = path.isAbsolute(outPathArg)
        ? outPathArg
        : path.join(process.cwd(), outPathArg);
      fs.writeFileSync(resolved, payload, 'utf8');
      console.log(`Wrote ${r.rows.length} rows to ${resolved}`);
    } else {
      console.log(payload);
    }
  } catch (e) {
    console.error(e.message);
    process.exitCode = 1;
  } finally {
    await pool.end();
  }
})();
