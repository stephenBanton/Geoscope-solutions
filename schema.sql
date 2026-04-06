-- =============================================================================
-- GeoScope PostGIS Schema
-- =============================================================================
-- Prerequisites:
--   1. PostgreSQL 14+ with the PostGIS extension
--   2. Run as a superuser: CREATE EXTENSION IF NOT EXISTS postgis;
--
-- Once populated, replace the live-API calls in gis-search.js with
-- ST_DWithin queries against these tables for near-instant 250 m searches
-- across all 150+ databases.
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS pg_trgm;   -- for fast text search

-- ---------------------------------------------------------------------------
-- Portable core architecture requested for stored multi-continent screening
-- These names are kept explicit for import tooling, DBeaver workflows, and
-- parity with the SQLite fallback store used by the application runtime.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS datasets_master (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    category TEXT,
    country TEXT
);

CREATE TABLE IF NOT EXISTS geo_points (
    id SERIAL PRIMARY KEY,
    dataset_id INT REFERENCES datasets_master(id),
    name TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    address TEXT,
    risk_level TEXT,
    source TEXT
);

CREATE TABLE IF NOT EXISTS features (
    id SERIAL PRIMARY KEY,
    name TEXT,
    address TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    type TEXT
);

CREATE INDEX IF NOT EXISTS geo_points_lat_lng_idx
    ON geo_points (latitude, longitude);

CREATE INDEX IF NOT EXISTS features_lat_lng_idx
    ON features (latitude, longitude);

-- ---------------------------------------------------------------------------
-- Lookup: recognised database names and their GeoScope category
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS database_catalog (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(200) NOT NULL UNIQUE,  -- e.g. "RCRA LQG"
    category    VARCHAR(50)  NOT NULL,          -- contamination|regulatory|hydrology|geology|receptors
    source_org  VARCHAR(100),                   -- EPA|FEMA|USGS|USFWS|DOT|NOAA
    description TEXT
);

-- Pre-populate categories
INSERT INTO database_catalog (name, category, source_org) VALUES
  ('RCRA LQG',           'contamination', 'EPA'),
  ('RCRA SQG',           'contamination', 'EPA'),
  ('RCRA TSDF',          'contamination', 'EPA'),
  ('RCRA VSQG',          'contamination', 'EPA'),
  ('CERCLIS',            'contamination', 'EPA'),
  ('NPL',                'contamination', 'EPA'),
  ('PROPOSED NPL',       'contamination', 'EPA'),
  ('DELISTED NPL',       'contamination', 'EPA'),
  ('CORRACTS',           'contamination', 'EPA'),
  ('EPA LUST',           'contamination', 'EPA'),
  ('EPA UST',            'contamination', 'EPA'),
  ('FEMA UST',           'contamination', 'FEMA'),
  ('PFAS NPL',           'contamination', 'EPA'),
  ('PFAS FEDERAL SITES', 'contamination', 'EPA'),
  ('PFAS TRIS',          'contamination', 'EPA'),
  ('PFAS SPILLS',        'contamination', 'EPA'),
  ('TRIS',               'contamination', 'EPA'),
  ('PCB TRANSFORMER',    'contamination', 'EPA'),
  ('MGP',                'contamination', 'EPA'),
  ('BROWNFIELDS',        'contamination', 'EPA'),
  ('ECHO',               'regulatory',   'EPA'),
  ('NPDES',              'regulatory',   'EPA'),
  ('AIR FACILITY',       'regulatory',   'EPA'),
  ('RMP',                'regulatory',   'EPA'),
  ('ICIS',               'regulatory',   'EPA'),
  ('DOD',                'regulatory',   'DOD'),
  ('FUDS',               'regulatory',   'DOD'),
  ('FEDERAL FACILITY',   'regulatory',   'EPA'),
  ('FLOOD DFIRM',        'hydrology',    'FEMA'),
  ('FLOOD Q3',           'hydrology',    'FEMA'),
  ('WETLANDS NWI',       'hydrology',    'USFWS'),
  ('STORMWATER',         'hydrology',    'EPA'),
  ('HYDROLOGIC UNIT',    'hydrology',    'USGS'),
  ('MINES',              'geology',      'USGS'),
  ('MINE OPERATIONS',    'geology',      'USGS'),
  ('USGS GEOLOGIC AGE',  'geology',      'USGS'),
  ('SSURGO',             'geology',      'USDA'),
  ('STATSGO',            'geology',      'USDA'),
  ('RADON EPA',          'geology',      'EPA'),
  ('COAL ASH EPA',       'geology',      'EPA'),
  ('ASBESTOS NOA',       'geology',      'EPA'),
  ('SCHOOLS PUBLIC',     'receptors',    'NCES'),
  ('SCHOOLS PRIVATE',    'receptors',    'NCES'),
  ('HOSPITALS',          'receptors',    'HHS'),
  ('DAYCARE',            'receptors',    'HHS'),
  ('NURSING HOMES',      'receptors',    'HHS'),
  ('COLLEGES',           'receptors',    'NCES'),
  ('PRISONS',            'receptors',    'DOJ'),
  ('PIPELINES',          'other',        'DOT')
ON CONFLICT (name) DO NOTHING;

-- ---------------------------------------------------------------------------
-- Main environmental sites table
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS environmental_sites (
    id            BIGSERIAL  PRIMARY KEY,

    -- Spatial column (WGS-84, SRID 4326)
    location      GEOMETRY(POINT, 4326) NOT NULL,

    -- Classification
    database_name VARCHAR(200) NOT NULL,     -- e.g. "RCRA LQG"
    category      VARCHAR(50)  NOT NULL,     -- contamination|regulatory|hydrology|geology|receptors
    source_org    VARCHAR(100),              -- EPA|FEMA|USGS …

    -- Descriptive fields
    site_name     VARCHAR(500),
    address       TEXT,
    city          VARCHAR(200),
    state         CHAR(2),
    zip           VARCHAR(20),
    status        VARCHAR(100),             -- Active|Inactive|Closed …

    -- Source system identifiers
    registry_id   VARCHAR(100),            -- EPA FRS Registry ID etc.
    source_id     VARCHAR(100),            -- ID in the origin system

    -- Extra attributes stored as JSON (varies by source)
    attributes    JSONB,

    -- Audit
    imported_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Area features table (location-based intelligence model)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS area_features (
    id            BIGSERIAL PRIMARY KEY,
    osm_id        VARCHAR(80) UNIQUE,
    name          TEXT,
    type          VARCHAR(100),             -- building|road|wetland|school|hospital|landuse
    address       TEXT,
    latitude      DOUBLE PRECISION,
    longitude     DOUBLE PRECISION,
    geom          GEOMETRY(POINT, 4326) NOT NULL,
    attributes    JSONB,
    imported_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Indexes
-- ---------------------------------------------------------------------------

-- PostGIS spatial index (critical for 250 m radius queries)
CREATE INDEX IF NOT EXISTS env_sites_location_gist
    ON environmental_sites USING GIST (location);

-- Fast category + database lookups
CREATE INDEX IF NOT EXISTS env_sites_category_idx
    ON environmental_sites (category);

CREATE INDEX IF NOT EXISTS env_sites_database_idx
    ON environmental_sites (database_name);

CREATE UNIQUE INDEX IF NOT EXISTS env_sites_source_id_uidx
    ON environmental_sites (source_id)
    WHERE source_id IS NOT NULL;

-- Full-text search on site_name
CREATE INDEX IF NOT EXISTS env_sites_name_trgm
    ON environmental_sites USING GIN (site_name gin_trgm_ops);

CREATE INDEX IF NOT EXISTS area_features_geom_gist
    ON area_features USING GIST (geom);

CREATE INDEX IF NOT EXISTS area_features_type_idx
    ON area_features (type);

-- ---------------------------------------------------------------------------
-- The 250 m spatial search query
-- ---------------------------------------------------------------------------
-- Equivalent Node.js call:
--
--   const { rows } = await pgPool.query(`
--     SELECT
--       id,
--       database_name,
--       category,
--       site_name,
--       address,
--       status,
--       ST_X(location) AS lng,
--       ST_Y(location) AS lat,
--       ST_Distance(
--         location::geography,
--         ST_MakePoint($1, $2)::geography
--       ) AS distance_m
--     FROM environmental_sites
--     WHERE ST_DWithin(
--       location::geography,
--       ST_MakePoint($1, $2)::geography,   -- $1=lng, $2=lat
--       $3                                  -- $3=radius in metres
--     )
--     ORDER BY distance_m
--   `, [lng, lat, radius_m]);
--
-- To hook this in, replace nearbySearch() in gis-search.js with the above
-- query wrapped in a try/catch, falling back to the live API calls.

-- ---------------------------------------------------------------------------
-- Orders table (mirrors in-memory orders in server.js)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS orders (
    id                  BIGSERIAL PRIMARY KEY,
    project_name        VARCHAR(500) NOT NULL,
    client_name         VARCHAR(500),
    client_company      VARCHAR(500),
    recipient_email_1   VARCHAR(320) NOT NULL,
    recipient_email_2   VARCHAR(320),
    address             TEXT,
    latitude            DOUBLE PRECISION,
    longitude           DOUBLE PRECISION,
    polygon             JSONB,              -- GeoJSON Feature
    subject_property    JSONB,              -- GeoJSON Point Feature
    geo_input_type      VARCHAR(20) DEFAULT 'star',  -- star|polygon
    notes               TEXT,
    status              VARCHAR(50)  DEFAULT 'received',
    analyst_id          INTEGER,
    report_path         TEXT,
    report_url          TEXT,
    source              VARCHAR(100) DEFAULT 'client-portal',
    dataset_date        DATE,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Users table (mirrors auth.js users)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    id            SERIAL PRIMARY KEY,
    role          VARCHAR(20) NOT NULL CHECK (role IN ('admin','analyst','gis','client')),
    name          VARCHAR(200),
    company       VARCHAR(200),
    email         VARCHAR(320) NOT NULL UNIQUE,
    password_hash VARCHAR(200) NOT NULL,   -- bcrypt hash
    created_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Data import helper: bulk load a CSV into environmental_sites
-- ---------------------------------------------------------------------------
-- COPY environmental_sites (database_name, category, site_name, address, city, state, zip, status, registry_id, location)
-- FROM '/path/to/epa_echo_export.csv'
-- CSV HEADER;
--
-- For shapefiles, use ogr2ogr:
--   ogr2ogr -f "PostgreSQL" PG:"host=localhost dbname=geoscope user=postgres" \
--     rcra_facilities.shp -nln environmental_sites -t_srs EPSG:4326
