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
    class_code    VARCHAR(80),               -- external source classification code
    class_description VARCHAR(255),
    priority_tier VARCHAR(20) DEFAULT 'standard',
    priority_score INTEGER DEFAULT 0,
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

ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS class_code VARCHAR(80);
ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS class_description VARCHAR(255);
ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS priority_tier VARCHAR(20) DEFAULT 'standard';
ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS priority_score INTEGER DEFAULT 0;

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

CREATE INDEX IF NOT EXISTS env_sites_class_code_idx
    ON environmental_sites (class_code);

CREATE INDEX IF NOT EXISTS env_sites_priority_tier_idx
    ON environmental_sites (priority_tier);

CREATE INDEX IF NOT EXISTS env_sites_priority_score_idx
    ON environmental_sites (priority_score DESC);

CREATE INDEX IF NOT EXISTS env_sites_priority_db_idx
    ON environmental_sites (priority_tier, database_name);

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
-- Invoices table (billing workflow linked to orders)
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS invoices (
    id                  BIGSERIAL PRIMARY KEY,
    invoice_number      VARCHAR(40) UNIQUE,
    order_id            BIGINT REFERENCES orders(id) ON DELETE SET NULL,
    client_name         VARCHAR(500),
    client_company      VARCHAR(500),
    client_email        VARCHAR(320) NOT NULL,
    amount_cents        INTEGER NOT NULL CHECK (amount_cents >= 0),
    currency            VARCHAR(3) NOT NULL DEFAULT 'USD',
    status              VARCHAR(20) NOT NULL DEFAULT 'unpaid' CHECK (status IN ('unpaid', 'paid')),
    issued_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    due_date            DATE,
    paid_at             TIMESTAMPTZ,
    line_items          JSONB NOT NULL DEFAULT '[]'::jsonb,
    notes               TEXT,
    pdf_path            TEXT,
    pdf_url             TEXT,
    invoice_url         TEXT,
    created_by          INTEGER,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS invoices_client_email_idx ON invoices (client_email);
CREATE INDEX IF NOT EXISTS invoices_order_id_idx ON invoices (order_id);
CREATE INDEX IF NOT EXISTS invoices_status_idx ON invoices (status);
CREATE INDEX IF NOT EXISTS invoices_created_at_idx ON invoices (created_at DESC);

-- ---------------------------------------------------------------------------
-- Durable generated report archive
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS generated_reports (
    order_id            BIGINT PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
    file_name           TEXT NOT NULL,
    mime_type           TEXT NOT NULL DEFAULT 'application/pdf',
    pdf_data            BYTEA NOT NULL,
    storage_path        TEXT,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS generated_reports_created_at_idx ON generated_reports (created_at DESC);

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
-- Schema extensions for federal bulk import (run once after initial setup)
-- ---------------------------------------------------------------------------

-- Unique constraint required by federal-bulk-download.js ON CONFLICT DO NOTHING
ALTER TABLE environmental_sites ADD COLUMN IF NOT EXISTS source_name TEXT;
DO $$ BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'environmental_sites_source_id_key'
    AND conrelid = 'environmental_sites'::regclass
  ) THEN
    ALTER TABLE environmental_sites
      ADD CONSTRAINT environmental_sites_source_id_key UNIQUE (source_id);
  END IF;
EXCEPTION WHEN OTHERS THEN NULL;
END $$;

-- Additional indexes for federal dataset queries
CREATE INDEX IF NOT EXISTS env_sites_source_name_idx  ON environmental_sites (source_name);
CREATE INDEX IF NOT EXISTS env_sites_source_org_idx   ON environmental_sites (source_org);
CREATE INDEX IF NOT EXISTS env_sites_state_idx        ON environmental_sites (state);
CREATE INDEX IF NOT EXISTS env_sites_zip_idx          ON environmental_sites (zip);
CREATE INDEX IF NOT EXISTS env_sites_composite_idx    ON environmental_sites (state, category, priority_tier);

-- Pre-populate database_catalog with all federal dataset names
-- (used by server.js getRiskLevel, categorizeSites, buildDatabaseDescriptionsHtml)
INSERT INTO database_catalog (name, category, description, risk_level, source_org) VALUES
  ('RCRA LQG',                  'contamination', 'Large Quantity Hazardous Waste Generator',    'High',     'EPA'),
  ('RCRA SQG',                  'contamination', 'Small Quantity Hazardous Waste Generator',    'Medium',   'EPA'),
  ('RCRA CESQG',                'contamination', 'Conditionally Exempt Small Quantity Generator','Low',     'EPA'),
  ('RCRA TSD',                  'contamination', 'Hazardous Waste Treatment Storage Disposal Facility', 'High', 'EPA'),
  ('RCRA Handler',              'contamination', 'RCRA Regulated Hazardous Waste Handler',      'Medium',   'EPA'),
  ('RCRAInfo',                  'contamination', 'EPA Resource Conservation and Recovery Act Info System', 'High', 'EPA'),
  ('NPDES',                     'hydrology',     'National Pollutant Discharge Elimination System Permit', 'High', 'EPA'),
  ('NPDES Major',               'hydrology',     'Major NPDES Permitted Discharger',            'High',     'EPA'),
  ('NPDES Minor',               'hydrology',     'Minor NPDES Permitted Discharger',            'Medium',   'EPA'),
  ('ICIS-Air',                  'contamination', 'Clean Air Act Stationary Source',             'Medium',   'EPA'),
  ('CAA Major Source',          'contamination', 'Clean Air Act Title V Major Source',          'High',     'EPA'),
  ('Air Emissions',             'contamination', 'Air Emissions Facility (NEI/GHGRP/TRI/CAMD)', 'High',    'EPA'),
  ('SDWA Public Water System',  'hydrology',     'Safe Drinking Water Act Public Water System', 'Medium',   'EPA'),
  ('SDWA CWS',                  'hydrology',     'Community Water System',                      'Medium',   'EPA'),
  ('TRI',                       'contamination', 'Toxic Release Inventory Facility',            'High',     'EPA'),
  ('TRI Release',               'contamination', 'Toxic Chemical Release Record',               'High',     'EPA'),
  ('Superfund NPL',             'contamination', 'National Priorities List Superfund Site',     'High',     'EPA'),
  ('Superfund SEMS',            'contamination', 'CERCLA/Superfund Environmental Management System Site', 'High', 'EPA'),
  ('EPA Brownfields',           'contamination', 'EPA Brownfields Cleanup Site',                'Medium',   'EPA'),
  ('UST',                       'contamination', 'Underground Storage Tank Facility',           'Medium',   'EPA'),
  ('LUST',                      'contamination', 'Leaking Underground Storage Tank Site',       'High',     'EPA'),
  ('ICIS FE&C',                 'regulatory',    'Federal Enforcement and Compliance Monitoring','High',    'EPA'),
  ('EPA ECHO',                  'regulatory',    'EPA Enforcement and Compliance History Online Facility', 'Medium', 'EPA'),
  ('EPA FRS',                   'regulatory',    'EPA Facility Registry Service Master Record', 'Low',      'EPA'),
  ('GHG Reporting',             'contamination', 'EPA Greenhouse Gas Reporting Program Facility','Medium',  'EPA'),
  ('FEMA NFIP Flood Claims',    'hydrology',     'FEMA National Flood Insurance Program Claim', 'High',     'FEMA'),
  ('FEMA NFIP Policies',        'hydrology',     'FEMA National Flood Insurance Program Policy','Medium',   'FEMA'),
  ('FEMA Flood Map',            'hydrology',     'FEMA Flood Insurance Rate Map Special Flood Hazard Area', 'High', 'FEMA')
ON CONFLICT (name) DO NOTHING;

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
