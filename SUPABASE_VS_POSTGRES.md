# Supabase vs Postgres Architecture Recommendation

## Executive Summary
**Recommendation: Hybrid approach** — Keep self-hosted Postgres for GIS/spatial data, add Supabase for auth, real-time features, and user-facing functionality.

---

## Current Architecture Analysis

### ✅ What Works Well with Self-Hosted Postgres
Your system currently uses PostgreSQL with **PostGIS extension** for:
- **15M+ environmental site records** with spatial indexing (ST_DWithin queries)
- **Class codes, priority tiers** — structured relational data
- **500+ complex analytical queries** for GIS searches
- **High-volume batch imports** (50M+ records)

**Cost per month:** ~$50-100 for cloud-hosted Postgres (AWS RDS, DigitalOcean, etc.)

---

## Supabase: What It Offers

### Core Features
| Feature | Postgres (self) | Supabase |
|---------|-----------------|----------|
| **Authentication** | Manual JWT setup | ✅ Pre-built, 0-config OAuth |
| **Real-time Subscriptions** | No | ✅ WebSocket-based |
| **PostgREST API** | Manual setup | ✅ Auto-generated REST API |
| **Vector Search** | pgvector extension | ✅ Built-in |
| **Storage** | Requires S3 config | ✅ Bucket management UI |
| **Edge Functions** | Not included | ✅ Serverless functions |
| **Pricing** | $50-300/month | **$25-250/month** |

---

## Recommended Data Split

### 📍 **Self-Hosted Postgres** (Keep As-Is)
Store high-volume, spatial-indexed data:
- ✅ **environmental_sites** (217 database categories, 15M+ records)
- ✅ **class_codes, priority_tiers** (indexed for fast sorting)
- ✅ **location geometries** (PostGIS ST_DWithin queries)
- ✅ **Temporary search cache** (results lifecycle: 24 hours)

**Why:** PostGIS performance is unmatched for spatial queries. Self-hosting avoids Supabase's row-count limits on free tier.

---

### 🔐 **Supabase** (Add For These)
Store user-facing, real-time data:

#### **1. User Accounts & Roles**
```sql
-- Supabase auth_users (managed)
-- - Email, password hashing
-- - OAuth integrations (Google, GitHub)
-- - Session management
-- - MFA/2FA

-- Custom profiles table
CREATE TABLE user_profiles (
  id UUID PRIMARY KEY REFERENCES auth.users(id),
  company_name VARCHAR(255),
  role VARCHAR(50), -- analyst, client, admin
  subscription_tier VARCHAR(50), -- premium, standard
  created_at TIMESTAMP,
  updated_at TIMESTAMP
);
```

#### **2. Order & Report History**
```sql
CREATE TABLE orders (
  id UUID PRIMARY KEY,
  user_id UUID REFERENCES user_profiles(id),
  property_address TEXT,
  report_status VARCHAR(50), -- pending, generating, complete
  latitude DECIMAL,
  longitude DECIMAL,
  created_at TIMESTAMP,
  completed_at TIMESTAMP,
  pdf_url TEXT,
  -- Real-time updates via Supabase subscriptions
);

CREATE TABLE report_metrics (
  order_id UUID REFERENCES orders(id),
  sites_found INT,
  databases_matched INT,
  risk_score INT,
  UNIQUE(order_id)
);
```

#### **3. Favorites & Search History**
```sql
CREATE TABLE favorite_locations (
  id UUID PRIMARY KEY,
  user_id UUID REFERENCES user_profiles(id),
  name VARCHAR(255),
  latitude DECIMAL,
  longitude DECIMAL,
  notes TEXT,
  created_at TIMESTAMP,
  UNIQUE(user_id, name)
);

-- Real-time subscription example (frontend):
-- supabase
//   .from('favorite_locations')
//   .on('*', payload => console.log('Updated:', payload))
//   .subscribe()
```

#### **4. Billing & Payments**
```sql
CREATE TABLE subscriptions (
  id UUID PRIMARY KEY,
  user_id UUID REFERENCES user_profiles(id),
  plan VARCHAR(50), -- starter, professional, enterprise
  monthly_price INT, -- in cents
  reports_limit INT, -- null = unlimited
  reports_used INT DEFAULT 0,
  renewal_date DATE,
  status VARCHAR(50), -- active, cancelled, expired
  stripe_customer_id VARCHAR(255),
  created_at TIMESTAMP
);

CREATE TABLE invoices (
  id UUID PRIMARY KEY,
  subscription_id UUID REFERENCES subscriptions(id),
  amount INT,
  pdf_url TEXT,
  stripe_invoice_id VARCHAR(255),
  created_at TIMESTAMP
);
```

#### **5. Contact/Support Messages**
```sql
CREATE TABLE contact_submissions (
  id UUID PRIMARY KEY,
  email VARCHAR(255),
  subject VARCHAR(255),
  message TEXT,
  status VARCHAR(50), -- new, read, responded
  created_at TIMESTAMP
);
```

---

## Migration Path (Step by Step)

### **Phase 1: Add Supabase (Week 1-2)**
1. Create Supabase project
2. Migrate user auth from JWT → Supabase Auth
3. Create orders, subscriptions, profiles tables
4. Update frontend to use Supabase client:
   ```javascript
   import { createClient } from '@supabase/supabase-js';
   const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);
   
   // Sign up
   const { user, error } = await supabase.auth.signUp({
     email, password
   });
   
   // Fetch user's orders (real-time)
   supabase
     .from('orders')
     .on('*', payload => setState(payload.new))
     .subscribe();
   ```

### **Phase 2: Real-Time Features (Week 3-4)**
1. WebSocket subscriptions for order status
2. Live report generation progress
3. Notifications when reports complete

### **Phase 3: Postgres Optimization (Week 5+)**
1. Keep PostGIS for spatial searches
2. Add connection pooling (PgBouncer)
3. Backup strategy via AWS RDS snapshots

---

## Cost Comparison

| Component | Self-Hosted | Supabase | Hybrid (Recommended) |
|-----------|-------------|----------|---------------------|
| **Postgres (15M+ records)** | $100/month | N/A (row limits) | $100 |
| **User Auth** | $0 (JWT) | Free tier | $0 (Supabase) |
| **Real-time** | $0 | Free tier | $0 (Supabase) |
| **Storage** | $20 | $5 | $5 |
| **Monthly Total** | **$120** | **$30-100** | **$105** |

---

## Implementation Checklist

### Immediate (Today)
- [x] Create Supabase project
- [ ] Copy current Postgres schema to Supabase
- [ ] Set up Supabase auth
- [ ] Update server.js to use Supabase client for auth
- [ ] Deploy updated backend

### Short-term (This Month)
- [ ] Migrate user/order data to Supabase
- [ ] Add Supabase real-time subscriptions
- [ ] Update frontend components
- [ ] Test OAuth flows (Google, GitHub)

### Long-term (Q2)
- [ ] Implement advanced real-time notifications
- [ ] Add Supabase Edge Functions for webhooks
- [ ] Optimize Postgres for 50M+ record ingestion

---

## Pros & Cons Summary

### Supabase ✅
- Pre-built auth, 0 config
- Real-time WebSocket support
- Simple row-level security (RLS)
- Free tier for small projects
- Managed backups
- GraphQL support (optional)

### Supabase ❌
- Free tier: 500MB storage limit
- Real-time can be unreliable under heavy load
- Less control vs self-hosted
- Row limits on free tier

### Self-Hosted Postgres ✅
- Full control & customization
- PostGIS spatial power
- Cost-effective at scale (15M+ records)
- No vendor lock-in

### Self-Hosted Postgres ❌
- Manual auth management
- No built-in real-time
- Requires DevOps knowledge
- Self-service backups

---

## Recommendation

**Use BOTH:**
1. **Keep** self-hosted Postgres + PostGIS → environmental_sites, spatial data
2. **Add** Supabase → user auth, orders, subscriptions, real-time
3. **Connect** them via server.js middleware:
   ```javascript
   // middleware/auth.js
   const { data: user } = await supabase.auth.getUser(token);
   
   // Then query Postgres for GIS
   const sites = await pgPool.query('SELECT * FROM environmental_sites WHERE ...');
   ```

This gives you:
- ✅ Best-in-class spatial querying
- ✅ Modern auth & real-time
- ✅ Cost efficiency
- ✅ Scalability to 50M+ records
- ✅ Zero vendor lock-in

---

## Next Steps

1. **Create Supabase project** at https://supabase.com
2. **Copy user schema** from current Postgres
3. **Update server.js** to accept both connections
4. **Deploy** and test both databases together
5. **Monitor** performance for 1 week
6. **Migrate user data** once validated

Would you like me to help with the Supabase setup or update the server.js integration?
