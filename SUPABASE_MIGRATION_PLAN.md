# Supabase Migration Plan — User Authentication & Data Layer

## Executive Summary

This document provides a detailed implementation plan for integrating Supabase into the Geoscope platform for user authentication, order management, and real-time features. The Postgres PostGIS database will remain for spatial/environmental data.

**Timeline:** 3 phases over 6 weeks
**Cost:** $105/month (self-hosted Postgres $100 + Supabase auth/free tier)
**Risk:** Low (Postgres remains unchanged; Supabase is additive)

---

## Phase 1: Supabase Project Setup (Week 1-2)

### 1.1 Create Supabase Project

```bash
# Visit https://supabase.com and create new project
# Select: US East region (closest to existing Postgres)
# Database: PostgreSQL 15+
# Password: Generate strong password (minimum 32 characters)
```

### 1.2 Supabase Project Details

After creation, you'll have:
- **Supabase URL:** `https://[project-id].supabase.co`
- **Anon Key:** (for client-side calls)
- **Service Role Key:** (for server-side calls only)
- **Postgres Connection:** Direct connection string

### 1.3 Create Tables in Supabase

Execute these SQL commands in Supabase SQL editor:

```sql
-- User Profiles
CREATE TABLE user_profiles (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  auth_id UUID REFERENCES auth.users(id) ON DELETE CASCADE,
  email VARCHAR(255) UNIQUE NOT NULL,
  company_name VARCHAR(255),
  role VARCHAR(50) DEFAULT 'client', -- 'admin', 'analyst', 'client'
  subscription_tier VARCHAR(50) DEFAULT 'free', -- 'free', 'standard', 'premium', 'enterprise'
  phone_number VARCHAR(20),
  avatar_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders
CREATE TABLE orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES user_profiles(id) ON DELETE SET NULL,
  property_address VARCHAR(255) NOT NULL,
  latitude FLOAT,
  longitude FLOAT,
  report_status VARCHAR(50) DEFAULT 'pending', -- 'pending', 'processing', 'completed', 'failed'
  pdf_url TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  completed_at TIMESTAMP,
  total_amount DECIMAL(10, 2),
  notes TEXT
);

-- Subscriptions
CREATE TABLE subscriptions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID UNIQUE REFERENCES user_profiles(id) ON DELETE CASCADE,
  plan VARCHAR(50), -- 'free', 'standard', 'premium'
  monthly_price DECIMAL(10, 2),
  reports_limit INT DEFAULT 5,
  reports_used INT DEFAULT 0,
  renewal_date DATE,
  status VARCHAR(50) DEFAULT 'active', -- 'active', 'canceled', 'expired'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Invoices
CREATE TABLE invoices (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES user_profiles(id) ON DELETE SET NULL,
  amount DECIMAL(10, 2),
  status VARCHAR(50) DEFAULT 'unpaid', -- 'unpaid', 'paid', 'failed'
  invoice_url TEXT,
  issued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  due_date DATE,
  paid_at TIMESTAMP
);

-- Favorite Locations
CREATE TABLE favorite_locations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id UUID REFERENCES user_profiles(id) ON DELETE CASCADE,
  address VARCHAR(255),
  latitude FLOAT,
  longitude FLOAT,
  name VARCHAR(100),
  notes TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Contact Submissions
CREATE TABLE contact_submissions (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  email VARCHAR(255),
  subject VARCHAR(255),
  message TEXT,
  status VARCHAR(50) DEFAULT 'new', -- 'new', 'read', 'responded'
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for performance
CREATE INDEX idx_user_profiles_email ON user_profiles(email);
CREATE INDEX idx_user_profiles_role ON user_profiles(role);
CREATE INDEX idx_orders_user_id ON orders(user_id);
CREATE INDEX idx_orders_created_at ON orders(created_at DESC);
CREATE INDEX idx_subscriptions_user_id ON subscriptions(user_id);
CREATE INDEX idx_favorite_locations_user_id ON favorite_locations(user_id);
```

### 1.4 Enable Authentication Providers

In Supabase dashboard → Authentication → Providers:

**Enable:**
- Email/Password ✅
- Google OAuth
- GitHub OAuth
- (Optional) Microsoft, Apple

**Configuration:**
```javascript
// Google OAuth
- Client ID: [Get from Google Cloud Console]
- Client Secret: [Get from Google Cloud Console]

// GitHub OAuth
- Client ID: [Get from GitHub Settings → Developer Settings]
- Client Secret: [Get from GitHub Settings]
```

### 1.5 Configure Row Level Security (RLS)

```sql
-- Enable RLS on all tables
ALTER TABLE user_profiles ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE invoices ENABLE ROW LEVEL SECURITY;
ALTER TABLE favorite_locations ENABLE ROW LEVEL SECURITY;
ALTER TABLE contact_submissions ENABLE ROW LEVEL SECURITY;

-- Policy: Users can only see their own data
CREATE POLICY "Users can read own data"
  ON user_profiles FOR SELECT
  USING (auth.uid() = auth_id);

CREATE POLICY "Users can update own profile"
  ON user_profiles FOR UPDATE
  USING (auth.uid() = auth_id);

-- Similar policies for other tables...
```

---

## Phase 2: Backend Integration (Week 3-4)

### 2.1 Install Supabase Client

```bash
cd /path/to/geoscope
npm install @supabase/supabase-js
npm install dotenv  # if not already installed
```

### 2.2 Add Environment Variables

Update `.env`:

```env
# Supabase Configuration
SUPABASE_URL=https://[project-id].supabase.co
SUPABASE_ANON_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
SUPABASE_SERVICE_ROLE_KEY=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...

# Keep existing Postgres for spatial data
PG_HOST=localhost
PG_PORT=5432
PG_DATABASE=geoscope
PG_USER=postgres
PG_PASSWORD=2030

# Existing settings
JWT_SECRET=replace-with-long-random-secret
PORT=5000
```

### 2.3 Create Supabase Client in server.js

Add to [geoscope/server.js](./server.js):

```javascript
// ===== SUPABASE INITIALIZATION =====
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const supabaseAnonKey = process.env.SUPABASE_ANON_KEY;

// Server-side client (use service key)
const supabaseAdmin = supabaseUrl && supabaseServiceKey
  ? createClient(supabaseUrl, supabaseServiceKey)
  : null;

// For client-side calls (use anon key)
const supabaseClient = supabaseUrl && supabaseAnonKey
  ? createClient(supabaseUrl, supabaseAnonKey)
  : null;

if (!supabaseAdmin) {
  console.warn('⚠️  Supabase not configured. User features will be limited.');
}
```

### 2.4 Update Auth Endpoints

**POST /auth/register** — Create Supabase user:

```javascript
app.post('/auth/register', async (req, res) => {
  const { name, email, password, role = 'client', company = '' } = req.body;

  if (!name || !email || !password) {
    return res.status(400).json({ error: 'Missing required fields' });
  }

  try {
    // Create user in Supabase
    if (!supabaseAdmin) {
      return res.status(500).json({ error: 'Auth service unavailable' });
    }

    const { data: authUser, error: authError } = await supabaseAdmin.auth.admin.createUser({
      email: email,
      password: password,
      email_confirm: false
    });

    if (authError) {
      return res.status(400).json({ error: authError.message });
    }

    // Create user profile
    const { data: profile, error: profileError } = await supabaseAdmin
      .from('user_profiles')
      .insert([{
        auth_id: authUser.user.id,
        email: email.toLowerCase(),
        company_name: company,
        role: role
      }])
      .select()
      .single();

    if (profileError) {
      // Clean up: delete auth user if profile creation fails
      await supabaseAdmin.auth.admin.deleteUser(authUser.user.id);
      return res.status(400).json({ error: 'Failed to create user profile' });
    }

    // Generate JWT token
    const token = jwt.sign(
      { userId: authUser.user.id, email, role, company },
      process.env.JWT_SECRET,
      { expiresIn: '24h' }
    );

    return res.status(201).json({
      success: true,
      message: 'User registered successfully',
      token,
      user: { id: authUser.user.id, email, role, company }
    });

  } catch (error) {
    console.error('Registration error:', error);
    res.status(500).json({ error: 'Registration failed' });
  }
});
```

**POST /auth/login** — Authenticate via Supabase:

```javascript
app.post('/auth/login', async (req, res) => {
  const { email, password } = req.body;

  if (!email || !password) {
    return res.status(400).json({ error: 'Email and password required' });
  }

  try {
    // Authenticate with Supabase
    const { data: { session }, error } = await supabaseClient.auth.signInWithPassword({
      email,
      password
    });

    if (error || !session) {
      return res.status(401).json({ error: 'Invalid credentials' });
    }

    // Get user profile
    const { data: profile } = await supabaseAdmin
      .from('user_profiles')
      .select('*')
      .eq('auth_id', session.user.id)
      .single();

    // Generate JWT token
    const token = jwt.sign(
      { userId: session.user.id, email: session.user.email, role: profile?.role },
      process.env.JWT_SECRET,
      { expiresIn: '24h' }
    );

    return res.json({
      success: true,
      token,
      user: { 
        id: session.user.id, 
        email: session.user.email, 
        role: profile?.role 
      }
    });

  } catch (error) {
    console.error('Login error:', error);
    res.status(500).json({ error: 'Login failed' });
  }
});
```

### 2.5 Create Order in Supabase

**POST /orders** — Save order to Supabase:

```javascript
app.post('/orders', authenticateToken, async (req, res) => {
  const { property_address, latitude, longitude } = req.body;
  const userId = req.user.userId;

  if (!supabaseAdmin) {
    return res.status(500).json({ error: 'Database service unavailable' });
  }

  try {
    const { data: order, error } = await supabaseAdmin
      .from('orders')
      .insert([{
        user_id: userId,
        property_address,
        latitude,
        longitude,
        report_status: 'pending'
      }])
      .select()
      .single();

    if (error) {
      return res.status(400).json({ error: error.message });
    }

    res.json({ success: true, order });

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
```

**GET /orders** — Retrieve user orders:

```javascript
app.get('/orders', authenticateToken, async (req, res) => {
  const userId = req.user.userId;

  if (!supabaseAdmin) {
    return res.status(500).json({ error: 'Database service unavailable' });
  }

  try {
    const { data: orders, error } = await supabaseAdmin
      .from('orders')
      .select('*')
      .eq('user_id', userId)
      .order('created_at', { ascending: false });

    if (error) {
      return res.status(400).json({ error: error.message });
    }

    res.json({ success: true, orders });

  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});
```

---

## Phase 3: Frontend Integration (Week 5-6)

### 3.1 Create Supabase Auth Hook (React)

Create [geoscope-workbench/src/hooks/useAuth.js](../workbench/src/hooks/useAuth.js):

```javascript
import { useContext, createContext, useState, useEffect } from 'react';
import { createClient } from '@supabase/supabase-js';

const AuthContext = createContext();

const supabase = createClient(
  process.env.REACT_APP_SUPABASE_URL,
  process.env.REACT_APP_SUPABASE_ANON_KEY
);

export function AuthProvider({ children }) {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check if user is already authenticated
    supabase.auth.onAuthStateChange((event, session) => {
      setUser(session?.user ?? null);
      setLoading(false);
    });
  }, []);

  const signUp = async (email, password) => {
    const { data, error } = await supabase.auth.signUp({ email, password });
    return { user: data?.user, error };
  };

  const signIn = async (email, password) => {
    const { data, error } = await supabase.auth.signInWithPassword({ email, password });
    return { user: data?.user, error };
  };

  const signOut = async () => {
    return supabase.auth.signOut();
  };

  const signInWithGoogle = async () => {
    const { data, error } = await supabase.auth.signInWithOAuth({
      provider: 'google',
      options: { redirectTo: `${window.location.origin}/dashboard` }
    });
    return { error };
  };

  return (
    <AuthContext.Provider value={{ user, loading, signUp, signIn, signOut, signInWithGoogle }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  return useContext(AuthContext);
}
```

### 3.2 Update App.js to use Auth

```javascript
// In App.js
import { AuthProvider } from './hooks/useAuth';

function App() {
  return (
    <AuthProvider>
      <Router>
        {/* Routes */}
      </Router>
    </AuthProvider>
  );
}
```

### 3.3 Create Login Page with Supabase

Create [geoscope-workbench/src/pages/LoginPage.js](../workbench/src/pages/LoginPage.js):

```javascript
import React, { useState } from 'react';
import { useAuth } from '../hooks/useAuth';
import { useNavigate } from 'react-router-dom';

export function LoginPage() {
  const { signIn, signInWithGoogle } = useAuth();
  const navigate = useNavigate();
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState(null);

  const handleLogin = async (e) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    const { user, error: signInError } = await signIn(email, password);
    
    if (signInError) {
      setError(signInError.message);
    } else if (user) {
      navigate('/dashboard');
    }
    
    setLoading(false);
  };

  const handleGoogleLogin = async () => {
    setLoading(true);
    const { error } = await signInWithGoogle();
    
    if (error) {
      setError(error.message);
    }
    
    setLoading(false);
  };

  return (
    <div className="login-page">
      <div className="login-container">
        <h1>Login to Geoscope</h1>
        
        {error && <div className="error-message">{error}</div>}
        
        <form onSubmit={handleLogin}>
          <input
            type="email"
            placeholder="Email"
            value={email}
            onChange={(e) => setEmail(e.target.value)}
            required
          />
          <input
            type="password"
            placeholder="Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            required
          />
          <button type="submit" disabled={loading}>
            {loading ? 'Logging in...' : 'Login'}
          </button>
        </form>

        <div className="divider">OR</div>

        <button onClick={handleGoogleLogin} className="google-btn">
          🔐 Login with Google
        </button>

        <p className="signup-link">
          Don't have an account? <a href="/signup">Sign up here</a>
        </p>
      </div>
    </div>
  );
}
```

---

## Cost Breakdown

| Service | Cost | Notes |
|---------|------|-------|
| **Supabase Free** | $0 | Up to 50,000 monthly active users, 1 GB database |
| **Supabase Auth** | $0-50 | Free tier included; paid for high volume |
| **Self-hosted Postgres** | $100/month | AWS RDS small instance (t2.small) |
| **PostGIS Extension** | Included | Free with Postgres |
| **Vercel** | $20/month | Pro plan for priority deployment |
| **Domain/SSL** | ~$15/year | Geoscopesolutions.com |
| **Monthly Total** | **$105-120/month** | All features for 100+ concurrent users |

---

## Data Migration Strategy

### Step 1: Export Existing Users

```bash
# From your current auth system:
SELECT id, email, role, created_at FROM users INTO OUTFILE 'users_export.csv';
```

### Step 2: Migrate to Supabase

```javascript
// Script to migrate users
const fs = require('fs');
const csv = require('csv-parser');
const { createClient } = require('@supabase/supabase-js');

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_SERVICE_ROLE_KEY);

fs.createReadStream('users_export.csv')
  .pipe(csv())
  .on('data', async (row) => {
    // Create user in Supabase
    await supabase.auth.admin.createUser({
      email: row.email,
      email_confirm: true
    });

    // Create profile
    await supabase.from('user_profiles').insert([{
      email: row.email,
      role: row.role,
      created_at: row.created_at
    }]);
  });
```

### Step 3: Keep Postgres for Spatial Data

All environmental_sites, GIS queries, and PostGIS data remain in self-hosted Postgres:
- `environmental_sites` table (15M+ records)
- All spatial indexes and functions
- Maintains sub-2-second query performance

---

## Implementation Checklist

### Phase 1 (Week 1-2)
- [ ] Create Supabase project
- [ ] Create tables and indexes
- [ ] Enable RLS policies
- [ ] Configure OAuth providers
- [ ] Test Supabase SQL editor
- [ ] Document project credentials

### Phase 2 (Week 3-4)
- [ ] Install Supabase JS client
- [ ] Add environment variables
- [ ] Implement auth endpoints
- [ ] Implement order endpoints
- [ ] Test auth flows
- [ ] Deploy to Vercel

### Phase 3 (Week 5-6)
- [ ] Create useAuth hook
- [ ] Build login page
- [ ] Build signup page
- [ ] Integrate with Dashboard
- [ ] Test OAuth flows
- [ ] Performance testing

---

## Security Considerations

### 1. API Keys Protection

```env
# .env (LOCAL ONLY)
SUPABASE_SERVICE_ROLE_KEY=secret_key_never_commit

# .env.example (COMMIT THIS)
SUPABASE_SERVICE_ROLE_KEY=your_key_here

# .gitignore (ENSURE IT INCLUDES)
.env
.env.local
*.key
```

### 2. Row Level Security

All Supabase tables use RLS to ensure users can only access their own data:

```sql
CREATE POLICY "Users see own profile"
  ON user_profiles FOR SELECT
  USING (auth.uid() = auth_id);
```

### 3. JWT Validation

All protected endpoints validate the JWT token:

```javascript
function authenticateToken(req, res, next) {
  const authHeader = req.headers['authorization'];
  const token = authHeader && authHeader.split(' ')[1];

  if (!token) return res.sendStatus(401);

  jwt.verify(token, process.env.JWT_SECRET, (err, user) => {
    if (err) return res.sendStatus(403);
    req.user = user;
    next();
  });
}
```

---

## Rollback Plan

If issues occur, rollback is simple:

1. **Revert auth endpoints** to use original system
2. **Disable Supabase** routes
3. **Keep Postgres unchanged** (all spatial data intact)
4. **No data loss** (nothing permanent deleted)

---

## Testing Checklist

```bash
# 1. Test user registration
curl -X POST http://localhost:5000/auth/register \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","email":"test@example.com","password":"pass123"}'

# 2. Test login
curl -X POST http://localhost:5000/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","password":"pass123"}'

# 3. Test creating order
curl -X POST http://localhost:5000/orders \
  -H "Authorization: Bearer [TOKEN]" \
  -H "Content-Type: application/json" \
  -d '{"property_address":"123 Main St","latitude":40.7128,"longitude":-74.0060}'

# 4. Test GIS search (should still work)
curl http://localhost:5000/nearby-search?lat=40.7128&lng=-74.0060&radius=1000
```

---

## Support & Resources

- **Supabase Docs:** https://supabase.com/docs
- **Supabase Auth Reference:** https://supabase.com/docs/reference/javascript/auth
- **Row Level Security:** https://supabase.com/docs/guides/auth/row-level-security
- **PostgreSQL Docs:** https://www.postgresql.org/docs/

---

## Next Steps

1. **Create Supabase account** at https://supabase.com
2. **Follow Phase 1** to set up database
3. **Follow Phase 2** to integrate backend
4. **Follow Phase 3** to update frontend
5. **Run testing checklist** to validate
6. **Monitor logs** during first week of production use

**Expected Timeline:** 4-6 weeks for full integration
**Recommended Start:** After current feature release stabilizes

---

Generated: 2026-04-25
Next Review: After Phase 1 completion
