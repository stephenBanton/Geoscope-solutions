const crypto = require('crypto');
const path = require('path');
const fs = require('fs');
const { pool } = require('./db');

const JWT_SECRET = process.env.JWT_SECRET || 'your-super-secret-jwt-key-change-in-production';
let DB_DIR = path.join(__dirname, '.data');
let USERS_FILE = path.join(DB_DIR, 'users.json');
let ORDERS_FILE = path.join(DB_DIR, 'orders.json');
let REPORTS_FILE = path.join(DB_DIR, 'reports.json');
const STAFF_EMAIL_DOMAINS = (process.env.STAFF_EMAIL_DOMAINS || 'geoscope.com,geoscopesolutions.com')
  .split(',')
  .map((d) => d.trim().toLowerCase())
  .filter(Boolean);
const STAFF_AUTO_PROVISION = String(process.env.STAFF_AUTO_PROVISION || 'true').toLowerCase() !== 'false';
let usersStoreReadyPromise = null;

const OFFICIAL_STAFF_SEEDS = [
  { name: 'Admin', email: 'admin@geoscope.com', password: 'admin123', role: 'admin', company: 'GeoScope' },
  { name: 'Admin', email: 'admin@geoscopesolutions.com', password: 'Solutions@2026', role: 'admin', company: 'GeoScope' },
  { name: 'Analyst', email: 'analyst@geoscope.com', password: 'analyst123', role: 'analyst', company: 'GeoScope' },
  { name: 'Analyst', email: 'analyst@geoscopesolutions.com', password: 'Solutions@2026', role: 'analyst', company: 'GeoScope' },
  { name: 'GIS Admin', email: 'gis@geoscope.com', password: 'gis123', role: 'gis', company: 'GeoScope' },
  { name: 'GIS Admin', email: 'gis@geoscopesolutions.com', password: 'Solutions@2026', role: 'gis', company: 'GeoScope' }
];

function isStaffRole(role) {
  return ['admin', 'analyst', 'gis'].includes(String(role || '').toLowerCase());
}

function isAllowedStaffEmail(email) {
  const domain = String(email || '').split('@')[1]?.toLowerCase() || '';
  if (!domain) return false;
  return STAFF_EMAIL_DOMAINS.includes(domain);
}

function buildDisplayNameFromEmail(email) {
  const local = String(email || '').split('@')[0] || 'Analyst';
  return local
    .split(/[._-]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ') || 'Analyst';
}

function normalizeUserRecord(user = {}) {
  return {
    id: Number(user.id),
    name: user.name || '',
    email: String(user.email || '').toLowerCase(),
    password: String(user.password || user.password_hash || ''),
    role: String(user.role || 'client').toLowerCase(),
    company: user.company || '',
    created_at: user.created_at,
    updated_at: user.updated_at,
    auto_provisioned: Boolean(user.auto_provisioned)
  };
}

function toSafeUser(user = {}) {
  return {
    id: Number(user.id),
    name: user.name || '',
    email: String(user.email || '').toLowerCase(),
    role: String(user.role || 'client').toLowerCase(),
    company: user.company || ''
  };
}

function buildSeedUsers() {
  const seededUsers = [];

  try {
    const fileUsers = readUsers();
    for (const user of fileUsers) {
      seededUsers.push(normalizeUserRecord(user));
    }
  } catch (_) {
    // Ignore file seed failures; official seed users below still apply.
  }

  for (const user of OFFICIAL_STAFF_SEEDS) {
    seededUsers.push(normalizeUserRecord(user));
  }

  const deduped = new Map();
  for (const user of seededUsers) {
    if (!user.email) continue;
    deduped.set(user.email, user);
  }

  return Array.from(deduped.values());
}

async function ensurePersistentUsersStore() {
  if (!usersStoreReadyPromise) {
    usersStoreReadyPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS users (
          id SERIAL PRIMARY KEY,
          role VARCHAR(20) NOT NULL CHECK (role IN ('admin','analyst','gis','client')),
          name VARCHAR(200),
          company VARCHAR(200),
          email VARCHAR(320) NOT NULL UNIQUE,
          password_hash VARCHAR(200) NOT NULL,
          auto_provisioned BOOLEAN DEFAULT FALSE,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);

      await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS company VARCHAR(200)`);
      await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS auto_provisioned BOOLEAN DEFAULT FALSE`);
      await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()`);

      const seedUsers = buildSeedUsers();
      for (const user of seedUsers) {
        await pool.query(
          `INSERT INTO users (name, email, password_hash, role, company, auto_provisioned, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7::timestamptz, NOW()), NOW())
           ON CONFLICT (email) DO UPDATE SET
             name = EXCLUDED.name,
             password_hash = EXCLUDED.password_hash,
             role = EXCLUDED.role,
             company = EXCLUDED.company,
             auto_provisioned = EXCLUDED.auto_provisioned,
             updated_at = NOW()`,
          [
            user.name,
            user.email,
            user.password,
            user.role,
            user.company,
            user.auto_provisioned,
            user.created_at || null
          ]
        );
      }
    })().catch((err) => {
      usersStoreReadyPromise = null;
      throw err;
    });
  }

  return usersStoreReadyPromise;
}

async function findPersistentUserByEmail(email) {
  await ensurePersistentUsersStore();
  const result = await pool.query(
    `SELECT id, name, email, password_hash, role, company, created_at, updated_at, auto_provisioned
       FROM users
      WHERE LOWER(email) = LOWER($1)
      LIMIT 1`,
    [String(email || '').trim().toLowerCase()]
  );
  return result.rows[0] ? normalizeUserRecord(result.rows[0]) : null;
}

async function findPersistentUserById(userId) {
  await ensurePersistentUsersStore();
  const result = await pool.query(
    `SELECT id, name, email, password_hash, role, company, created_at, updated_at, auto_provisioned
       FROM users
      WHERE id = $1
      LIMIT 1`,
    [Number(userId)]
  );
  return result.rows[0] ? normalizeUserRecord(result.rows[0]) : null;
}

async function loginUserPersistent(email, password) {
  try {
    const normalizedEmail = String(email || '').trim().toLowerCase();
    let user = await findPersistentUserByEmail(normalizedEmail);

    if (!user) {
      if (STAFF_AUTO_PROVISION && isAllowedStaffEmail(normalizedEmail)) {
        const inserted = await pool.query(
          `INSERT INTO users (name, email, password_hash, role, company, auto_provisioned, created_at, updated_at)
           VALUES ($1, $2, $3, $4, $5, TRUE, NOW(), NOW())
           RETURNING id, name, email, password_hash, role, company, created_at, updated_at, auto_provisioned`,
          [buildDisplayNameFromEmail(normalizedEmail), normalizedEmail, password, 'analyst', 'GeoScope']
        );
        user = normalizeUserRecord(inserted.rows[0]);
      } else {
        return { success: false, error: 'User not found' };
      }
    }

    if (user.password !== password) {
      return { success: false, error: 'Invalid password' };
    }

    if (isStaffRole(user.role) && !isAllowedStaffEmail(user.email)) {
      return { success: false, error: 'Staff access requires an approved company email domain' };
    }

    const token = createToken({ id: user.id, email: user.email, role: user.role });
    return { success: true, token, user: toSafeUser(user) };
  } catch (err) {
    console.error('Persistent login error:', err.message);
    return loginUser(email, password);
  }
}

async function registerUserPersistent(name, email, password, role = 'client', company = '') {
  try {
    const normalizedEmail = String(email || '').trim().toLowerCase();
    const normalizedRole = String(role || 'client').toLowerCase();
    await ensurePersistentUsersStore();

    const existing = await findPersistentUserByEmail(normalizedEmail);
    if (existing) {
      return { success: false, error: 'Email already registered' };
    }

    if (isStaffRole(normalizedRole) && !isAllowedStaffEmail(normalizedEmail)) {
      return { success: false, error: 'Staff accounts must use an approved company email domain' };
    }

    const inserted = await pool.query(
      `INSERT INTO users (name, email, password_hash, role, company, auto_provisioned, created_at, updated_at)
       VALUES ($1, $2, $3, $4, $5, FALSE, NOW(), NOW())
       RETURNING id, name, email, password_hash, role, company, created_at, updated_at, auto_provisioned`,
      [name, normalizedEmail, password, normalizedRole, company]
    );

    const user = normalizeUserRecord(inserted.rows[0]);
    const token = createToken({ id: user.id, email: user.email, role: user.role });
    return { success: true, token, user: toSafeUser(user) };
  } catch (err) {
    console.error('Persistent registration error:', err.message);
    return registerUser(name, email, password, role, company);
  }
}

async function getUserByIdPersistent(userId) {
  try {
    const user = await findPersistentUserById(userId);
    return user ? toSafeUser(user) : getUserById(userId);
  } catch (err) {
    console.error('Persistent get user error:', err.message);
    return getUserById(userId);
  }
}

async function getAllUsersPersistent() {
  try {
    await ensurePersistentUsersStore();
    const result = await pool.query(
      `SELECT id, name, email, role, company, created_at, updated_at, auto_provisioned
         FROM users
        ORDER BY created_at DESC, id DESC`
    );
    return result.rows.map(toSafeUser);
  } catch (err) {
    console.error('Persistent get users error:', err.message);
    return getAllUsers();
  }
}

async function getAnalystsPersistent() {
  try {
    await ensurePersistentUsersStore();
    const result = await pool.query(
      `SELECT id, name, email, role, company
         FROM users
        WHERE role IN ('analyst', 'gis')
        ORDER BY created_at DESC, id DESC`
    );
    return result.rows.map(toSafeUser);
  } catch (err) {
    console.error('Persistent get analysts error:', err.message);
    return getAnalysts();
  }
}

async function updateUserRolePersistent(userId, newRole) {
  try {
    await ensurePersistentUsersStore();
    const result = await pool.query(
      `UPDATE users
          SET role = $2,
              updated_at = NOW()
        WHERE id = $1`,
      [Number(userId), String(newRole || '').toLowerCase()]
    );
    if (result.rowCount === 0) {
      return { success: false, error: 'User not found' };
    }
    return { success: true };
  } catch (err) {
    console.error('Persistent update role error:', err.message);
    return updateUserRole(userId, newRole);
  }
}

async function deleteUserPersistent(userId) {
  try {
    await ensurePersistentUsersStore();
    const result = await pool.query('DELETE FROM users WHERE id = $1', [Number(userId)]);
    if (result.rowCount === 0) {
      return { success: false, error: 'User not found' };
    }
    return { success: true };
  } catch (err) {
    console.error('Persistent delete user error:', err.message);
    return deleteUser(userId);
  }
}

// =====================
// SIMPLE JWT IMPLEMENTATION
// =====================

function base64UrlEncode(str) {
  return Buffer.from(str).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=/g, '');
}

function base64UrlDecode(str) {
  str += Array(5 - str.length % 4).join('=');
  return Buffer.from(str.replace(/\-/g, '+').replace(/_/g, '/'), 'base64').toString();
}

function createToken(payload, expiresIn = 2592000) { // 30 days default
  const header = { alg: 'HS256', typ: 'JWT' };
  const now = Math.floor(Date.now() / 1000);
  const claims = {
    ...payload,
    iat: now,
    exp: now + expiresIn
  };

  const headerEncoded = base64UrlEncode(JSON.stringify(header));
  const claimsEncoded = base64UrlEncode(JSON.stringify(claims));
  const message = headerEncoded + '.' + claimsEncoded;

  const signature = crypto
    .createHmac('sha256', JWT_SECRET)
    .update(message)
    .digest('base64')
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');

  return message + '.' + signature;
}

function verifyToken(token) {
  try {
    const parts = token.split('.');
    if (parts.length !== 3) return null;

    const [headerEncoded, claimsEncoded, signatureReceived] = parts;
    const message = headerEncoded + '.' + claimsEncoded;

    const signatureExpected = crypto
      .createHmac('sha256', JWT_SECRET)
      .update(message)
      .digest('base64')
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=/g, '');

    if (signatureReceived !== signatureExpected) return null;

    const claims = JSON.parse(base64UrlDecode(claimsEncoded));

    if (claims.exp < Math.floor(Date.now() / 1000)) {
      return null; // Token expired
    }

    return claims;
  } catch (err) {
    return null;
  }
}

// =====================
// DATABASE INITIALIZATION
// =====================

function setDbPaths(baseDir) {
  DB_DIR = baseDir;
  USERS_FILE = path.join(DB_DIR, 'users.json');
  ORDERS_FILE = path.join(DB_DIR, 'orders.json');
  REPORTS_FILE = path.join(DB_DIR, 'reports.json');
}

// Initialize data directory and files
function initializeDatabase() {
  try {
    if (!fs.existsSync(DB_DIR)) {
      try {
        fs.mkdirSync(DB_DIR, { recursive: true });
      } catch (mkdirErr) {
        // Vercel serverless filesystem is read-only except /tmp.
        const fallbackDir = path.join('/tmp', 'geoscope-data');
        setDbPaths(fallbackDir);
        fs.mkdirSync(DB_DIR, { recursive: true });
      }
      console.log('✓ Data directory created');
    }

    // Initialize users.json
    if (!fs.existsSync(USERS_FILE)) {
      const defaultAdmin = {
        id: 1,
        name: 'Admin',
        email: 'admin@geoscope.com',
        password: 'admin123',
        role: 'admin',
        company: 'GeoScope',
        created_at: new Date().toISOString()
      };
      fs.writeFileSync(USERS_FILE, JSON.stringify([defaultAdmin], null, 2));
      console.log('✓ Default admin created: admin@geoscope.com / admin123');
    }

    // Initialize orders.json
    if (!fs.existsSync(ORDERS_FILE)) {
      fs.writeFileSync(ORDERS_FILE, JSON.stringify([], null, 2));
    }

    // Initialize reports.json
    if (!fs.existsSync(REPORTS_FILE)) {
      fs.writeFileSync(REPORTS_FILE, JSON.stringify([], null, 2));
    }

    console.log('✓ Database files initialized');
  } catch (err) {
    console.error('✗ Database initialization error:', err.message);
  }
}

// File-based data store helpers
function readUsers() {
  try {
    const data = fs.readFileSync(USERS_FILE, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    console.error('Error reading users:', err.message);
    return [];
  }
}

function writeUsers(users) {
  try {
    fs.writeFileSync(USERS_FILE, JSON.stringify(users, null, 2));
  } catch (err) {
    console.error('Error writing users:', err.message);
  }
}

function readOrders() {
  try {
    const data = fs.readFileSync(ORDERS_FILE, 'utf8');
    return JSON.parse(data);
  } catch (err) {
    console.error('Error reading orders:', err.message);
    return [];
  }
}

function writeOrders(orders) {
  try {
    fs.writeFileSync(ORDERS_FILE, JSON.stringify(orders, null, 2));
  } catch (err) {
    console.error('Error writing orders:', err.message);
  }
}

initializeDatabase();

// =====================
// AUTH FUNCTIONS
// =====================

/**
 * Login user
 */
function loginUser(email, password) {
  try {
    const users = readUsers();
    let user = users.find(u => u.email === email);

    if (!user) {
      if (STAFF_AUTO_PROVISION && isAllowedStaffEmail(email)) {
        const newUser = {
          id: Math.max(...users.map(u => u.id || 0), 0) + 1,
          name: buildDisplayNameFromEmail(email),
          email,
          password,
          role: 'analyst',
          company: 'GeoScope',
          created_at: new Date().toISOString(),
          auto_provisioned: true
        };
        users.push(newUser);
        writeUsers(users);
        user = newUser;
      } else {
        return { success: false, error: 'User not found' };
      }
    }

    if (user.password !== password) {
      return { success: false, error: 'Invalid password' };
    }

    if (isStaffRole(user.role) && !isAllowedStaffEmail(user.email)) {
      return { success: false, error: 'Staff access requires an approved company email domain' };
    }

    const token = createToken({ id: user.id, email: user.email, role: user.role });

    return {
      success: true,
      token,
      user: {
        id: user.id,
        name: user.name,
        email: user.email,
        role: user.role,
        company: user.company
      }
    };
  } catch (err) {
    console.error('Login error:', err.message);
    return { success: false, error: 'Server error' };
  }
}

/**
 * Register user
 */
function registerUser(name, email, password, role = 'client', company = '') {
  try {
    const users = readUsers();
    
    // Check if user exists
    const existing = users.find(u => u.email === email);
    if (existing) {
      return { success: false, error: 'Email already registered' };
    }

    if (isStaffRole(role) && !isAllowedStaffEmail(email)) {
      return { success: false, error: 'Staff accounts must use an approved company email domain' };
    }

    const newUser = {
      id: Math.max(...users.map(u => u.id || 0), 0) + 1,
      name,
      email,
      password,
      role,
      company,
      created_at: new Date().toISOString()
    };

    users.push(newUser);
    writeUsers(users);

    const token = createToken({ id: newUser.id, email, role });

    return {
      success: true,
      token,
      user: {
        id: newUser.id,
        name,
        email,
        role,
        company
      }
    };
  } catch (err) {
    console.error('Registration error:', err.message);
    return { success: false, error: 'Server error' };
  }
}

/**
 * Get user by ID
 */
function getUserById(userId) {
  try {
    const users = readUsers();
    const user = users.find(u => u.id === userId);
    if (!user) return null;
    
    const { password, ...safeUser } = user;
    return safeUser;
  } catch (err) {
    console.error('Get user error:', err.message);
    return null;
  }
}

/**
 * Get all users (admin only)
 */
function getAllUsers() {
  try {
    const users = readUsers();
    return users.map(u => {
      const { password, ...safeUser } = u;
      return safeUser;
    });
  } catch (err) {
    console.error('Get users error:', err.message);
    return [];
  }
}

/**
 * Get analysts only
 */
function getAnalysts() {
  try {
    const users = readUsers();
    return users
      .filter(u => u.role === 'analyst')
      .map(u => ({
        id: u.id,
        name: u.name,
        email: u.email,
        company: u.company
      }));
  } catch (err) {
    console.error('Get analysts error:', err.message);
    return [];
  }
}

/**
 * Update user role (admin only)
 */
function updateUserRole(userId, newRole) {
  try {
    const users = readUsers();
    const user = users.find(u => u.id === userId);
    if (!user) {
      return { success: false, error: 'User not found' };
    }
    
    user.role = newRole;
    user.updated_at = new Date().toISOString();
    writeUsers(users);
    return { success: true };
  } catch (err) {
    console.error('Update role error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Delete user (admin only)
 */
function deleteUser(userId) {
  try {
    let users = readUsers();
    const index = users.findIndex(u => u.id === userId);
    if (index === -1) {
      return { success: false, error: 'User not found' };
    }
    
    users.splice(index, 1);
    writeUsers(users);
    return { success: true };
  } catch (err) {
    console.error('Delete user error:', err.message);
    return { success: false, error: err.message };
  }
}

// =====================
// ORDER FUNCTIONS
// =====================

/**
 * Create order
 */
function createOrder(clientId, projectName, address, latitude, longitude, polygon = null) {
  try {
    const orders = readOrders();
    const newOrder = {
      id: Math.max(...orders.map(o => Number(o.id) || 0), 999) + 1,
      client_id: clientId,
      project_name: projectName,
      address,
      latitude,
      longitude,
      polygon,
      status: 'pending',
      assigned_to: null,
      priority: 'normal',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    orders.push(newOrder);
    writeOrders(orders);

    return { success: true, orderId: newOrder.id };
  } catch (err) {
    console.error('Create order error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Get user's orders
 */
function getUserOrders(clientId) {
  try {
    const orders = readOrders();
    return orders.filter(o => o.client_id === clientId).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  } catch (err) {
    console.error('Get orders error:', err.message);
    return [];
  }
}

/**
 * Get assigned orders (analyst)
 */
function getAnalystOrders(analystId) {
  try {
    const orders = readOrders();
    const users = readUsers();
    
    return orders
      .filter(o => o.assigned_to === analystId)
      .map(o => {
        const client = users.find(u => u.id === o.client_id);
        return {
          ...o,
          client_name: client?.name || 'Unknown',
          client_email: client?.email || ''
        };
      })
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  } catch (err) {
    console.error('Get analyst orders error:', err.message);
    return [];
  }
}

/**
 * Get all orders (admin)
 */
function getAllOrders() {
  try {
    const orders = readOrders();
    const users = readUsers();
    
    return orders.map(o => {
      const client = users.find(u => u.id === o.client_id);
      const analyst = users.find(u => u.id === o.assigned_to);
      
      return {
        ...o,
        client_name: client?.name || 'Unknown',
        client_email: client?.email || '',
        analyst_name: analyst?.name || null,
        analyst_email: analyst?.email || null
      };
    }).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  } catch (err) {
    console.error('Get all orders error:', err.message);
    return [];
  }
}

/**
 * Assign order to analyst
 */
function assignOrder(orderId, analystId) {
  try {
    const orders = readOrders();
    const order = orders.find(o => o.id === orderId);
    if (!order) {
      return { success: false, error: 'Order not found' };
    }
    
    order.assigned_to = analystId;
    order.status = 'assigned';
    order.updated_at = new Date().toISOString();
    writeOrders(orders);
    return { success: true };
  } catch (err) {
    console.error('Assign order error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Update order status
 */
function updateOrderStatus(orderId, status) {
  try {
    const orders = readOrders();
    const order = orders.find(o => o.id === orderId);
    if (!order) {
      return { success: false, error: 'Order not found' };
    }
    
    order.status = status;
    order.updated_at = new Date().toISOString();
    writeOrders(orders);
    return { success: true };
  } catch (err) {
    console.error('Update order status error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Update order workflow fields
 */
function updateOrderWorkflow(orderId, updates = {}) {
  try {
    const orders = readOrders();
    const order = orders.find(o => o.id === orderId);
    if (!order) {
      return { success: false, error: 'Order not found' };
    }

    const allowedFields = [
      'status',
      'stage',
      'report_status',
      'report_path',
      'report_url',
      'processed_at',
      'updated_at'
    ];

    for (const key of allowedFields) {
      if (Object.prototype.hasOwnProperty.call(updates, key)) {
        order[key] = updates[key];
      }
    }

    order.updated_at = new Date().toISOString();
    writeOrders(orders);
    return { success: true };
  } catch (err) {
    console.error('Update order workflow error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Get order by ID
 */
function getOrderById(orderId) {
  try {
    const orders = readOrders();
    const users = readUsers();
    const order = orders.find(o => o.id === orderId);
    if (!order) return null;
    
    const client = users.find(u => u.id === order.client_id);
    const analyst = users.find(u => u.id === order.assigned_to);
    
    return {
      ...order,
      client_name: client?.name || 'Unknown',
      client_email: client?.email || '',
      analyst_name: analyst?.name || null,
      analyst_email: analyst?.email || null
    };
  } catch (err) {
    console.error('Get order error:', err.message);
    return null;
  }
}

/**
 * Get user by ID
 */
function getUserById(userId) {
  try {
    const users = readUsers();
    const user = users.find(u => u.id === userId);
    if (!user) return null;
    
    const { password, ...safeUser } = user;
    return safeUser;
  } catch (err) {
    console.error('Get user error:', err.message);
    return null;
  }
}

/**
 * Get all users (admin only)
 */
function getAllUsers() {
  try {
    const users = readUsers();
    return users.map(u => {
      const { password, ...safeUser } = u;
      return safeUser;
    });
  } catch (err) {
    console.error('Get users error:', err.message);
    return [];
  }
}

/**
 * Get analysts only
 */
function getAnalysts() {
  try {
    const users = readUsers();
    return users
      .filter(u => u.role === 'analyst')
      .map(u => ({
        id: u.id,
        name: u.name,
        email: u.email,
        company: u.company
      }));
  } catch (err) {
    console.error('Get analysts error:', err.message);
    return [];
  }
}

/**
 * Update user role (admin only)
 */
function updateUserRole(userId, newRole) {
  try {
    const users = readUsers();
    const user = users.find(u => u.id === userId);
    if (!user) {
      return { success: false, error: 'User not found' };
    }
    
    user.role = newRole;
    user.updated_at = new Date().toISOString();
    writeUsers(users);
    return { success: true };
  } catch (err) {
    console.error('Update role error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Delete user (admin only)
 */
function deleteUser(userId) {
  try {
    let users = readUsers();
    const index = users.findIndex(u => u.id === userId);
    if (index === -1) {
      return { success: false, error: 'User not found' };
    }
    
    users.splice(index, 1);
    writeUsers(users);
    return { success: true };
  } catch (err) {
    console.error('Delete user error:', err.message);
    return { success: false, error: err.message };
  }
}

// =====================
// ORDER FUNCTIONS
// =====================

/**
 * Create order
 */
function createOrder(clientId, projectName, address, latitude, longitude, polygon = null) {
  try {
    const orders = readOrders();
    const newOrder = {
      id: Math.max(...orders.map(o => Number(o.id) || 0), 999) + 1,
      client_id: clientId,
      project_name: projectName,
      address,
      latitude,
      longitude,
      polygon,
      status: 'pending',
      assigned_to: null,
      priority: 'normal',
      created_at: new Date().toISOString(),
      updated_at: new Date().toISOString()
    };

    orders.push(newOrder);
    writeOrders(orders);

    return { success: true, orderId: newOrder.id };
  } catch (err) {
    console.error('Create order error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Get user's orders
 */
function getUserOrders(clientId) {
  try {
    const orders = readOrders();
    return orders.filter(o => o.client_id === clientId).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  } catch (err) {
    console.error('Get orders error:', err.message);
    return [];
  }
}

/**
 * Get assigned orders (analyst)
 */
function getAnalystOrders(analystId) {
  try {
    const orders = readOrders();
    const users = readUsers();
    
    return orders
      .filter(o => o.assigned_to === analystId)
      .map(o => {
        const client = users.find(u => u.id === o.client_id);
        return {
          ...o,
          client_name: client?.name || 'Unknown',
          client_email: client?.email || ''
        };
      })
      .sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  } catch (err) {
    console.error('Get analyst orders error:', err.message);
    return [];
  }
}

/**
 * Get all orders (admin)
 */
function getAllOrders() {
  try {
    const orders = readOrders();
    const users = readUsers();
    
    return orders.map(o => {
      const client = users.find(u => u.id === o.client_id);
      const analyst = users.find(u => u.id === o.assigned_to);
      
      return {
        ...o,
        client_name: client?.name || 'Unknown',
        client_email: client?.email || '',
        analyst_name: analyst?.name || null,
        analyst_email: analyst?.email || null
      };
    }).sort((a, b) => new Date(b.created_at) - new Date(a.created_at));
  } catch (err) {
    console.error('Get all orders error:', err.message);
    return [];
  }
}

/**
 * Assign order to analyst
 */
function assignOrder(orderId, analystId) {
  try {
    const orders = readOrders();
    const order = orders.find(o => o.id === orderId);
    if (!order) {
      return { success: false, error: 'Order not found' };
    }
    
    order.assigned_to = analystId;
    order.status = 'assigned';
    order.updated_at = new Date().toISOString();
    writeOrders(orders);
    return { success: true };
  } catch (err) {
    console.error('Assign order error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Update order status
 */
function updateOrderStatus(orderId, status) {
  try {
    const orders = readOrders();
    const order = orders.find(o => o.id === orderId);
    if (!order) {
      return { success: false, error: 'Order not found' };
    }
    
    order.status = status;
    order.updated_at = new Date().toISOString();
    writeOrders(orders);
    return { success: true };
  } catch (err) {
    console.error('Update order status error:', err.message);
    return { success: false, error: err.message };
  }
}

/**
 * Get order by ID
 */
function getOrderById(orderId) {
  try {
    const orders = readOrders();
    const users = readUsers();
    const order = orders.find(o => o.id === orderId);
    if (!order) return null;
    
    const client = users.find(u => u.id === order.client_id);
    const analyst = users.find(u => u.id === order.assigned_to);
    
    return {
      ...order,
      client_name: client?.name || 'Unknown',
      client_email: client?.email || '',
      analyst_name: analyst?.name || null,
      analyst_email: analyst?.email || null
    };
  } catch (err) {
    console.error('Get order error:', err.message);
    return null;
  }
}

// =====================
// PERSISTENT ORDER FUNCTIONS (Postgres)
// =====================

let ordersStoreReadyPromise = null;

async function ensurePersistentOrdersStore() {
  if (!ordersStoreReadyPromise) {
    ordersStoreReadyPromise = (async () => {
      await pool.query(`
        CREATE TABLE IF NOT EXISTS orders (
          id BIGSERIAL PRIMARY KEY,
          project_name VARCHAR(500) NOT NULL,
          client_name VARCHAR(500),
          client_company VARCHAR(500),
          recipient_email_1 VARCHAR(320),
          address TEXT,
          latitude DOUBLE PRECISION,
          longitude DOUBLE PRECISION,
          polygon JSONB,
          status VARCHAR(50) DEFAULT 'received',
          analyst_id INTEGER,
          report_path TEXT,
          report_url TEXT,
          source VARCHAR(100) DEFAULT 'client-portal',
          created_at TIMESTAMPTZ DEFAULT NOW(),
          updated_at TIMESTAMPTZ DEFAULT NOW()
        )
      `);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS client_id INTEGER`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS assigned_to INTEGER`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS priority VARCHAR(20) DEFAULT 'normal'`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS stage VARCHAR(50)`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS report_status VARCHAR(50)`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS subject_property JSONB`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS geo_input_type VARCHAR(20) DEFAULT 'star'`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS notes TEXT`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS recipient_email_2 VARCHAR(320)`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS dataset_date DATE`);
      // Short human-readable order number (sequential, unique, never repeats)
      await pool.query(`CREATE SEQUENCE IF NOT EXISTS orders_order_number_seq START WITH 1001`);
      await pool.query(`ALTER TABLE orders ADD COLUMN IF NOT EXISTS order_number INT DEFAULT nextval('orders_order_number_seq')`);
      await pool.query(`UPDATE orders SET order_number = nextval('orders_order_number_seq') WHERE order_number IS NULL`);
      await pool.query(`CREATE UNIQUE INDEX IF NOT EXISTS orders_order_number_idx ON orders (order_number)`);

      // Fix auto-increment sequence to continue from highest existing ID
      try {
        const maxIdResult = await pool.query(`SELECT MAX(id) as max_id FROM orders`);
        const maxId = maxIdResult.rows[0]?.max_id || 0;
        const nextId = Math.max(maxId + 1, 1001);
        await pool.query(`ALTER SEQUENCE orders_id_seq RESTART WITH ${nextId}`);
      } catch (seqErr) {
        // Silently ignore if sequence fix fails
        console.debug('Sequence auto-fix info:', seqErr.message);
      }
    })().catch((err) => {
      ordersStoreReadyPromise = null;
      throw err;
    });
  }
  return ordersStoreReadyPromise;
}

function normalizeOrderRecord(row) {
  const analystId = row.analyst_id != null ? Number(row.analyst_id) : (row.assigned_to != null ? Number(row.assigned_to) : null);
  return {
    id: Number(row.id),
    client_id: row.client_id != null ? Number(row.client_id) : null,
    project_name: row.project_name || '',
    client_name: row.client_name || null,
    client_email: row.recipient_email_1 || null,
    client_company: row.client_company || null,
    recipient_email_1: row.recipient_email_1 || null,
    recipient_email_2: row.recipient_email_2 || null,
    address: row.address || '',
    latitude: row.latitude != null ? Number(row.latitude) : null,
    longitude: row.longitude != null ? Number(row.longitude) : null,
    polygon: row.polygon || null,
    subject_property: row.subject_property || null,
    geo_input_type: row.geo_input_type || 'star',
    notes: row.notes || '',
    status: row.status || 'received',
    stage: row.stage || null,
    report_status: row.report_status || null,
    analyst_id: analystId,
    assigned_to: analystId,
    analyst_name: row.analyst_name || null,
    analyst_email: row.analyst_email || null,
    priority: row.priority || 'normal',
    report_path: row.report_path || null,
    report_url: row.report_url || null,
    source: row.source || 'client-portal',
    dataset_date: row.dataset_date || null,
    processed_at: row.processed_at || null,
    created_at: row.created_at || null,
    updated_at: row.updated_at || null,
    order_number: row.order_number != null ? Number(row.order_number) : null,
  };
}

async function createOrderPersistent(clientId, projectName, address, latitude, longitude, polygon = null) {
  try {
    await ensurePersistentOrdersStore();
    let clientName = null;
    let recipientEmail = null;
    let clientCompany = null;
    if (clientId) {
      const userRow = await pool.query(
        `SELECT name, email, company FROM users WHERE id = $1 LIMIT 1`,
        [Number(clientId)]
      );
      if (userRow.rows[0]) {
        clientName = userRow.rows[0].name || null;
        recipientEmail = userRow.rows[0].email || null;
        clientCompany = userRow.rows[0].company || null;
      }
    }
    const polygonVal = polygon
      ? (typeof polygon === 'string' ? JSON.parse(polygon) : polygon)
      : null;
    const result = await pool.query(
      `INSERT INTO orders
         (client_id, project_name, client_name, client_company, recipient_email_1,
          address, latitude, longitude, polygon, status, priority, source, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,'received','normal','client-portal',NOW(),NOW())
       RETURNING id`,
      [
        clientId ? Number(clientId) : null,
        projectName,
        clientName,
        clientCompany,
        recipientEmail,
        address,
        latitude != null ? Number(latitude) : null,
        longitude != null ? Number(longitude) : null,
        polygonVal ? JSON.stringify(polygonVal) : null
      ]
    );
    return { success: true, orderId: Number(result.rows[0].id) };
  } catch (err) {
    console.error('createOrderPersistent error:', err.message);
    return { success: false, error: err.message };
  }
}

async function getUserOrdersPersistent(clientId) {
  try {
    await ensurePersistentOrdersStore();
    const result = await pool.query(
      `SELECT * FROM orders WHERE client_id = $1 ORDER BY created_at DESC`,
      [Number(clientId)]
    );
    return result.rows.map(normalizeOrderRecord);
  } catch (err) {
    console.error('getUserOrdersPersistent error:', err.message);
    return [];
  }
}

async function getAnalystOrdersPersistent(analystId) {
  try {
    await ensurePersistentOrdersStore();
    const result = await pool.query(
      `SELECT o.*,
              u.name AS client_name_u, u.email AS client_email_u
         FROM orders o
         LEFT JOIN users u ON u.id = o.client_id
        WHERE o.analyst_id = $1 OR o.assigned_to = $1
        ORDER BY o.created_at DESC`,
      [Number(analystId)]
    );
    return result.rows.map((row) => ({
      ...normalizeOrderRecord(row),
      client_name: row.client_name || row.client_name_u || 'Unknown',
      client_email: row.recipient_email_1 || row.client_email_u || ''
    }));
  } catch (err) {
    console.error('getAnalystOrdersPersistent error:', err.message);
    return [];
  }
}

async function getAllOrdersPersistent() {
  try {
    await ensurePersistentOrdersStore();
    const result = await pool.query(
      `SELECT o.*,
              c.name AS client_name_u, c.email AS client_email_u,
              a.name AS analyst_name_u, a.email AS analyst_email_u
         FROM orders o
         LEFT JOIN users c ON c.id = o.client_id
         LEFT JOIN users a ON a.id = COALESCE(o.analyst_id, o.assigned_to)
        ORDER BY o.created_at DESC`
    );
    return result.rows.map((row) => ({
      ...normalizeOrderRecord(row),
      client_name: row.client_name || row.client_name_u || 'Unknown',
      client_email: row.recipient_email_1 || row.client_email_u || '',
      analyst_name: row.analyst_name || row.analyst_name_u || null,
      analyst_email: row.analyst_email_u || null
    }));
  } catch (err) {
    console.error('getAllOrdersPersistent error:', err.message);
    return [];
  }
}

async function assignOrderPersistent(orderId, analystId) {
  try {
    await ensurePersistentOrdersStore();
    const result = await pool.query(
      `UPDATE orders
          SET analyst_id = $2, assigned_to = $2, status = 'assigned', updated_at = NOW()
        WHERE id = $1
        RETURNING id`,
      [Number(orderId), Number(analystId)]
    );
    if (result.rowCount === 0) return { success: false, error: 'Order not found' };
    return { success: true };
  } catch (err) {
    console.error('assignOrderPersistent error:', err.message);
    return { success: false, error: err.message };
  }
}

async function updateOrderStatusPersistent(orderId, status) {
  try {
    await ensurePersistentOrdersStore();
    const result = await pool.query(
      `UPDATE orders SET status = $2, updated_at = NOW() WHERE id = $1 RETURNING id`,
      [Number(orderId), status]
    );
    if (result.rowCount === 0) return { success: false, error: 'Order not found' };
    return { success: true };
  } catch (err) {
    console.error('updateOrderStatusPersistent error:', err.message);
    return { success: false, error: err.message };
  }
}

async function updateOrderWorkflowPersistent(orderId, updates = {}) {
  try {
    await ensurePersistentOrdersStore();
    const allowedFields = ['status', 'stage', 'report_status', 'report_path', 'report_url', 'processed_at', 'analyst_id', 'assigned_to'];
    const setClauses = [];
    const values = [];
    let idx = 1;
    for (const key of allowedFields) {
      if (Object.prototype.hasOwnProperty.call(updates, key)) {
        setClauses.push(`${key} = $${idx++}`);
        values.push(updates[key]);
      }
    }
    if (setClauses.length === 0) return { success: true };
    setClauses.push(`updated_at = NOW()`);
    values.push(Number(orderId));
    const result = await pool.query(
      `UPDATE orders SET ${setClauses.join(', ')} WHERE id = $${idx} RETURNING id`,
      values
    );
    if (result.rowCount === 0) return { success: false, error: 'Order not found' };
    return { success: true };
  } catch (err) {
    console.error('updateOrderWorkflowPersistent error:', err.message);
    return { success: false, error: err.message };
  }
}

async function getOrderByIdPersistent(orderId) {
  try {
    await ensurePersistentOrdersStore();
    const result = await pool.query(
      `SELECT o.*,
              c.name AS client_name_u, c.email AS client_email_u,
              a.name AS analyst_name_u, a.email AS analyst_email_u
         FROM orders o
         LEFT JOIN users c ON c.id = o.client_id
         LEFT JOIN users a ON a.id = COALESCE(o.analyst_id, o.assigned_to)
        WHERE o.id = $1
        LIMIT 1`,
      [Number(orderId)]
    );
    if (!result.rows[0]) return null;
    const row = result.rows[0];
    return {
      ...normalizeOrderRecord(row),
      client_name: row.client_name || row.client_name_u || 'Unknown',
      client_email: row.recipient_email_1 || row.client_email_u || '',
      analyst_name: row.analyst_name || row.analyst_name_u || null,
      analyst_email: row.analyst_email_u || null
    };
  } catch (err) {
    console.error('getOrderByIdPersistent error:', err.message);
    return null;
  }
}

function createResetToken(email, expiresInMinutes = 60) {
  const timestamp = Date.now();
  const expiry = timestamp + (expiresInMinutes * 60 * 1000);
  const random = Math.random().toString(36).substr(2, 32);
  const token = Buffer.from(`${email}:${timestamp}:${random}`).toString('base64');
  return { token, expiry, expiresInMinutes };
}

function verifyResetToken(token, email, checkExpiry = null) {
  try {
    const decoded = Buffer.from(token, 'base64').toString();
    const [tokenEmail, timestamp, random] = decoded.split(':');
    
    if (tokenEmail !== email) return null;
    
    const expiry = parseInt(timestamp) + (60 * 60 * 1000); // 1 hour default
    if (checkExpiry && Date.now() > expiry) return null;
    
    return { email: tokenEmail, timestamp, random, expiry };
  } catch (err) {
    return null;
  }
}

async function requestPasswordResetPersistent(email) {
  if (!email) return { success: false, error: 'Email is required' };
  
  try {
    const result = await pool.query('SELECT id FROM users WHERE LOWER(email) = LOWER($1)', [email]);
    if (result.rows.length === 0) {
      // Don't reveal if email exists, for security
      return { success: true, message: 'If an account exists with that email, a reset link will be sent.' };
    }
    
    const userId = result.rows[0].id;
    const { token, expiry } = createResetToken(email, 60);
    
    // Store token in DB (could add reset_token, reset_expiry columns to users table)
    // For now, return token for the frontend to use
    return { 
      success: true, 
      message: 'Password reset token generated. This would be sent via email.',
      userId,
      token,
      expiresIn: '60 minutes'
    };
  } catch (err) {
    console.error('requestPasswordResetPersistent error:', err.message);
    return { success: false, error: 'Failed to process password reset request' };
  }
}

async function resetPasswordWithTokenPersistent(email, token, newPassword) {
  if (!email || !token || !newPassword) {
    return { success: false, error: 'Missing required fields' };
  }
  
  try {
    // Verify token format
    const verified = verifyResetToken(token, email, true);
    if (!verified) {
      return { success: false, error: 'Invalid or expired reset token' };
    }
    
    // Verify user exists
    const userResult = await pool.query(
      'SELECT id FROM users WHERE LOWER(email) = LOWER($1)',
      [email]
    );
    if (userResult.rows.length === 0) {
      return { success: false, error: 'User not found' };
    }
    
    const userId = userResult.rows[0].id;
    
    // Hash password using bcrypt or just store as plaintext for demo
    // In production, use proper hashing
    const hashedPassword = await hashPasswordForStorage(newPassword);
    
    // Update password
    await pool.query(
      'UPDATE users SET password_hash = $1, updated_at = NOW() WHERE id = $2',
      [hashedPassword, userId]
    );
    
    return { success: true, message: 'Password reset successfully' };
  } catch (err) {
    console.error('resetPasswordWithTokenPersistent error:', err.message);
    return { success: false, error: 'Failed to reset password' };
  }
}

async function hashPasswordForStorage(password) {
  // For demo, just use the password as-is
  // In production, use bcrypt: const bcrypt = require('bcrypt'); return bcrypt.hash(password, 10);
  return password;
}

module.exports = {
  // Token functions
  createToken,
  verifyToken,
  // Auth functions
  loginUser,
  loginUserPersistent,
  registerUser,
  registerUserPersistent,
  getUserById,
  getUserByIdPersistent,
  getAllUsers,
  getAllUsersPersistent,
  getAnalysts,
  getAnalystsPersistent,
  updateUserRole,
  updateUserRolePersistent,
  deleteUser,
  deleteUserPersistent,
  // Order functions
  createOrder,
  getUserOrders,
  getAnalystOrders,
  getAllOrders,
  assignOrder,
  updateOrderStatus,
  updateOrderWorkflow,
  getOrderById,
  // Persistent order functions (Postgres)
  createOrderPersistent,
  getUserOrdersPersistent,
  getAnalystOrdersPersistent,
  getAllOrdersPersistent,
  assignOrderPersistent,
  updateOrderStatusPersistent,
  updateOrderWorkflowPersistent,
  getOrderByIdPersistent,
  // Password reset functions
  requestPasswordResetPersistent,
  resetPasswordWithTokenPersistent,
  createResetToken,
  verifyResetToken,
  pgPool: pool
};

