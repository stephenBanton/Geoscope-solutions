const crypto = require('crypto');
const path = require('path');
const fs = require('fs');

const JWT_SECRET = process.env.JWT_SECRET || 'your-super-secret-jwt-key-change-in-production';
const DB_DIR = path.join(__dirname, '.data');
const USERS_FILE = path.join(DB_DIR, 'users.json');
const ORDERS_FILE = path.join(DB_DIR, 'orders.json');
const REPORTS_FILE = path.join(DB_DIR, 'reports.json');
const STAFF_EMAIL_DOMAINS = (process.env.STAFF_EMAIL_DOMAINS || 'geoscope.com')
  .split(',')
  .map((d) => d.trim().toLowerCase())
  .filter(Boolean);

function isStaffRole(role) {
  return ['admin', 'analyst', 'gis'].includes(String(role || '').toLowerCase());
}

function isAllowedStaffEmail(email) {
  const domain = String(email || '').split('@')[1]?.toLowerCase() || '';
  if (!domain) return false;
  return STAFF_EMAIL_DOMAINS.includes(domain);
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

// Initialize data directory and files
function initializeDatabase() {
  try {
    if (!fs.existsSync(DB_DIR)) {
      fs.mkdirSync(DB_DIR, { recursive: true });
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
    const user = users.find(u => u.email === email);

    if (!user) {
      return { success: false, error: 'User not found' };
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

module.exports = {
  // Token functions
  createToken,
  verifyToken,
  // Auth functions
  loginUser,
  registerUser,
  getUserById,
  getAllUsers,
  getAnalysts,
  updateUserRole,
  deleteUser,
  // Order functions
  createOrder,
  getUserOrders,
  getAnalystOrders,
  getAllOrders,
  assignOrder,
  updateOrderStatus,
  updateOrderWorkflow,
  getOrderById
};

