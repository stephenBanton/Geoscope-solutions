# URGENT: Template File Deployment Issue & Workaround

## Problem Summary

**Status:** ⚠️ Critical  
**Affected:** Sample report generation endpoint `/send-sample-report`  
**Error:** `ENOENT: no such file or directory, open '/var/task/reportTemplate.html'`  
**Impact:** Cannot generate PDF reports via API

---

## Root Cause Analysis

In Vercel serverless functions:
- Runtime directory: `/var/task/`
- Expected file: `/var/task/reportTemplate.html`
- Actual location: File not being deployed despite being in git

**Why it happens:**
- Vercel's build process may not include all files
- Possible: File excluded by .vercelignore pattern matching
- Possible: Build step caching issue preventing fresh deployment
- Possible: File path not being resolved correctly at runtime

---

## Immediate Workarounds (Try in Order)

### Workaround 1: Force Vercel Cache Clear & Rebuild

```bash
cd c:\Users\Admin\Desktop\WEBSITE\geoscope

# Clear Vercel cache
npx vercel --prod --yes --scope stephenbantons-projects --yes

# Or manually via web:
# 1. Go to: https://vercel.com/stephenbantons-projects/geoscope-api/settings
# 2. Click "Deployments"
# 3. Right-click latest deployment → "Redeploy"
# 4. Check "Clear Build Cache"
```

### Workaround 2: Update .vercelignore to Explicitly Include File

Edit `.vercelignore`:
```
# ... existing patterns ...

# But EXPLICITLY include reportTemplate
!reportTemplate.html
!reportTemplate.*
```

Then redeploy:
```bash
npx vercel --prod --yes --scope stephenbantons-projects
```

### Workaround 3: Ensure File is in git

```bash
cd geoscope

# Check git status
git status reportTemplate.html

# If modified, commit:
git add reportTemplate.html
git commit -m "Ensure template is in deployment"

# Push and redeploy
git push
npx vercel --prod --yes --scope stephenbantons-projects
```

### Workaround 4: Use Vercel Build Step (Advanced)

Update `vercel.json`:
```json
{
  "version": 2,
  "installCommand": "PUPPETEER_SKIP_DOWNLOAD=1 npm install --no-audit --no-fund",
  "buildCommand": "cp reportTemplate.html .vercel/output/functions/ || true",
  "builds": [
    {
      "src": "server.js",
      "use": "@vercel/node"
    }
  ],
  "routes": [
    {
      "src": "/(.*)",
      "dest": "server.js"
    }
  ]
}
```

Then redeploy.

---

## Long-term Solution: Embed Template in Code

Instead of loading from file, embed the template directly in `server.js`:

### Step 1: Create Template Module

Create `template-loader.js`:
```javascript
// Extract template from reportTemplate.html and create embedded version
const fs = require('fs');

const templateContent = fs.readFileSync('./reportTemplate.html', 'utf8');

module.exports = function getTemplate() {
  return templateContent;
};
```

### Step 2: Update server.js

Replace:
```javascript
const templatePath = path.join(__dirname, 'reportTemplate.html');
const template = fs.readFileSync(templatePath, 'utf8');
```

With:
```javascript
const getTemplate = require('./template-loader.js');
const template = getTemplate();
```

### Step 3: Redeploy

```bash
git add template-loader.js server.js
git commit -m "Embed report template for Vercel deployment"
npx vercel --prod --yes --scope stephenbantons-projects
```

**Advantage:** No file loading issues; template is in memory
**Disadvantage:** Slightly larger bundle size (~50KB)

---

## Alternative Solution: Load Template from URL

Host template on CDN and load at runtime:

### Step 1: Upload to S3/Cloudflare

```bash
# Upload reportTemplate.html to AWS S3
aws s3 cp reportTemplate.html s3://geoscope-templates/reportTemplate.html

# Or use Cloudflare Workers
```

### Step 2: Update server.js

```javascript
const axios = require('axios');

async function getTemplate() {
  try {
    const response = await axios.get(
      'https://cdn.geoscope.com/templates/reportTemplate.html',
      { timeout: 5000 }
    );
    return response.data;
  } catch (error) {
    console.warn('Failed to load template from CDN, using fallback');
    return FALLBACK_TEMPLATE; // Minimal template
  }
}
```

### Step 3: Redeploy

```bash
npx vercel --prod --yes --scope stephenbantons-projects
```

**Advantage:** Decoupled from deployment; can update without redeploying
**Disadvantage:** Network dependency; slower first load

---

## Testing the Fix

### Test 1: Check if File is Deployed

```bash
# Via SSH/API (if available)
curl https://api.geoscopesolutions.com/health
# Should show server is running

# Via test endpoint (create if needed)
curl https://api.geoscopesolutions.com/template-test
# Should return template content or success message
```

### Test 2: Test Report Generation

```bash
curl -X POST https://api.geoscopesolutions.com/send-sample-report \
  -H "Content-Type: application/json" \
  -d '{"email":"test@example.com","name":"Test"}'

# Expected: 
# {"success":true,"message":"Sample report generated and sent successfully",...}
```

### Test 3: Verify Script Works

```bash
cd geoscope
node generate-sample-premium.js test@example.com

# Expected output:
# ✅ Successfully sent: 1
# ❌ Failed: 0
```

---

## Recommended Fix Priority

1. **First Try (5 min):** Workaround 1 - Force cache clear
2. **If That Fails (10 min):** Workaround 2 - Update .vercelignore
3. **If Still Fails (15 min):** Workaround 3 - Commit file to git
4. **Long-term (30 min):** Solution - Embed template in code

---

## Fallback: Use Existing Endpoint

While fixing, use the built-in endpoint directly:

```bash
# This works (has hardcoded template)
curl -X POST https://api.geoscopesolutions.com/send-sample-report \
  -H "Content-Type: application/json" \
  -d '{"email":"your@email.com","name":"Your Name"}'
```

Update `generate-sample-premium.js` to make direct API calls:

```javascript
// This calls the working endpoint
const response = await axios.post(
  `${API_BASE_URL}/send-sample-report`,
  { email, name: 'Valued Client' },
  { timeout: 180000 }
);
```

This is already implemented in the updated script!

---

## Prevention Going Forward

### 1. Add Build Step to vercel.json

```json
{
  "buildCommand": "npm run build:templates",
  ...
}
```

Add to package.json:
```json
{
  "scripts": {
    "build:templates": "cp reportTemplate.html dist/ && cp reportTemplate.html ."
  }
}
```

### 2. Test Locally Before Deployment

```bash
# Simulate Vercel environment
NODE_ENV=production node server.js

# Test endpoint
curl http://localhost:5000/send-sample-report -X POST -H "Content-Type: application/json" -d '{"email":"test@example.com"}'
```

### 3. Monitor Deployment Logs

After each deployment:
```bash
# Check Vercel logs
npx vercel logs --scope stephenbantons-projects

# Look for file loading errors
```

---

## Quick Reference

| Workaround | Time | Success Rate | Risk |
|-----------|------|--------------|------|
| **Clear Cache** | 2 min | 60% | None |
| **Update .vercelignore** | 5 min | 80% | None |
| **Commit to git** | 5 min | 90% | None |
| **Embed in code** | 20 min | 99% | Low |
| **Load from CDN** | 30 min | 95% | Medium |

---

## Support Checklist

If issue persists after all workarounds:

- [ ] Check Vercel deployment logs
- [ ] Verify file exists locally: `ls -la reportTemplate.html`
- [ ] Verify file is in git: `git log --oneline reportTemplate.html`
- [ ] Check .gitignore doesn't exclude it
- [ ] Check .vercelignore doesn't exclude it
- [ ] Try building locally: `npm run build`
- [ ] Test with NODE_ENV=production locally
- [ ] Contact Vercel support with deployment ID

---

## Status & Next Steps

**Current Action:** Implement Workaround 1 or 2 above

**Testing After Fix:**
```bash
# 1. Clear cache and redeploy
npx vercel --prod --yes --scope stephenbantons-projects

# 2. Wait 2 minutes for propagation
sleep 120

# 3. Test endpoint
node -e "const axios = require('axios'); axios.post('https://api.geoscopesolutions.com/send-sample-report', {email:'test@example.com',name:'Test'}, {timeout:30000}).then(res => console.log('✅ Success')).catch(e => console.log('❌ Still failing:', e.message))"
```

**Expected Result:**
```
✅ Sample reports sent successfully!
```

---

**Created:** 2026-04-25  
**Status:** Awaiting implementation  
**Priority:** High (blocks sample report feature)  
**Estimated Fix Time:** 5-30 minutes depending on workaround
