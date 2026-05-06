# Email Configuration Fix Guide

## Issue
Gmail authentication failing with: "Invalid login: Username and Password not accepted"

## Solution: Set Up Gmail App Password

Gmail doesn't allow direct use of account passwords with third-party apps. You need an "App Password".

### Step 1: Enable 2-Factor Authentication
1. Go to https://myaccount.google.com
2. Click "Security" in the left menu
3. Enable "2-Step Verification" if not already enabled

### Step 2: Generate App Password
1. Go to https://myaccount.google.com/apppasswords
2. Select "Mail" and "Windows Computer" (or your setup)
3. Google generates a 16-character password
4. Copy this password exactly (including spaces)

### Step 3: Update Environment Variables

**For Local Development** (geoscope/.env):
```bash
GMAIL_USER=info@geoscopesolutions.com
GMAIL_PASS=xxxx xxxx xxxx xxxx  # 16-char app password (with spaces)
ADMIN_NOTIFICATION_EMAIL=info@geoscopesolutions.com
```

**For Vercel Production**:
1. Go to https://vercel.com/stephenbantons-projects/geoscope-api/settings/environment-variables
2. Add/Update:
   - Key: `GMAIL_USER` → Value: `info@geoscopesolutions.com`
   - Key: `GMAIL_PASS` → Value: `<16-char app password>`
   - Key: `ADMIN_NOTIFICATION_EMAIL` → Value: `info@geoscopesolutions.com`
3. Redeploy: `npx vercel --prod --yes --scope stephenbantons-projects`

### Step 4: Test Email Configuration
```bash
# From geoscope/ directory
curl -X POST https://api.geoscopesolutions.com/contact \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Test User",
    "email": "test@example.com",
    "subject": "Test Email",
    "message": "This is a test"
  }'
```

Expected Response:
```json
{
  "success": true,
  "message": "Message received. We will follow up within 24 hours.",
  "timestamp": "2026-04-26T..."
}
```

Check email at `info@geoscopesolutions.com` to verify emails are being received.

## Alternative: Use Namecheap Private Email (SMTP)

If Gmail isn't working, use your Namecheap email with SMTP:

**Environment Variables**:
```bash
# Option 1: Use Gmail
GMAIL_USER=info@geoscopesolutions.com
GMAIL_PASS=xxxx xxxx xxxx xxxx  # App password

# Option 2: Use Namecheap SMTP (less preferred, requires code changes)
SMTP_HOST=mail.privateemail.com
SMTP_PORT=587
SMTP_USER=info@geoscopesolutions.com
SMTP_PASS=<your-email-password>
```

**Note**: Current code uses Gmail transport. To use SMTP, modify [server.js line 4401-4407](geoscope/server.js#L4401) to use nodemailer's generic SMTP transport.

## Email Notification Types

Once configured, you'll receive:

1. **Contact Form Submissions** → info@geoscopesolutions.com
2. **New Orders** → info@geoscopesolutions.com (with "[NEW ORDER]" tag)
3. **Test Client Orders** → info@geoscopesolutions.com (with "[TEST CLIENT]" tag)
4. **Order Confirmations** → client's recipient_email_1

## Troubleshooting

### Still getting "Invalid login" error?
- [ ] Verify 2-Factor Authentication is enabled on Gmail account
- [ ] Go to https://myaccount.google.com/apppasswords and regenerate the password
- [ ] Copy password exactly (16 chars + 3 spaces)
- [ ] Restart server after updating .env
- [ ] On Vercel: Redeploy after updating environment variables
- [ ] Check Vercel logs: `npx vercel logs api.geoscopesolutions.com --follow`

### Emails sending but not received?
- [ ] Check spam/junk folder
- [ ] Verify recipient email address is correct
- [ ] Add `info@geoscopesolutions.com` to contacts
- [ ] Check Gmail forwarding rules (Settings > Forwarding)
- [ ] Test with different email address

### Rate limiting / too many sends?
- Gmail: Max ~500/day per account
- Solution: Batch emails or use SendGrid/Mailgun for scale

## Deployed Email Service Status

- **Backend**: Vercel serverless (geoscope-api)
- **Email Service**: Gmail SMTP
- **Admin Email**: info@geoscopesolutions.com
- **Test Clients**: nyangelos4@gmail.com (not charged)
- **Status**: ✅ Contact form working
- **Status**: ⚠️ Order emails pending Gmail credentials
- **Status**: ✅ GIS search improved error handling
