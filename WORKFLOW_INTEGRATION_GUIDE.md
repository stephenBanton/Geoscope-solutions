# GeoScope Workflow & Integration Guide

## Overview
This document describes the smooth workflow and communications between PostgreSQL, PostGIS, Vercel, and Supabase.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    GeoScope Ecosystem                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌──────────────┐           ┌──────────────────┐            │
│  │  Frontend    │           │  Backend API     │            │
│  │  (React)     │◄────────►│  (Node/Express)  │            │
│  │  Vercel      │           │  Vercel Serverless           │
│  └──────────────┘           └──────────┬───────┘            │
│        ▲                                │                    │
│        │                                ▼                    │
│        │         ┌──────────────────────────────┐            │
│        └────────►│  PostgreSQL + PostGIS        │            │
│                  │  (Primary Data Source)       │            │
│                  │  - Orders                    │            │
│                  │  - User Accounts             │            │
│                  │  - Environmental Datasets    │            │
│                  └──────────────────────────────┘            │
│                                │                             │
│                  ┌─────────────┼─────────────┐               │
│                  ▼             ▼             ▼               │
│            ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│            │ Supabase │  │  Nodemailer  │  GIS   │         │
│            │ Storage  │  │  (Email)     │ Search │         │
│            │ (Files)  │  │              │        │         │
│            └──────────┘  └──────────┘  └──────────┘         │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Order Creation Workflow
```
Client Portal (Frontend)
    ↓
POST /client-orders
    ↓
✓ Save to PostgreSQL (primary)
✓ Email to Admin (info@geoscopesolutions.com)
✓ Email to Client (confirmation)
✓ Check test client whitelist (nyangelos4@gmail.com)
    ↓
Analyst Dashboard (Workbench)
    ↓
Download order → Analyze → Generate Report
```

### 2. Contact Form Workflow
```
Contact Form (Frontend)
    ↓
POST /contact
    ↓
✓ Email to Admin (info@geoscopesolutions.com)
✓ Confirmation email to Submitter
    ↓
Support Team Responds
```

### 3. Report Generation Workflow
```
Order → POST /generate-report
    ↓
Fetch Environmental Data (PostgreSQL + GIS Search)
    ↓
Enrich with PostGIS Queries
    ↓
Generate Premium PDF (Puppeteer → Serverless)
    ↓
Save to Supabase Storage
    ↓
Email Report Link to Client
```

## Component Descriptions

### PostgreSQL (Primary Data Source)
- **Role**: Primary persistent data storage
- **Data**: Orders, users, environmental datasets, report history
- **Connection**: Direct TCP via pgPool in Node.js
- **Backup**: PostGIS extensions for spatial queries

### Supabase
- **Role**: Lightweight data & file storage
- **Data**: Report files, temporary uploads, non-spatial metadata
- **Connection**: REST API with auth token
- **Why**: Scales independently, excellent for file storage

### PostGIS
- **Role**: Spatial database extension for PostgreSQL
- **Data**: Environmental sites, flood zones, facility locations
- **Queries**: Distance calculations, polygon intersections
- **Integration**: Used in GIS search module

### Nodemailer
- **Role**: Email notifications
- **Services**:
  - Order confirmations (client → recipient_email_1)
  - Admin notifications (all orders → info@geoscopesolutions.com)
  - Contact form responses
  - Report delivery notifications
- **Config**: Gmail or Namecheap Private Email

### GIS Search
- **Role**: Environmental data discovery
- **Sources**: EPA, FEMA, USGS, OSM APIs
- **Integration**: Queries via REST, results cached in PostgreSQL
- **Error Handling**: Graceful degradation with fallback empty results

## Email Notification System

### Automatic Emails Sent

1. **Contact Form Submission**
   - To: Admin (info@geoscopesolutions.com)
   - Content: Name, email, subject, message
   - To: Submitter (confirmation)

2. **New Order Created**
   - To: Admin (info@geoscopesolutions.com) with order details
   - To: Client (recipient_email_1) with confirmation
   - Note: Test clients flagged with "[TEST CLIENT]" tag

3. **Order Status Updates**
   - When analyst moves order through stages
   - Notification to relevant stakeholders

4. **Report Ready**
   - To: Client (recipient_email_1)
   - Content: Report link + download URL

## Test Client Whitelist

**Purpose**: Development/testing without charges

**Whitelisted Emails**:
- nyangelos4@gmail.com
- steveochibo@gmail.com
- test@geoscope.com

**Configuration**: Edit `testClientWhitelist` array in [server.js](geoscope/server.js#L4420)

**Effect**: 
- Orders marked as "is_test_client: true"
- Flagged in admin emails
- No payment processing

## Database Priority

### On Order Creation
1. Try PostgreSQL first (primary)
2. Then Supabase (backup/async)
3. Fall back to in-memory storage

### On Data Retrieval
1. PostgreSQL query (live data)
2. Supabase if PostgreSQL unavailable
3. In-memory cache as final fallback

## Error Handling

### GIS Search Errors
- **Old Behavior**: Return 500 error ❌
- **New Behavior**: Return empty data with fallback flag ✓
- **Log**: Console error for debugging

### Email Errors
- **Non-blocking**: Log error but don't fail order creation
- **User informed**: Response includes email status
- **Retry**: Manual resend available in admin panel

### Supabase Connection
- **Auto-fallback**: If unavailable, uses PostgreSQL only
- **No data loss**: PostgreSQL is authoritative

## Configuration Required

Set these environment variables in `.env` or Vercel Project Settings:

```bash
# PostgreSQL
PG_HOST=
PG_PORT=
PG_DATABASE=
PG_USER=
PG_PASSWORD=

# Email
GMAIL_USER=info@geoscopesolutions.com
GMAIL_PASS=<app-password>
ADMIN_NOTIFICATION_EMAIL=info@geoscopesolutions.com

# Supabase (optional)
SUPABASE_URL=
SUPABASE_ANON_KEY=
SUPABASE_SERVICE_KEY=
```

## Testing the Workflow

### Local Testing
```bash
# Start server
npm run dev

# Test contact form
curl -X POST http://localhost:6001/contact \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","email":"test@example.com","subject":"Test","message":"Hello"}'

# Test order creation
curl -X POST http://localhost:6001/client-orders \
  -H "Content-Type: application/json" \
  -d '{"project_name":"Test Project","client_company":"Test Co","recipient_email_1":"nyangelos4@gmail.com","address":"123 Main St"}'
```

### Production Testing (Vercel)
- Use Vercel logs to monitor email sends
- Check `info@geoscopesolutions.com` inbox for notifications
- Verify test client orders are flagged correctly

## Troubleshooting

### Emails Not Sending
- [ ] Check `GMAIL_USER` and `GMAIL_PASS` in environment variables
- [ ] Verify Gmail app password (not account password)
- [ ] Check `ADMIN_NOTIFICATION_EMAIL` is set
- [ ] Review Vercel logs for "Failed to send email" errors

### GIS Search Failing
- [ ] Check internet connectivity (APIs require external calls)
- [ ] Verify API rate limits not exceeded
- [ ] Check coordinates are valid (lat/lng range)
- [ ] Review server logs for specific API errors

### Orders Not in Database
- [ ] Verify PostgreSQL connection string in `.env`
- [ ] Check PostgreSQL server is running
- [ ] Verify orders table exists in database
- [ ] Check Vercel logs for connection errors

## Performance Optimization

- PostgreSQL queries cached for 5 minutes
- GIS search results cached per coordinate/radius
- Email sends async (non-blocking)
- Report PDFs generated in background
- Supabase files cached at CDN edge

## Future Enhancements

- [ ] Batch email notifications
- [ ] SMS alerts for urgent orders
- [ ] Webhook integrations (Slack, Teams)
- [ ] Custom email templates
- [ ] Report scheduling (recurring orders)
- [ ] API rate limiting per client
