'use strict';

const fs = require('fs');
const path = require('path');

const BASE = 'http://localhost:5000';
const REPORTS_DIR = path.join(__dirname, '..', 'reports');

async function requestJson(url, options = {}) {
  const res = await fetch(url, options);
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    data = { raw: text };
  }
  if (!res.ok) {
    const message = data?.error || data?.message || `${res.status} ${res.statusText}`;
    throw new Error(message);
  }
  return data;
}

async function login(email, password) {
  return requestJson(`${BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ email, password })
  });
}

async function main() {
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const projectName = `Sample-Client-Flow-${stamp}`;

  const summary = {
    project_name: projectName,
    radius_miles: 0.5,
    checks: {},
    errors: []
  };

  try {
    const admin = await login('admin@geoscope.com', '1234');
    const analyst = await login('analyst@geoscope.com', '1234');
    const client = await login('client@geoscope.com', 'client123');
    summary.checks.logins = {
      admin: !!admin?.success,
      analyst: !!analyst?.success,
      client: !!client?.success
    };

    const analystHeaders = { Authorization: `Bearer ${analyst.token}` };

    const createForm = new FormData();
    createForm.set('project_name', projectName);
    createForm.set('client_name', 'ClientCo');
    createForm.set('client_company', 'ClientCo');
    createForm.set('recipient_email_1', 'client@geoscope.com');
    createForm.set('recipient_email_2', 'qa-notify@geoscope.com');
    createForm.set('address', 'Downtown Miami, Miami, Florida');
    createForm.set('latitude', '25.7617');
    createForm.set('longitude', '-80.1918');
    createForm.set('notes', 'End-to-end QA sample order');

    const created = await requestJson(`${BASE}/orders`, {
      method: 'POST',
      body: createForm
    });
    const orderId = created?.order?.id;
    summary.order_id = orderId;
    summary.checks.order_created = Number.isFinite(Number(orderId));

    const nearby = await requestJson(`${BASE}/nearby-search?lat=25.7617&lng=-80.1918&radius=0.5`);
    const results = Array.isArray(nearby?.results) ? nearby.results : [];
    summary.checks.analyst_nearby_search = true;
    summary.nearby_total_records = nearby?.summary?.total || results.length;
    summary.nearby_top_databases = Object.entries(nearby?.summary?.by_database || {})
      .sort((a, b) => b[1] - a[1])
      .slice(0, 8)
      .map(([k, v]) => `${k}:${v}`);

    const scout = await requestJson(`${BASE}/data/missing-databases/suggestions?lat=25.7617&lng=-80.1918&radius=0.5`, {
      headers: analystHeaders
    });
    const recommendations = Array.isArray(scout?.recommendations) ? scout.recommendations : [];
    summary.checks.missing_db_scout = true;
    summary.missing_db_recommendation_count = recommendations.length;

    let missingSavedName = null;
    if (recommendations.length > 0) {
      const first = recommendations[0];
      const saved = await requestJson(`${BASE}/data/missing-databases/register`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...analystHeaders
        },
        body: JSON.stringify({
          name: first.name,
          category: first.category,
          source_program: first.source_program,
          useful_info: `QA flow stored missing db request for ${projectName}`,
          search_terms: first.search_terms || [],
          country: 'USA',
          priority: 'high'
        })
      });
      missingSavedName = saved?.saved?.name || null;
    }
    summary.missing_db_saved_name = missingSavedName;

    await requestJson(`${BASE}/orders/${orderId}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Analyst',
        message: 'Reviewed 0.5mi radius datasets; proceeding to report generation.'
      })
    });
    summary.checks.analyst_message_posted = true;

    const sampleSites = results.slice(0, 40).map((r) => ({
      id: r.id,
      name: r.site_name,
      database: r.database,
      address: r.address,
      distance: Number.isFinite(Number(r.distance_m)) ? `${(Number(r.distance_m) / 1609.344).toFixed(2)} mi` : 'N/A',
      status: r.status,
      lat: r.lat,
      lng: r.lng
    }));

    const floodZones = results
      .filter((r) => r.database === 'FLOOD DFIRM')
      .map((r) => ({ attributes: { FLD_ZONE: String(r.site_name || '').replace('Flood Zone ', '') } }));

    const generated = await requestJson(`${BASE}/generate-report`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        order_id: orderId,
        project_name: projectName,
        client_name: 'ClientCo',
        client_company: 'ClientCo',
        address: 'Downtown Miami, Miami, Florida',
        latitude: 25.7617,
        longitude: -80.1918,
        radius: 0.5,
        paid: true,
        summary: 'Detailed environmental screening sample generated through full client -> analyst -> admin workflow.',
        addressLevelReport: null,
        environmentalData: {
          environmentalSites: sampleSites,
          floodZones,
          schools: [],
          governmentRecords: [],
          rainfall: []
        }
      })
    });

    summary.checks.report_generated = !!generated?.success;
    summary.report_download_url = generated?.downloadUrl || null;

    const stageUpdate = await requestJson(`${BASE}/orders/${orderId}/stage`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        stage: 'ADMIN_REVIEW',
        from: 'Analyst',
        note: 'Detailed sample report generated and queued for admin review.'
      })
    });

    summary.checks.stage_to_admin_review = !!stageUpdate?.success;
    summary.stage_after_analyst = stageUpdate?.order?.stage || null;
    summary.status_after_analyst = stageUpdate?.order?.status || null;

    await requestJson(`${BASE}/orders/${orderId}/messages`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        from: 'Admin',
        message: 'Admin reviewed sample report and triggered client delivery notification.'
      })
    });
    summary.checks.admin_message_posted = true;

    let latestReport = null;
    try {
      const files = fs.readdirSync(REPORTS_DIR)
        .filter((f) => f.toLowerCase().endsWith('.pdf'))
        .map((f) => ({
          name: f,
          fullPath: path.join(REPORTS_DIR, f),
          mtime: fs.statSync(path.join(REPORTS_DIR, f)).mtimeMs
        }))
        .sort((a, b) => b.mtime - a.mtime);
      latestReport = files[0] || null;
    } catch {
      latestReport = null;
    }

    summary.latest_report_file = latestReport?.fullPath || null;
    summary.checks.report_file_exists = !!latestReport;

    let sendResult = null;
    let sendError = null;
    if (latestReport) {
      try {
        sendResult = await requestJson(`${BASE}/send-to-client`, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            email: 'client@geoscope.com',
            filePath: latestReport.fullPath
          })
        });
      } catch (err) {
        sendError = err.message;
      }
    }

    summary.checks.client_send_attempted = !!latestReport;
    summary.checks.client_send_success = !!sendResult;
    summary.client_send_response = sendResult?.message || null;
    summary.client_send_error = sendError;

    const finalOrder = await requestJson(`${BASE}/orders/${orderId}`);
    summary.final_stage = finalOrder?.stage || null;
    summary.final_status = finalOrder?.status || null;
    summary.messages_count = Array.isArray(finalOrder?.messages) ? finalOrder.messages.length : 0;
    summary.latest_messages = Array.isArray(finalOrder?.messages) ? finalOrder.messages.slice(-4) : [];
    summary.checks.messages_working = summary.messages_count >= 2;
  } catch (err) {
    summary.errors.push(err.message);
  }

  console.log(JSON.stringify(summary, null, 2));
}

main();