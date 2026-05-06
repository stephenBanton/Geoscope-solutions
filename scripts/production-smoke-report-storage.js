const axios = require('axios');

const baseUrl = process.env.API_BASE_URL || 'https://geoscope-api.vercel.app';

function stageTimer() {
  const start = Date.now();
  return () => Date.now() - start;
}

function summarizeError(error) {
  return {
    message: error?.message || 'Unknown error',
    status: error?.response?.status || null,
    data: error?.response?.data || null,
  };
}

async function main() {
  let orderResponse;
  const orderElapsed = stageTimer();
  let orderElapsedMs = null;
  try {
    orderResponse = await axios.post(`${baseUrl}/order`, {
      project_name: 'Durable Report Smoke Test',
      client_name: 'GeoScope QA',
      email: 'info@geoscopesolutions.com',
      address: '100 Biscayne Blvd, Miami, FL 33132',
      latitude: 25.7617,
      longitude: -80.1918,
      dataset_date: new Date().toISOString().slice(0, 10),
    }, {
      timeout: 120000,
    });
  } catch (error) {
    throw new Error(JSON.stringify({
      stage: 'order',
      elapsedMs: orderElapsed(),
      ...summarizeError(error),
    }));
  }
  orderElapsedMs = orderElapsed();

  const orderId = orderResponse.data?.order_id
    || orderResponse.data?.id
    || orderResponse.data?.order?.id
    || orderResponse.data?.data?.id;
  if (!orderId) {
    throw new Error(`Order creation did not return an order id: ${JSON.stringify(orderResponse.data)}`);
  }

  let reportResponse;
  const reportElapsed = stageTimer();
  let reportElapsedMs = null;
  try {
    reportResponse = await axios.post(`${baseUrl}/generate-report`, {
      project_name: 'Durable Report Smoke Test',
      client_name: 'GeoScope QA',
      client_company: 'GeoScope Solutions',
      address: '100 Biscayne Blvd, Miami, FL 33132',
      latitude: 25.7617,
      longitude: -80.1918,
      fast_mode: true,
      paid: true,
      order_id: orderId,
      summary: 'Production durable storage smoke test.',
      environmentalData: {
        environmentalSites: [
          {
            id: 'S1',
            name: 'Fuel Terminal',
            database: 'EPA FUELS',
            database_name: 'EPA FUELS',
            address: '111 Harbor Rd',
            distance: 480,
            status: 'Active',
          },
          {
            id: 'S2',
            name: 'Historic Dry Cleaner',
            database: 'SCRD DRYCLEANERS',
            database_name: 'SCRD DRYCLEANERS',
            address: '222 Market St',
            distance: 960,
            status: 'Closed',
          },
        ],
        floodZones: [{ attributes: { FLD_ZONE: 'AE', SFHA_TF: 'T' } }],
        schools: [],
        wetlands: [],
        rainfall: [{ date: '2024-01-01', precipitation: '18 mm' }],
        governmentRecords: [],
      },
    }, {
      timeout: 420000,
    });
  } catch (error) {
    throw new Error(JSON.stringify({
      stage: 'generate-report',
      orderId,
      elapsedMs: reportElapsed(),
      ...summarizeError(error),
    }));
  }
  reportElapsedMs = reportElapsed();

  let downloadResponse;
  const downloadElapsed = stageTimer();
  let downloadElapsedMs = null;
  try {
    downloadResponse = await axios.get(`${baseUrl}${reportResponse.data.downloadUrl}`, {
      maxRedirects: 0,
      validateStatus: () => true,
      timeout: 120000,
    });
  } catch (error) {
    throw new Error(JSON.stringify({
      stage: 'download',
      orderId,
      elapsedMs: downloadElapsed(),
      ...summarizeError(error),
    }));
  }
  downloadElapsedMs = downloadElapsed();

  console.log(JSON.stringify({
    orderId,
    orderElapsedMs,
    reportElapsedMs,
    downloadElapsedMs,
    reportStored: reportResponse.data?.reportStored,
    storageAttempt: reportResponse.data?.storageAttempt || null,
    archiveAttempt: reportResponse.data?.archiveAttempt || null,
    reportPath: reportResponse.data?.reportPath,
    downloadUrl: reportResponse.data?.downloadUrl,
    downloadStatus: downloadResponse.status,
    redirectLocation: downloadResponse.headers?.location || null,
  }, null, 2));
}

main().catch((error) => {
  const details = error.response?.data || error.message;
  console.error(JSON.stringify({ error: details }, null, 2));
  process.exit(1);
});
