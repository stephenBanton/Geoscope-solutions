const axios = require('axios');

const baseUrl = process.env.API_BASE_URL || 'https://geoscope-api.vercel.app';

const scenarios = [
  {
    state: 'Florida',
    city: 'Miami',
    address: '100 Biscayne Blvd, Miami, FL 33132',
    latitude: 25.7617,
    longitude: -80.1918,
  },
  {
    state: 'Texas',
    city: 'Houston',
    address: '1000 Main St, Houston, TX 77002',
    latitude: 29.7604,
    longitude: -95.3698,
  },
  {
    state: 'California',
    city: 'Los Angeles',
    address: '200 N Spring St, Los Angeles, CA 90012',
    latitude: 34.0522,
    longitude: -118.2437,
  },
  {
    state: 'New York',
    city: 'New York',
    address: '1 Centre St, New York, NY 10007',
    latitude: 40.7128,
    longitude: -74.0060,
  },
];

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

async function runScenario(scenario) {
  const output = {
    state: scenario.state,
    city: scenario.city,
    address: scenario.address,
  };

  let orderResponse;
  const orderElapsed = stageTimer();
  try {
    orderResponse = await axios.post(`${baseUrl}/order`, {
      project_name: `Durable Report Smoke Test - ${scenario.state}`,
      client_name: 'GeoScope QA',
      email: 'info@geoscopesolutions.com',
      address: scenario.address,
      latitude: scenario.latitude,
      longitude: scenario.longitude,
      dataset_date: new Date().toISOString().slice(0, 10),
    }, {
      timeout: 120000,
    });
  } catch (error) {
    return {
      ...output,
      stage: 'order',
      elapsedMs: orderElapsed(),
      error: summarizeError(error),
    };
  }

  const orderId = orderResponse.data?.order_id
    || orderResponse.data?.id
    || orderResponse.data?.order?.id
    || orderResponse.data?.data?.id;

  if (!orderId) {
    return {
      ...output,
      stage: 'order',
      elapsedMs: orderElapsed(),
      error: {
        message: `Order creation did not return order id: ${JSON.stringify(orderResponse.data)}`,
        status: null,
        data: orderResponse.data,
      },
    };
  }

  let reportResponse;
  const reportElapsed = stageTimer();
  try {
    reportResponse = await axios.post(`${baseUrl}/generate-report`, {
      project_name: `Durable Report Smoke Test - ${scenario.state}`,
      client_name: 'GeoScope QA',
      client_company: 'GeoScope Solutions',
      address: scenario.address,
      latitude: scenario.latitude,
      longitude: scenario.longitude,
      fast_mode: true,
      paid: true,
      order_id: orderId,
      summary: `Production durable storage smoke test for ${scenario.state}.`,
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
    return {
      ...output,
      orderId,
      stage: 'generate-report',
      orderElapsedMs: orderElapsed(),
      elapsedMs: reportElapsed(),
      error: summarizeError(error),
    };
  }

  const downloadElapsed = stageTimer();
  let downloadResponse;
  try {
    downloadResponse = await axios.get(`${baseUrl}${reportResponse.data.downloadUrl}`, {
      maxRedirects: 0,
      validateStatus: () => true,
      timeout: 120000,
    });
  } catch (error) {
    return {
      ...output,
      orderId,
      stage: 'download',
      orderElapsedMs: orderElapsed(),
      reportElapsedMs: reportElapsed(),
      elapsedMs: downloadElapsed(),
      reportStored: reportResponse.data?.reportStored,
      storageAttempt: reportResponse.data?.storageAttempt || null,
      archiveAttempt: reportResponse.data?.archiveAttempt || null,
      error: summarizeError(error),
    };
  }

  return {
    ...output,
    stage: 'completed',
    orderId,
    orderElapsedMs: orderElapsed(),
    reportElapsedMs: reportElapsed(),
    downloadElapsedMs: downloadElapsed(),
    reportStored: reportResponse.data?.reportStored,
    reportPath: reportResponse.data?.reportPath,
    downloadUrl: reportResponse.data?.downloadUrl,
    downloadStatus: downloadResponse.status,
    redirectLocation: downloadResponse.headers?.location || null,
    storageAttempt: reportResponse.data?.storageAttempt || null,
    archiveAttempt: reportResponse.data?.archiveAttempt || null,
  };
}

async function main() {
  const results = [];

  for (const scenario of scenarios) {
    // Run serially to reduce server load and keep logs easier to inspect.
    /* eslint-disable no-await-in-loop */
    const result = await runScenario(scenario);
    results.push(result);
    console.log(JSON.stringify(result, null, 2));
    /* eslint-enable no-await-in-loop */
  }

  const summary = {
    total: results.length,
    completed: results.filter((r) => r.stage === 'completed').length,
    reportStoredTrue: results.filter((r) => r.reportStored === true).length,
    download200: results.filter((r) => r.downloadStatus === 200).length,
    failedStages: results
      .filter((r) => r.stage !== 'completed')
      .map((r) => ({ state: r.state, stage: r.stage, status: r.error?.status || null, message: r.error?.message || null })),
  };

  console.log(JSON.stringify({ summary }, null, 2));

  if (summary.reportStoredTrue !== results.length || summary.download200 !== results.length) {
    process.exitCode = 1;
  }
}

main().catch((error) => {
  console.error(JSON.stringify({ fatal: error?.message || String(error) }, null, 2));
  process.exit(1);
});
