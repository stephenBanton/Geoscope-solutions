const axios = require('axios');

const api = 'https://geoscope-api.vercel.app/order';
const orders = [
  { state: 'NY', project_name: 'Batch Order NY', client_name: 'Geo NY LLC', email: 'nyangelos4@gmail.com', address: '350 5th Ave, New York, NY 10118', latitude: 40.7484, longitude: -73.9857 },
  { state: 'CA', project_name: 'Batch Order CA', client_name: 'Geo CA LLC', email: 'nyangelos4@gmail.com', address: '1 Market St, San Francisco, CA 94105', latitude: 37.7946, longitude: -122.3943 },
  { state: 'TX', project_name: 'Batch Order TX', client_name: 'Geo TX LLC', email: 'nyangelos4@gmail.com', address: '1100 Congress Ave, Austin, TX 78701', latitude: 30.2747, longitude: -97.7403 },
  { state: 'FL', project_name: 'Batch Order FL', client_name: 'Geo FL LLC', email: 'nyangelos4@gmail.com', address: '400 S Monroe St, Tallahassee, FL 32399', latitude: 30.4381, longitude: -84.2813 },
  { state: 'IL', project_name: 'Batch Order IL', client_name: 'Geo IL LLC', email: 'nyangelos4@gmail.com', address: '100 W Randolph St, Chicago, IL 60601', latitude: 41.8841, longitude: -87.6315 },
  { state: 'WA', project_name: 'Batch Order WA', client_name: 'Geo WA LLC', email: 'nyangelos4@gmail.com', address: '700 5th Ave, Seattle, WA 98104', latitude: 47.6067, longitude: -122.3325 },
  { state: 'CO', project_name: 'Batch Order CO', client_name: 'Geo CO LLC', email: 'nyangelos4@gmail.com', address: '200 E Colfax Ave, Denver, CO 80203', latitude: 39.7392, longitude: -104.9847 },
  { state: 'AZ', project_name: 'Batch Order AZ', client_name: 'Geo AZ LLC', email: 'nyangelos4@gmail.com', address: '1700 W Washington St, Phoenix, AZ 85007', latitude: 33.4483, longitude: -112.0970 },
  { state: 'GA', project_name: 'Batch Order GA', client_name: 'Geo GA LLC', email: 'nyangelos4@gmail.com', address: '206 Washington St SW, Atlanta, GA 30334', latitude: 33.7490, longitude: -84.3880 },
  { state: 'NC', project_name: 'Batch Order NC', client_name: 'Geo NC LLC', email: 'nyangelos4@gmail.com', address: '16 W Jones St, Raleigh, NC 27601', latitude: 35.7804, longitude: -78.6391 }
];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function createWithRetry(order, maxAttempts = 6) {
  let lastMessage = 'unknown';
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      const resp = await axios.post(api, order, { timeout: 45000 });
      const orderId = resp?.data?.data?.id ?? resp?.data?.order?.id ?? null;
      return { state: order.state, project: order.project_name, success: true, orderId, attempts: attempt, message: resp?.data?.message || 'ok' };
    } catch (err) {
      lastMessage = err?.response?.data?.error || err?.response?.data?.message || err.message;
      await sleep(1500 * attempt);
    }
  }
  return { state: order.state, project: order.project_name, success: false, orderId: null, attempts: maxAttempts, message: lastMessage };
}

(async () => {
  const results = [];
  for (const order of orders) {
    const result = await createWithRetry(order, 6);
    results.push(result);
    console.log(`${result.state}: ${result.success ? `CREATED ${result.orderId}` : `FAILED (${result.message})`} in ${result.attempts} attempt(s)`);
  }

  console.table(results);
  const success = results.filter((r) => r.success);
  const failed = results.filter((r) => !r.success);
  console.log(`SUCCESS_COUNT=${success.length}`);
  console.log(`FAILED_COUNT=${failed.length}`);
  console.log('CREATED=' + success.map((s) => `${s.state}:${s.orderId}`).join(','));
})();
