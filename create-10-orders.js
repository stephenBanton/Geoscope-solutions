const axios = require('axios');

const api = 'https://geoscope-api.vercel.app/client-orders';
const orders = [
  { project_name: 'Multi-State Order NY', client_company: 'Geo NY LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '350 5th Ave, New York, NY 10118', latitude: 40.7484, longitude: -73.9857, notes: 'batch create' },
  { project_name: 'Multi-State Order CA', client_company: 'Geo CA LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '1 Market St, San Francisco, CA 94105', latitude: 37.7946, longitude: -122.3943, notes: 'batch create' },
  { project_name: 'Multi-State Order TX', client_company: 'Geo TX LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '1100 Congress Ave, Austin, TX 78701', latitude: 30.2747, longitude: -97.7403, notes: 'batch create' },
  { project_name: 'Multi-State Order FL', client_company: 'Geo FL LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '400 S Monroe St, Tallahassee, FL 32399', latitude: 30.4381, longitude: -84.2813, notes: 'batch create' },
  { project_name: 'Multi-State Order IL', client_company: 'Geo IL LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '100 W Randolph St, Chicago, IL 60601', latitude: 41.8841, longitude: -87.6315, notes: 'batch create' },
  { project_name: 'Multi-State Order WA', client_company: 'Geo WA LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '700 5th Ave, Seattle, WA 98104', latitude: 47.6067, longitude: -122.3325, notes: 'batch create' },
  { project_name: 'Multi-State Order CO', client_company: 'Geo CO LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '200 E Colfax Ave, Denver, CO 80203', latitude: 39.7392, longitude: -104.9847, notes: 'batch create' },
  { project_name: 'Multi-State Order AZ', client_company: 'Geo AZ LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '1700 W Washington St, Phoenix, AZ 85007', latitude: 33.4483, longitude: -112.0970, notes: 'batch create' },
  { project_name: 'Multi-State Order GA', client_company: 'Geo GA LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '206 Washington St SW, Atlanta, GA 30334', latitude: 33.7490, longitude: -84.3880, notes: 'batch create' },
  { project_name: 'Multi-State Order NC', client_company: 'Geo NC LLC', recipient_email_1: 'nyangelos4@gmail.com', address: '16 W Jones St, Raleigh, NC 27601', latitude: 35.7804, longitude: -78.6391, notes: 'batch create' }
];

(async () => {
  const jobs = orders.map(async (o) => {
    try {
      const resp = await axios.post(api, o, { timeout: 120000 });
      const orderId = resp?.data?.order?.id ?? resp?.data?.persistedOrderId ?? null;
      return { project: o.project_name, state: o.address.split(',').pop().trim(), success: true, orderId, message: resp?.data?.message || 'ok' };
    } catch (err) {
      const msg = err?.response?.data?.error || err?.response?.data?.message || err.message;
      return { project: o.project_name, state: o.address.split(',').pop().trim(), success: false, orderId: null, message: msg };
    }
  });

  const results = await Promise.all(jobs);
  console.table(results);

  const success = results.filter((r) => r.success);
  const failed = results.filter((r) => !r.success);
  console.log(`SUCCESS_COUNT=${success.length}`);
  console.log(`FAILED_COUNT=${failed.length}`);
  if (success.length > 0) {
    console.log('CREATED_ORDER_IDS=' + success.map((s) => s.orderId).join(','));
  }
})();
