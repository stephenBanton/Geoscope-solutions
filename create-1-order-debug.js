const axios = require('axios');

(async () => {
  const api = 'https://geoscope-api.vercel.app/client-orders';
  const payload = {
    project_name: 'Debug Order NY',
    client_company: 'Geo Debug LLC',
    recipient_email_1: 'nyangelos4@gmail.com',
    address: '350 5th Ave, New York, NY 10118',
    latitude: 40.7484,
    longitude: -73.9857,
    notes: 'debug create'
  };

  try {
    const resp = await axios.post(api, payload, { timeout: 90000 });
    console.log('SUCCESS', resp.status, resp.data);
  } catch (err) {
    console.log('FAILED');
    console.log('status:', err?.response?.status);
    console.log('data:', JSON.stringify(err?.response?.data || { message: err.message }, null, 2));
  }
})();
