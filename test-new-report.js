const fs = require('fs');

(async () => {
  try {
    const payload = {
      project_name: 'Downtown Commercial Development',
      client_name: 'Metro Properties Inc',
      address: '456 Market Street, Boston, MA 02101',
      latitude: 42.3582,
      longitude: -71.0636,
      paid: true,
      summary: 'Environmental assessment with address-by-address structured analysis',
      radius: 250
    };

    console.log('📋 Generating address-by-address environmental report...\n');
    const res = await fetch('http://127.0.0.1:5000/generate-report', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const data = await res.json();
    console.log('✓ Response Status:', res.status);
    console.log('✓ Report File:', data.fileName);
    console.log('✓ Order ID:', data.orderId);
    
    if (data.filePath && fs.existsSync(data.filePath)) {
      const stats = fs.statSync(data.filePath);
      console.log('✓ File Size:', stats.size, 'bytes');
      console.log('\n✅ NEW ADDRESS-BY-ADDRESS REPORT GENERATED SUCCESSFULLY!');
      console.log('\nFile Location:', data.filePath);
      console.log('\n📊 Report Structure (NEW):');
      console.log('├─ Section 1: Executive Summary');
      console.log('├─ Section 2: Site Overview & Map');
      console.log('├─ Section 3: Detailed Location-Based Environmental Analysis (NEW!)');
      console.log('│  ├─ Location 1: [Address]');
      console.log('│  │  ├─ Type: [Feature Type]');
      console.log('│  │  ├─ Risk Level: [HIGH/MEDIUM/LOW]');
      console.log('│  │  └─ Environmental Findings: [Nearby Records]');
      console.log('│  ├─ Location 2: [Address]');
      console.log('│  └─ Location N: [Address]');
      console.log('├─ Section 4: Environmental Database Records (Reference)');
      console.log('├─ Section 5-17: Supporting Analysis');
      console.log('└─ Appendix: Database Coverage\n');
    } else {
      console.log('✗ Report file not found at:', data.filePath);
    }
  } catch (error) {
    console.error('❌ Error:', error.message);
  }
})();
