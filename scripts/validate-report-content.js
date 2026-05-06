const fs = require('fs');
const pdfParse = require('pdf-parse');

async function main() {
  const file = process.argv[2];
  if (!file) {
    console.error('Usage: node scripts/validate-report-content.js <pdf-path>');
    process.exit(1);
  }
  const dataBuffer = fs.readFileSync(file);
  const data = await pdfParse(dataBuffer);
  const text = String(data.text || '');

  const checks = [
    'Proximity Interpretation',
    'Site Summary Table',
    'Risk by Distance (Count)',
    'Risk Category Mix',
    'Dataset Distribution',
    'Recognized Environmental Conditions',
    'LENDER-GRADE SCREENING CONCLUSION',
    'SSURGO Soil Analysis',
    'Pathway Analysis',
  ];

  const out = checks.map((c) => ({ phrase: c, found: text.includes(c) }));
  out.forEach((r) => {
    console.log(`${r.found ? 'FOUND' : 'MISS'}: ${r.phrase}`);
  });

  const pass = out.filter((r) => r.found).length;
  console.log(`\nSummary: ${pass}/${out.length} phrases found.`);
}

main().catch((err) => {
  console.error('Validation failed:', err.message);
  process.exit(1);
});
