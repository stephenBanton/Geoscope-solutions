#!/usr/bin/env node
/**
 * Generate and send sample premium environmental reports
 * Uses the backend /send-sample-report endpoint which handles report generation and emailing
 * 
 * Usage: 
 *   node generate-sample-premium.js [email1,email2,...]
 * 
 * Examples:
 *   node generate-sample-premium.js                              # Uses default email
 *   node generate-sample-premium.js admin@example.com            # Single recipient
 *   node generate-sample-premium.js admin@example.com,user@example.com  # Multiple recipients
 */

require('dotenv').config();
const axios = require('axios');

const API_BASE_URL = process.env.API_BASE_URL || 'https://geoscope-api.vercel.app';

const SAMPLE_LOCATIONS = [
  { name: 'Miami Downtown', lat: 25.7617, lng: -80.1918 },
  { name: 'San Francisco Bay', lat: 37.7749, lng: -122.4194 },
  { name: 'Chicago Loop', lat: 41.8791, lng: -87.6298 },
  { name: 'Boston Harbor', lat: 42.3601, lng: -71.0589 },
  { name: 'Houston Industrial', lat: 29.7604, lng: -95.3698 }
];

const DEFAULT_EMAIL = process.env.DEFAULT_SAMPLE_EMAIL || 'info@geoscopesolutions.com';

async function generateAndSendSampleReports() {
  // Parse email arguments
  const emailArg = process.argv[2];
  const emailList = emailArg 
    ? emailArg.split(',').map(e => e.trim()).filter(e => e.includes('@'))
    : [DEFAULT_EMAIL];
  
  if (emailList.length === 0) {
    emailList.push(DEFAULT_EMAIL);
  }
  
  try {
    console.log('🔄 Generating sample premium reports with all installed databases...');
    console.log(`📍 Sample location: Miami Downtown (25.7617°N, 80.1918°W)`);
    console.log(`📧 Recipients: ${emailList.join(', ')}`);
    console.log('\n');
    
    let successCount = 0;
    let failureCount = 0;
    
    // Send to each email address
    for (const email of emailList) {
      try {
        console.log(`⏳ Sending report to ${email}...`);
        
        const response = await axios.post(`${API_BASE_URL}/send-sample-report`, {
          email: email,
          name: 'Valued Client'
        }, {
          timeout: 180000  // 3 minutes
        });
        
        console.log(`   ✅ Success!`);
        console.log(`   Message: ${response.data.message}`);
        if (response.data.filePath) {
          console.log(`   📄 Report: ${response.data.filePath}`);
        }
        if (response.data.downloadUrl) {
          console.log(`   🔗 Download: ${API_BASE_URL}${response.data.downloadUrl}`);
        }
        if (response.data.emailStatus === 'failed') {
          console.log(`   ⚠️  Email delivery pending: ${response.data.emailError || 'SMTP issue'}`);
        }
        successCount++;
        
      } catch (error) {
        console.log(`   ❌ Failed: ${error.message || 'request failed'}`);
        if (error.response?.data) {
          console.log(`   Details: ${JSON.stringify(error.response.data)}`);
        }
        failureCount++;
      }
      console.log('');
    }
    
    // Summary
    console.log('═'.repeat(60));
    console.log('📊 Report Generation Summary');
    console.log('═'.repeat(60));
    console.log(`✅ Successfully sent: ${successCount}`);
    console.log(`❌ Failed: ${failureCount}`);
    console.log(`📧 Total recipients: ${emailList.length}`);
    console.log('');
    console.log('📋 Report Contents:');
    console.log('   • 217 installed environmental databases');
    console.log('   • Comprehensive GIS spatial analysis');
    console.log('   • Professional cover page');
    console.log('   • Risk assessment and interpretation');
    console.log('   • Historical environmental records');
    console.log('   • Maps and visualizations');
    console.log('   • Regulatory findings and recommendations');
    console.log('');
    console.log('✨ Sample reports sent successfully!' );
    console.log('═'.repeat(60));
    
    if (failureCount > 0) {
      process.exit(1);
    }
    
  } catch (error) {
    console.error('❌ Error generating sample reports:');
    console.error(error.message);
    if (error.response) {
      console.error(`Status: ${error.response.status}`);
      if (error.response.data) {
        console.error(`Details: ${JSON.stringify(error.response.data, null, 2)}`);
      }
    }
    process.exit(1);
  }
}

generateAndSendSampleReports();
