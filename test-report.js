// Test script for comprehensive PDF report generation
const axios = require('axios');

async function testComprehensiveReport() {
  try {
    console.log('Testing comprehensive PDF report generation...');

    // Test data with polygon analysis
    const testData = {
      project_name: 'Downtown Miami Development',
      client_name: 'ABC Construction Corp',
      address: '123 Main St, Miami, FL 33101',
      latitude: '25.7617',
      longitude: '-80.1918',
      radius: '1500',
      paid: true,
      summary: 'Comprehensive environmental analysis completed for the downtown Miami development project. The analysis identified several environmental sites within the project area, including fuel stations and industrial facilities. Risk assessment indicates moderate to high environmental concerns that should be evaluated further.',
      polygonAnalysis: {
        area: 25000,
        areaAcres: 6.18,
        perimeter: 650
      },
      environmentalData: {
        environmentalSites: [
          {
            id: 'DEMO1',
            name: 'Miami Fuel Depot',
            database: 'EPA NPL',
            address: '100 Fuel St, Miami, FL',
            distance: '0.2 mi',
            status: 'Active'
          },
          {
            id: 'DEMO2',
            name: 'Industrial Waste Facility',
            database: 'CERCLIS',
            address: '200 Industrial Ave, Miami, FL',
            distance: '0.5 mi',
            status: 'Closed'
          },
          {
            id: 'DEMO3',
            name: 'City School District',
            database: 'NCES',
            address: '300 Education Blvd, Miami, FL',
            distance: '0.8 mi',
            status: 'Active'
          }
        ],
        floodZones: [{ attributes: { FLD_ZONE: 'AE' } }],
        schools: [{ attributes: { NAME: 'Miami Central High School' } }],
        governmentRecords: []
      }
    };

    const response = await axios.post('http://localhost:5000/generate-report', testData);

    console.log('Report generated successfully:', response.data);

  } catch (error) {
    console.error('Error testing report generation:', error.response?.data || error.message);
  }
}

// Run the test
testComprehensiveReport();