// Test script for polygon and report generation functionality
const axios = require('axios');

async function testPolygonReportGeneration() {
  try {
    console.log('Testing comprehensive PDF report generation with polygon analysis...');

    // Create polygon data
    const polygon = {
      type: 'Feature',
      geometry: {
        type: 'Polygon',
        coordinates: [[
          [-80.1918, 25.7617],
          [-80.1818, 25.7717],
          [-80.2018, 25.7717],
          [-80.1918, 25.7617]
        ]]
      }
    };

    // Calculate polygon analysis using Turf.js
    const turf = require('@turf/turf');
    const poly = turf.polygon(polygon.geometry.coordinates);
    const area = turf.area(poly);
    const perimeter = turf.length(poly, { units: 'meters' });

    const polygonAnalysis = {
      area: Math.round(area),
      areaAcres: area / 4046.86,
      perimeter: perimeter
    };

    // Fetch environmental data (this would normally be done with polygon filtering)
    const envResponse = await axios.get('http://localhost:5000/environmental-data', {
      params: {
        lat: '25.7617',
        lng: '-80.1918',
        radius: 2000
      }
    });

    // Filter sites inside polygon
    const filteredSites = envResponse.data.environmentalSites.filter(site => {
      const pt = turf.point([site.lng || site.longitude, site.lat || site.latitude]);
      return turf.booleanPointInPolygon(pt, poly);
    });

    const environmentalData = {
      ...envResponse.data,
      environmentalSites: filteredSites
    };

    // Generate report with polygon data
    const reportData = {
      project_name: 'Test Polygon Project',
      client_name: 'Test Client',
      address: '123 Test St, Miami, FL',
      latitude: 25.7617,
      longitude: -80.1918,
      paid: true,
      summary: 'This is a comprehensive environmental analysis for the polygon-defined property area.',
      environmentalData: environmentalData,
      polygonAnalysis: polygonAnalysis,
      radius: 2000
    };

    const response = await axios.post('http://localhost:5000/generate-report', reportData, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    console.log('Polygon report generated successfully!');
    console.log('Report details:', response.data);
    console.log('Sites in polygon:', filteredSites.length);

  } catch (error) {
    console.error('Error testing polygon report generation:', error.response?.data || error.message);
  }
}

async function testOrderCreation() {
  try {
    console.log('Testing order creation...');

    // Create test data
    const orderData = {
      project_name: 'Test Polygon Project',
      client_name: 'Test Client',
      email: 'test@example.com',
      address: '123 Test St, Miami, FL',
      latitude: '25.7617',
      longitude: '-80.1918',
      dataset_date: new Date().toISOString().split('T')[0]
    };

    const response = await axios.post('http://localhost:5000/order', orderData, {
      headers: {
        'Content-Type': 'application/json'
      }
    });

    console.log('Order created successfully:', response.data);

  } catch (error) {
    console.error('Error testing order creation:', error.response?.data || error.message);
  }
}

// Run the tests
async function runTests() {
  await testPolygonReportGeneration();
  await testOrderCreation();
}

runTests();