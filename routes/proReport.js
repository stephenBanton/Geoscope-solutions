const express = require('express');
const router = express.Router();

const { getFeatures } = require('../services/features');
const { getDatasets } = require('../services/datasets');
const { assignRisksToAddresses } = require('../services/proAnalysis');

router.post('/generate-pro-report', async (req, res) => {
  try {
    const { lat, lng, radius = 250 } = req.body;

    if (!lat || !lng) {
      return res.status(400).json({ error: 'lat and lng are required' });
    }

    const features = await getFeatures(lat, lng, radius);
    const datasets = await getDatasets(lat, lng, radius);
    const enriched = assignRisksToAddresses(features, datasets, radius);

    res.json({
      success: true,
      total_addresses: enriched.length,
      high: enriched.filter((item) => item.risks.length > 2).length,
      medium: enriched.filter((item) => item.risks.length > 0 && item.risks.length <= 2).length,
      low: enriched.filter((item) => item.risks.length === 0).length,
      results: enriched
    });
  } catch (error) {
    console.error('[generate-pro-report]', error.message);
    res.status(500).json({ error: error.message || 'Failed to generate pro report data' });
  }
});

module.exports = router;
