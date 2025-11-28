const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

// Get configuration from command line arguments
const PORT = process.argv[2] || 4001;
const MASTER_URL = process.argv[3] || 'http://localhost:3000';
const MY_ADDRESS = `http://localhost:${PORT}`;

// Reduce function: aggregate counts for a word
app.post('/reduce', (req, res) => {
  const { word, values } = req.body;
  
  console.log(`\n[Reducer ${PORT}] Processing word: "${word}"`);
  console.log(`Values from mappers:`, values);
  
  const totalCount = values.reduce((sum, val) => sum + val.count, 0);
  
  const result = {
    word,
    count: totalCount,
    sources: values.map(v => `Mapper-${v.mapperId}`)
  };
  
  console.log(`[Reducer ${PORT}] Total count for "${word}": ${totalCount}`);
  
  res.json(result);
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', type: 'reducer', address: MY_ADDRESS });
});

// Register with master on startup
async function registerWithMaster() {
  try {
    console.log(`\n[Reducer ${PORT}] Attempting to register with master at ${MASTER_URL}...`);
    
    const response = await axios.post(`${MASTER_URL}/register`, {
      type: 'reducer',
      address: MY_ADDRESS
    });
    
    console.log(`[Reducer ${PORT}] ✅ Successfully registered!`);
    console.log(`[Reducer ${PORT}] Worker ID: ${response.data.workerId}`);
    console.log(`[Reducer ${PORT}] Total mappers: ${response.data.totalMappers}`);
    console.log(`[Reducer ${PORT}] Total reducers: ${response.data.totalReducers}`);
    
  } catch (error) {
    console.error(`[Reducer ${PORT}] ❌ Failed to register with master:`, error.message);
    console.log(`[Reducer ${PORT}] Will retry in 5 seconds...`);
    setTimeout(registerWithMaster, 5000);
  }
}

// Graceful shutdown - unregister from master
process.on('SIGINT', async () => {
  console.log(`\n[Reducer ${PORT}] Shutting down...`);
  try {
    await axios.post(`${MASTER_URL}/unregister`, {
      type: 'reducer',
      address: MY_ADDRESS
    });
    console.log(`[Reducer ${PORT}] Unregistered from master`);
  } catch (error) {
    console.error(`[Reducer ${PORT}] Failed to unregister:`, error.message);
  }
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`\n🟢 Reducer node listening on port ${PORT}`);
  console.log(`📍 Address: ${MY_ADDRESS}`);
  console.log(`🎯 Master: ${MASTER_URL}`);
  registerWithMaster();
});
