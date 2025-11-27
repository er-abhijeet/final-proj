const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

// Get configuration from command line arguments
const PORT = process.argv[2] || 3001;
const MASTER_URL = process.argv[3] || 'http://localhost:3000';
const MY_ADDRESS = `http://localhost:${PORT}`;

// Map function: count words in chunk
app.post('/map', (req, res) => {
  const { chunk } = req.body;
  
  console.log(`\n[Mapper ${PORT}] Processing chunk ${chunk.id}`);
  console.log(`Text: "${chunk.text}"`);
  
  const words = chunk.text.toLowerCase().split(/\s+/);
  const wordCount = {};
  
  words.forEach(word => {
    if (word) {
      wordCount[word] = (wordCount[word] || 0) + 1;
    }
  });
  
  const result = {
    mapperId: chunk.id,
    counts: wordCount
  };
  
  console.log(`[Mapper ${PORT}] Result:`, wordCount);
  
  res.json(result);
});

// Health check endpoint
app.get('/health', (req, res) => {
  res.json({ status: 'healthy', type: 'mapper', address: MY_ADDRESS });
});

// Register with master on startup
async function registerWithMaster() {
  try {
    console.log(`\n[Mapper ${PORT}] Attempting to register with master at ${MASTER_URL}...`);
    
    const response = await axios.post(`${MASTER_URL}/register`, {
      type: 'mapper',
      address: MY_ADDRESS
    });
    
    console.log(`[Mapper ${PORT}] ✅ Successfully registered!`);
    console.log(`[Mapper ${PORT}] Worker ID: ${response.data.workerId}`);
    console.log(`[Mapper ${PORT}] Total mappers: ${response.data.totalMappers}`);
    console.log(`[Mapper ${PORT}] Total reducers: ${response.data.totalReducers}`);
    
  } catch (error) {
    console.error(`[Mapper ${PORT}] ❌ Failed to register with master:`, error.message);
    console.log(`[Mapper ${PORT}] Will retry in 5 seconds...`);
    setTimeout(registerWithMaster, 5000);
  }
}

// Graceful shutdown - unregister from master
process.on('SIGINT', async () => {
  console.log(`\n[Mapper ${PORT}] Shutting down...`);
  try {
    await axios.post(`${MASTER_URL}/unregister`, {
      type: 'mapper',
      address: MY_ADDRESS
    });
    console.log(`[Mapper ${PORT}] Unregistered from master`);
  } catch (error) {
    console.error(`[Mapper ${PORT}] Failed to unregister:`, error.message);
  }
  process.exit(0);
});

app.listen(PORT, () => {
  console.log(`\n🔵 Mapper node listening on port ${PORT}`);
  console.log(`📍 Address: ${MY_ADDRESS}`);
  console.log(`🎯 Master: ${MASTER_URL}`);
  registerWithMaster();
});