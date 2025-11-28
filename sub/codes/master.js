const express = require('express');
const axios = require('axios');
const cors=require('cors');

const app = express();
app.use(cors());
app.use(express.json());

// Dynamic worker registry
const workers = {
  mappers: [],
  reducers: []
};

const MIN_MAPPERS = 2;
const MIN_REDUCERS = 2;

// Worker registration endpoint
app.post('/register', (req, res) => {
  const { type, address } = req.body;
  
  if (!type || !address) {
    return res.status(400).json({ 
      error: 'Missing required fields: type and address' 
    });
  }
  
  if (type !== 'mapper' && type !== 'reducer') {
    return res.status(400).json({ 
      error: 'Invalid worker type. Must be "mapper" or "reducer"' 
    });
  }
  
  const workerList = type === 'mapper' ? workers.mappers : workers.reducers;
  
  // Check if already registered
  if (workerList.includes(address)) {
    console.log(`[Master] ${type} at ${address} already registered`);
    return res.json({ 
      success: true, 
      message: 'Already registered',
      workerId: workerList.indexOf(address)
    });
  }
  
  // Register new worker
  workerList.push(address);
  const workerId = workerList.length - 1;
  
  console.log(`[Master] ✅ Registered ${type} #${workerId} at ${address}`);
  console.log(`[Master] Current workers:`, {
    mappers: workers.mappers.length,
    reducers: workers.reducers.length
  });
  
  res.json({ 
    success: true, 
    message: 'Registration successful',
    workerId,
    totalMappers: workers.mappers.length,
    totalReducers: workers.reducers.length
  });
});

// Get worker status
app.get('/workers', (req, res) => {
  res.json({
    mappers: workers.mappers.map((addr, idx) => ({ id: idx, address: addr })),
    reducers: workers.reducers.map((addr, idx) => ({ id: idx, address: addr })),
    counts: {
      mappers: workers.mappers.length,
      reducers: workers.reducers.length
    },
    requirements: {
      minMappers: MIN_MAPPERS,
      minReducers: MIN_REDUCERS
    },
    ready: workers.mappers.length >= MIN_MAPPERS && 
           workers.reducers.length >= MIN_REDUCERS
  });
});

// Unregister worker (optional - for graceful shutdown)
app.post('/unregister', (req, res) => {
  const { type, address } = req.body;
  const workerList = type === 'mapper' ? workers.mappers : workers.reducers;
  const index = workerList.indexOf(address);
  
  if (index > -1) {
    workerList.splice(index, 1);
    console.log(`[Master] ❌ Unregistered ${type} at ${address}`);
  }
  
  res.json({ success: true });
});

// Split text into chunks
function splitIntoChunks(text, numChunks) {
  const words = text.trim().split(/\s+/);
  const chunkSize = Math.ceil(words.length / numChunks);
  const chunks = [];
  
  for (let i = 0; i < numChunks; i++) {
    const start = i * chunkSize;
    const end = Math.min(start + chunkSize, words.length);
    if (start < words.length) {
      chunks.push({
        id: i,
        text: words.slice(start, end).join(' ')
      });
    }
  }
  return chunks;
}

// Shuffle: group map outputs by key
function shuffle(mapResults) {
  const grouped = {};
  
  mapResults.forEach(result => {
    Object.entries(result.counts).forEach(([word, count]) => {
      if (!grouped[word]) {
        grouped[word] = [];
      }
      grouped[word].push({ mapperId: result.mapperId, count });
    });
  });
  
  return grouped;
}

// Main MapReduce endpoint
app.post('/mapreduce', async (req, res) => {
  const { text } = req.body;
  
  // Check if enough workers are registered
  if (workers.mappers.length < MIN_MAPPERS) {
    return res.status(503).json({
      error: 'Insufficient mapper workers',
      message: `Need at least ${MIN_MAPPERS} mappers, but only ${workers.mappers.length} registered`,
      hint: 'Start more mapper workers and register them with the master'
    });
  }
  
  if (workers.reducers.length < MIN_REDUCERS) {
    return res.status(503).json({
      error: 'Insufficient reducer workers',
      message: `Need at least ${MIN_REDUCERS} reducers, but only ${workers.reducers.length} registered`,
      hint: 'Start more reducer workers and register them with the master'
    });
  }
  
  console.log('\n=== Starting MapReduce Job ===');
  console.log('Input:', text);
  console.log(`Using ${workers.mappers.length} mappers and ${workers.reducers.length} reducers`);
  
  try {
    // Step 1: Split input into chunks (use number of available mappers)
    const chunks = splitIntoChunks(text, workers.mappers.length);
    console.log(`\nSplit into ${chunks.length} chunks`);
    
    // Step 2: Map Phase - Send chunks to registered mappers
    console.log('\n--- MAP PHASE ---');
    const mapPromises = chunks.map((chunk, idx) => {
      const mapperUrl = workers.mappers[idx];
      console.log(`Sending chunk ${chunk.id} to ${mapperUrl}`);
      return axios.post(`${mapperUrl}/map`, { chunk }, { timeout: 5000 })
        .then(response => response.data)
        .catch(err => {
          console.error(`Mapper ${mapperUrl} failed:`, err.message);
          return null;
        });
    });
    
    const mapResults = (await Promise.all(mapPromises)).filter(r => r !== null);
    
    if (mapResults.length === 0) {
      return res.status(500).json({
        error: 'All mappers failed',
        message: 'No mapper returned results'
      });
    }
    
    console.log('\nMap results received:', mapResults.length);
    
    // Step 3: Shuffle Phase
    console.log('\n--- SHUFFLE PHASE ---');
    const shuffled = shuffle(mapResults);
    console.log('Grouped by keys:', Object.keys(shuffled));
    
    // Step 4: Reduce Phase - Distribute to registered reducers
    console.log('\n--- REDUCE PHASE ---');
    const words = Object.keys(shuffled);
    const reducePromises = words.map((word, idx) => {
      const reducerUrl = workers.reducers[idx % workers.reducers.length];
      console.log(`Sending word "${word}" to ${reducerUrl}`);
      return axios.post(`${reducerUrl}/reduce`, {
        word,
        values: shuffled[word]
      }, { timeout: 5000 })
        .then(response => response.data)
        .catch(err => {
          console.error(`Reducer ${reducerUrl} failed:`, err.message);
          return null;
        });
    });
    
    const reduceResults = (await Promise.all(reducePromises)).filter(r => r !== null);
    
    // Step 5: Collect final results
    const finalCounts = {};
    reduceResults.forEach(result => {
      finalCounts[result.word] = result.count;
    });
    
    console.log('\n=== MapReduce Job Complete ===');
    console.log('Final word counts:', finalCounts);
    
    res.json({
      success: true,
      workersUsed: {
        mappers: workers.mappers.length,
        reducers: workers.reducers.length
      },
      chunks,
      mapResults,
      shuffled,
      reduceResults,
      finalCounts
    });
    
  } catch (error) {
    console.error('MapReduce job failed:', error);
    res.status(500).json({ error: error.message });
  }
});

const PORT = process.argv[2] || 3000;
app.listen(PORT, () => {
  console.log(`\n🎯 Master node listening on port ${PORT}`);
  console.log(`📋 Worker registration endpoint: http://localhost:${PORT}/register`);
  console.log(`📊 Worker status endpoint: http://localhost:${PORT}/workers`);
  console.log(`🚀 MapReduce endpoint: http://localhost:${PORT}/mapreduce`);
  console.log(`\n⚠️  Minimum requirements: ${MIN_MAPPERS} mappers, ${MIN_REDUCERS} reducers`);
  console.log(`\n✨ Waiting for workers to register...`);
});