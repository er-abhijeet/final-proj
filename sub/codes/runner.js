const { spawn } = require('child_process');
const os = require('os');

const platform = os.platform();
console.log('Starting MapReduce Distributed System...\n');

// Configuration
const servers = [
  { name: 'Master Node', command: 'node', args: ['master.js'], port: 3000 },
  { name: 'Mapper 1', command: 'node', args: ['mapper.js', '3001'], port: 3001 },
  { name: 'Mapper 2', command: 'node', args: ['mapper.js', '3002'], port: 3002 },
  { name: 'Reducer 1', command: 'node', args: ['reducer.js', '4001'], port: 4001 },
  { name: 'Reducer 2', command: 'node', args: ['reducer.js', '4002'], port: 4002 }
];

// Function to start a server in a new terminal
function startServerInTerminal(server, index, total) {
  console.log(`[${index + 1}/${total}] Starting ${server.name} on port ${server.port}...`);
  
  let terminalCommand, terminalArgs;
  
  if (platform === 'win32') {
    // Windows
    terminalCommand = 'cmd';
    terminalArgs = ['/c', 'start', `"${server.name}"`, 'cmd', '/k', 
                    `${server.command} ${server.args.join(' ')}`];
  } else if (platform === 'darwin') {
    // macOS
    const script = `tell app "Terminal" to do script "cd ${process.cwd()} && echo '=== ${server.name} ===' && ${server.command} ${server.args.join(' ')}"`;
    terminalCommand = 'osascript';
    terminalArgs = ['-e', script];
  } else {
    // Linux
    terminalCommand = 'gnome-terminal';
    terminalArgs = ['--title', server.name, '--', 'bash', '-c', 
                    `echo '=== ${server.name} ==='; ${server.command} ${server.args.join(' ')}; exec bash`];
  }
  
  try {
    const child = spawn(terminalCommand, terminalArgs, {
      detached: true,
      stdio: 'ignore',
      shell: platform === 'win32'
    });
    child.unref();
    
    return new Promise(resolve => setTimeout(resolve, index === 0 ? 2000 : 1000));
  } catch (error) {
    console.error(`Failed to start ${server.name}:`, error.message);
    return Promise.resolve();
  }
}

// Start all servers sequentially
async function startAllServers() {
  for (let i = 0; i < servers.length; i++) {
    await startServerInTerminal(servers[i], i, servers.length);
  }
  
  console.log('\n=====================================');
  console.log('All servers started successfully!');
  console.log('=====================================\n');
  
  servers.forEach(server => {
    console.log(`${server.name.padEnd(15)} http://localhost:${server.port}`);
  });
  
  console.log('\nCheck worker status: curl http://localhost:3000/workers');
//   console.log('\nStarting frontend server...\n');
  
  // Wait a bit for all servers to register
  await new Promise(resolve => setTimeout(resolve, 3000));
  
//   // Start frontend
//   console.log('Starting Frontend Server on port 8080...');
//   await startServerInTerminal(
//     { name: 'Frontend Server', command: 'node', args: ['frontend-server.js', '8080'], port: 8080 },
//     5, 6
//   );
  
//   console.log('\n✅ Frontend available at: http://localhost:8080');
  console.log('\nTo stop all servers, close each terminal window.');
  console.log('Press Ctrl+C here to exit this launcher (servers will keep running).\n');
}

// Run the launcher
startAllServers().catch(err => {
  console.error('Error starting servers:', err);
  process.exit(1);
});