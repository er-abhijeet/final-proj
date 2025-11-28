const { spawn } = require('child_process');
const os = require('os');
const fs = require('fs');
const path = require('path');

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

// Check if running in WSL
function isWSL() {
  if (platform !== 'linux') return false;
  
  try {
    const osRelease = fs.readFileSync('/proc/version', 'utf8').toLowerCase();
    return osRelease.includes('microsoft') || osRelease.includes('wsl');
  } catch {
    return false;
  }
}

// Function to check which terminal is available on Linux
function getLinuxTerminal() {
  const terminals = [
    { cmd: 'x-terminal-emulator', args: (title, command) => ['-e', `bash -c "${command}; exec bash"`] },
    { cmd: 'gnome-terminal', args: (title, command) => ['--title', title, '--', 'bash', '-c', `${command}; exec bash`] },
    { cmd: 'konsole', args: (title, command) => ['--title', title, '-e', `bash -c "${command}; exec bash"`] },
    { cmd: 'xfce4-terminal', args: (title, command) => ['--title', title, '-e', `bash -c "${command}; exec bash"`] },
    { cmd: 'xterm', args: (title, command) => ['-title', title, '-e', `bash -c "${command}; exec bash"`] },
    { cmd: 'mate-terminal', args: (title, command) => ['--title', title, '-e', `bash -c "${command}; exec bash"`] },
    { cmd: 'terminator', args: (title, command) => ['--title', title, '-e', `bash -c "${command}; exec bash"`] }
  ];

  for (const terminal of terminals) {
    try {
      const result = spawn('which', [terminal.cmd], { stdio: 'pipe' });
      const output = result.stdout?.toString().trim();
      if (output) {
        return terminal;
      }
    } catch {
      continue;
    }
  }
  
  return null;
}

// Function to start a server in background (for WSL or headless systems)
function startServerInBackground(server, index, total) {
  console.log(`[${index + 1}/${total}] Starting ${server.name} on port ${server.port} (background mode)...`);
  
  const logDir = path.join(process.cwd(), 'logs');
  if (!fs.existsSync(logDir)) {
    fs.mkdirSync(logDir);
  }
  
  const logFile = path.join(logDir, `${server.name.replace(/\s+/g, '_')}.log`);
  
  // Write header to log file
  fs.appendFileSync(logFile, `\n\n=== ${server.name} started at ${new Date().toISOString()} ===\n\n`);
  
  // Open file descriptor for logging
  const logFd = fs.openSync(logFile, 'a');
  
  const child = spawn(server.command, server.args, {
    detached: true,
    stdio: ['ignore', logFd, logFd],
    cwd: process.cwd()
  });
  
  child.unref();
  
  // Close the file descriptor in the parent process
  fs.close(logFd, () => {});
  
  console.log(`   → Logs: ${logFile}`);
  
  return new Promise(resolve => setTimeout(resolve, index === 0 ? 2000 : 1000));
}

// Function to start a server in a new terminal
function startServerInTerminal(server, index, total) {
  console.log(`[${index + 1}/${total}] Starting ${server.name} on port ${server.port}...`);
  
  let terminalCommand, terminalArgs;
  
  if (platform === 'win32') {
    // Windows - unchanged
    terminalCommand = 'cmd';
    terminalArgs = ['/c', 'start', `"${server.name}"`, 'cmd', '/k', 
                    `${server.command} ${server.args.join(' ')}`];
  } else if (platform === 'darwin') {
    // macOS - unchanged
    const script = `tell app "Terminal" to do script "cd ${process.cwd()} && echo '=== ${server.name} ===' && ${server.command} ${server.args.join(' ')}"`;
    terminalCommand = 'osascript';
    terminalArgs = ['-e', script];
  } else {
    // Linux or WSL
    if (isWSL() || !process.env.DISPLAY) {
      // WSL or headless - run in background
      return startServerInBackground(server, index, total);
    }
    
    // Linux with GUI
    const terminal = getLinuxTerminal();
    
    if (!terminal) {
      console.log(`   → No GUI terminal found, running in background mode...`);
      return startServerInBackground(server, index, total);
    }
    
    const command = `echo '=== ${server.name} ==='; ${server.command} ${server.args.join(' ')}`;
    terminalCommand = terminal.cmd;
    terminalArgs = terminal.args(server.name, command);
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
    console.log('   → Falling back to background mode...');
    return startServerInBackground(server, index, total);
  }
}

// Start all servers sequentially
async function startAllServers() {
  const runMode = (platform === 'linux' && (isWSL() || !process.env.DISPLAY)) ? 'background' : 'terminal';
  
  if (runMode === 'background') {
    console.log('🔧 Running in background mode (WSL or headless system detected)\n');
  }
  
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
  
  if (runMode === 'background') {
    console.log('\n📁 Server logs are in the ./logs directory');
    console.log('   To view logs: tail -f logs/<server_name>.log');
    console.log('\n⚠️  To stop servers, run: pkill -f "node.*master.js|node.*mapper.js|node.*reducer.js"');
  } else {
    console.log('\nTo stop all servers, close each terminal window.');
  }
  
  console.log('Press Ctrl+C here to exit this launcher (servers will keep running).\n');
  
  // Wait a bit for all servers to register
  await new Promise(resolve => setTimeout(resolve, 3000));
}

// Run the launcher
startAllServers().catch(err => {
  console.error('Error starting servers:', err);
  process.exit(1);
});