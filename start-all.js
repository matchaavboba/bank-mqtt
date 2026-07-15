const { spawn } = require('child_process');

const processes = [
  { name: 'Dashboard',          args: ['dashboard/server.js'],              color: '\x1b[36m' },
  { name: 'ATM Publisher',      args: ['publishers/atm-publisher.js'],      color: '\x1b[32m' },
  { name: 'Transfer Publisher', args: ['publishers/transfer-publisher.js'], color: '\x1b[33m' },
  { name: 'Fraud Publisher',    args: ['publishers/fraud-publisher.js'],    color: '\x1b[31m' },
  { name: 'Logger Worker-1',    args: ['subscribers/logger-subscriber.js'], color: '\x1b[35m', env: { WORKER_ID: '1' } },
  { name: 'Logger Worker-2',    args: ['subscribers/logger-subscriber.js'], color: '\x1b[95m', env: { WORKER_ID: '2' } },
  { name: 'Alert Subscriber',   args: ['subscribers/alert-subscriber.js'],  color: '\x1b[94m' },
];

const reset = '\x1b[0m';

console.log('\x1b[1m');
console.log('╔══════════════════════════════════════════╗');
console.log('║   🏦 BankWatch MQTT — Starting All       ║');
console.log('╚══════════════════════════════════════════╝');
console.log(reset);
console.log('Dashboard → http://localhost:3000\n');

const children = [];

processes.forEach(({ name, args, color, env }, i) => {
  setTimeout(() => {
    const proc = spawn('node', args, {
      cwd: __dirname,
      env: { ...process.env, ...env },
      shell: true,
    });

    children.push(proc);
    const prefix = `${color}[${name}]${reset}`;

    proc.stdout.on('data', d =>
      d.toString().split('\n').filter(Boolean).forEach(l => console.log(`${prefix} ${l}`))
    );
    proc.stderr.on('data', d =>
      d.toString().split('\n').filter(Boolean).forEach(l => console.log(`${prefix} \x1b[31m${l}${reset}`))
    );
    proc.on('exit', code => console.log(`${prefix} process exited (${code})`));

    console.log(`${prefix} Starting...`);
  }, i * 800);
});

process.on('SIGINT', () => {
  console.log('\n\nShutting down all processes...');
  children.forEach(p => p.kill('SIGINT'));
  setTimeout(() => process.exit(0), 2000);
});
