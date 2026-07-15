const mqtt = require('mqtt');
const fs = require('fs');
const path = require('path');
const { BROKER } = require('../config/mqtt-config');

// Shared Subscription
// Format: $share/{group}/{topic}
// Jalankan 2 instance logger → beban dibagi otomatis oleh broker
const WORKER_ID = process.env.WORKER_ID || '1';
const CLIENT_ID = `bankwatch-logger-${WORKER_ID}-` + Math.random().toString(16).slice(2, 6);

const client = mqtt.connect(`mqtt://${BROKER.host}:${BROKER.portTCP}`, {
  clientId: CLIENT_ID,
  protocolVersion: 5,
  // Flow Control
  // Batasi jumlah pesan in-flight yang diterima sebelum ACK
  properties: {
    receiveMaximum: 20,   // Max 20 pesan in-flight sekaligus
  },
  keepalive: 60,
  reconnectPeriod: 5000,
  clean: false,           // Persistent session → pesan tidak hilang saat offline
});

// ── Log file setup ────────────────────────────────────────────────────────────
const LOG_DIR = path.join(__dirname, '../logs');
if (!fs.existsSync(LOG_DIR)) fs.mkdirSync(LOG_DIR, { recursive: true });

const logFile = path.join(LOG_DIR, `transactions-worker${WORKER_ID}.log`);
const logStream = fs.createWriteStream(logFile, { flags: 'a' });

let messageCount = 0;
let fraudCount = 0;
let alertCount = 0;

function writeLog(data) {
  const entry = JSON.stringify({ ...data, logged_at: new Date().toISOString() });
  logStream.write(entry + '\n');
}

// ── Connect & Subscribe ───────────────────────────────────────────────────────

client.on('connect', () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log(`║   📋 Logger Subscriber Worker ${WORKER_ID}           ║`);
  console.log(`║   Client: ${CLIENT_ID.slice(-16)}     ║`);
  console.log('╚══════════════════════════════════════════╝\n');

  // Shared Subscription
  // $share/logger-group/bankwatch/# → semua worker dalam grup berbagi beban
  const sharedTopic = '$share/logger-group/bankwatch/#';

  client.subscribe(sharedTopic, { qos: 1 }, (err) => {
    if (err) {
      console.error('❌ Subscribe failed:', err.message);
      return;
    }
    console.log(`✅ Subscribed to shared topic: ${sharedTopic}`);
    console.log(`📂 Logging to: ${logFile}\n`);
  });
});

// Message Handler

client.on('message', (topic, payload, packet) => {
  messageCount++;
  let data;

  try {
    data = JSON.parse(payload.toString());
  } catch {
    return; // Skip non-JSON
  }

  // Ekstrak User Properties
  const userProps = packet.properties?.userProperties || {};
  const publisherRole = userProps['publisher-role'] || 'unknown';

  // Log semua ke file
  writeLog({ topic, data, user_properties: userProps });

  // Console output berdasarkan topic
  if (topic.includes('fraud/alert')) {
    alertCount++;
    console.log(`🚨 [ALERT #${alertCount}] Worker${WORKER_ID} | ${data.severity} | ${data.pattern} @ ${data.location}`);
  } else if (topic.includes('fraud/score')) {
    fraudCount++;
    if (messageCount % 5 === 0) { // Print setiap 5 pesan biar tidak spam
      console.log(`📊 [FRAUD] Worker${WORKER_ID} | Score: ${data.fraud_score} | ${data.risk_level}`);
    }
  } else if (topic.includes('atm') && topic.includes('transaction')) {
    if (messageCount % 10 === 0) {
      console.log(`💳 [ATM] Worker${WORKER_ID} | msg #${messageCount} | ${data.type} | ${data.atm_id}`);
    }
  } else if (topic.includes('transfer')) {
    if (messageCount % 8 === 0) {
      console.log(`💸 [TRANSFER] Worker${WORKER_ID} | msg #${messageCount} | ${data.type}`);
    }
  }

  // Stats tiap 50 pesan
  if (messageCount % 50 === 0) {
    console.log(`\n📈 [STATS Worker${WORKER_ID}] Total: ${messageCount} | Alerts: ${alertCount} | Fraud: ${fraudCount}\n`);
  }
});

client.on('error', (err) => console.error('❌ Logger error:', err.message));
client.on('reconnect', () => console.log(`🔄 Logger Worker${WORKER_ID} reconnecting...`));
client.on('offline', () => console.log(`📴 Logger Worker${WORKER_ID} offline — messages queued`));

process.on('SIGINT', () => {
  console.log(`\n👋 Logger Worker${WORKER_ID} shutting down... Logged ${messageCount} messages.`);
  logStream.end();
  client.end();
  process.exit(0);
});