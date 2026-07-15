const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');
const { BROKER } = require('../config/mqtt-config');

const CLIENT_ID = 'bankwatch-alert-sub-' + Math.random().toString(16).slice(2, 6);
const RESPONSE_TOPIC = `bankwatch/system/response/${CLIENT_ID}`;

const client = mqtt.connect(`mqtt://${BROKER.host}:${BROKER.portTCP}`, {
  clientId: CLIENT_ID,
  protocolVersion: 5,
  // Flow Control
  properties: {
    receiveMaximum: 5,   // Alert subscriber: batasi ketat, prioritas tinggi
  },
  keepalive: 30,
  reconnectPeriod: 3000,
});

let criticalCount = 0;
let highCount = 0;
const pendingRequests = new Map(); // Untuk request-response tracking

client.on('connect', () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   🔔 Alert Subscriber Connected           ║');
  console.log('║   Monitoring fraud alerts & system...     ║');
  console.log('╚══════════════════════════════════════════╝\n');

  // Wildcard + (single-level) — subscribe semua transfer
  client.subscribe('bankwatch/transfer/+', { qos: 1 }, (err) => {
    if (!err) console.log('✅ Subscribed: bankwatch/transfer/+ (all transfer types)');
  });

  // Subscribe fraud alerts dengan QoS 2
  client.subscribe('bankwatch/fraud/alert', { qos: 2 }, (err) => {
    if (!err) console.log('✅ Subscribed: bankwatch/fraud/alert (QoS 2)');
  });

  // Subscribe system status (retain → langsung dapat data terbaru)
  client.subscribe('bankwatch/system/status', { qos: 1 }, (err) => {
    if (!err) console.log('✅ Subscribed: bankwatch/system/status (retain)');
  });

  // Subscribe ke response topic untuk request-response
  client.subscribe(RESPONSE_TOPIC, { qos: 1 }, (err) => {
    if (!err) console.log(`✅ Subscribed: ${RESPONSE_TOPIC} (response topic)\n`);
  });

  // Kirim request ke semua publisher setelah 3 detik
  setTimeout(() => sendSystemRequest('ATM-PUBLISHER'), 3000);
  setTimeout(() => sendSystemRequest('TRANSFER-PUBLISHER'), 5000);
  setTimeout(() => sendSystemRequest('FRAUD-PUBLISHER'), 7000);
});

// Request-Response — kirim request ke publisher

function sendSystemRequest(target) {
  const correlationId = uuidv4();
  const payload = JSON.stringify({
    request_id: correlationId,
    target,
    requested_by: CLIENT_ID,
    timestamp: new Date().toISOString(),
  });

  pendingRequests.set(correlationId, { target, sentAt: Date.now() });

  client.publish('bankwatch/system/request', payload, {
    qos: 1,
    properties: {
      // Response Topic: publisher tahu harus balas ke mana
      responseTopic: RESPONSE_TOPIC,
      // Correlation Data: untuk matching request ↔ response
      correlationData: Buffer.from(correlationId),
      userProperties: {
        'request-type': 'system-info',
        'requester': CLIENT_ID,
      },
    },
  });

  console.log(`📤 [REQUEST] Sent to ${target} | CorrelationID: ${correlationId.slice(0, 8)}...`);
}

// ── Message handler ───────────────────────────────────────────────────────────

client.on('message', (topic, payload, packet) => {
  let data;
  try { data = JSON.parse(payload.toString()); } catch { return; }

  const userProps = packet.properties?.userProperties || {};

  // Fraud alerts
  if (topic === 'bankwatch/fraud/alert') {
    if (data.severity === 'CRITICAL') {
      criticalCount++;
      console.log(`\n🚨🚨 CRITICAL FRAUD ALERT #${criticalCount} 🚨🚨`);
      console.log(`   Pattern  : ${data.pattern}`);
      console.log(`   Location : ${data.location}`);
      console.log(`   Bank     : ${data.bank}`);
      console.log(`   Action   : ${data.action}`);
      console.log(`   Score    : ${data.fraud_score}/100`);
      console.log(`   Time     : ${data.timestamp}\n`);
    } else if (data.severity === 'HIGH') {
      highCount++;
      console.log(`⚠️  [HIGH ALERT #${highCount}] ${data.pattern} @ ${data.location} | Score: ${data.fraud_score}`);
    }
    return;
  }

  // Transfer monitoring
  if (topic.startsWith('bankwatch/transfer/')) {
    if (data.is_high_value) {
      console.log(`💰 [HIGH VALUE TRANSFER] ${data.type} | ${data.currency} ${(data.amount || 0).toLocaleString()} | ${data.status}`);
    }
    return;
  }

  // System status (retain messages)
  if (topic === 'bankwatch/system/status') {
    console.log(`📡 [SYSTEM] ${data.publisher}: ${data.status}`);
    return;
  }

  // Handle response dari publisher
  if (topic === RESPONSE_TOPIC) {
    const corrId = packet.properties?.correlationData?.toString();
    if (corrId && pendingRequests.has(corrId)) {
      const req = pendingRequests.get(corrId);
      const rtt = Date.now() - req.sentAt;
      pendingRequests.delete(corrId);

      console.log(`\n📨 [RESPONSE] From ${data.publisher} (RTT: ${rtt}ms)`);
      console.log(`   Status: ${data.status || 'OK'}`);
      if (data.atm_count) console.log(`   ATMs online: ${data.atm_count}`);
      if (data.patterns_monitored) console.log(`   Fraud patterns: ${data.patterns_monitored}`);
      console.log('');
    }
    return;
  }
});

client.on('error', (err) => console.error('❌ Alert Subscriber error:', err.message));
client.on('reconnect', () => console.log('🔄 Alert Subscriber reconnecting...'));

process.on('SIGINT', () => {
  console.log(`\n👋 Alert Subscriber shutting down.`);
  console.log(`   Critical alerts: ${criticalCount}`);
  console.log(`   High alerts: ${highCount}`);
  client.end();
  process.exit(0);
});