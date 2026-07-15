const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');
const { BROKER, ATM_NODES, BANKS } = require('../config/mqtt-config');

const CLIENT_ID = 'bankwatch-atm-publisher-' + Math.random().toString(16).slice(2, 6);

// Last Will Testament (LWT)
const client = mqtt.connect(`mqtt://${BROKER.host}:${BROKER.portTCP}`, {
  clientId: CLIENT_ID,
  protocolVersion: 5,         // MQTT v5 untuk User Properties & Topic Alias
  will: {
    topic: 'bankwatch/system/status',
    payload: JSON.stringify({
      publisher: 'ATM-PUBLISHER',
      status: 'OFFLINE',
      reason: 'Unexpected disconnection',
      timestamp: new Date().toISOString(),
    }),
    qos: 1,
    retain: true,
    properties: {
      willDelayInterval: 5,   // Tunggu 5 detik sebelum publish LWT
    },
  },
  keepalive: 30,
  reconnectPeriod: 3000,
});

// Flow control
const RECEIVE_MAXIMUM = 10;

// Topic Alias map
const topicAliasMap = {};
let aliasCounter = 1;

function getTopicAlias(topic) {
  if (!topicAliasMap[topic]) {
    topicAliasMap[topic] = aliasCounter++;
  }
  return topicAliasMap[topic];
}

// ── Transaction generators ────────────────────────────────────────────────────

const TX_TYPES = ['WITHDRAWAL', 'BALANCE_CHECK', 'TRANSFER', 'DEPOSIT'];

function randomAmount(min, max) {
  return Math.floor(Math.random() * (max - min + 1) + min) * 50000;
}

function generateTransaction(atm) {
  const txType = TX_TYPES[Math.floor(Math.random() * TX_TYPES.length)];
  const amount = txType === 'BALANCE_CHECK' ? 0 : randomAmount(1, 40);
  const isSuspicious = Math.random() < 0.08; // 8% chance suspicious

  return {
    tx_id: uuidv4(),
    atm_id: atm.id,
    location: atm.location,
    zone: atm.zone,
    type: txType,
    amount,
    currency: 'IDR',
    bank: BANKS[Math.floor(Math.random() * BANKS.length)],
    card_type: Math.random() > 0.3 ? 'DEBIT' : 'CREDIT',
    is_suspicious: isSuspicious,
    status: Math.random() > 0.05 ? 'SUCCESS' : 'FAILED',
    timestamp: new Date().toISOString(),
  };
}

// ── Publish ATM Status (RETAIN) ───────────────────────────────────────────────

function publishATMStatus(atm) {
  const topic = `bankwatch/atm/${atm.id}/status`;
  const payload = JSON.stringify({
    atm_id: atm.id,
    location: atm.location,
    zone: atm.zone,
    status: 'ONLINE',
    cash_level: Math.floor(Math.random() * 40 + 60),
    last_maintenance: new Date(Date.now() - Math.random() * 86400000 * 7).toISOString(),
    timestamp: new Date().toISOString(),
  });

  // Retain: subscriber baru langsung dapat status terkini
  client.publish(topic, payload, {
    qos: 1,
    retain: true,
    properties: {
      userProperties: {
        'app-version': '1.0.0',
        'publisher-role': 'atm-status',
        'data-type': 'status',
      },
    },
  });

  console.log(`📡 [RETAIN] ATM Status: ${atm.id} @ ${atm.location}`);
}

// ── Publish ATM Transaction ───────────────────────────────────────────────────

function publishTransaction(atm) {
  const tx = generateTransaction(atm);
  const topic = `bankwatch/atm/${atm.id}/transaction`;

  // Topic Alias
  const alias = getTopicAlias(topic);

  client.publish(topic, JSON.stringify(tx), {
    qos: tx.is_suspicious ? 2 : 1,   // QoS 2 untuk transaksi mencurigakan
    properties: {
      topicAlias: alias,
      userProperties: {
        'app-version': '1.0.0',
        'publisher-role': 'atm-transaction',
        'zone': atm.zone,
        'is-suspicious': tx.is_suspicious ? 'true' : 'false',
      },
    },
  });

  const icon = tx.is_suspicious ? '⚠️ ' : '💳';
  const amountStr = tx.amount > 0 ? `Rp ${tx.amount.toLocaleString('id-ID')}` : '-';
  console.log(`${icon} [ATM] ${atm.id} | ${tx.type} | ${amountStr} | ${tx.status}`);
}

// ── Main loop ─────────────────────────────────────────────────────────────────

client.on('connect', () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   🏧 ATM Publisher Connected              ║');
  console.log(`║   Client ID: ${CLIENT_ID.slice(-12)}      ║`);
  console.log('║   Publishing ATM transactions...          ║');
  console.log('╚══════════════════════════════════════════╝\n');

  // Request-Response
  client.subscribe('bankwatch/system/request', { qos: 1 });

  // Publish status semua ATM dengan RETAIN
  ATM_NODES.forEach(atm => publishATMStatus(atm));

  // Publish status publisher online
  client.publish('bankwatch/system/status', JSON.stringify({
    publisher: 'ATM-PUBLISHER',
    status: 'ONLINE',
    atm_count: ATM_NODES.length,
    timestamp: new Date().toISOString(),
  }), { qos: 1, retain: true });

  // Publish transaksi tiap 2 detik dari ATM random
  setInterval(() => {
    const atm = ATM_NODES[Math.floor(Math.random() * ATM_NODES.length)];
    publishTransaction(atm);
  }, 2000);

  // Refresh ATM status tiap 30 detik
  setInterval(() => {
    ATM_NODES.forEach(atm => publishATMStatus(atm));
  }, 30000);
});

// Handle Request-Response

client.on('message', (topic, payload, packet) => {
  if (topic === 'bankwatch/system/request') {
    const req = JSON.parse(payload.toString());
    if (req.target !== 'ATM-PUBLISHER') return;

    const responseTopic = packet.properties?.responseTopic || 'bankwatch/system/response/default';
    const correlationData = packet.properties?.correlationData;

    const responsePayload = {
      publisher: 'ATM-PUBLISHER',
      atm_count: ATM_NODES.length,
      atms: ATM_NODES.map(a => ({ id: a.id, location: a.location, zone: a.zone })),
      timestamp: new Date().toISOString(),
    };

    client.publish(responseTopic, JSON.stringify(responsePayload), {
      qos: 1,
      properties: {
        correlationData,
        userProperties: { 'response-for': req.target },
      },
    });

    console.log(`📨 [REQUEST-RESPONSE] Replied to ${responseTopic}`);
  }
});

client.on('error', (err) => console.error('❌ ATM Publisher error:', err.message));
client.on('reconnect', () => console.log('🔄 ATM Publisher reconnecting...'));
client.on('offline', () => console.log('📴 ATM Publisher offline'));

// Graceful shutdown
process.on('SIGINT', () => {
  console.log('\n👋 ATM Publisher shutting down gracefully...');
  client.publish('bankwatch/system/status', JSON.stringify({
    publisher: 'ATM-PUBLISHER',
    status: 'OFFLINE',
    reason: 'Graceful shutdown',
    timestamp: new Date().toISOString(),
  }), { qos: 1, retain: true }, () => {
    client.end();
    process.exit(0);
  });
});