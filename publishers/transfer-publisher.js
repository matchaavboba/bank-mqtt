const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');
const { BROKER, BANKS, CURRENCIES } = require('../config/mqtt-config');

const CLIENT_ID = 'bankwatch-transfer-publisher-' + Math.random().toString(16).slice(2, 6);

const client = mqtt.connect(`mqtt://${BROKER.host}:${BROKER.portTCP}`, {
  clientId: CLIENT_ID,
  protocolVersion: 5,
  // LWT
  will: {
    topic: 'bankwatch/system/status',
    payload: JSON.stringify({
      publisher: 'TRANSFER-PUBLISHER',
      status: 'OFFLINE',
      reason: 'Unexpected disconnection',
      timestamp: new Date().toISOString(),
    }),
    qos: 1,
    retain: true,
    properties: { willDelayInterval: 5 },
  },
  keepalive: 30,
  reconnectPeriod: 3000,
});

// ── Data generators ───────────────────────────────────────────────────────────

const DOMESTIC_CITIES = ['Jakarta', 'Surabaya', 'Bandung', 'Medan', 'Makassar', 'Bali', 'Yogyakarta'];
const COUNTRIES = ['Singapore', 'Malaysia', 'Australia', 'USA', 'Netherlands', 'Japan', 'UAE'];

function randomRupiah(minJuta, maxJuta) {
  return Math.floor(Math.random() * (maxJuta - minJuta + 1) + minJuta) * 1_000_000;
}

function generateDomesticTransfer() {
  const fromCity = DOMESTIC_CITIES[Math.floor(Math.random() * DOMESTIC_CITIES.length)];
  let toCity;
  do { toCity = DOMESTIC_CITIES[Math.floor(Math.random() * DOMESTIC_CITIES.length)]; }
  while (toCity === fromCity);

  const amount = randomRupiah(1, 500);
  const isHighValue = amount >= 100_000_000; // >= 100 juta = high value

  return {
    tx_id: uuidv4(),
    type: 'DOMESTIC_TRANSFER',
    from_bank: BANKS[Math.floor(Math.random() * BANKS.length)],
    to_bank: BANKS[Math.floor(Math.random() * BANKS.length)],
    from_city: fromCity,
    to_city: toCity,
    amount,
    currency: 'IDR',
    is_high_value: isHighValue,
    fee: Math.floor(amount * 0.0025),
    status: Math.random() > 0.03 ? 'SUCCESS' : 'FAILED',
    channel: Math.random() > 0.5 ? 'MOBILE_BANKING' : 'INTERNET_BANKING',
    timestamp: new Date().toISOString(),
  };
}

function generateIntlTransfer() {
  const currency = CURRENCIES[Math.floor(Math.random() * CURRENCIES.length)];
  const amount = currency === 'IDR'
    ? randomRupiah(10, 1000)
    : Math.floor(Math.random() * 9900 + 100);

  return {
    tx_id: uuidv4(),
    type: 'INTERNATIONAL_TRANSFER',
    from_bank: BANKS[Math.floor(Math.random() * BANKS.length)],
    to_country: COUNTRIES[Math.floor(Math.random() * COUNTRIES.length)],
    amount,
    currency,
    is_high_value: amount > 5000 || (currency === 'IDR' && amount > 500_000_000),
    exchange_rate: currency === 'IDR' ? 1 : Math.floor(Math.random() * 2000 + 14000),
    swift_code: 'SWIFT' + Math.random().toString(36).slice(2, 8).toUpperCase(),
    status: Math.random() > 0.05 ? 'SUCCESS' : 'FAILED',
    processing_time_ms: Math.floor(Math.random() * 3000 + 500),
    timestamp: new Date().toISOString(),
  };
}

// ── Publish functions ─────────────────────────────────────────────────────────

function publishDomestic() {
  const tx = generateDomesticTransfer();

  // QoS 0: domestic regular, QoS 2: high-value
  const qos = tx.is_high_value ? 2 : 0;

  client.publish('bankwatch/transfer/domestic', JSON.stringify(tx), {
    qos,
    properties: {
      userProperties: {
        'app-version': '1.0.0',
        'publisher-role': 'transfer-domestic',
        'is-high-value': tx.is_high_value ? 'true' : 'false',
        'channel': tx.channel,
      },
    },
  });

  const hvTag = tx.is_high_value ? ' 🔴 HIGH VALUE' : '';
  const amtStr = `Rp ${tx.amount.toLocaleString('id-ID')}`;
  console.log(`💸 [DOMESTIC] ${tx.from_city} → ${tx.to_city} | ${amtStr} | QoS ${qos}${hvTag}`);
}

function publishInternational() {
  const tx = generateIntlTransfer();

  // International selalu QoS 1 — pasti sampai, compliance requirement
  client.publish('bankwatch/transfer/international', JSON.stringify(tx), {
    qos: 1,
    properties: {
      userProperties: {
        'app-version': '1.0.0',
        'publisher-role': 'transfer-international',
        'currency': tx.currency,
        'destination': tx.to_country,
      },
    },
  });

  console.log(`🌍 [INTL] → ${tx.to_country} | ${tx.amount} ${tx.currency} | ${tx.status}`);
}

// ── Main ──────────────────────────────────────────────────────────────────────

client.on('connect', () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   💸 Transfer Publisher Connected         ║');
  console.log('║   Publishing domestic & intl transfers... ║');
  console.log('╚══════════════════════════════════════════╝\n');

  // Subscribe untuk request-response
  client.subscribe('bankwatch/system/request', { qos: 1 });

  // Publish online status
  client.publish('bankwatch/system/status', JSON.stringify({
    publisher: 'TRANSFER-PUBLISHER',
    status: 'ONLINE',
    timestamp: new Date().toISOString(),
  }), { qos: 1, retain: true });

  // Domestic transfer tiap 1.5 detik
  setInterval(publishDomestic, 1500);

  // International transfer tiap 4 detik
  setInterval(publishInternational, 4000);
});

// Request-Response
client.on('message', (topic, payload, packet) => {
  if (topic !== 'bankwatch/system/request') return;
  const req = JSON.parse(payload.toString());
  if (req.target !== 'TRANSFER-PUBLISHER') return;

  const responseTopic = packet.properties?.responseTopic || 'bankwatch/system/response/default';
  client.publish(responseTopic, JSON.stringify({
    publisher: 'TRANSFER-PUBLISHER',
    status: 'ONLINE',
    domestic_interval_ms: 1500,
    intl_interval_ms: 4000,
    timestamp: new Date().toISOString(),
  }), {
    qos: 1,
    properties: {
      correlationData: packet.properties?.correlationData,
      userProperties: { 'response-for': req.target },
    },
  });
  console.log(`📨 [REQUEST-RESPONSE] Replied to ${responseTopic}`);
});

client.on('error', (err) => console.error('❌ Transfer Publisher error:', err.message));
client.on('reconnect', () => console.log('🔄 Transfer Publisher reconnecting...'));

process.on('SIGINT', () => {
  console.log('\n👋 Transfer Publisher shutting down...');
  client.publish('bankwatch/system/status', JSON.stringify({
    publisher: 'TRANSFER-PUBLISHER',
    status: 'OFFLINE',
    reason: 'Graceful shutdown',
    timestamp: new Date().toISOString(),
  }), { qos: 1, retain: true }, () => { client.end(); process.exit(0); });
});