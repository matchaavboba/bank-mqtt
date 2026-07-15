const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');
const { BROKER, ATM_NODES, BANKS } = require('../config/mqtt-config');

const CLIENT_ID = 'bankwatch-fraud-publisher-' + Math.random().toString(16).slice(2, 6);

const client = mqtt.connect(`mqtt://${BROKER.host}:${BROKER.portTCP}`, {
  clientId: CLIENT_ID,
  protocolVersion: 5,
  // LWT
  will: {
    topic: 'bankwatch/system/status',
    payload: JSON.stringify({
      publisher: 'FRAUD-PUBLISHER',
      status: 'OFFLINE',
      reason: 'Fraud engine disconnected unexpectedly!',
      timestamp: new Date().toISOString(),
    }),
    qos: 2,
    retain: true,
    properties: { willDelayInterval: 3 },
  },
  keepalive: 30,
  reconnectPeriod: 3000,
});

// ── Fraud score simulator ─────────────────────────────────────────────────────

const FRAUD_PATTERNS = [
  { name: 'CARD_CLONING',         weight: 20, severity: 'HIGH',     description: 'Duplicate card activity detected' },
  { name: 'VELOCITY_CHECK',       weight: 30, severity: 'MEDIUM',   description: 'Too many transactions in short time' },
  { name: 'UNUSUAL_LOCATION',     weight: 15, severity: 'HIGH',     description: 'Transaction from unusual location' },
  { name: 'AMOUNT_ANOMALY',       weight: 25, severity: 'MEDIUM',   description: 'Amount deviates significantly from pattern' },
  { name: 'AFTER_HOURS',          weight: 10, severity: 'LOW',      description: 'Transaction outside normal hours' },
  { name: 'INTERNATIONAL_SPIKE',  weight: 8,  severity: 'HIGH',     description: 'Sudden international transactions' },
  { name: 'ACCOUNT_TAKEOVER',     weight: 5,  severity: 'CRITICAL', description: 'Multiple failed attempts then success' },
];

function pickPattern() {
  const total = FRAUD_PATTERNS.reduce((s, p) => s + p.weight, 0);
  let r = Math.random() * total;
  for (const p of FRAUD_PATTERNS) {
    r -= p.weight;
    if (r <= 0) return p;
  }
  return FRAUD_PATTERNS[0];
}

function generateFraudScore(txId) {
  const score = Math.floor(Math.random() * 100);
  const pattern = pickPattern();
  const atm = ATM_NODES[Math.floor(Math.random() * ATM_NODES.length)];

  return {
    score_id: uuidv4(),
    tx_id: txId || uuidv4(),
    fraud_score: score,
    risk_level: score >= 80 ? 'CRITICAL' : score >= 60 ? 'HIGH' : score >= 40 ? 'MEDIUM' : 'LOW',
    pattern: pattern.name,
    description: pattern.description,
    atm_id: atm.id,
    location: atm.location,
    bank: BANKS[Math.floor(Math.random() * BANKS.length)],
    action_taken: score >= 80 ? 'BLOCKED' : score >= 60 ? 'FLAGGED' : 'MONITORED',
    timestamp: new Date().toISOString(),
  };
}

function generateFraudAlert(score) {
  return {
    alert_id: uuidv4(),
    severity: score.risk_level,
    fraud_score: score.fraud_score,
    pattern: score.pattern,
    description: score.description,
    atm_id: score.atm_id,
    location: score.location,
    bank: score.bank,
    action: score.action_taken,
    requires_immediate_action: score.fraud_score >= 80,
    timestamp: new Date().toISOString(),
  };
}

// ── Publish functions ─────────────────────────────────────────────────────────

function publishFraudScore() {
  const score = generateFraudScore();

  // Publish fraud score
  client.publish('bankwatch/fraud/score', JSON.stringify(score), {
    qos: 1,
    properties: {
      userProperties: {
        'app-version': '1.0.0',
        'publisher-role': 'fraud-engine',
        'risk-level': score.risk_level,
        'model-version': 'fraud-detect-v2.1',
      },
    },
  });

  const icon = score.risk_level === 'CRITICAL' ? '🚨' :
               score.risk_level === 'HIGH'     ? '🔴' :
               score.risk_level === 'MEDIUM'   ? '🟡' : '🟢';
  console.log(`${icon} [FRAUD SCORE] ${score.pattern} | Score: ${score.fraud_score} | ${score.risk_level} | ${score.action_taken}`);

  // Kalau score tinggi → publish alert juga
  if (score.fraud_score >= 60) {
    publishFraudAlert(score);
  }
}

function publishFraudAlert(fraudScore) {
  const alert = generateFraudAlert(fraudScore);
  const expiry = alert.severity === 'CRITICAL' ? 30 : 60; // detik

  // Publish fraud alert with expiry
  client.publish('bankwatch/fraud/alert', JSON.stringify(alert), {
    qos: 2,
    properties: {
      messageExpiryInterval: expiry,
      userProperties: {
        'app-version': '1.0.0',
        'publisher-role': 'fraud-alert',
        'severity': alert.severity,
        'expiry-seconds': String(expiry),
      },
    },
  });

  console.log(`🚨 [FRAUD ALERT] ${alert.severity} | ${alert.pattern} @ ${alert.location} | Expires in ${expiry}s`);
}

function publishSystemScore() {
  // Publish overall fraud stats tiap 10 detik dengan RETAIN
  const stats = {
    type: 'FRAUD_STATS',
    total_analyzed: Math.floor(Math.random() * 500 + 1000),
    flagged_today: Math.floor(Math.random() * 20 + 5),
    blocked_today: Math.floor(Math.random() * 5 + 1),
    false_positive_rate: (Math.random() * 3).toFixed(2) + '%',
    model_accuracy: (Math.random() * 5 + 94).toFixed(2) + '%',
    timestamp: new Date().toISOString(),
  };

  // Publish system score with retain
  client.publish('bankwatch/fraud/score', JSON.stringify(stats), {
    qos: 1,
    retain: true,
    properties: {
      userProperties: {
        'data-type': 'aggregate-stats',
        'publisher-role': 'fraud-engine',
      },
    },
  });
}

// ── Main ──────────────────────────────────────────────────────────────────────

client.on('connect', () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   🚨 Fraud Publisher Connected            ║');
  console.log('║   Fraud detection engine running...       ║');
  console.log('╚══════════════════════════════════════════╝\n');

  client.subscribe('bankwatch/system/request', { qos: 1 });

  client.publish('bankwatch/system/status', JSON.stringify({
    publisher: 'FRAUD-PUBLISHER',
    status: 'ONLINE',
    engine: 'fraud-detect-v2.1',
    timestamp: new Date().toISOString(),
  }), { qos: 1, retain: true });

  // Analisis fraud tiap 3 detik
  setInterval(publishFraudScore, 3000);

  // Stats agregat tiap 10 detik (retain)
  setInterval(publishSystemScore, 10000);
  publishSystemScore(); // langsung publish sekali
});

// Request-Response
client.on('message', (topic, payload, packet) => {
  if (topic !== 'bankwatch/system/request') return;
  const req = JSON.parse(payload.toString());
  if (req.target !== 'FRAUD-PUBLISHER') return;

  const responseTopic = packet.properties?.responseTopic || 'bankwatch/system/response/default';
  client.publish(responseTopic, JSON.stringify({
    publisher: 'FRAUD-PUBLISHER',
    engine_version: 'fraud-detect-v2.1',
    status: 'ONLINE',
    patterns_monitored: FRAUD_PATTERNS.length,
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

client.on('error', (err) => console.error('❌ Fraud Publisher error:', err.message));
client.on('reconnect', () => console.log('🔄 Fraud Publisher reconnecting...'));

process.on('SIGINT', () => {
  console.log('\n👋 Fraud Publisher shutting down...');
  client.publish('bankwatch/system/status', JSON.stringify({
    publisher: 'FRAUD-PUBLISHER',
    status: 'OFFLINE',
    reason: 'Graceful shutdown',
    timestamp: new Date().toISOString(),
  }), { qos: 1, retain: true }, () => { client.end(); process.exit(0); });
});