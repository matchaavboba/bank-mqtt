const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const mqtt = require('mqtt');
const { v4: uuidv4 } = require('uuid');
const path = require('path');
const { BROKER } = require('../config/mqtt-config');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.static(path.join(__dirname, 'public')));
app.get('/', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

const DASHBOARD_CLIENT_ID = 'bankwatch-dashboard-' + Math.random().toString(16).slice(2, 6);
const RESPONSE_TOPIC = `bankwatch/system/response/${DASHBOARD_CLIENT_ID}`;

const mqttClient = mqtt.connect(`mqtt://${BROKER.host}:${BROKER.portTCP}`, {
  clientId: DASHBOARD_CLIENT_ID,
  protocolVersion: 5,
  properties: { receiveMaximum: 50 },
  keepalive: 30,
  reconnectPeriod: 3000,
});

const stats = {
  totalTransactions: 0,
  fraudAlerts: 0,
  criticalAlerts: 0,
  highValueTransfers: 0,
  atmStatuses: {},
  recentAlerts: [],      // last 20
  recentTransactions: [], // last 50
  publisherStatus: {},
  fraudStats: null,
  startTime: Date.now(),
};

function broadcast(type, data) {
  const msg = JSON.stringify({ type, data, ts: new Date().toISOString() });
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) ws.send(msg);
  });
}

mqttClient.on('connect', () => {
  console.log('✅ Dashboard MQTT connected to HiveMQ');
  mqttClient.subscribe('bankwatch/#', { qos: 1 });
  mqttClient.subscribe(RESPONSE_TOPIC, { qos: 1 });
  console.log('📡 Subscribed: bankwatch/# + response topic');
});

mqttClient.on('message', (topic, payload, packet) => {
  let data;
  try { data = JSON.parse(payload.toString()); } catch { return; }

  const userProps = packet.properties?.userProperties || {};
  const isRetained = packet.retain;

  stats.totalTransactions++;

  if (topic.includes('atm') && topic.includes('transaction')) {
    stats.recentTransactions.unshift({ topic, data, userProps });
    if (stats.recentTransactions.length > 50) stats.recentTransactions.pop();
    broadcast('atm_transaction', { data, topic, userProps, isRetained });

  } else if (topic.includes('atm') && topic.includes('status')) {
    stats.atmStatuses[data.atm_id] = data;
    broadcast('atm_status', { data, isRetained });

  } else if (topic.includes('transfer/domestic')) {
    if (data.is_high_value) stats.highValueTransfers++;
    broadcast('transfer_domestic', { data, userProps });

  } else if (topic.includes('transfer/international')) {
    broadcast('transfer_international', { data, userProps });

  } else if (topic === 'bankwatch/fraud/alert') {
    stats.fraudAlerts++;
    if (data.severity === 'CRITICAL') stats.criticalAlerts++;
    stats.recentAlerts.unshift(data);
    if (stats.recentAlerts.length > 20) stats.recentAlerts.pop();
    broadcast('fraud_alert', { data, userProps });

  } else if (topic === 'bankwatch/fraud/score') {
    if (data.type === 'FRAUD_STATS') {
      stats.fraudStats = data;
      broadcast('fraud_stats', { data });
    } else {
      broadcast('fraud_score', { data, userProps });
    }

  } else if (topic === 'bankwatch/system/status') {
    if (data.publisher) {
      stats.publisherStatus[data.publisher] = {
        status: data.status,
        lastSeen: new Date().toISOString(),
        isRetained,
      };
    }
    broadcast('system_status', { data, isRetained });

  } else if (topic === RESPONSE_TOPIC) {
    broadcast('system_response', { data });
  }

  if (stats.totalTransactions % 10 === 0) {
    broadcast('stats_update', {
      totalTransactions: stats.totalTransactions,
      fraudAlerts: stats.fraudAlerts,
      criticalAlerts: stats.criticalAlerts,
      highValueTransfers: stats.highValueTransfers,
      publisherStatus: stats.publisherStatus,
      uptime: Math.floor((Date.now() - stats.startTime) / 1000),
    });
  }
});

wss.on('connection', (ws) => {
  console.log(`[WS] Dashboard client connected. Total: ${wss.clients.size}`);

  ws.send(JSON.stringify({ type: 'init', data: {
    stats: {
      totalTransactions: stats.totalTransactions,
      fraudAlerts: stats.fraudAlerts,
      criticalAlerts: stats.criticalAlerts,
      highValueTransfers: stats.highValueTransfers,
      publisherStatus: stats.publisherStatus,
      uptime: Math.floor((Date.now() - stats.startTime) / 1000),
    },
    recentAlerts: stats.recentAlerts,
    recentTransactions: stats.recentTransactions.slice(0, 20),
    atmStatuses: stats.atmStatuses,
    fraudStats: stats.fraudStats,
    ts: new Date().toISOString(),
  }}));

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'request_publisher_info') {
      const targets = ['ATM-PUBLISHER', 'TRANSFER-PUBLISHER', 'FRAUD-PUBLISHER'];
      targets.forEach((target, i) => {
        setTimeout(() => {
          const corrId = uuidv4();
          mqttClient.publish('bankwatch/system/request', JSON.stringify({
            request_id: corrId,
            target,
            requested_by: DASHBOARD_CLIENT_ID,
            timestamp: new Date().toISOString(),
          }), {
            qos: 1,
            properties: {
              responseTopic: RESPONSE_TOPIC,
              correlationData: Buffer.from(corrId),
              userProperties: { 'request-type': 'dashboard-query' },
            },
          });
          console.log(`[Dashboard] Request sent to ${target}`);
        }, i * 500);
      });
    }
  });

  ws.on('close', () => console.log(`[WS] Client disconnected. Total: ${wss.clients.size}`));
});

app.get('/api/stats', (req, res) => res.json(stats));
app.get('/api/alerts', (req, res) => res.json(stats.recentAlerts));
app.get('/api/atm', (req, res) => res.json(stats.atmStatuses));

const PORT = 3000;
server.listen(PORT, () => {
  console.log('╔══════════════════════════════════════════╗');
  console.log('║   🏦 BankWatch Dashboard                  ║');
  console.log(`║   http://localhost:${PORT}                ║`);
  console.log('╚══════════════════════════════════════════╝');
});