// config/broker-test.js
const mqtt = require('mqtt');

console.log('🔍 Testing connection to HiveMQ public broker...');

const client = mqtt.connect('mqtt://broker.hivemq.com:1883', {
  clientId: 'bankwatch-test-' + Math.random().toString(16).slice(2, 8),
  connectTimeout: 10000,
});

client.on('connect', () => {
  console.log('✅ Connected to HiveMQ broker successfully!');
  console.log('📡 Broker: broker.hivemq.com:1883');
  console.log('🚀 You are ready to run the system.\n');

  // Test pub/sub
  client.subscribe('bankwatch/test', (err) => {
    if (!err) {
      client.publish('bankwatch/test', JSON.stringify({ msg: 'Hello BankWatch!' }));
    }
  });
});

client.on('message', (topic, message) => {
  console.log('📨 Test message received:', JSON.parse(message.toString()));
  console.log('\n✅ Pub/Sub working correctly!');
  client.end();
  process.exit(0);
});

client.on('error', (err) => {
  console.error('❌ Connection failed:', err.message);
  console.log('💡 Check your internet connection and try again.');
  process.exit(1);
});

setTimeout(() => {
  console.error('❌ Connection timeout after 10 seconds.');
  process.exit(1);
}, 11000);