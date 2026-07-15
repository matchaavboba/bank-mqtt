// config/mqtt-config.js
// Konfigurasi broker dan topic structure

const BROKER = {
  host: 'broker.hivemq.com',
  port: 8884,          // WebSocket Secure (WSS) — untuk browser
  portTCP: 1883,       // TCP — untuk Node.js publishers/subscribers
  protocol: 'mqtt',
};

// Topic Tree:
// bankwatch/
//   atm/
//     {atm_id}/transaction    → ATM Publisher
//     {atm_id}/status         → ATM Publisher (retain)
//   transfer/
//     domestic                → Transfer Publisher
//     international           → Transfer Publisher
//   fraud/
//     alert                   → Fraud Publisher (expiry 60s)
//     score                   → Fraud Publisher
//   system/
//     status                  → semua publisher (LWT)
//     request                 → Request-Response
//     response/{client_id}    → Request-Response

const TOPICS = {
  ATM_TRANSACTION:    'bankwatch/atm/+/transaction',
  ATM_STATUS:         'bankwatch/atm/+/status',
  TRANSFER_DOMESTIC:  'bankwatch/transfer/domestic',
  TRANSFER_INTL:      'bankwatch/transfer/international',
  FRAUD_ALERT:        'bankwatch/fraud/alert',
  FRAUD_SCORE:        'bankwatch/fraud/score',
  SYSTEM_STATUS:      'bankwatch/system/status',
  SYSTEM_REQUEST:     'bankwatch/system/request',
  SYSTEM_RESPONSE:    'bankwatch/system/response',
  ALL:                'bankwatch/#',              // Wildcard multi-level
  ALL_ATM:            'bankwatch/atm/#',          // Wildcard semua ATM
  ALL_TRANSFER:       'bankwatch/transfer/+',     // Wildcard single-level
};

// ATM locations
const ATM_NODES = [
  { id: 'ATM-001', location: 'Surabaya - Tunjungan Plaza', zone: 'retail' },
  { id: 'ATM-002', location: 'Jakarta - Sudirman', zone: 'business' },
  { id: 'ATM-003', location: 'Bali - Kuta Beach', zone: 'tourism' },
  { id: 'ATM-004', location: 'Bandung - Dago', zone: 'campus' },
  { id: 'ATM-005', location: 'Malang - Alun-alun', zone: 'retail' },
];

const BANKS = ['BRI', 'BCA', 'Mandiri', 'BNI', 'CIMB', 'Danamon'];

const CURRENCIES = ['IDR', 'USD', 'EUR', 'SGD', 'AUD'];

module.exports = { BROKER, TOPICS, ATM_NODES, BANKS, CURRENCIES };