BankWatch — MQTT Banking Monitor

## Apa ini?

BankWatch adalah sistem monitoring transaksi perbankan real-time yang dibangun di atas protokol MQTT. Sistem ini mensimulasikan aktivitas ATM, transfer dana, dan deteksi fraud secara bersamaan. Semua data mengalir melalui broker MQTT HiveMQ dan ditampilkan di dashboard web secara live.

Proyek ini dibuat untuk mata kuliah Integrasi Sistem sebagai implementasi protokol MQTT dengan seluruh fiturnya: Pub/Sub, QoS, Wildcard, Topic Alias, User Properties, Retain, Message Expiry, Last Will Testament, Request-Response, Shared Subscription, dan Flow Control.

## Struktur Folder

```
bank-mqtt/
├── config/
│   ├── mqtt-config.js        # Konfigurasi broker, topic, dan data master
│   └── broker-test.js        # Script tes koneksi ke HiveMQ
├── publishers/
│   ├── atm-publisher.js      # Simulasi 5 mesin ATM
│   ├── transfer-publisher.js # Simulasi transfer domestik & internasional
│   └── fraud-publisher.js    # Engine deteksi fraud
├── subscribers/
│   ├── logger-subscriber.js  # Logger transaksi (2 worker)
│   └── alert-subscriber.js   # Monitor alert & request-response
├── dashboard/
│   ├── server.js             # Web server + MQTT bridge
│   └── public/index.html     # Tampilan dashboard
└── package.json
```

## Cara Menjalankan

Install dependencies dulu:
```bash
npm install
```

Test koneksi broker sebelum lanjut:
```bash
npm run broker:test
```
Pastikan muncul pesan "Connected successfully" sebelum jalankan yang lain.

Setelah itu buka 7 terminal dan jalankan masing-masing:
```bash
# Terminal 1
npm run pub:atm

# Terminal 2
npm run pub:transfer

# Terminal 3
npm run pub:fraud

# Terminal 4
npm run sub:logger

# Terminal 5 (Windows)
set WORKER_ID=2 && node subscribers/logger-subscriber.js

# Terminal 5 (Mac/Linux)
WORKER_ID=2 npm run sub:logger

# Terminal 6
npm run sub:alert

# Terminal 7
npm run dashboard
```

Buka browser di http://localhost:3000

## Kenapa 7 Terminal?

Setiap komponen berjalan sebagai proses Node.js yang terpisah dan berjalan bersamaan, seperti sistem nyata di mana ATM, server transfer, dan fraud engine berjalan di mesin yang berbeda. Masing-masing punya tugas sendiri dan berkomunikasi lewat broker MQTT sebagai perantara.

## Penjelasan Tiap Komponen

**ATM Publisher** mensimulasikan 5 mesin ATM di Surabaya, Jakarta, Bali, Bandung, dan Malang. Tiap 2 detik dia mengirim data transaksi ke broker. Sesekali muncul transaksi mencurigakan yang akan ditangkap oleh fraud engine.

**Transfer Publisher** mensimulasikan dua jenis transfer, domestik antar kota Indonesia setiap 1.5 detik dan internasional ke luar negeri setiap 4 detik. Transfer di atas 100 juta rupiah otomatis ditandai sebagai high-value dan diperlakukan dengan jaminan pengiriman lebih ketat.

**Fraud Publisher** adalah engine deteksi fraud yang bekerja tiap 3 detik. Dia menganalisis pola transaksi, menghasilkan fraud score dari 0 sampai 100, dan mengirim alert kalau scorenya tinggi. Alert ini punya waktu kedaluwarsa, kalau tidak diproses dalam 60 detik broker otomatis hapus supaya tidak menumpuk.

**Logger Subscriber** merekam semua aktivitas sistem ke file log. Dijalankan dua instance (Worker 1 dan Worker 2) supaya kalau traffic tinggi, beban dibagi otomatis oleh broker, satu pesan ke Worker 1 dan pesan berikutnya ke Worker 2.

**Alert Subscriber** khusus memantau fraud alert dan transfer high-value. Dia juga yang menginisiasi komunikasi Request-Response, saat pertama jalan dia mengirim ping ke semua publisher untuk minta laporan status dan publisher membalas ke topik response khususnya.

**Dashboard Server** menjadi jembatan antara MQTT dan browser. Dia mendengarkan semua yang terjadi di sistem lewat wildcard topic, lalu meneruskan ke browser via WebSocket. Dia juga menyimpan data terkini di memory supaya saat browser baru dibuka langsung dapat semua data tanpa harus nunggu pesan baru datang.

## Alur Sistem

Contoh alur satu kejadian: ATM di Bali mencatat tarik tunai Rp 8.000.000, ATM Publisher kirim ke broker, broker teruskan ke semua subscriber, Fraud Publisher hitung scorenya tinggi lalu kirim fraud alert, Alert Subscriber cetak WARNING di terminal, Dashboard tampilkan notifikasi merah di browser, alert otomatis hilang dalam 60 detik kalau tidak diproses.

```
Publishers              Broker (HiveMQ)         Subscribers
ATM Publisher    ──────────────────────────► Logger Worker 1
Transfer Pub.    ──────────────────────────► Logger Worker 2
Fraud Pub.       ──────────────────────────► Alert Subscriber
                                           ► Dashboard Server
                                                    │
                                              WebSocket
                                                    │
                                              Browser UI
```

## 10 Fitur MQTT yang Diimplementasikan

**Pub/Sub** adalah dasar dari MQTT. Publisher kirim pesan tanpa tahu siapa yang terima, subscriber dengerin topik tertentu tanpa tahu siapa yang kirim. HiveMQ jadi perantaranya.

**QoS (Quality of Service)** adalah jaminan pengiriman pesan dalam tiga level. QoS 0 untuk data biasa yang tidak kritikal. QoS 1 untuk transaksi ATM, pastikan sampai minimal sekali. QoS 2 untuk fraud alert dan high-value transfer, pastikan sampai tepat sekali tidak boleh dobel.

**Wildcard** memungkinkan dashboard subscribe ke semua topik sekaligus pakai `bankwatch/#` tanpa perlu daftar satu per satu. Alert Subscriber pakai `bankwatch/transfer/+` untuk dengerin semua jenis transfer.

**Topic Alias** mengganti nama topik yang panjang dengan angka setelah pertama kali dikirim. Menghemat bandwidth karena nama seperti `bankwatch/atm/ATM-001/transaction` tidak perlu dikirim ulang di setiap pesan.

**User Properties** menyisipkan metadata tambahan di luar data utama setiap pesan, seperti versi aplikasi, role publisher, zona ATM, dan apakah transaksi mencurigakan, tanpa mengubah format data utamanya.

**Retain Message** menyimpan status terbaru tiap ATM dan publisher di broker. Saat dashboard baru dibuka langsung dapat semua status terkini tanpa harus nunggu pesan baru datang.

**Message Expiry** memberi waktu kedaluwarsa pada fraud alert antara 30 sampai 60 detik. Kalau tidak diproses dalam waktu itu broker otomatis hapus supaya sistem tidak dibanjiri alert yang sudah tidak relevan.

**Last Will Testament** membuat setiap publisher menitipkan pesan darurat ke broker saat pertama konek. Kalau publisher tiba-tiba crash tanpa sempat bilang offline, broker otomatis broadcast pesan OFFLINE atas namanya ke semua subscriber.

**Shared Subscription** mendaftarkan Logger Worker 1 dan Worker 2 dalam grup yang sama. Broker otomatis membagi pesan di antara keduanya supaya tidak ada satu worker pun yang kewalahan sendirian.

**Flow Control** membatasi berapa banyak pesan yang boleh diterima subscriber sekaligus sebelum dikonfirmasi. Alert Subscriber batasi 5 karena perlu proses tiap alert dengan serius. Dashboard batasi 50 karena cukup tampilkan data.# bank-mqtt
