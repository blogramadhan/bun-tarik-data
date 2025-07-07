# WhatsApp AI Gateway

Aplikasi ini terdiri dari dua layanan terpisah:
1. **WhatsApp Service**: Menangani koneksi WhatsApp dan menjawab pertanyaan dengan AI
2. **Upload Service**: Mengelola upload materi pembelajaran

## Struktur Folder

```
whatsapp-ai-gateway/
├── start-all-docker.sh (untuk menjalankan kedua layanan Docker secara bersamaan)
├── stop-all-docker.sh (untuk menghentikan kedua layanan Docker)
├── start-local.sh (untuk menjalankan kedua layanan secara lokal)
├── whatsapp-service/
│   ├── .env (konfigurasi API key untuk DeepInfra)
│   ├── docker-compose.yml (untuk menjalankan WhatsApp Service saja)
│   ├── start-docker.sh (untuk menjalankan WhatsApp Service saja)
│   ├── stop-docker.sh (untuk menghentikan WhatsApp Service)
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.ts
└── upload-service/
    ├── .env (konfigurasi URL untuk WhatsApp Service)
    ├── docker-compose.yml (untuk menjalankan Upload Service saja)
    ├── start-docker.sh (untuk menjalankan Upload Service saja)
    ├── stop-docker.sh (untuk menghentikan Upload Service)
    ├── Dockerfile
    ├── package.json
    └── src/
        ├── index.ts
        └── utils/
            └── extractor.ts
```

## Konfigurasi File .env

### WhatsApp Service (.env)
```
# API Key untuk DeepInfra
DEEPINFRA_API_KEY=your_api_key_here
```
Ganti `your_api_key_here` dengan API key DeepInfra yang valid untuk menggunakan fitur AI.

### Upload Service (.env)
```
# URL untuk WhatsApp Service
WHATSAPP_SERVICE_URL=http://host.docker.internal:8788
```
Nilai default menggunakan `host.docker.internal` untuk komunikasi antar kontainer Docker pada host yang sama. Jika WhatsApp Service berjalan di server terpisah, ubah URL sesuai kebutuhan.

## Cara Menjalankan

### Opsi 1: Menjalankan Kedua Layanan Bersama dengan Docker

Menggunakan script di folder utama:

1. Pastikan file `.env` ada di folder whatsapp-service dengan API key yang diperlukan, atau script akan membuat file contoh:
   ```bash
   ./start-all-docker.sh
   ```

2. Untuk menghentikan layanan:
   ```bash
   ./stop-all-docker.sh
   ```

### Opsi 2: Menjalankan Layanan Secara Terpisah dengan Docker

#### WhatsApp Service

1. Masuk ke folder whatsapp-service:
   ```bash
   cd whatsapp-service
   ```

2. Pastikan file `.env` berisi API key DeepInfra yang valid:
   ```bash
   echo "DEEPINFRA_API_KEY=your_api_key_here" > .env
   ```

3. Jalankan dengan Docker Compose:
   ```bash
   ./start-docker.sh
   ```

4. Untuk menghentikan layanan:
   ```bash
   ./stop-docker.sh
   ```

#### Upload Service

1. Masuk ke folder upload-service:
   ```bash
   cd upload-service
   ```

2. Pastikan file `.env` berisi URL yang benar untuk WhatsApp Service:
   ```bash
   echo "WHATSAPP_SERVICE_URL=http://host.docker.internal:8788" > .env
   ```

3. Jalankan dengan Docker Compose:
   ```bash
   ./start-docker.sh
   ```

4. Untuk menghentikan layanan:
   ```bash
   ./stop-docker.sh
   ```

### Opsi 3: Menjalankan Secara Lokal (Tanpa Docker)

```bash
./start-local.sh
```

## Cara Scan QR Code WhatsApp

Saat pertama kali menjalankan WhatsApp Service, Anda perlu melakukan autentikasi dengan WhatsApp Web. Berikut langkah-langkahnya:

### Saat Menjalankan dengan Docker

1. Setelah menjalankan WhatsApp Service, lihat log untuk mendapatkan QR code:
   ```bash
   # Jika menjalankan kedua layanan bersama
   cd whatsapp-ai-gateway && docker-compose logs -f whatsapp-service
   
   # Jika menjalankan WhatsApp Service secara terpisah
   cd whatsapp-service && docker-compose logs -f
   ```

2. QR code akan ditampilkan dalam bentuk ASCII di terminal. Scan QR code ini menggunakan aplikasi WhatsApp di smartphone Anda:
   - Buka WhatsApp di smartphone
   - Ketuk Menu (tiga titik) > WhatsApp Web
   - Arahkan kamera ke QR code yang ditampilkan di terminal

3. Setelah berhasil scan, Anda akan melihat pesan "✅ WhatsApp client siap!" di log.

### Saat Menjalankan Secara Lokal

1. Saat menjalankan `./start-local.sh`, QR code akan ditampilkan langsung di terminal.

2. Scan QR code menggunakan aplikasi WhatsApp di smartphone Anda seperti langkah di atas.

### Catatan Penting

- Autentikasi hanya perlu dilakukan sekali karena data sesi disimpan di volume Docker `.wwebjs_auth`.
- Jika Anda menghapus volume Docker atau folder `.wwebjs_auth`, Anda perlu melakukan autentikasi ulang.
- QR code hanya valid untuk beberapa menit. Jika kedaluwarsa, restart layanan untuk mendapatkan QR code baru.
- Pastikan smartphone Anda terhubung ke internet saat melakukan scan QR code.

## Alamat Layanan

- WhatsApp Service: http://localhost:8788
- Upload Service: http://localhost:8789

## Fitur

### WhatsApp Service
- Menerima dan memproses pesan WhatsApp
- Menjawab pertanyaan dengan AI berdasarkan materi yang dipilih
- Endpoint API untuk mengirim pesan WhatsApp

### Upload Service
- Upload file materi pembelajaran (PDF, DOC, DOCX, TXT)
- Ekstraksi teks otomatis dari file
- Notifikasi ke WhatsApp Service untuk memuat ulang knowledge base

## Endpoint API

### WhatsApp Service (port 8788)
- `POST /send-message`: Mengirim pesan WhatsApp
  ```json
  {
    "number": "6281234567890",
    "message": "Hello World"
  }
  ```
- `POST /reload-kb`: Memuat ulang knowledge base

### Upload Service (port 8789)
- `GET /`: Halaman web untuk upload materi
- `POST /upload`: Endpoint untuk upload file 