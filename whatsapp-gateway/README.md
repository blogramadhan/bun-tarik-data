# WhatsApp Gateway

Aplikasi ini adalah gateway sederhana untuk mengirim dan menerima pesan WhatsApp melalui API.

## Fitur Utama

- **Koneksi WhatsApp Web**: Menggunakan whatsapp-web.js untuk koneksi ke WhatsApp
- **API Endpoint**: Menyediakan endpoint REST API untuk mengirim pesan
- **Logging**: Mencatat semua pesan yang diterima untuk monitoring

## Struktur Folder

```
whatsapp-gateway/
├── start-all-docker.sh (untuk menjalankan WhatsApp service dengan Docker)
├── stop-all-docker.sh (untuk menghentikan service Docker)
├── start-local.sh (untuk menjalankan service secara lokal)
├── restart-services.sh (untuk restart service)
└── whatsapp-service/
    ├── docker-compose.yml
    ├── start-docker.sh
    ├── stop-docker.sh
    ├── Dockerfile
    ├── package.json
    └── src/
        └── index.ts
```

## API Endpoints

- **GET /**: Status check endpoint
- **POST /send-message**: Mengirim pesan WhatsApp
  ```json
  {
    "number": "628123456789",
    "message": "Hello World!"
  }
  ```

## Cara Menggunakan

Gateway ini menyediakan endpoint untuk mengirim pesan WhatsApp. Bot akan merespon dengan pesan bantuan sederhana jika menerima perintah `/bantuan` atau `/help`.

## Cara Menjalankan

### Opsi 1: Menjalankan dengan Docker (Recommended untuk Production)

1. Pastikan Docker dan Docker Compose terinstal di sistem Anda
2. Install dependencies terlebih dahulu:
   ```bash
   cd whatsapp-service
   bun install
   cd ..
   ```
3. Jalankan service:
   ```bash
   ./start-all-docker.sh
   ```
4. Untuk menghentikan service:
   ```bash
   ./stop-all-docker.sh
   ```
5. Untuk restart service:
   ```bash
   ./restart-services.sh
   ```

### Opsi 2: Menjalankan Secara Lokal (Tanpa Docker)

1. Pastikan Bun terinstal di sistem Anda
2. Install dependencies:
   ```bash
   cd whatsapp-service
   bun install
   ```
3. Jalankan:
   ```bash
   cd ..
   ./start-local.sh
   ```

## Konfigurasi Production

Service ini sudah dikonfigurasi untuk production dengan:

- **Health Check**: Monitoring otomatis setiap 30 detik
- **Auto Restart**: Service akan restart otomatis jika crash
- **Memory Management**: Shared memory 2GB untuk Puppeteer/Chromium
- **Temporary Files**: tmpfs 512MB untuk cache Chromium
- **Security**: Konfigurasi sandbox Chromium yang optimal
- **Cleanup Script**: Automatic cleanup temporary files saat start

### Troubleshooting Production

Jika service tidak berjalan di production:

1. **Check logs**:
   ```bash
   docker logs whatsapp-service
   ```

2. **Restart container**:
   ```bash
   docker restart whatsapp-service
   ```

3. **Rebuild image** (jika ada perubahan kode):
   ```bash
   docker-compose down
   docker-compose build --no-cache
   docker-compose up -d
   ```

4. **Check health status**:
   ```bash
   docker inspect whatsapp-service | grep -A 10 Health
   ```

## Pengembangan

Untuk mengembangkan lebih lanjut:

1. Semua kode WhatsApp Service ada di [whatsapp-service/src/index.ts](whatsapp-service/src/index.ts)
2. Service berjalan di port 8788
3. Gunakan endpoint `/send-message` untuk mengirim pesan melalui API
4. TypeScript configuration ada di [whatsapp-service/tsconfig.json](whatsapp-service/tsconfig.json) 