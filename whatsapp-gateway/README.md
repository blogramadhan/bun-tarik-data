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

### Opsi 1: Menjalankan dengan Docker

1. Pastikan Docker dan Docker Compose terinstal di sistem Anda
2. Jalankan perintah:
   ```
   ./start-all-docker.sh
   ```
3. Untuk menghentikan service:
   ```
   ./stop-all-docker.sh
   ```
4. Untuk restart service:
   ```
   ./restart-services.sh
   ```

### Opsi 2: Menjalankan Secara Lokal (Tanpa Docker)

1. Pastikan Bun terinstal di sistem Anda
2. Jalankan:
   ```
   ./start-local.sh
   ```

## Pengembangan

Untuk mengembangkan lebih lanjut:

1. Semua kode WhatsApp Service ada di `whatsapp-service/src/index.ts`
2. Service berjalan di port 8788
3. Gunakan endpoint `/send-message` untuk mengirim pesan melalui API 