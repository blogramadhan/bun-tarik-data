# Bun Tarik Data

Aplikasi untuk mengupload data ke penyimpanan Cloudflare R2 dengan notifikasi WhatsApp.

## Struktur Proyek

- `src/upload.ts` - Kode utama untuk upload file ke Cloudflare R2
- `src/whatsapp.ts` - Modul untuk fungsionalitas WhatsApp
- `src/auth-whatsapp.ts` - Script untuk autentikasi WhatsApp
- `src/logout-whatsapp.ts` - Script untuk logout WhatsApp

## Instalasi

```bash
bun install
```

## Konfigurasi

Buat file `.env` dengan konfigurasi berikut:

```
# Cloudflare R2 Configuration
R2_ENDPOINT=https://your-account.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your_access_key_id
R2_SECRET_ACCESS_KEY=your_secret_access_key
R2_BUCKET_NAME=your_bucket_name

# WhatsApp Configuration
# Format: nomor tanpa tanda + (contoh: 628123456789,628123456788)
WHATSAPP_RECIPIENT_NUMBERS=628123456789,628987654321
```

## Arsitektur Aplikasi

Aplikasi ini terdiri dari dua komponen utama:

1. **Layanan WhatsApp di Docker**:
   - Berjalan di container Docker terpisah
   - Menangani autentikasi WhatsApp dan mempertahankan sesi
   - Menyediakan API untuk pengiriman pesan WhatsApp

2. **Upload Script (upload.ts)**:
   - Berjalan di host/sistem utama
   - Menangani proses upload file ke Cloudflare R2
   - Menggunakan layanan WhatsApp di Docker untuk mengirim notifikasi status

## Menjalankan WhatsApp di Docker

### Prasyarat
- Docker dan Docker Compose terinstal di sistem Anda

### Langkah-langkah
1. Build dan jalankan container WhatsApp:
   ```bash
   docker-compose build
   docker-compose up -d
   ```

2. Scan QR code yang tersimpan di folder `qrcode` untuk autentikasi WhatsApp

3. Setelah berhasil autentikasi, data sesi akan tersimpan di folder `.wwebjs_auth`

### Opsi Command
Anda dapat mengubah command yang dijalankan di dalam container dengan mengedit `docker-compose.yml`:

- Untuk autentikasi (default):
  ```yaml
  command: ["bun", "src/auth-whatsapp.ts"]
  ```

- Untuk logout:
  ```yaml
  command: ["bun", "src/logout-whatsapp.ts"]
  ```

### Troubleshooting Docker
- Jika mengalami masalah dengan library yang hilang, pastikan semua dependensi sudah terinstal di Dockerfile
- Untuk debugging, tambahkan:
  ```yaml
  environment:
    - DEBUG=true
  ```

## Menggunakan Upload Script dengan Notifikasi WhatsApp

Setelah layanan WhatsApp di Docker berjalan dan telah diautentikasi:

1. Jalankan script upload di host:
   ```bash
   bun run upload
   ```

2. Script upload akan menghubungi layanan WhatsApp di Docker untuk mengirim notifikasi status upload ke nomor yang telah dikonfigurasi

## Fitur

- Upload file ke Cloudflare R2
- Mengirim notifikasi status upload melalui WhatsApp
- Aturan upload berdasarkan tahun pada path file
- Mendukung pengiriman ke multiple nomor WhatsApp
- Arsitektur terdistribusi dengan WhatsApp di Docker
- Penyimpanan sesi WhatsApp untuk autentikasi sekali
- QR Code tersimpan sebagai file gambar untuk pemindaian yang mudah 