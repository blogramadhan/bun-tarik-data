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

## Penggunaan WhatsApp

WhatsApp dapat dijalankan dengan dua cara: langsung di host atau melalui Docker.

### Cara 1: Menjalankan WhatsApp di Docker (Direkomendasikan)

#### Prasyarat
- Docker dan Docker Compose terinstal di sistem Anda

#### Langkah-langkah
1. Build dan jalankan container:
   ```bash
   docker-compose build
   docker-compose up
   ```

2. Scan QR code yang muncul di terminal atau lihat file QR code yang tersimpan di folder `qrcode`

3. Setelah berhasil autentikasi, data sesi akan tersimpan di folder `.wwebjs_auth`

#### Opsi Command
Anda dapat mengubah command yang dijalankan di dalam container dengan mengedit `docker-compose.yml`:

- Untuk autentikasi (default):
  ```yaml
  command: ["bun", "src/auth-whatsapp.ts"]
  ```

- Untuk logout:
  ```yaml
  command: ["bun", "src/logout-whatsapp.ts"]
  ```

#### Troubleshooting Docker
- Jika mengalami masalah dengan library yang hilang, pastikan semua dependensi sudah terinstal di Dockerfile
- Untuk debugging, tambahkan:
  ```yaml
  environment:
    - DEBUG=true
  ```

### Cara 2: Menjalankan WhatsApp Langsung di Host

#### Autentikasi WhatsApp (Hanya Sekali)

```bash
bun run auth-whatsapp
```

Ini akan menampilkan QR Code di terminal yang perlu Anda scan dengan aplikasi WhatsApp di handphone.

#### Logout WhatsApp (Opsional)

Jika Anda ingin menghapus sesi WhatsApp dan melakukan autentikasi ulang:

```bash
bun run logout-whatsapp
```

### Upload Data ke R2

Setelah autentikasi WhatsApp berhasil (baik via Docker atau langsung), jalankan proses upload:

```bash
bun run upload
```

Notifikasi status upload akan dikirimkan ke nomor WhatsApp yang telah dikonfigurasi.

## Fitur

- Upload file ke Cloudflare R2
- Mengirim notifikasi status upload melalui WhatsApp
- Aturan upload berdasarkan tahun pada path file
- Mendukung pengiriman ke multiple nomor WhatsApp
- Dukungan Docker untuk WhatsApp
- Modul WhatsApp terpisah untuk kemudahan maintenance
- Penyimpanan sesi WhatsApp untuk autentikasi sekali
- QR Code tersimpan sebagai file gambar untuk penggunaan di server 