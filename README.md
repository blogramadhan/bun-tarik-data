# Bun Tarik Data

Aplikasi untuk mengupload data ke penyimpanan Cloudflare R2 dengan notifikasi WhatsApp.

## Struktur Proyek

- `src/upload.ts` - Kode utama untuk upload file ke Cloudflare R2
- `src/whatsapp.ts` - Modul terpisah untuk fungsionalitas WhatsApp
- `src/auth-whatsapp.ts` - Script untuk autentikasi WhatsApp terpisah
- `src/logout-whatsapp.ts` - Script untuk logout dan menghapus sesi WhatsApp

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

## Penggunaan

### Autentikasi WhatsApp (Hanya Sekali)

Sebelum menggunakan fitur notifikasi WhatsApp, Anda perlu melakukan autentikasi terlebih dahulu:

#### Di Komputer Lokal:

```bash
bun run auth-whatsapp
```

Ini akan menampilkan QR Code di terminal yang perlu Anda scan dengan aplikasi WhatsApp di handphone. 

#### Di Server melalui SSH:

Jika Anda menjalankan di server dan perlu menutup terminal SSH, gunakan PM2:

```bash
# Install PM2 jika belum ada
bun install -g pm2

# Jalankan autentikasi WhatsApp dengan PM2
bun run auth-whatsapp:pm2

# Lihat log untuk mendapatkan QR code atau cek file gambar QR
pm2 logs whatsapp-auth

# QR code juga disimpan sebagai file gambar di:
# ./qrcode/whatsapp-qrcode.png
```

Setelah berhasil melakukan scan dan autentikasi, Anda bisa menghentikan proses:

```bash
bun run auth-whatsapp:pm2:stop
```

Setelah berhasil, sesi akan disimpan dan Anda tidak perlu melakukan scan ulang setiap kali menjalankan aplikasi.

### Upload Data ke R2

Setelah autentikasi WhatsApp berhasil, jalankan proses upload:

```bash
bun run upload
```

Notifikasi status upload akan dikirimkan ke nomor WhatsApp yang telah dikonfigurasi.

### Logout WhatsApp (Opsional)

Jika Anda ingin menghapus sesi WhatsApp dan melakukan autentikasi ulang:

```bash
bun run logout-whatsapp
```

## Fitur

- Upload file ke Cloudflare R2
- Mengirim notifikasi status upload melalui WhatsApp
- Aturan upload berdasarkan tahun pada path file
- Mendukung pengiriman ke multiple nomor WhatsApp
- Modul WhatsApp terpisah untuk kemudahan maintenance
- Penyimpanan sesi WhatsApp untuk autentikasi sekali
- QR Code tersimpan sebagai file gambar untuk penggunaan di server 

# Bun Tarik Data dengan WhatsApp

## Menjalankan WhatsApp di Docker

### Prasyarat
- Docker dan Docker Compose terinstal di sistem Anda

### Langkah-langkah
1. Build dan jalankan container:
   ```bash
   docker-compose build
   docker-compose up
   ```

2. Scan QR code yang muncul di terminal atau lihat file QR code yang tersimpan di folder `qrcode`

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

### Troubleshooting
- Jika mengalami masalah dengan library yang hilang, pastikan semua dependensi sudah terinstal di Dockerfile
- Untuk debugging, tambahkan:
  ```yaml
  environment:
    - DEBUG=true
  ```

## Struktur Aplikasi
- `src/whatsapp.ts` - Modul untuk fungsionalitas WhatsApp
- `src/auth-whatsapp.ts` - Script untuk autentikasi WhatsApp
- `src/logout-whatsapp.ts` - Script untuk logout WhatsApp
- `src/upload.ts` - Script untuk mengupload data dengan notifikasi WhatsApp

## Environment Variables
Buat file `.env` dengan konfigurasi berikut:

```
# WhatsApp Configuration
WHATSAPP_RECIPIENT_NUMBERS=628123456789,628987654321
```

### Autentikasi WhatsApp
Autentikasi WhatsApp dilakukan melalui Docker dengan perintah:
```bash
docker-compose up
```

Ini akan menampilkan QR Code yang perlu Anda scan dengan aplikasi WhatsApp di handphone. 