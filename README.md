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

```bash
bun run auth-whatsapp
```

Ini akan menampilkan QR Code yang perlu Anda scan dengan aplikasi WhatsApp di handphone. Setelah berhasil, sesi akan disimpan dan Anda tidak perlu melakukan scan ulang setiap kali menjalankan aplikasi.

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