# Setup Environment Variables untuk Cloudflare R2

## Langkah-langkah Setup

### 1. Buat file `.env` di root project

Buat file `.env` dengan konfigurasi berikut:

```env
# Cloudflare R2 Configuration
R2_ENDPOINT=https://your-account-id.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your-access-key-id
R2_SECRET_ACCESS_KEY=your-secret-access-key
R2_BUCKET_NAME=your-bucket-name
```

### 2. Dapatkan Credentials dari Cloudflare R2

1. **Login ke Cloudflare Dashboard**
   - Buka [https://dash.cloudflare.com](https://dash.cloudflare.com)
   - Login dengan akun Cloudflare Anda

2. **Akses R2 Object Storage**
   - Di sidebar kiri, klik "R2 Object Storage"
   - Jika belum ada bucket, buat bucket baru

3. **Buat API Token**
   - Klik "Manage R2 API tokens"
   - Klik "Create API token"
   - Pilih "Custom token"
   - Beri nama token (misal: "tarik-data-upload")
   - Set permissions:
     - **Account**: R2 Object Storage:Edit
     - **Zone**: R2 Object Storage:Edit
   - Klik "Create API Token"

4. **Copy Credentials**
   - Copy **Access Key ID**
   - Copy **Secret Access Key**
   - Copy **Account ID** (untuk endpoint)

### 3. Update file `.env`

Ganti nilai-nilai placeholder dengan credentials yang sebenarnya:

```env
R2_ENDPOINT=https://[ACCOUNT_ID].r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=[ACCESS_KEY_ID]
R2_SECRET_ACCESS_KEY=[SECRET_ACCESS_KEY]
R2_BUCKET_NAME=[BUCKET_NAME]
```

### 4. Validasi Konfigurasi

Jalankan script untuk memvalidasi konfigurasi:

```bash
bun run src/env.config.ts
```

## Struktur File

```
tarik-data/
├── .env                    # Environment variables (tidak di-commit)
├── env.config.ts          # Konfigurasi dan validasi env
├── ENV_SETUP.md           # Dokumentasi ini
└── src/
    └── sendR2.ts          # Script upload ke R2
```

## Keamanan

- File `.env` sudah ada di `.gitignore` sehingga tidak akan di-commit ke repository
- Jangan pernah share credentials API di public repository
- Gunakan environment variables yang berbeda untuk development dan production

## Troubleshooting

### Error: "Cannot find module '@aws-sdk/client-s3'"

Install dependency yang diperlukan:

```bash
bun add @aws-sdk/client-s3
```

### Error: "Environment variables yang diperlukan tidak ditemukan"

Pastikan file `.env` sudah dibuat dengan format yang benar dan tidak ada spasi di sekitar tanda `=`. 