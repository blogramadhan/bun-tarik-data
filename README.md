# Tarik Data - Cloudflare R2 Upload Tool

Aplikasi untuk mengupload file **Parquet** ke Cloudflare R2 Object Storage menggunakan Bun.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
bun install
```

### 2. Setup Environment Variables

Buat file `.env` di root project dengan konfigurasi Cloudflare R2:

```env
# Cloudflare R2 Configuration
R2_ENDPOINT=https://your-account-id.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID=your-access-key-id
R2_SECRET_ACCESS_KEY=your-secret-access-key
R2_BUCKET_NAME=your-bucket-name
```

Untuk panduan lengkap setup credentials, lihat [ENV_SETUP.md](./ENV_SETUP.md).

### 3. Validasi Konfigurasi

```bash
bun run validate-env
```

### 4. Upload File Parquet ke R2

```bash
# Upload file .parquet ke R2 (versi standar)
bun run upload-r2

# Upload file .parquet ke R2 (versi robust untuk file besar)
bun run upload-r2-robust

# Upload file .parquet ke R2 (versi simple dengan timeout besar)
bun run upload-r2-simple

# Atau jalankan dengan watch mode untuk development
bun run dev
```

## 📁 Struktur Project

```
tarik-data/
├── .env                    # Environment variables (tidak di-commit)
├── env.config.ts          # Konfigurasi dan validasi env
├── ENV_SETUP.md           # Panduan setup environment
├── README.md              # Dokumentasi ini
├── package.json           # Dependencies dan scripts
└── src/
    ├── sendR2.ts          # Script upload file .parquet ke R2 (standar)
    ├── sendR2-robust.ts   # Script upload file .parquet ke R2 (robust untuk file besar)
    ├── sendR2-simple.ts   # Script upload file .parquet ke R2 (simple dengan timeout besar)
    ├── validate-env.ts    # Script validasi environment
    ├── config/            # Konfigurasi lainnya
    ├── katalog.ts         # Script katalog
    ├── katalogv6.ts       # Script katalog v6
    ├── rup.ts             # Script RUP
    └── spse.ts            # Script SPSE
```

## 🛠️ Scripts Available

- `bun run validate-env` - Validasi environment variables
- `bun run upload-r2` - Upload file .parquet ke Cloudflare R2 (versi standar)
- `bun run upload-r2-robust` - Upload file .parquet ke Cloudflare R2 (versi robust untuk file besar)
- `bun run upload-r2-simple` - Upload file .parquet ke Cloudflare R2 (versi simple dengan timeout besar)
- `bun run dev` - Jalankan dengan watch mode

## 🔧 Perbedaan Versi Upload

### Versi Standar (`upload-r2`)
- **Target**: File .parquet kecil hingga menengah (< 100MB)
- **Timeout**: 5 menit
- **Retry**: 3 kali
- **Fitur**: Upload langsung, cepat untuk file kecil

### Versi Robust (`upload-r2-robust`)
- **Target**: File .parquet besar (> 100MB)
- **Timeout**: 5 menit
- **Retry**: 3 kali
- **Fitur**: Multipart upload, chunking, progress tracking detail

### Versi Simple (`upload-r2-simple`)
- **Target**: Semua ukuran file .parquet
- **Timeout**: 10 menit
- **Retry**: 5 kali
- **Fitur**: Timeout sangat besar, retry agresif

## 📊 Fitur Upload Parquet

- ✅ **Hanya upload file .parquet** - File lain akan di-skip
- ✅ **Progress tracking** - Melihat progress upload
- ✅ **Statistik upload** - Jumlah file berhasil/gagal
- ✅ **Recursive upload** - Upload semua subfolder
- ✅ **Error handling** - Retry otomatis jika gagal
- ✅ **File size detection** - Otomatis pilih metode upload

## 🔧 Dependencies

- **Bun** - Runtime JavaScript/TypeScript
- **@aws-sdk/client-s3** - AWS SDK untuk Cloudflare R2
- **duckdb** - Database untuk processing data

## 📖 Dokumentasi Lengkap

- [Setup Environment Variables](./ENV_SETUP.md) - Panduan lengkap setup Cloudflare R2
- [Upload Guide](./UPLOAD_GUIDE.md) - Panduan detail semua versi upload
- [Cloudflare R2 Documentation](https://developers.cloudflare.com/r2/) - Dokumentasi resmi Cloudflare R2

## 🔒 Keamanan

- File `.env` sudah ada di `.gitignore`
- Jangan pernah commit credentials ke repository
- Gunakan environment variables yang berbeda untuk development dan production

## 🐛 Troubleshooting

### Error: "Cannot find module '@aws-sdk/client-s3'"
```bash
bun add @aws-sdk/client-s3
```

### Error: "Environment variables yang diperlukan tidak ditemukan"
Pastikan file `.env` sudah dibuat dengan format yang benar. Lihat [ENV_SETUP.md](./ENV_SETUP.md) untuk panduan lengkap.

### Error: "TimeoutError: The socket connection was closed unexpectedly"
- Gunakan versi simple: `bun run upload-r2-simple`
- Periksa koneksi internet
- Pastikan file tidak terlalu besar untuk koneksi Anda

### Error: "Tidak ada file .parquet ditemukan"
- Pastikan ada file dengan ekstensi `.parquet` di folder yang dituju
- Periksa struktur folder dan nama file
- File dengan ekstensi lain akan di-skip otomatis
