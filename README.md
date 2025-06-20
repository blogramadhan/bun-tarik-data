# Tarik Data - Cloudflare R2 Upload Tool

Aplikasi untuk mengupload data ke Cloudflare R2 Object Storage menggunakan Bun.

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

### 4. Upload Data ke R2

```bash
# Upload folder ke R2
bun run upload-r2

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
    ├── sendR2.ts          # Script upload ke R2
    ├── validate-env.ts    # Script validasi environment
    ├── config/            # Konfigurasi lainnya
    ├── katalog.ts         # Script katalog
    ├── katalogv6.ts       # Script katalog v6
    ├── rup.ts             # Script RUP
    └── spse.ts            # Script SPSE
```

## 🛠️ Scripts Available

- `bun run validate-env` - Validasi environment variables
- `bun run upload-r2` - Upload data ke Cloudflare R2
- `bun run dev` - Jalankan dengan watch mode

## 🔧 Dependencies

- **Bun** - Runtime JavaScript/TypeScript
- **@aws-sdk/client-s3** - AWS SDK untuk Cloudflare R2
- **duckdb** - Database untuk processing data

## 📖 Dokumentasi Lengkap

- [Setup Environment Variables](./ENV_SETUP.md) - Panduan lengkap setup Cloudflare R2
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
