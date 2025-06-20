# Panduan Upload File Parquet ke Cloudflare R2

## 🎯 Versi Upload yang Tersedia

### 1. **Versi Standar** (`upload-r2`)
```bash
bun run upload-r2
```
- **Target**: File .parquet kecil hingga menengah (< 100MB)
- **Timeout**: 5 menit
- **Retry**: 3 kali
- **Fitur**: Upload langsung, cepat untuk file kecil
- **Filter**: Hanya upload file dengan ekstensi `.parquet`

### 2. **Versi Robust** (`upload-r2-robust`)
```bash
bun run upload-r2-robust
```
- **Target**: File .parquet besar (> 100MB)
- **Timeout**: 5 menit
- **Retry**: 3 kali
- **Fitur**: Multipart upload, chunking, progress tracking detail
- **Filter**: Hanya upload file dengan ekstensi `.parquet`

### 3. **Versi Simple** (`upload-r2-simple`)
```bash
bun run upload-r2-simple
```
- **Target**: Semua ukuran file .parquet
- **Timeout**: 10 menit
- **Retry**: 5 kali
- **Fitur**: Timeout sangat besar, retry agresif
- **Filter**: Hanya upload file dengan ekstensi `.parquet`

## 🔧 Perbandingan Versi

| Fitur | Standar | Robust | Simple |
|-------|---------|--------|--------|
| Target File | .parquet < 100MB | .parquet > 100MB | .parquet semua ukuran |
| Timeout | 5 menit | 5 menit | 10 menit |
| Retry | 3x | 3x | 5x |
| Multipart | ❌ | ✅ | ❌ |
| Progress | Basic | Detailed | Basic |
| Speed | Fast | Medium | Slow |
| File Filter | .parquet only | .parquet only | .parquet only |

## 🚀 Rekomendasi Penggunaan

### Untuk File Parquet Kecil (< 10MB)
```bash
bun run upload-r2
```

### Untuk File Parquet Menengah (10MB - 100MB)
```bash
bun run upload-r2-simple
```

### Untuk File Parquet Besar (> 100MB)
```bash
bun run upload-r2-robust
```

### Jika Sering Timeout
```bash
bun run upload-r2-simple
```

## 📊 Fitur Upload Parquet

### ✅ File Filtering
- **Otomatis filter**: Hanya file dengan ekstensi `.parquet` yang diupload
- **Skip file lain**: File `.json`, `.csv`, `.txt`, dll akan di-skip
- **Logging**: Menampilkan file yang di-skip

### ✅ Progress Tracking
```
📁 Memulai upload folder: ./data
📊 Ditemukan 3 file .parquet untuk diupload
📂 Memproses folder: spse
📄 [1/3] Uploading: data.parquet
📤 Mengupload: spse/data.parquet (0.19 MB)
✅ Berhasil mengupload: spse/data.parquet
⏭️  Skip file non-parquet: data.json
```

### ✅ Statistik Upload
```
🎉 Upload folder selesai: ./data
📊 Statistik: 3/3 file .parquet berhasil
```

## 📊 Troubleshooting

### Error: "TimeoutError: The socket connection was closed unexpectedly"

**Solusi:**
1. Gunakan versi simple: `bun run upload-r2-simple`
2. Periksa koneksi internet
3. Pastikan file tidak terlalu besar
4. Coba upload file satu per satu

### Error: "ECONNRESET"

**Solusi:**
1. Gunakan versi robust: `bun run upload-r2-robust`
2. Periksa firewall/antivirus
3. Coba dari jaringan yang berbeda

### Error: "Access Denied"

**Solusi:**
1. Periksa credentials di file `.env`
2. Pastikan bucket name benar
3. Periksa permissions di Cloudflare R2

### Error: "Tidak ada file .parquet ditemukan"

**Solusi:**
1. Pastikan ada file dengan ekstensi `.parquet` di folder
2. Periksa struktur folder dan nama file
3. File dengan ekstensi lain akan di-skip otomatis

## 🔍 Monitoring Upload

### Progress Tracking
- **Standar**: Basic progress dengan emoji dan file count
- **Robust**: Detailed progress dengan part tracking untuk file besar
- **Simple**: Basic progress dengan retry info

### Log Output
```
📁 Memulai upload folder: ./data
📊 Ditemukan 5 file .parquet untuk diupload
📂 Memproses folder: spse
📄 [1/5] Uploading: data.parquet
📤 Mengupload: spse/data.parquet (2.72 MB)
✅ Berhasil mengupload: spse/data.parquet
⏭️  Skip file non-parquet: data.json
```

## ⚡ Tips Performa

1. **Gunakan versi yang tepat** untuk ukuran file .parquet Anda
2. **Upload file besar** di waktu yang tidak sibuk
3. **Monitor koneksi internet** saat upload
4. **Gunakan retry** jika terjadi error
5. **Split file besar** jika sering timeout
6. **Pastikan hanya file .parquet** yang ada di folder untuk upload lebih cepat

## 🛠️ Customization

### Mengubah File Extension Filter
Edit file `src/sendR2.ts` (atau versi lainnya):
```typescript
// Ubah dari .parquet ke ekstensi lain jika diperlukan
const parquetFiles = files.filter(f => f.isFile() && f.name.endsWith('.parquet'));
```

### Mengubah Timeout
Edit file `src/sendR2-simple.ts`:
```typescript
requestTimeout: 600000, // 10 menit
connectionTimeout: 120000, // 2 menit
```

### Mengubah Retry Count
Edit file `src/sendR2-simple.ts`:
```typescript
maxAttempts: 5, // Retry hingga 5 kali
```

### Mengubah Chunk Size (Robust)
Edit file `src/sendR2-robust.ts`:
```typescript
const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunks
```

## 📋 Contoh Struktur Folder

```
data/
├── spse/
│   ├── 97/
│   │   └── SPSE-PencatatanSwakelolaRealisasi/
│   │       └── 2024/
│   │           ├── data.parquet    ✅ (akan diupload)
│   │           └── data.json       ❌ (akan di-skip)
│   └── 98/
│       └── data.parquet            ✅ (akan diupload)
└── rup/
    └── data.parquet                ✅ (akan diupload)
``` 