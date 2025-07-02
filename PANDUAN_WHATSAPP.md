# 📱 Panduan WhatsApp dengan Docker

## 📋 Prerequisites

1. **Docker dan Docker Compose** terinstall
2. **Bun** terinstall (jika ingin menjalankan tanpa Docker)
3. **Nomor WhatsApp** yang akan menerima notifikasi

## ⚙️ Konfigurasi Awal

### 1. Setup Environment
```bash
# Copy file environment
cp .env.example .env

# Edit file .env
nano .env
```

Pastikan mengisi `WHATSAPP_RECIPIENT_NUMBERS` dengan nomor yang benar:
```env
WHATSAPP_RECIPIENT_NUMBERS=628123456789
```

**Format Nomor:**
- Gunakan kode negara Indonesia: `62`
- Hilangkan angka `0` di depan
- Contoh: `081234567890` → `628123456789`

## 🚀 Cara Menjalankan

### 🔐 1. Autentikasi WhatsApp (Pertama Kali)

#### Menggunakan Script Helper (DIREKOMENDASIKAN):
```bash
# Menggunakan script otomatis
./run-auth.sh
```

#### Atau menggunakan Docker langsung:
```bash
# Build dan jalankan container untuk autentikasi
docker-compose up --build
```

#### Atau tanpa Docker:
```bash
bun run src/auth-whatsapp.ts
```

**Proses Autentikasi:**
1. Container akan mulai dan menampilkan QR Code di terminal
2. QR Code juga akan disimpan sebagai file `qrcode/whatsapp-qrcode.png`
3. **Download file QR Code** dari folder `qrcode/`
4. **Scan QR Code** menggunakan WhatsApp di HP Anda:
   - Buka WhatsApp di HP
   - Pergi ke **Pengaturan** → **Perangkat Tertaut** 
   - Klik **Tautkan Perangkat**
   - Scan QR Code
5. Setelah berhasil, sesi akan disimpan di folder `.wwebjs_auth/`
6. Container akan berhenti otomatis setelah autentikasi selesai

### 📤 2. Logout WhatsApp

#### Menggunakan Docker:
```bash
# Edit docker-compose.yml, ganti command menjadi:
# command: ["bun", "src/logout-whatsapp.ts"]

# Kemudian jalankan:
docker-compose up
```

#### Atau tanpa Docker:
```bash
bun run src/logout-whatsapp.ts
```

**Catatan:** Logout akan menghapus sesi tersimpan. Anda perlu autentikasi ulang untuk menggunakan WhatsApp lagi.

## 🔧 Konfigurasi Docker

### docker-compose.yml
File ini sudah dikonfigurasi dengan benar:
- **Volumes:** Menyimpan data autentikasi dan QR code
- **Environment:** Konfigurasi Puppeteer untuk Docker
- **Command:** Bisa diganti antara autentikasi dan logout

### Mengganti Mode Operasi
Edit `docker-compose.yml` bagian `command`:

```yaml
# Untuk autentikasi:
command: ["bun", "src/auth-whatsapp.ts"]

# Untuk logout:
command: ["bun", "src/logout-whatsapp.ts"]
```

## 📁 Struktur File Penting

```
├── .wwebjs_auth/          # Data sesi WhatsApp (jangan dihapus!)
├── qrcode/                # QR Code untuk autentikasi
├── src/
│   ├── whatsapp.ts        # Konfigurasi utama WhatsApp
│   ├── auth-whatsapp.ts   # Script autentikasi
│   └── logout-whatsapp.ts # Script logout
├── docker-compose.yml     # Konfigurasi Docker
├── Dockerfile            # Image Docker
└── .env                  # Konfigurasi environment
```

## 🔍 Troubleshooting

### ❌ Error "Protocol error (Network.setUserAgentOverride): Session closed"
**Masalah:** Browser session tertutup prematur setelah scan QR code berhasil.

**Solusi:**
```bash
# 1. Reset semua container dan volume
docker-compose down
docker system prune -f
docker volume prune -f

# 2. Hapus data auth lama
rm -rf .wwebjs_auth/

# 3. Jalankan ulang dengan script helper
./run-auth.sh
```

**Penyebab:** Konfigurasi Puppeteer yang tidak cocok dengan Docker environment sudah diperbaiki di versi terbaru.

### ❌ QR Code muncul berulang kali
**Masalah:** QR Code baru muncul setelah scan berhasil.

**Solusi:**
1. **JANGAN SCAN** QR code kedua dan seterusnya
2. Tunggu hingga container berhenti otomatis  
3. Cek folder `.wwebjs_auth/` - jika ada isi, autentikasi sudah berhasil
4. Restart container untuk test koneksi

### ❌ QR Code tidak muncul
- Pastikan Docker container berjalan dengan benar
- Cek file `qrcode/whatsapp-qrcode.png`
- Restart container jika diperlukan

### ❌ Autentikasi gagal
- Pastikan scan QR code dalam waktu 5 menit
- Pastikan koneksi internet stabil
- Coba hapus folder `.wwebjs_auth/` dan autentikasi ulang

### ❌ WhatsApp tidak terkirim
- Pastikan nomor di `.env` format yang benar
- Pastikan WhatsApp masih terautentikasi
- Cek log untuk error

### ❌ Container gagal start
```bash
# Cek log error
docker-compose logs

# Reset semua dan coba lagi
./run-auth.sh
```

## 🧪 Testing Koneksi

Setelah autentikasi berhasil, Anda bisa test kirim pesan dengan menjalankan script upload atau fungsi lain yang menggunakan WhatsApp.

## 🔒 Keamanan

1. **Jangan commit** folder `.wwebjs_auth/` ke git
2. **Backup** folder `.wwebjs_auth/` untuk menghindari autentikasi ulang
3. **Protect** file `.env` jangan dishare

## 📝 Tips

1. **Sesi WhatsApp bertahan lama** - tidak perlu autentikasi berulang
2. **QR Code expire dalam 20 detik** - scan segera setelah muncul
3. **Backup data autentikasi** - copy folder `.wwebjs_auth/` ke tempat aman
4. **Multiple nomor** - pisahkan dengan koma di file `.env`

---

✅ **Status:** Semua fungsi WhatsApp telah dikonfigurasi dengan benar dan siap digunakan! 