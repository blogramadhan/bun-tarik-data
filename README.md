# Bun Tarik Data

Aplikasi untuk mengupload data ke penyimpanan Cloudflare R2.

## Struktur Proyek

- `src/upload.ts` - Kode utama untuk upload file ke Cloudflare R2
- `src/upload_full.ts` - Script upload lengkap
- `src/rup.ts` - Script untuk data RUP
- `src/spse.ts` - Script untuk data SPSE
- `src/katalog.ts` - Script untuk data katalog
- `src/daring.ts` - Script untuk data daring
- `src/sikap.ts` - Script untuk data SIKAP

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
```

## Menjalankan Aplikasi

1. Jalankan script upload:
   ```bash
   bun run upload
   ```

2. Atau jalankan script spesifik:
   ```bash
   bun run rup      # Upload data RUP
   bun run spse     # Upload data SPSE
   bun run katalog  # Upload data katalog
   bun run daring   # Upload data daring
   bun run sikap    # Upload data SIKAP
   ```

## Fitur

- Upload file ke Cloudflare R2
- Aturan upload berdasarkan tahun pada path file
- Multiple script untuk berbagai jenis data
- Notifikasi console untuk status upload
- Optimized untuk performance dengan Bun runtime 

# 📲 Bun WhatsApp Gateway (Dockerized)

Gateway pengiriman pesan WhatsApp berbasis **Bun JS** dan **whatsapp-web.js**, dilengkapi REST API untuk menerima notifikasi dari sistem lain.

---

## 🚀 Fitur

- Kirim pesan WhatsApp via HTTP `POST /send-message`
- Dibangun dengan [Bun](https://bun.sh/) untuk performa tinggi
- Gunakan [whatsapp-web.js](https://github.com/pedroslopez/whatsapp-web.js)
- Dockerized, siap untuk deploy production
- Menyimpan session login agar tidak perlu scan QR setiap kali

---

## 📦 Struktur


---

## ⚙️ Build & Jalankan

### 1. Build Docker image

```bash
docker build -t bun-whatsapp-gateway .

docker run -d \
  --name whatsapp-gateway \
  -p 3000:3000 \
  -v $(pwd)/.wwebjs_auth:/app/.wwebjs_auth \
  bun-whatsapp-gateway

docker run -d \
  --name whatsapp-ai-gateway \
  -e DEEPINFRA_API_KEY=sk-xxx \
  -p 3000:3000 \
  -v $(pwd)/.wwebjs_auth:/app/.wwebjs_auth \
  -v $(pwd)/data:/app/data \
  whatsapp-ai-gateway

docker logs -f whatsapp-gateway

curl -X POST http://localhost:3000/send-message \
  -H 'Content-Type: application/json' \
  -d '{"number": "6281234567890", "message": "Halo dari Bun WhatsApp Gateway"}'

import axios from 'axios';

await axios.post('http://localhost:3000/send-message', {
  number: '6281234567890',
  message: 'Notifikasi dari sistem Anda'
});

docker stop whatsapp-gateway
docker rm whatsapp-gateway

