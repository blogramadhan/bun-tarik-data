# WhatsApp AI Gateway

Aplikasi ini terdiri dari dua layanan terpisah:
1. **WhatsApp Service**: Menangani koneksi WhatsApp dan menjawab pertanyaan dengan AI
2. **Upload Service**: Mengelola upload materi pembelajaran

## Fitur Utama

- **Pemahaman Konteks Otomatis**: AI secara otomatis memahami konteks dari semua materi yang diupload tanpa perlu memilih materi terlebih dahulu
- **Ekstraksi Teks Cerdas**: Mendukung berbagai format file (PDF, DOCX, DOC, TXT, PPTX, XLSX, JPG/PNG dengan OCR, HTML, RTF, ODT, dll.)
- **Deteksi Bahasa**: Mendukung teks dalam Bahasa Indonesia dan Inggris
- **Respon Cepat**: Jawaban langsung tanpa perlu perintah khusus

## Struktur Folder

```
whatsapp-ai-gateway/
├── start-all-docker.sh (untuk menjalankan kedua layanan Docker secara bersamaan)
├── stop-all-docker.sh (untuk menghentikan kedua layanan Docker)
├── start-local.sh (untuk menjalankan kedua layanan secara lokal)
├── whatsapp-service/
│   ├── .env (konfigurasi API key untuk DeepInfra)
│   ├── docker-compose.yml (untuk menjalankan WhatsApp Service saja)
│   ├── start-docker.sh (untuk menjalankan WhatsApp Service saja)
│   ├── stop-docker.sh (untuk menghentikan WhatsApp Service)
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.ts
└── upload-service/
    ├── .env (konfigurasi URL untuk WhatsApp Service)
    ├── docker-compose.yml (untuk menjalankan Upload Service saja)
    ├── start-docker.sh (untuk menjalankan Upload Service saja)
    ├── stop-docker.sh (untuk menghentikan Upload Service)
    ├── Dockerfile
    ├── package.json
    └── src/
        ├── index.ts
        └── utils/
            └── extractor.ts
```

## Format File yang Didukung

Sistem mendukung ekstraksi teks dari berbagai format file:

- **Dokumen**: PDF, DOCX, DOC, TXT, RTF, ODT
- **Presentasi**: PPTX, ODP
- **Spreadsheet**: XLSX, XLS, ODS
- **Web**: HTML, HTM
- **Gambar**: JPG, JPEG, PNG (dengan OCR)

## Cara Menggunakan Bot WhatsApp

Bot WhatsApp mendukung perintah berikut:

- `/materi` - Melihat daftar materi yang tersedia
- `/pilih [nama_materi]` - Memilih materi spesifik (opsional)
- `/ai [pertanyaan]` - Bertanya dengan semua materi secara otomatis
- `/tanya [pertanyaan]` - Sama dengan /ai, bertanya dengan semua materi
- `/bantuan` - Menampilkan panduan penggunaan

**Fitur Baru**: Anda juga dapat langsung mengirim pertanyaan tanpa perintah khusus, dan bot akan secara otomatis menggunakan semua materi untuk menjawab.

## Konfigurasi File .env

### WhatsApp Service (.env)
```
# API Key untuk DeepInfra
DEEPINFRA_API_KEY=your_api_key_here
# Opsional: Model AI yang akan digunakan
DEEPINFRA_MODEL=deepseek-ai/DeepSeek-R1
```
Ganti `your_api_key_here` dengan API key DeepInfra yang valid untuk menggunakan fitur AI.

### Upload Service (.env)
```
# URL untuk WhatsApp Service
WHATSAPP_SERVICE_URL=http://host.docker.internal:8788
```
Nilai default menggunakan `host.docker.internal` untuk komunikasi antar kontainer Docker pada host yang sama. Jika WhatsApp Service berjalan di server terpisah, ubah URL sesuai kebutuhan.

## Cara Menjalankan

### Opsi 1: Menjalankan Kedua Layanan Bersama dengan Docker

1. Pastikan Docker dan Docker Compose terinstal di sistem Anda
2. Buat file `.env` di folder `whatsapp-service` dengan API key yang valid
3. Jalankan perintah:
   ```
   ./start-all-docker.sh
   ```
4. Untuk menghentikan semua layanan:
   ```
   ./stop-all-docker.sh
   ```

### Opsi 2: Menjalankan Layanan Secara Terpisah

#### WhatsApp Service
1. Masuk ke folder `whatsapp-service`
2. Buat file `.env` dengan API key yang valid
3. Jalankan:
   ```
   ./start-docker.sh
   ```

#### Upload Service
1. Masuk ke folder `upload-service`
2. Buat file `.env` dengan URL WhatsApp Service yang benar
3. Jalankan:
   ```
   ./start-docker.sh
   ```

### Opsi 3: Menjalankan Secara Lokal (Tanpa Docker)

1. Pastikan Bun terinstal di sistem Anda
2. Pastikan semua dependensi ekstraksi teks terinstal (poppler-utils, pandoc, tesseract-ocr, dll.)
3. Jalankan:
   ```
   ./start-local.sh
   ```

## Pengembangan

Untuk mengembangkan lebih lanjut:

1. Semua kode WhatsApp Service ada di `whatsapp-service/src/index.ts`
2. Kode Upload Service ada di `upload-service/src/index.ts`
3. Kode ekstraksi teks ada di `upload-service/src/utils/extractor.ts` 