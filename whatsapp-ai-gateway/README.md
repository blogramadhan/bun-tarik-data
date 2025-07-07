# WhatsApp AI Gateway

Aplikasi ini terdiri dari dua layanan terpisah:
1. **WhatsApp Service**: Menangani koneksi WhatsApp dan menjawab pertanyaan dengan AI
2. **Upload Service**: Mengelola upload materi pembelajaran

## Struktur Folder

```
whatsapp-ai-gateway/
├── docker-compose.yml (untuk menjalankan kedua layanan bersama)
├── start-all-docker.sh (untuk menjalankan kedua layanan Docker secara bersamaan)
├── stop-all-docker.sh (untuk menghentikan kedua layanan Docker)
├── start-local.sh (untuk menjalankan kedua layanan secara lokal)
├── whatsapp-service/
│   ├── docker-compose.yml (untuk menjalankan WhatsApp Service saja)
│   ├── start-docker.sh (untuk menjalankan WhatsApp Service saja)
│   ├── stop-docker.sh (untuk menghentikan WhatsApp Service)
│   ├── Dockerfile
│   ├── package.json
│   └── src/
│       └── index.ts
└── upload-service/
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

## Cara Menjalankan

### Opsi 1: Menjalankan Kedua Layanan Bersama dengan Docker

Menggunakan script di folder utama:

1. Pastikan file `.env` ada di folder whatsapp-service dengan API key yang diperlukan, atau script akan membuat file contoh:
   ```bash
   ./start-all-docker.sh
   ```

2. Untuk menghentikan layanan:
   ```bash
   ./stop-all-docker.sh
   ```

### Opsi 2: Menjalankan Layanan Secara Terpisah dengan Docker

#### WhatsApp Service

1. Masuk ke folder whatsapp-service:
   ```bash
   cd whatsapp-service
   ```

2. Salin file `.env.example` menjadi `.env` dan isi dengan API key yang diperlukan (jika belum ada):
   ```bash
   echo "DEEPINFRA_API_KEY=your_api_key_here" > .env
   ```

3. Edit file `.env` dan isi `DEEPINFRA_API_KEY` dengan API key Anda.

4. Jalankan dengan Docker Compose:
   ```bash
   ./start-docker.sh
   ```

5. Untuk menghentikan layanan:
   ```bash
   ./stop-docker.sh
   ```

#### Upload Service

1. Masuk ke folder upload-service:
   ```bash
   cd upload-service
   ```

2. Jalankan dengan Docker Compose:
   ```bash
   ./start-docker.sh
   ```

3. Untuk menghentikan layanan:
   ```bash
   ./stop-docker.sh
   ```

### Opsi 3: Menjalankan Secara Lokal (Tanpa Docker)

```bash
./start-local.sh
```

## Alamat Layanan

- WhatsApp Service: http://localhost:8788
- Upload Service: http://localhost:8789

## Fitur

### WhatsApp Service
- Menerima dan memproses pesan WhatsApp
- Menjawab pertanyaan dengan AI berdasarkan materi yang dipilih
- Endpoint API untuk mengirim pesan WhatsApp

### Upload Service
- Upload file materi pembelajaran (PDF, DOC, DOCX, TXT)
- Ekstraksi teks otomatis dari file
- Notifikasi ke WhatsApp Service untuk memuat ulang knowledge base

## Endpoint API

### WhatsApp Service (port 8788)
- `POST /send-message`: Mengirim pesan WhatsApp
  ```json
  {
    "number": "6281234567890",
    "message": "Hello World"
  }
  ```
- `POST /reload-kb`: Memuat ulang knowledge base

### Upload Service (port 8789)
- `GET /`: Halaman web untuk upload materi
- `POST /upload`: Endpoint untuk upload file 