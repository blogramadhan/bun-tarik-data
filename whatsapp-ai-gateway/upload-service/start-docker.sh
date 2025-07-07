#!/bin/bash

# Periksa apakah file .env ada
if [ ! -f .env ]; then
  echo "File .env tidak ditemukan. Membuat dari contoh..."
  echo "# URL untuk WhatsApp Service" > .env
  echo "WHATSAPP_SERVICE_URL=http://host.docker.internal:8788" >> .env
  echo "File .env dibuat dengan konfigurasi default."
fi

# Membangun dan menjalankan kontainer
echo "Membangun dan menjalankan Upload Service..."
docker-compose up --build -d

echo "Upload Service berjalan di: http://localhost:8789"
echo ""
echo "Untuk melihat log, jalankan: docker-compose logs -f"
echo "Untuk menghentikan layanan, jalankan: docker-compose down" 