#!/bin/bash

# Periksa apakah file .env ada
if [ ! -f .env ]; then
  echo "File .env tidak ditemukan. Membuat dari contoh..."
  echo "# API Key untuk DeepInfra" > .env
  echo "DEEPINFRA_API_KEY=your_api_key_here" >> .env
  echo "Silakan edit file .env dan masukkan API key yang valid."
  exit 1
fi

# Membangun dan menjalankan kontainer
echo "Membangun dan menjalankan WhatsApp Service..."
docker-compose up --build -d

echo "WhatsApp Service berjalan di: http://localhost:8788"
echo ""
echo "Untuk melihat log, jalankan: docker-compose logs -f"
echo "Untuk menghentikan layanan, jalankan: docker-compose down" 