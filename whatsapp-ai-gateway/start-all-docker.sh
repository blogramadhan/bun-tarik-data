#!/bin/bash

# Periksa apakah file .env ada untuk DeepInfra API Key
if [ ! -f .env ]; then
  echo "File .env tidak ditemukan. Membuat dari contoh..."
  echo "# API Key untuk DeepInfra" > .env
  echo "DEEPINFRA_API_KEY=your_api_key_here" >> .env
  echo "Silakan edit file .env dan masukkan API key yang valid."
  exit 1
fi

# Membangun dan menjalankan kedua layanan dengan docker-compose utama
echo "Membangun dan menjalankan WhatsApp AI Gateway..."
docker-compose up --build -d

echo "Layanan berjalan:"
echo "- WhatsApp Service: http://localhost:8788"
echo "- Upload Service: http://localhost:8789"
echo ""
echo "Untuk melihat log: docker-compose logs -f"
echo "Untuk menghentikan layanan: ./stop-all-docker.sh" 