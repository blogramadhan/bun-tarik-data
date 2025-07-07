#!/bin/bash

# Periksa apakah file .env ada di folder whatsapp-service
if [ ! -f whatsapp-service/.env ]; then
  echo "File .env tidak ditemukan di whatsapp-service. Membuat dari contoh..."
  echo "# API Key untuk DeepInfra" > whatsapp-service/.env
  echo "DEEPINFRA_API_KEY=your_api_key_here" >> whatsapp-service/.env
  echo "Silakan edit file whatsapp-service/.env dan masukkan API key yang valid."
  exit 1
fi

# Periksa apakah file .env ada di folder upload-service
if [ ! -f upload-service/.env ]; then
  echo "File .env tidak ditemukan di upload-service. Membuat dari contoh..."
  echo "# URL untuk WhatsApp Service" > upload-service/.env
  echo "WHATSAPP_SERVICE_URL=http://host.docker.internal:8788" >> upload-service/.env
  echo "File .env dibuat dengan konfigurasi default di upload-service."
fi

# Memulai WhatsApp Service
echo "Memulai WhatsApp Service..."
cd whatsapp-service
docker-compose up --build -d
cd ..

# Memulai Upload Service
echo "Memulai Upload Service..."
cd upload-service
docker-compose up --build -d
cd ..

echo "Kedua layanan berjalan:"
echo "- WhatsApp Service: http://localhost:8788"
echo "- Upload Service: http://localhost:8789"
echo ""
echo "Untuk melihat log WhatsApp Service: cd whatsapp-service && docker-compose logs -f"
echo "Untuk melihat log Upload Service: cd upload-service && docker-compose logs -f"
echo "Untuk menghentikan kedua layanan, jalankan: ./stop-all-docker.sh" 