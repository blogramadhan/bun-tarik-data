#!/bin/bash

# Memulai WhatsApp Service
echo "Memulai WhatsApp Service..."
cd whatsapp-service
bun install
DEEPINFRA_API_KEY=${DEEPINFRA_API_KEY:-"your_api_key_here"} bun run src/index.ts &
WHATSAPP_PID=$!

# Memulai Upload Service
echo "Memulai Upload Service..."
cd ../upload-service
bun install
WHATSAPP_SERVICE_URL=http://localhost:8788 bun run src/index.ts &
UPLOAD_PID=$!

# Fungsi untuk menangani sinyal interupsi
cleanup() {
  echo "Menghentikan layanan..."
  kill $WHATSAPP_PID
  kill $UPLOAD_PID
  exit 0
}

# Menangkap sinyal SIGINT (Ctrl+C)
trap cleanup SIGINT

# Menunggu hingga dihentikan
echo "Kedua layanan berjalan:"
echo "- WhatsApp Service: http://localhost:8788"
echo "- Upload Service: http://localhost:8789"
echo "Tekan Ctrl+C untuk menghentikan."
wait 