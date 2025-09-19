#!/bin/bash

# Memulai WhatsApp Service
echo "Memulai WhatsApp Service..."
cd whatsapp-service
bun install
bun run src/index.ts &
WHATSAPP_PID=$!

# Fungsi untuk menangani sinyal interupsi
cleanup() {
  echo "Menghentikan layanan..."
  kill $WHATSAPP_PID
  exit 0
}

# Menangkap sinyal SIGINT (Ctrl+C)
trap cleanup SIGINT

# Menunggu hingga dihentikan
echo "WhatsApp Gateway berjalan:"
echo "- WhatsApp Service: http://localhost:8788"
echo "Tekan Ctrl+C untuk menghentikan."
wait 