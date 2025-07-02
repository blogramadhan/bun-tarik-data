#!/bin/bash

# Script untuk menjalankan autentikasi WhatsApp dengan Docker
# Usage: ./run-auth.sh

echo "🚀 Menjalankan Autentikasi WhatsApp dengan Docker"
echo "================================================"

# Fungsi untuk mendeteksi command Docker Compose yang tersedia
detect_docker_compose() {
    if command -v docker-compose &> /dev/null; then
        echo "docker-compose"
    elif docker compose version &> /dev/null 2>&1; then
        echo "docker compose"
    else
        echo ""
    fi
}

# Cek apakah Docker dan Docker Compose tersedia
DOCKER_COMPOSE_CMD=$(detect_docker_compose)

if [ -z "$DOCKER_COMPOSE_CMD" ]; then
    echo "❌ Docker Compose tidak ditemukan!"
    echo "📦 Silakan install Docker dan Docker Compose terlebih dahulu:"
    echo "   https://docs.docker.com/compose/install/"
    echo ""
    echo "💡 Alternatif: Jalankan tanpa Docker dengan Bun:"
    echo "   bun run src/auth-whatsapp.ts"
    exit 1
fi

echo "✅ Menggunakan: $DOCKER_COMPOSE_CMD"

# Cek apakah file .env ada
if [ ! -f ".env" ]; then
    echo "❌ File .env tidak ditemukan!"
    if [ -f "env.template" ]; then
        echo "📋 Membuat .env dari template..."
        cp env.template .env
        echo "✅ File .env dibuat. Silakan edit file .env dan isi WHATSAPP_RECIPIENT_NUMBERS"
        echo "📝 Contoh: WHATSAPP_RECIPIENT_NUMBERS=628123456789"
        echo "🔧 Edit dengan: nano .env"
        exit 1
    else
        echo "❌ Template .env tidak ditemukan!"
        exit 1
    fi
fi

# Cek apakah WHATSAPP_RECIPIENT_NUMBERS sudah diset
if ! grep -q "WHATSAPP_RECIPIENT_NUMBERS=62" .env; then
    echo "⚠️ WHATSAPP_RECIPIENT_NUMBERS belum dikonfigurasi dengan benar!"
    echo "📝 Edit file .env dan isi dengan nomor WhatsApp yang benar"
    echo "🔧 Edit dengan: nano .env"
    exit 1
fi

# Bersihkan container yang lama jika ada
echo "🧹 Membersihkan container lama..."
$DOCKER_COMPOSE_CMD down 2>/dev/null || true

# Build dan jalankan container
echo "🔨 Building dan menjalankan container..."
$DOCKER_COMPOSE_CMD up --build

echo "✅ Proses selesai!" 