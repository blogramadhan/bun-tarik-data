#!/bin/bash

# Script untuk membersihkan Docker containers dan mengatasi masalah Chromium lock
# Usage: ./cleanup-docker.sh

echo "🧹 Membersihkan Docker Environment untuk WhatsApp"
echo "================================================="

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

DOCKER_COMPOSE_CMD=$(detect_docker_compose)

if [ -z "$DOCKER_COMPOSE_CMD" ]; then
    echo "❌ Docker Compose tidak ditemukan!"
    exit 1
fi

echo "✅ Menggunakan: $DOCKER_COMPOSE_CMD"

# Stop dan hapus containers
echo "🛑 Menghentikan containers..."
$DOCKER_COMPOSE_CMD down --remove-orphans

# Hapus containers yang ada
echo "🗑️ Menghapus containers lama..."
docker container prune -f

# Hapus images yang tidak terpakai
echo "🗑️ Menghapus images yang tidak terpakai..."
docker image prune -f

# Hapus volumes yang tidak terpakai
echo "🗑️ Menghapus volumes yang tidak terpakai..."
docker volume prune -f

# Hapus networks yang tidak terpakai
echo "🗑️ Menghapus networks yang tidak terpakai..."
docker network prune -f

# Hapus build cache
echo "🗑️ Menghapus build cache..."
docker builder prune -f

# Hapus semua temporary chrome data
echo "🧹 Membersihkan Chrome user data..."
sudo rm -rf /tmp/chromium-user-data* 2>/dev/null || true
rm -rf /tmp/chromium-user-data* 2>/dev/null || true

# Reset folder auth jika bermasalah
if [ -d ".wwebjs_auth" ]; then
    echo "🔄 Reset folder autentikasi..."
    sudo rm -rf .wwebjs_auth/ 2>/dev/null || true
    mkdir -p .wwebjs_auth
fi

# Pastikan folder yang diperlukan ada
echo "📁 Membuat folder yang diperlukan..."
mkdir -p qrcode
mkdir -p .wwebjs_auth

echo ""
echo "✅ Cleanup selesai!"
echo "🚀 Sekarang jalankan: ./run-auth.sh" 