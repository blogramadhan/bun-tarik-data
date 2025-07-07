#!/bin/bash

# Membangun dan menjalankan kontainer
echo "Membangun dan menjalankan Upload Service..."
docker-compose up --build -d

echo "Upload Service berjalan di: http://localhost:8789"
echo ""
echo "Untuk melihat log, jalankan: docker-compose logs -f"
echo "Untuk menghentikan layanan, jalankan: docker-compose down" 