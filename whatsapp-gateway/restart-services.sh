#!/bin/bash

echo "Menghentikan WhatsApp Gateway..."
docker-compose down

echo "Membangun dan menjalankan WhatsApp Gateway..."
docker-compose up --build -d

echo "Layanan berjalan:"
echo "- WhatsApp Service: http://localhost:8788"
echo ""
echo "Untuk melihat log: docker-compose logs -f"
echo "Untuk menghentikan layanan: ./stop-all-docker.sh" 