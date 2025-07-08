#!/bin/bash

echo "Menghentikan semua layanan WhatsApp AI Gateway..."
docker-compose down

echo "Membersihkan volume data untuk memastikan konsistensi..."
docker volume rm whatsapp-ai-gateway_shared-data-volume || true

echo "Membangun dan menjalankan WhatsApp AI Gateway..."
docker-compose up --build -d

echo "Layanan berjalan:"
echo "- WhatsApp Service: http://localhost:8788"
echo "- Upload Service: http://localhost:8789"
echo ""
echo "Untuk melihat log: docker-compose logs -f"
echo "Untuk menghentikan layanan: ./stop-all-docker.sh" 