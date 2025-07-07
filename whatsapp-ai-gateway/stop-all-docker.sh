#!/bin/bash

echo "Menghentikan WhatsApp Service..."
cd whatsapp-service
docker-compose down
cd ..

echo "Menghentikan Upload Service..."
cd upload-service
docker-compose down
cd ..

echo "Kedua layanan dihentikan." 