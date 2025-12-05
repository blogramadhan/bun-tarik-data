# Perbaikan WhatsApp Gateway untuk Production

## Tanggal: 2025-12-05

## Masalah yang Ditemukan

1. **Dependencies tidak terinstall** - Menyebabkan error saat runtime
2. **Dockerfile kurang optimal** - Tidak ada layer caching yang baik
3. **docker-compose.yml tidak konsisten** - Konfigurasi berbeda antara 2 file
4. **Missing TypeScript config** - Tidak ada tsconfig.json
5. **Missing .dockerignore** - Build image tidak optimal
6. **Kurang konfigurasi production** - Tidak ada health check, environment variables

## Perbaikan yang Dilakukan

### 1. Dockerfile ([whatsapp-service/Dockerfile](whatsapp-service/Dockerfile))

**Perubahan:**
- ✅ Tambah layer caching untuk dependencies (COPY package.json dulu)
- ✅ Install dependencies dengan `--frozen-lockfile --production`
- ✅ Cleanup temporary files lebih agresif
- ✅ Tambah health check
- ✅ Set NODE_ENV=production
- ✅ Improved cleanup script untuk temporary files Chromium
- ✅ Set proper permissions untuk .wwebjs_auth directory

**Manfaat:**
- Build lebih cepat karena layer caching
- Image size lebih kecil
- Lebih stabil di production

### 2. docker-compose.yml

**File yang diperbaiki:**
- [whatsapp-gateway/docker-compose.yml](docker-compose.yml)
- [whatsapp-service/docker-compose.yml](whatsapp-service/docker-compose.yml)

**Perubahan:**
- ✅ Update version ke 3.8
- ✅ Tambah `tmpfs` untuk temporary Chromium files (512MB)
- ✅ Tambah `shm_size: '2gb'` untuk shared memory
- ✅ Tambah environment variables (NODE_ENV, PUPPETEER_*)
- ✅ Tambah health check configuration
- ✅ Tambah `security_opt` dan `cap_add` untuk Chromium
- ✅ Konsistensi konfigurasi antara 2 file

**Manfaat:**
- Service tidak crash karena memory issues
- Auto restart jika ada masalah
- Health monitoring otomatis

### 3. TypeScript Configuration ([whatsapp-service/tsconfig.json](whatsapp-service/tsconfig.json))

**Fitur:**
- ✅ Configured untuk Bun runtime
- ✅ Strict mode enabled
- ✅ ESNext target
- ✅ Module resolution: bundler

**Manfaat:**
- Type checking lebih baik
- Error detection saat development
- Better IDE support

### 4. Docker Ignore ([whatsapp-service/.dockerignore](whatsapp-service/.dockerignore))

**Konten:**
- ✅ node_modules
- ✅ .git dan git files
- ✅ .env files
- ✅ Log files
- ✅ WhatsApp auth cache
- ✅ Editor files (.vscode, .idea)

**Manfaat:**
- Build lebih cepat
- Image size lebih kecil
- Tidak copy file yang tidak perlu

### 5. Documentation ([README.md](README.md))

**Penambahan:**
- ✅ Production configuration guide
- ✅ Troubleshooting section
- ✅ Installation steps yang jelas
- ✅ Docker commands untuk debugging

## Langkah Deployment di Production

### Pertama kali deploy:

```bash
# 1. Pull latest code
git pull origin main

# 2. Masuk ke directory
cd whatsapp-gateway/whatsapp-service

# 3. Install dependencies
bun install

# 4. Kembali ke root whatsapp-gateway
cd ..

# 5. Stop container lama jika ada
./stop-all-docker.sh

# 6. Build dan start dengan docker compose
docker-compose down
docker-compose build --no-cache
docker-compose up -d

# 7. Check logs
docker logs -f whatsapp-service
```

### Update code setelah deploy:

```bash
cd whatsapp-gateway
git pull origin main
docker-compose down
docker-compose build --no-cache
docker-compose up -d
docker logs -f whatsapp-service
```

## Monitoring di Production

### Check status service:
```bash
docker ps | grep whatsapp-service
```

### Check logs:
```bash
docker logs whatsapp-service
docker logs -f whatsapp-service  # follow mode
docker logs --tail 100 whatsapp-service  # last 100 lines
```

### Check health:
```bash
docker inspect whatsapp-service | grep -A 10 Health
curl http://localhost:8788/
```

### Restart jika ada masalah:
```bash
docker restart whatsapp-service
```

## Error yang Sudah Diperbaiki

1. ❌ **Error: Cannot find package 'hono'**
   - ✅ **Fix**: Install dependencies dengan `bun install`

2. ❌ **Error: Cannot find package 'https-proxy-agent' from puppeteer**
   - ✅ **Fix**: Override postinstall script di package.json untuk skip puppeteer download (kita sudah install chromium via apt-get)

3. ❌ **Chromium crashes di production**
   - ✅ **Fix**: Tambah `shm_size: '2gb'` dan `tmpfs` configuration

4. ❌ **Container restart terus menerus**
   - ✅ **Fix**: Tambah health check dan cleanup script

5. ❌ **Build Docker lambat**
   - ✅ **Fix**: Layer caching di Dockerfile dan .dockerignore

6. ❌ **No health monitoring**
   - ✅ **Fix**: Health check configuration di docker-compose

## Testing

Setelah deploy, test dengan:

```bash
# Test status endpoint
curl http://localhost:8788/

# Expected response:
# {"status":"ok","whatsapp":"connected"}

# Test send message
curl -X POST http://localhost:8788/send-message \
  -H "Content-Type: application/json" \
  -d '{"number":"628123456789","message":"Test message"}'
```

## Notes untuk Production

- Service akan generate QR code di logs pertama kali jalan
- Scan QR code dengan WhatsApp untuk authenticate
- Auth data disimpan di Docker volume `whatsapp-gateway_whatsapp-auth`
- Port 8788 harus di-expose untuk API access
- Chromium membutuhkan minimum 2GB shared memory
- Gunakan `docker logs` untuk monitoring

## Checklist Sebelum Deploy

- [ ] Dependencies sudah diinstall (`bun install`)
- [ ] Docker dan Docker Compose terinstall
- [ ] Port 8788 available
- [ ] Minimal 4GB RAM available untuk container
- [ ] Backup WhatsApp auth data jika ada (`whatsapp-gateway_whatsapp-auth` volume)
