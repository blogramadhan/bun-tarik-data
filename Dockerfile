# Gunakan image Bun sebagai base
FROM oven/bun:latest

# Set working directory
WORKDIR /app

# Install dependensi untuk Chrome Headless dan Puppeteer
RUN apt-get update && apt-get install -y \
    chromium \
    fonts-ipafont-gothic fonts-wqy-zenhei fonts-thai-tlwg fonts-kacst fonts-symbola fonts-noto \
    fonts-freefont-ttf \
    gconf-service \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libgbm1 \
    libgconf-2-4 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    libasound2 \
    libpangocairo-1.0-0 \
    libx11-xcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxi6 \
    libpango-1.0-0 \
    libcups2 \
    --no-install-recommends \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variable untuk Puppeteer
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true
ENV PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium
ENV DEBIAN_FRONTEND=noninteractive
ENV DISPLAY=:99
ENV NODE_OPTIONS="--max-old-space-size=4096"

# Copy package.json dan bun.lockb
COPY package.json bun.lockb* ./

# Install dependensi
RUN bun install

# Copy source code
COPY . .

# Bersihkan Chrome profile locks jika ada
RUN rm -rf /tmp/chromium-user-data* || true

# Volume untuk menyimpan auth data
VOLUME ["/app/.wwebjs_auth"]

# Command untuk menjalankan aplikasi
CMD ["bun", "src/auth-whatsapp.ts"] 