FROM oven/bun:1.2.17

# Install Puppeteer dependencies
RUN apt-get update && apt-get install -y \
    chromium \
    chromium-sandbox \
    fonts-liberation \
    fonts-ipafont-gothic \
    fonts-wqy-zenhei \
    fonts-thai-tlwg \
    fonts-kacst \
    fonts-freefont-ttf \
    libappindicator3-1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libcups2 \
    libdbus-1-3 \
    libgdk-pixbuf2.0-0 \
    libnspr4 \
    libnss3 \
    libx11-xcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libxss1 \
    libxtst6 \
    xdg-utils \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && rm -rf /tmp/*

# Set environment variables for Puppeteer
ENV PUPPETEER_SKIP_CHROMIUM_DOWNLOAD=true \
    PUPPETEER_EXECUTABLE_PATH=/usr/bin/chromium \
    NODE_ENV=production

WORKDIR /app

# Copy package files first for better caching
COPY package.json bun.lock* ./

# Install dependencies
RUN bun install --frozen-lockfile --production

# Copy source code
COPY src ./src

# Create cleanup and start script
RUN echo '#!/bin/sh\n\
# Clean up temporary files\n\
rm -rf /tmp/chromium-data* /tmp/.X* /tmp/puppeteer* 2>/dev/null || true\n\
\n\
# Start the application\n\
exec bun run src/index.ts' > /app/start.sh && \
    chmod +x /app/start.sh

# Create directory for auth data with proper permissions
RUN mkdir -p /app/.wwebjs_auth && chmod 777 /app/.wwebjs_auth

# Volume untuk menyimpan data autentikasi WhatsApp
VOLUME ["/app/.wwebjs_auth"]

EXPOSE 8788

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD bun run -e "fetch('http://localhost:8788/').then(r => r.ok ? process.exit(0) : process.exit(1)).catch(() => process.exit(1))"

CMD ["/app/start.sh"] 