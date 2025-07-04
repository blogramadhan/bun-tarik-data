FROM oven/bun:1.2.17

# Install library untuk Puppeteer (Chromium dependencies)
RUN apt-get update && apt-get install -y \
  fonts-liberation \
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
  xdg-utils \
  libgbm-dev \
  libgconf-2-4 \
  libgobject-2.0-0 \
  libgtk-3-0 \
  libpango-1.0-0 \
  libxss1 \
  lsb-release \
  wget \
  curl \
  gnupg \
  unzip \
  poppler-utils \
  --no-install-recommends \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN bun install

EXPOSE 8787
CMD ["bun", "run", "src/index.ts"]