FROM oven/bun:1.2.17

# Alat untuk ekstraksi teks dari berbagai format file
RUN apt-get update && apt-get install -y \
    poppler-utils \
    pandoc \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY . .
RUN bun install

# Volume untuk menyimpan data materi
VOLUME ["/app/data"]

EXPOSE 8789
CMD ["bun", "run", "src/index.ts"] 