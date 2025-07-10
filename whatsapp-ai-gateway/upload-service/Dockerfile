FROM oven/bun:1.2.17

# Alat untuk ekstraksi teks dari berbagai format file
RUN apt-get update && apt-get install -y \
    poppler-utils \
    pandoc \
    tesseract-ocr \
    tesseract-ocr-ind \
    tesseract-ocr-eng \
    antiword \
    gnumeric \
    libreoffice \
    unzip \
    ghostscript \
    imagemagick \
    locales \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set locale untuk mendukung karakter Unicode
RUN sed -i -e 's/# id_ID.UTF-8 UTF-8/id_ID.UTF-8 UTF-8/' /etc/locale.gen && \
    sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG id_ID.UTF-8
ENV LANGUAGE id_ID:id
ENV LC_ALL id_ID.UTF-8

WORKDIR /app
COPY . .
RUN bun install

# Volume untuk menyimpan data materi
VOLUME ["/app/data"]

EXPOSE 8789
CMD ["bun", "run", "src/index.ts"] 