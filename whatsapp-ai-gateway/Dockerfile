FROM oven/bun:1.2.17

RUN apt-get update && apt-get install -y poppler-utils

WORKDIR /app
COPY . .
RUN bun install

EXPOSE 8787
CMD ["bun", "run", "src/index.ts"]