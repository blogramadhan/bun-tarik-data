import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import formidable from 'formidable';
import { writeFileSync, mkdirSync, readdirSync, readFileSync } from 'fs';
import axios from 'axios';
import { extractTextFromFile } from './utils/extractor';

// Konfigurasi URL layanan WhatsApp
const WHATSAPP_SERVICE_URL = process.env.WHATSAPP_SERVICE_URL || 'http://whatsapp-service:8788';

// Aplikasi Hono
const app = new Hono();

app.get('/', (c) => {
  // Ambil daftar materi dari folder data/text
  let list = '';
  try {
    if (readdirSync('data/text').length > 0) {
      list = readdirSync('data/text')
        .map(file => `<li>${file.replace('.txt', '')}</li>`)
        .join('');
    }
  } catch (e) {
    list = '<li>Belum ada materi</li>';
  }

  return c.html(`
    <h1>Upload Materi Pembelajaran</h1>
    <form method="post" enctype="multipart/form-data" action="/upload">
      <input type="file" name="file" />
      <button type="submit">Upload</button>
    </form>
    <h2>Materi Tersedia:</h2>
    <ul>${list}</ul>
  `);
});

app.post('/upload', async (c) => {
  const form = formidable({ uploadDir: '/tmp', keepExtensions: true });

  const body = await new Promise((resolve, reject) => {
    form.parse(c.req.raw, (err: any, fields: any, files: any) => {
      if (err) reject(err);
      else resolve({ fields, files });
    });
  });

  const file = (body as any).files.file;
  const name = file.originalFilename || 'materi';
  const ext = name.split('.').pop();
  const base = name.replace(/\.[^/.]+$/, '');
  const rawPath = `data/raw/${base}.${ext}`;
  const txtPath = `data/text/${base}.txt`;

  mkdirSync('data/raw', { recursive: true });
  mkdirSync('data/text', { recursive: true });
  writeFileSync(rawPath, readFileSync(file.filepath));

  const teks = await extractTextFromFile(rawPath);
  writeFileSync(txtPath, teks);

  // Notifikasi layanan WhatsApp untuk memuat ulang knowledge base
  try {
    await axios.post(`${WHATSAPP_SERVICE_URL}/reload-kb`);
    console.log(`✅ Notifikasi reload KB berhasil dikirim ke ${WHATSAPP_SERVICE_URL}`);
  } catch (err) {
    console.error(`❌ Gagal mengirim notifikasi reload KB ke ${WHATSAPP_SERVICE_URL}:`, err);
  }

  return c.redirect('/');
});

// Server
serve({ fetch: app.fetch, port: 8789 });
console.log('📚 Upload Service aktif di http://localhost:8789'); 