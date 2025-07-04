import { Hono } from 'hono';
import formidable from 'formidable';
import { writeFileSync, mkdirSync, readdirSync, readFileSync } from 'fs';
import { extractTextFromFile } from './utils/extractor';
import { loadKnowledgeBase, KB, client } from './whatsapp';

const app = new Hono();

app.get('/', (c) => {
  const list = Object.keys(KB).filter(k => k !== '_last_loaded')
    .map(k => `<li>${k}</li>`).join('');

  return c.html(`
    <h1>Upload Materi Pembelajaran</h1>
    <form method="post" enctype="multipart/form-data" action="/upload">
      <input type="file" name="file" />
      <button type="submit">Upload</button>
    </form>
    <h2>Materi Tersedia:</h2>
    <ul>${list}</ul>
    <small>Last reload: ${KB._last_loaded}</small>
  `);
});

app.post('/upload', async (c) => {
  const form = formidable({ uploadDir: '/tmp', keepExtensions: true });

  const body = await new Promise((resolve, reject) => {
    form.parse(c.req.raw, (err, fields, files) => {
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

  loadKnowledgeBase();
  return c.redirect('/');
});

app.post('/send-message', async (c) => {
  const { number, message } = await c.req.json();
  if (!number || !message) {
    return c.json({ status: false, error: 'number dan message wajib' }, 400);
  }
  try {
    await client.sendMessage(`${number}@c.us`, message);
    return c.json({ status: true, message: '✅ Pesan terkirim.' });
  } catch (err: any) {
    return c.json({ status: false, error: err.message }, 500);
  }
});

export default app;