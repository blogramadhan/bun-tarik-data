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
  let totalFiles = 0;
  try {
    const files = readdirSync('data/text');
    totalFiles = files.length;
    if (files.length > 0) {
      list = files
        .map(file => `
          <div class="material-item">
            <div class="material-icon">📄</div>
            <div class="material-name">${file.replace('.txt', '')}</div>
            <div class="material-status">✅ Siap</div>
          </div>
        `)
        .join('');
    }
  } catch (e) {
    list = '<div class="no-materials">📂 Belum ada materi yang diupload</div>';
  }

  return c.html(`
    <!DOCTYPE html>
    <html lang="id">
    <head>
      <meta charset="UTF-8">
      <meta name="viewport" content="width=device-width, initial-scale=1.0">
      <title>WhatsApp AI Gateway - Upload Materi</title>
      <style>
        * {
          margin: 0;
          padding: 0;
          box-sizing: border-box;
        }

        body {
          font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          min-height: 100vh;
          display: flex;
          align-items: center;
          justify-content: center;
          padding: 20px;
        }

        .container {
          background: white;
          border-radius: 20px;
          box-shadow: 0 30px 60px rgba(0, 0, 0, 0.1);
          padding: 40px;
          max-width: 800px;
          width: 100%;
          position: relative;
          overflow: hidden;
        }

        .container::before {
          content: '';
          position: absolute;
          top: 0;
          left: 0;
          right: 0;
          height: 5px;
          background: linear-gradient(90deg, #25D366, #128C7E, #075E54);
        }

        .header {
          text-align: center;
          margin-bottom: 40px;
        }

        .title {
          font-size: 32px;
          font-weight: 700;
          color: #2c3e50;
          margin-bottom: 10px;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 15px;
        }

        .subtitle {
          color: #7f8c8d;
          font-size: 16px;
          font-weight: 400;
        }

        .upload-section {
          background: #f8f9ff;
          border-radius: 15px;
          padding: 30px;
          margin-bottom: 40px;
          border: 2px dashed #e1e8ff;
          transition: all 0.3s ease;
          position: relative;
        }

        .upload-section:hover {
          border-color: #667eea;
          background: #f0f3ff;
          transform: translateY(-2px);
        }

        .upload-form {
          display: flex;
          flex-direction: column;
          align-items: center;
          gap: 20px;
        }

        .file-input-container {
          position: relative;
          overflow: hidden;
          display: inline-block;
          cursor: pointer;
          width: 100%;
          max-width: 400px;
        }

        .file-input {
          position: absolute;
          left: -9999px;
          opacity: 0;
        }

        .file-input-label {
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 12px;
          padding: 20px 30px;
          background: linear-gradient(135deg, #667eea, #764ba2);
          color: white;
          border-radius: 12px;
          cursor: pointer;
          transition: all 0.3s ease;
          font-weight: 600;
          font-size: 16px;
          border: none;
          width: 100%;
        }

        .file-input-label:hover {
          transform: translateY(-2px);
          box-shadow: 0 10px 25px rgba(102, 126, 234, 0.3);
        }

        .upload-btn {
          background: linear-gradient(135deg, #25D366, #128C7E);
          color: white;
          border: none;
          padding: 15px 40px;
          border-radius: 12px;
          font-size: 16px;
          font-weight: 600;
          cursor: pointer;
          transition: all 0.3s ease;
          display: flex;
          align-items: center;
          gap: 10px;
        }

        .upload-btn:hover {
          transform: translateY(-2px);
          box-shadow: 0 10px 25px rgba(37, 211, 102, 0.3);
        }

        .upload-btn:disabled {
          background: #95a5a6;
          cursor: not-allowed;
          transform: none;
          box-shadow: none;
        }

        .materials-section {
          margin-top: 40px;
        }

        .section-header {
          display: flex;
          align-items: center;
          justify-content: space-between;
          margin-bottom: 25px;
        }

        .section-title {
          font-size: 24px;
          font-weight: 600;
          color: #2c3e50;
          display: flex;
          align-items: center;
          gap: 10px;
        }

        .materials-count {
          background: linear-gradient(135deg, #667eea, #764ba2);
          color: white;
          padding: 8px 16px;
          border-radius: 20px;
          font-size: 14px;
          font-weight: 600;
        }

        .materials-grid {
          display: grid;
          grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
          gap: 20px;
        }

        .material-item {
          background: white;
          border: 1px solid #e8ecf3;
          border-radius: 12px;
          padding: 20px;
          display: flex;
          align-items: center;
          gap: 15px;
          transition: all 0.3s ease;
          box-shadow: 0 2px 8px rgba(0, 0, 0, 0.05);
        }

        .material-item:hover {
          transform: translateY(-3px);
          box-shadow: 0 8px 25px rgba(0, 0, 0, 0.1);
          border-color: #667eea;
        }

        .material-icon {
          font-size: 24px;
          width: 50px;
          height: 50px;
          background: linear-gradient(135deg, #667eea, #764ba2);
          border-radius: 12px;
          display: flex;
          align-items: center;
          justify-content: center;
          color: white;
          flex-shrink: 0;
        }

        .material-name {
          flex: 1;
          font-weight: 600;
          color: #2c3e50;
          font-size: 16px;
        }

        .material-status {
          color: #27ae60;
          font-size: 14px;
          font-weight: 600;
        }

        .no-materials {
          text-align: center;
          padding: 60px 20px;
          color: #7f8c8d;
          font-size: 18px;
          display: flex;
          align-items: center;
          justify-content: center;
          gap: 10px;
          background: #f8f9fa;
          border-radius: 12px;
        }

        .supported-formats {
          margin-top: 20px;
          text-align: center;
          color: #7f8c8d;
          font-size: 14px;
        }

        .format-tags {
          display: flex;
          justify-content: center;
          gap: 8px;
          margin-top: 10px;
          flex-wrap: wrap;
        }

        .format-tag {
          background: #e8ecf3;
          color: #5a6c7d;
          padding: 4px 12px;
          border-radius: 20px;
          font-size: 12px;
          font-weight: 500;
        }

        .loading {
          display: none;
          text-align: center;
          color: #667eea;
          font-size: 16px;
          margin-top: 20px;
        }

        .loading.show {
          display: block;
        }

        @keyframes pulse {
          0%, 100% { opacity: 1; }
          50% { opacity: 0.5; }
        }

        .loading {
          animation: pulse 1.5s ease-in-out infinite;
        }

        @media (max-width: 768px) {
          .container {
            padding: 20px;
            margin: 10px;
          }

          .title {
            font-size: 24px;
          }

          .upload-section {
            padding: 20px;
          }

          .materials-grid {
            grid-template-columns: 1fr;
          }

          .section-header {
            flex-direction: column;
            align-items: flex-start;
            gap: 15px;
          }
        }
      </style>
    </head>
    <body>
      <div class="container">
        <div class="header">
          <h1 class="title">
            📚 Upload Materi Pembelajaran
          </h1>
          <p class="subtitle">Upload file materi untuk WhatsApp AI Gateway</p>
        </div>

        <div class="upload-section">
          <form method="post" enctype="multipart/form-data" action="/upload" class="upload-form" id="uploadForm">
            <div class="file-input-container">
              <input type="file" name="file" id="fileInput" class="file-input" accept=".pdf,.doc,.docx,.txt" required>
              <label for="fileInput" class="file-input-label">
                📁 Pilih File Materi
              </label>
            </div>
            
            <button type="submit" class="upload-btn" id="uploadBtn">
              ⬆️ Upload Materi
            </button>
            
            <div class="supported-formats">
              <p>Format yang didukung:</p>
              <div class="format-tags">
                <span class="format-tag">PDF</span>
                <span class="format-tag">DOC</span>
                <span class="format-tag">DOCX</span>
                <span class="format-tag">TXT</span>
              </div>
            </div>
          </form>
          
          <div class="loading" id="loading">
            📤 Sedang mengupload dan memproses file...
          </div>
        </div>

        <div class="materials-section">
          <div class="section-header">
            <h2 class="section-title">
              📋 Materi Tersedia
            </h2>
            <div class="materials-count">${totalFiles} Materi</div>
          </div>
          
          <div class="materials-grid">
            ${list}
          </div>
        </div>
      </div>

      <script>
        const form = document.getElementById('uploadForm');
        const fileInput = document.getElementById('fileInput');
        const uploadBtn = document.getElementById('uploadBtn');
        const loading = document.getElementById('loading');
        const fileLabel = document.querySelector('.file-input-label');

        fileInput.addEventListener('change', function() {
          if (this.files && this.files[0]) {
            fileLabel.innerHTML = '📄 ' + this.files[0].name;
            uploadBtn.disabled = false;
          } else {
            fileLabel.innerHTML = '📁 Pilih File Materi';
            uploadBtn.disabled = true;
          }
        });

        form.addEventListener('submit', function() {
          if (fileInput.files && fileInput.files[0]) {
            uploadBtn.disabled = true;
            uploadBtn.innerHTML = '⏳ Mengupload...';
            loading.classList.add('show');
          }
        });

        // Auto refresh setelah upload berhasil
        if (window.location.search.includes('uploaded=true')) {
          setTimeout(() => {
            window.location.href = '/';
          }, 2000);
        }
      </script>
    </body>
    </html>
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

  return c.redirect('/?uploaded=true');
});

// Server
serve({ fetch: app.fetch, port: 8789 });
console.log('📚 Upload Service aktif di http://localhost:8789'); 