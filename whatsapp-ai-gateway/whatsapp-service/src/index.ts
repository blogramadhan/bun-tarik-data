import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { Client, LocalAuth, type Message } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';
import { readFileSync, readdirSync, existsSync } from 'fs';
import path from 'path';

// Variabel untuk menyimpan knowledgebase
let knowledgeBase: string[] = [];

// Fungsi untuk memuat knowledgebase dari file
function loadKnowledgeBase() {
  try {
    const textDir = path.join(process.cwd(), '..', 'upload-service', 'data', 'text');
    console.log(`📚 Mencoba memuat knowledgebase dari: ${textDir}`);
    
    if (existsSync(textDir)) {
      const files = readdirSync(textDir);
      console.log(`📚 Ditemukan ${files.length} file knowledgebase`);
      
      knowledgeBase = files
        .filter(file => file.endsWith('.txt'))
        .map(file => {
          const filePath = path.join(textDir, file);
          try {
            const content = readFileSync(filePath, 'utf8');
            console.log(`✅ Berhasil memuat: ${file}`);
            return content;
          } catch (err) {
            console.error(`❌ Gagal memuat file ${file}:`, err);
            return '';
          }
        })
        .filter(content => content.length > 0);
      
      console.log(`📚 Total ${knowledgeBase.length} dokumen dimuat ke dalam knowledgebase`);
    } else {
      console.log(`⚠️ Direktori knowledgebase tidak ditemukan: ${textDir}`);
    }
  } catch (err) {
    console.error('❌ Error saat memuat knowledgebase:', err);
  }
}

// WhatsApp Client
export const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] },
});

// API untuk mengirim pesan
const app = new Hono();

app.post('/send-message', async (c) => {
  try {
    const body = await c.req.json();
    console.log('📩 Menerima permintaan send-message:', body);
    const { number, message } = body;
    
    if (!number || !message) {
      console.log('❌ Error: number dan message wajib');
      return c.json({ status: false, error: 'number dan message wajib' }, 400);
    }
    
    console.log(`📤 Mencoba mengirim pesan ke ${number}...`);
    await client.sendMessage(`${number}@c.us`, message);
    console.log('✅ Pesan terkirim.');
    return c.json({ status: true, message: '✅ Pesan terkirim.' });
  } catch (err: any) {
    console.error('❌ Error saat mengirim pesan:', err);
    return c.json({ status: false, error: err.message }, 500);
  }
});

// Endpoint untuk memuat ulang knowledgebase
app.post('/reload-kb', async (c) => {
  try {
    console.log('🔄 Memuat ulang knowledgebase...');
    loadKnowledgeBase();
    return c.json({ 
      status: true, 
      message: '✅ Knowledgebase berhasil dimuat ulang',
      count: knowledgeBase.length
    });
  } catch (err: any) {
    console.error('❌ Error saat memuat ulang knowledgebase:', err);
    return c.json({ status: false, error: err.message }, 500);
  }
});

// Status API
app.get('/', (c) => {
  console.log('📊 Menerima permintaan status');
  return c.json({ 
    status: 'ok', 
    whatsapp: client.info ? 'connected' : 'initializing',
    knowledgebase: {
      loaded: knowledgeBase.length > 0,
      documentCount: knowledgeBase.length
    }
  });
});

// Mulai server HTTP terlebih dahulu
console.log('🚀 Memulai server HTTP di port 8788...');
serve({
  fetch: app.fetch,
  port: 8788,
});
console.log('✅ Server HTTP berjalan di port 8788');

// Kemudian inisialisasi WhatsApp client
client.on('qr', (qr) => {
  console.log('📱 Scan QR code untuk login WhatsApp:');
  qrcode.generate(qr, { small: true });
});

client.on('ready', () => {
  console.log('✅ WhatsApp client siap!');
  console.log('🤖 Bot siap menerima pesan dan permintaan API');
  
  // Muat knowledgebase saat client siap
  loadKnowledgeBase();
});

// Fungsi untuk mencari informasi di knowledgebase
async function searchKnowledgeBase(query: string): Promise<string | null> {
  if (knowledgeBase.length === 0) {
    return null;
  }
  
  console.log(`🔍 Mencari di knowledgebase untuk: "${query}"`);
  
  // Cari di semua dokumen
  const relevantPassages: {text: string, score: number}[] = [];
  
  // Buat array kata kunci dari query
  const keywords = query.toLowerCase()
    .replace(/[.,?!;:(){}[\]"']/g, '')
    .split(/\s+/)
    .filter(word => word.length > 2); // Hanya kata dengan panjang > 2
  
  if (keywords.length === 0) {
    return null;
  }
  
  // Cari di setiap dokumen
  for (const doc of knowledgeBase) {
    // Bagi dokumen menjadi paragraf
    const paragraphs = doc.split(/\n\s*\n/);
    
    for (const paragraph of paragraphs) {
      if (paragraph.length < 20) continue; // Skip paragraf pendek
      
      // Hitung skor relevansi berdasarkan jumlah kata kunci yang cocok
      let score = 0;
      const paragraphLower = paragraph.toLowerCase();
      
      for (const keyword of keywords) {
        // Tambahkan skor berdasarkan jumlah kemunculan kata kunci
        const regex = new RegExp(keyword, 'gi');
        const matches = paragraphLower.match(regex);
        if (matches) {
          score += matches.length;
        }
      }
      
      // Jika skor cukup tinggi, tambahkan ke hasil
      if (score > 0) {
        relevantPassages.push({
          text: paragraph,
          score
        });
      }
    }
  }
  
  // Urutkan berdasarkan skor tertinggi
  relevantPassages.sort((a, b) => b.score - a.score);
  
  // Jika tidak ada hasil yang relevan
  if (relevantPassages.length === 0) {
    console.log('❌ Tidak ditemukan informasi yang relevan di knowledgebase');
    return null;
  }
  
  // Ambil hingga 3 paragraf paling relevan
  const topPassages = relevantPassages.slice(0, 3);
  console.log(`✅ Ditemukan ${topPassages.length} paragraf relevan`);
  
  // Gabungkan hasil
  return topPassages.map(p => p.text).join('\n\n');
}

// Fungsi untuk mengirim pesan AI
async function sendAIResponse(msg: Message, prompt: string) {
  const contact = await msg.getContact();
  const chat = await msg.getChat();

  try {
    // Cari di knowledgebase terlebih dahulu
    const knowledgeResult = await searchKnowledgeBase(prompt);
    
    let finalPrompt: string;
    
    if (knowledgeResult) {
      // Jika ada hasil dari knowledgebase, gunakan sebagai konteks tambahan
      finalPrompt = `Pertanyaan:
${prompt}

Informasi dari knowledgebase:
${knowledgeResult}

Catatan: Jawab pertanyaan berdasarkan informasi dari knowledgebase di atas. Jika informasi di knowledgebase tidak mencukupi, gunakan pengetahuan umum Anda. Berikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban.`;
    } else {
      // Jika tidak ada hasil dari knowledgebase, gunakan pengetahuan umum
      finalPrompt = `Pertanyaan:
${prompt}

Catatan: Jawab pertanyaan ini dengan pengetahuan umum Anda. Berikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban.`;
    }

    let completeResponse = '';
    let hasMore = true;
    let nextToken = null;

    while (hasMore) {
      const res = await fetch('https://api.deepinfra.com/v1/openai/chat/completions', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${process.env.DEEPINFRA_API_KEY}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          model: process.env.DEEPINFRA_MODEL || 'deepseek-ai/DeepSeek-R1',
          messages: [{ role: 'user', content: finalPrompt }],
          max_tokens: 1000,
          next_token: nextToken,
        })
      });

      const json: any = await res.json();
      const replyPart = json?.choices?.[0]?.message?.content || '';
      completeResponse += replyPart;
      hasMore = json?.choices?.[0]?.has_more || false;
      nextToken = json?.choices?.[0]?.next_token || null;
    }

    // Hapus konten dalam tag <think></think>
    completeResponse = completeResponse.replace(/<think>[\s\S]*?<\/think>/g, '');

    // Hapus spasi di awal respons
    completeResponse = completeResponse.trimStart();
    
    const reply = completeResponse || '❌ Gagal menjawab.';

    if (chat.isGroup) {
      const mentionText = `@${contact.id.user}`;
      // Tambahkan anotasi tipe untuk contact
      await chat.sendMessage(`${mentionText}\n${reply}`, { mentions: [contact as any] });
    } else {
      await msg.reply(reply);
    }
  } catch (error) {
    console.error('Error in AI response:', error);
    await msg.reply('❌ Terjadi kesalahan saat memproses pertanyaan Anda.');
  }
}

client.on('message_create', async (msg: Message) => {
  if (msg.fromMe) return;
  const text = msg.body?.trim();
  if (!text) return;

  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📚 Panduan Penggunaan Bot:

/ai [pertanyaan] - Bertanya ke AI
/tanya [pertanyaan] - Sama dengan /ai
/kb [pertanyaan] - Mencari di knowledgebase
/bantuan - Menampilkan panduan ini`);
    return;
  }

  // Perintah /kb untuk mencari khusus di knowledgebase
  if (text.startsWith('/kb ')) {
    const prompt = text.replace('/kb ', '').trim();
    const knowledgeResult = await searchKnowledgeBase(prompt);
    
    if (knowledgeResult) {
      await msg.reply(`📚 Hasil pencarian di knowledgebase:\n\n${knowledgeResult}`);
    } else {
      await msg.reply('❌ Tidak ditemukan informasi yang relevan di knowledgebase.');
    }
    return;
  }
  
  // Perintah /tanya 
  if (text.startsWith('/tanya ')) {
    const prompt = text.replace('/tanya ', '').trim();
    await sendAIResponse(msg, prompt);
    return;
  }

  // Perintah /ai
  if (text.startsWith('/ai ')) {
    const prompt = text.replace('/ai ', '').trim();
    await sendAIResponse(msg, prompt);
    return;
  }
  
  // Tambahkan fitur untuk menerima pesan langsung tanpa awalan
  // Ini akan mempermudah pengguna untuk bertanya tanpa perlu mengetik /ai atau /tanya
  if (!text.startsWith('/')) {
    await sendAIResponse(msg, text);
    return;
  }
});

// Inisialisasi WhatsApp client terakhir
console.log('📱 Menginisialisasi WhatsApp client...');
client.initialize(); 