import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { Client, LocalAuth, type Message } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';
import { existsSync, readFileSync, readdirSync, mkdirSync } from 'fs';

// Knowledge Base
export const KB: Record<string, string> = {};
export function loadKnowledgeBase() {
    console.log('🔄 Memuat knowledge base...');
    KB._last_loaded = new Date().toISOString();
    
    if (!existsSync('data/text')) {
        console.log('❌ Direktori data/text tidak ditemukan. Membuat direktori...');
        try {
            mkdirSync('data/text', { recursive: true });
            console.log('✅ Direktori data/text berhasil dibuat.');
        } catch (err) {
            console.error('❌ Gagal membuat direktori data/text:', err);
        }
        return;
    }
    
    try {
        const files = readdirSync('data/text');
        console.log(`📚 Menemukan ${files.length} file di data/text`);
        
        files.forEach(file => {
            try {
                const key = file.replace('.txt', '');
                KB[key] = readFileSync(`data/text/${file}`, 'utf8');
                console.log(`✅ Berhasil memuat: ${file}`);
            } catch (err) {
                console.error(`❌ Gagal memuat file ${file}:`, err);
            }
        });
        
        console.log(`📚 Total ${Object.keys(KB).length - 1} materi dimuat ke knowledge base`);
    } catch (err) {
        console.error('❌ Gagal membaca direktori data/text:', err);
    }
}

loadKnowledgeBase();

// WhatsApp Client
export const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] },
});

client.on('qr', (qr) => qrcode.generate(qr, { small: true }));
client.on('ready', () => console.log('✅ WhatsApp client siap!'));
client.initialize();

const selectedContext: Record<string, string> = {};

// Fungsi untuk mengirim pesan AI
async function sendAIResponse(msg: Message, prompt: string, context: string) {
  const contact = await msg.getContact();
  const chat = await msg.getChat();

  const finalPrompt = context ? `Berikut materi:
${context.slice(0, 3000)}

Pertanyaan:
${prompt}

Catatan: Jawab langsung tanpa menampilkan <think></think> di jawaban.` : `${prompt}

Catatan: Jawab langsung tanpa menampilkan <think></think> di jawaban.`;

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
    await chat.sendMessage(`@${contact.id.user}\n${reply}`, { mentions: [contact] });
  } else {
    await msg.reply(reply);
  }
}

client.on('message_create', async (msg: Message) => {
  if (msg.fromMe) return;
  const text = msg.body?.trim();
  if (!text) return;

  if (text === '/materi') {
    const list = Object.keys(KB).filter(k => k !== '_last_loaded').map(k => `- ${k}`).join('\n');
    await msg.reply(`📚 Materi:
${list}`);
    return;
  }

  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📚 Panduan Penggunaan Bot:

/materi - Melihat daftar materi yang tersedia
/pilih [nama_materi] - Memilih materi spesifik
/ai [pertanyaan] - Bertanya dengan materi yang dipilih atau semua materi
/tanya [pertanyaan] - Bertanya langsung dengan semua materi
/bantuan - Menampilkan panduan ini`);
    return;
  }

  if (text.startsWith('/pilih ')) {
    const pilihan = text.replace('/pilih ', '').trim();
    if (KB[pilihan]) {
      selectedContext[msg.from] = pilihan;
      await msg.reply(`✅ Materi aktif: ${pilihan}`);
    } else {
      await msg.reply('❌ Materi tidak ditemukan.');
    }
    return;
  }

  // Perintah /tanya untuk langsung menggunakan semua materi
  if (text.startsWith('/tanya ')) {
    const prompt = text.replace('/tanya ', '').trim();
    
    // Gabungkan semua materi
    const allMaterials = Object.keys(KB)
      .filter(k => k !== '_last_loaded')
      .map(k => KB[k]);
    
    let context = '';
    if (allMaterials.length > 0) {
      context = allMaterials.join('\n\n=== BATAS MATERI ===\n\n');
    }

    await sendAIResponse(msg, prompt, context);
    return;
  }

  // Perintah /ai tanpa perlu memilih materi
  if (text.startsWith('/ai ')) {
    const prompt = text.replace('/ai ', '').trim();
    
    // Dapatkan materi yang dipilih atau gunakan semua materi jika tidak ada yang dipilih
    const contextKey = selectedContext[msg.from];
    let context = '';
    
    if (contextKey && KB[contextKey]) {
      // Jika ada materi yang dipilih, gunakan materi tersebut
      context = KB[contextKey];
    } else {
      // Jika tidak ada materi yang dipilih, gabungkan semua materi
      const allMaterials = Object.keys(KB)
        .filter(k => k !== '_last_loaded')
        .map(k => KB[k]);
      
      if (allMaterials.length > 0) {
        context = allMaterials.join('\n\n=== BATAS MATERI ===\n\n');
      }
    }

    await sendAIResponse(msg, prompt, context);
    return;
  }
});

// API untuk mengirim pesan
const app = new Hono();

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

// Endpoint untuk reload knowledge base
app.post('/reload-kb', (c) => {
  loadKnowledgeBase();
  console.log('📚 Knowledge Base dimuat ulang:', KB._last_loaded);
  return c.json({ status: true, message: '✅ Knowledge Base dimuat ulang.' });
});

// Server
serve({ fetch: app.fetch, port: 8788 });
console.log('📱 WhatsApp Gateway aktif di http://localhost:8788'); 