import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { Client, LocalAuth, type Message } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';
import { existsSync, readFileSync, readdirSync, mkdirSync } from 'fs';

// Knowledge Base
export const KB: Record<string, string> = {};
// Variabel untuk menyimpan gabungan semua materi
export let COMBINED_KB: string = '';

// Fungsi sederhana untuk mencari teks yang relevan
function findRelevantContent(query: string, content: string, maxResults: number = 3): string[] {
  // Bagi konten menjadi paragraf
  const paragraphs = content.split(/\n\n+/);
  
  // Buat skor untuk setiap paragraf berdasarkan kecocokan kata kunci
  const keywords = query.toLowerCase().split(/\s+/).filter(word => word.length > 3);
  
  // Jika tidak ada kata kunci yang cukup panjang, gunakan semua kata
  const effectiveKeywords = keywords.length > 0 ? keywords : query.toLowerCase().split(/\s+/);
  
  // Hitung skor untuk setiap paragraf
  const scoredParagraphs = paragraphs.map(paragraph => {
    const paragraphLower = paragraph.toLowerCase();
    let score = 0;
    
    // Hitung kemunculan kata kunci
    for (const keyword of effectiveKeywords) {
      // Berikan skor lebih tinggi untuk kecocokan kata lengkap
      const exactMatches = (paragraphLower.match(new RegExp(`\\b${keyword}\\b`, 'g')) || []).length;
      const partialMatches = (paragraphLower.match(new RegExp(keyword, 'g')) || []).length - exactMatches;
      
      score += exactMatches * 10 + partialMatches * 3;
    }
    
    return { paragraph, score };
  });
  
  // Urutkan paragraf berdasarkan skor dan ambil yang tertinggi
  const relevantParagraphs = scoredParagraphs
    .filter(item => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, maxResults)
    .map(item => item.paragraph);
  
  return relevantParagraphs;
}

// Fungsi untuk memuat knowledge base
export async function loadKnowledgeBase() {
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
        
        for (const file of files) {
            try {
                const key = file.replace('.txt', '');
                const content = readFileSync(`data/text/${file}`, 'utf8');
                KB[key] = content;
                console.log(`✅ Berhasil memuat: ${file}`);
            } catch (err) {
                console.error(`❌ Gagal memuat file ${file}:`, err);
            }
        }
        
        // Gabungkan semua materi menjadi satu
        const allMaterials = Object.keys(KB)
            .filter(k => k !== '_last_loaded')
            .map(k => `=== MATERI: ${k} ===\n${KB[k]}`);
        
        if (allMaterials.length > 0) {
            COMBINED_KB = allMaterials.join('\n\n=== BATAS MATERI ===\n\n');
            console.log(`📚 Berhasil menggabungkan ${allMaterials.length} materi (${COMBINED_KB.length} karakter)`);
        }
        
        console.log(`📚 Total ${Object.keys(KB).length - 1} materi dimuat ke knowledge base`);
    } catch (err) {
        console.error('❌ Gagal membaca direktori data/text:', err);
    }
}

// Inisialisasi knowledge base
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

// Fungsi untuk mengirim pesan AI dengan konteks cerdas
async function sendAIResponse(msg: Message, prompt: string, context?: string, useKnowledgeBaseOnly: boolean = false) {
  const contact = await msg.getContact();
  const chat = await msg.getChat();

  try {
    let finalPrompt = '';
    let hasRelevantContent = false;
    
    // Cari konten yang relevan dari knowledge base
    const contentToSearch = context || COMBINED_KB;
    
    if (contentToSearch && contentToSearch.length > 0) {
      const relevantParagraphs = findRelevantContent(prompt, contentToSearch, 5);
      
      if (relevantParagraphs.length > 0) {
        hasRelevantContent = true;
        const relevantContent = relevantParagraphs.join('\n\n');
        
        // Batasi konteks untuk menghindari token yang terlalu banyak
        const trimmedContext = relevantContent.slice(0, 3000);

        finalPrompt = `Berikut materi yang relevan dengan pertanyaan:
${trimmedContext}

Pertanyaan:
${prompt}

${useKnowledgeBaseOnly ? 
  "Catatan: Jawab hanya berdasarkan informasi dari materi yang diberikan. Jika informasi tidak ada dalam materi, katakan dengan jujur bahwa informasi tersebut tidak tersedia dalam materi." : 
  "Catatan: Jawab berdasarkan informasi dari materi yang diberikan jika relevan. Jika informasi tidak ada dalam materi atau tidak lengkap, Anda dapat menggunakan pengetahuan umum Anda untuk memberikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban."}`;
      }
    }
    
    // Jika tidak ada konten yang relevan, gunakan metode lama atau pengetahuan umum
    if (!hasRelevantContent) {
      if (contentToSearch && contentToSearch.length > 0) {
        // Ada konten di knowledge base, tapi tidak ada yang relevan
        // Ambil bagian awal saja untuk konteks
        const trimmedContext = contentToSearch.slice(0, 3000);

        finalPrompt = `Berikut materi:
${trimmedContext}

Pertanyaan:
${prompt}

${useKnowledgeBaseOnly ? 
  "Catatan: Jawab hanya berdasarkan informasi dari materi yang diberikan. Jika informasi tidak ada dalam materi, katakan dengan jujur bahwa informasi tersebut tidak tersedia dalam materi." : 
  "Catatan: Jawab berdasarkan informasi dari materi yang diberikan jika relevan. Jika informasi tidak ada dalam materi atau tidak lengkap, Anda dapat menggunakan pengetahuan umum Anda untuk memberikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban."}`;
      } else {
        // Tidak ada konten di knowledge base, gunakan pengetahuan umum
        finalPrompt = `Pertanyaan:
${prompt}

Catatan: Jawab pertanyaan ini dengan pengetahuan umum Anda. Berikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban.`;
      }
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
      await chat.sendMessage(`${mentionText}\n${reply}`, { mentions: [contact] });
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

  if (text === '/materi') {
    const list = Object.keys(KB).filter(k => k !== '_last_loaded').map(k => `- ${k}`).join('\n');
    if (list.length > 0) {
      await msg.reply(`📚 Materi:
${list}`);
    } else {
      await msg.reply(`📚 Belum ada materi yang tersedia. Silakan upload materi terlebih dahulu.`);
    }
    return;
  }

  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📚 Panduan Penggunaan Bot:

/materi - Melihat daftar materi yang tersedia
/pilih [nama_materi] - Memilih materi spesifik (opsional)
/ai [pertanyaan] - Bertanya dengan semua materi secara otomatis
/tanya [pertanyaan] - Sama dengan /ai, bertanya dengan semua materi
/kb [pertanyaan] - Bertanya hanya menggunakan knowledge base (tidak menggunakan pengetahuan umum)
/bantuan - Menampilkan panduan ini`);
    return;
  }

  if (text.startsWith('/pilih ')) {
    const pilihan = text.replace('/pilih ', '').trim();
    if (KB[pilihan]) {
      // Dapatkan ID chat yang dapat digunakan sebagai kunci
      const chatId = typeof msg.from === 'string' ? msg.from : (msg.from ? msg.from.toString() : '');
      selectedContext[chatId] = pilihan;
      await msg.reply(`✅ Materi aktif: ${pilihan}`);
    } else {
      await msg.reply('❌ Materi tidak ditemukan.');
    }
    return;
  }

  // Perintah /tanya untuk langsung menggunakan semua materi
  if (text.startsWith('/tanya ')) {
    const prompt = text.replace('/tanya ', '').trim();
    await sendAIResponse(msg, prompt);
    return;
  }

  // Perintah /ai menggunakan semua materi secara otomatis
  if (text.startsWith('/ai ')) {
    const prompt = text.replace('/ai ', '').trim();
    
    // Dapatkan materi yang dipilih atau gunakan semua materi secara otomatis
    const chatId = typeof msg.from === 'string' ? msg.from : (msg.from ? msg.from.toString() : '');
    const contextKey = selectedContext[chatId];
    
    if (contextKey && KB[contextKey]) {
      // Jika ada materi yang dipilih, gunakan materi tersebut
      await sendAIResponse(msg, prompt, KB[contextKey]);
    } else {
      // Jika tidak ada materi yang dipilih, gunakan semua materi
      await sendAIResponse(msg, prompt);
    }
    return;
  }
  
  // Perintah /kb untuk bertanya hanya menggunakan knowledge base
  if (text.startsWith('/kb ')) {
    const prompt = text.replace('/kb ', '').trim();
    
    // Dapatkan materi yang dipilih atau gunakan semua materi secara otomatis
    const chatId = typeof msg.from === 'string' ? msg.from : (msg.from ? msg.from.toString() : '');
    const contextKey = selectedContext[chatId];
    
    if (contextKey && KB[contextKey]) {
      // Jika ada materi yang dipilih, gunakan materi tersebut
      await sendAIResponse(msg, prompt, KB[contextKey], true);
    } else {
      // Jika tidak ada materi yang dipilih, gunakan semua materi
      await sendAIResponse(msg, prompt, undefined, true);
    }
    return;
  }
  
  // Tambahkan fitur untuk menerima pesan langsung tanpa awalan
  // Ini akan mempermudah pengguna untuk bertanya tanpa perlu mengetik /ai atau /tanya
  if (!text.startsWith('/')) {
    await sendAIResponse(msg, text);
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

// API untuk reload knowledge base
app.post('/reload-kb', async (c) => {
  try {
    await loadKnowledgeBase();
    return c.json({ status: true, message: '✅ Knowledge base berhasil dimuat ulang.' });
  } catch (err: any) {
    return c.json({ status: false, error: err.message }, 500);
  }
});

// Status API
app.get('/', (c) => c.json({ status: 'ok', lastLoaded: KB._last_loaded }));

serve(app);
console.log('✅ WhatsApp service berjalan di port 8788'); 