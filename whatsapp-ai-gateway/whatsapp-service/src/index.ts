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
    // Sesuaikan path dengan volume Docker
    const textDir = path.join('/app/data/text');
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
      
      // Coba buat direktori jika tidak ada
      try {
        const dataDir = '/app/data';
        if (existsSync(dataDir)) {
          console.log(`📁 Direktori data ditemukan di: ${dataDir}`);
          console.log(`📁 Mencoba membuat direktori text...`);
          const fs = require('fs');
          fs.mkdirSync(textDir, { recursive: true });
          console.log(`✅ Direktori text berhasil dibuat: ${textDir}`);
        } else {
          console.log(`⚠️ Direktori data tidak ditemukan: ${dataDir}`);
        }
      } catch (err) {
        console.error(`❌ Gagal membuat direktori text:`, err);
      }
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
    // Pertama, gunakan pengetahuan umum untuk menjawab
    console.log(`🤖 Mencoba menjawab dengan pengetahuan umum: "${prompt}"`);
    
    const generalKnowledgePrompt = `Pertanyaan:
${prompt}

Catatan: Jawab pertanyaan ini dengan pengetahuan umum Anda. Berikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban.`;

    let completeResponse = '';
    let hasMore = true;
    let nextToken = null;
    let retries = 0;
    const maxRetries = 2;
    let success = false;

    // Daftar model fallback jika model utama gagal
    const fallbackModels = [
      'mistralai/Mistral-7B-Instruct-v0.2',
      'meta-llama/Llama-2-7b-chat-hf',
      'google/gemma-7b-it'
    ];

    while (!success && retries <= maxRetries) {
      try {
        // Pilih model: gunakan env var, atau fallback ke model alternatif jika gagal
        let currentModel = process.env.DEEPINFRA_MODEL;
        if (retries > 0) {
          currentModel = fallbackModels[retries - 1];
          console.log(`🔄 Mencoba dengan model fallback (percobaan ${retries}): ${currentModel}`);
        }

        completeResponse = '';
        hasMore = true;
        nextToken = null;

        while (hasMore) {
          console.log(`📤 Mengirim permintaan ke API dengan model: ${currentModel}`);
          const res = await fetch('https://api.deepinfra.com/v1/openai/chat/completions', {
            method: 'POST',
            headers: {
              Authorization: `Bearer ${process.env.DEEPINFRA_API_KEY}`,
              'Content-Type': 'application/json',
            },
            body: JSON.stringify({
              model: currentModel,
              messages: [{ role: 'user', content: generalKnowledgePrompt }],
              max_tokens: 1000,
              next_token: nextToken,
              temperature: 0.7
            })
          });

          if (!res.ok) {
            const errorText = await res.text();
            console.error(`❌ API error (${res.status}): ${errorText}`);
            throw new Error(`API error: ${res.status} ${errorText}`);
          }

          const json: any = await res.json();
          
          if (!json || !json.choices || json.choices.length === 0) {
            console.error('❌ Invalid API response:', json);
            throw new Error('Invalid API response');
          }
          
          const replyPart = json?.choices?.[0]?.message?.content || '';
          if (replyPart) {
            completeResponse += replyPart;
            hasMore = json?.choices?.[0]?.has_more || false;
            nextToken = json?.choices?.[0]?.next_token || null;
          } else {
            hasMore = false;
          }
        }

        // Jika kita sampai di sini tanpa error, tandai sebagai sukses
        success = true;
        
      } catch (err) {
        console.error(`❌ Error saat mengakses API (percobaan ${retries + 1}/${maxRetries + 1}):`, err);
        retries++;
        
        // Jika sudah mencoba semua model dan masih gagal
        if (retries > maxRetries) {
          console.error('❌ Semua model gagal, mencoba dengan knowledgebase...');
          // Lanjut ke pencarian knowledgebase
          break;
        }
        
        // Tunggu sebentar sebelum mencoba lagi
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }

    // Hapus konten dalam tag <think></think>
    completeResponse = completeResponse.replace(/<think>[\s\S]*?<\/think>/g, '');

    // Hapus spasi di awal respons
    completeResponse = completeResponse.trimStart();
    
    // Cek apakah jawaban kosong atau mengandung "tidak tahu"
    const unknownPhrases = [
      "tidak tahu", "tidak memiliki informasi", "tidak memiliki pengetahuan", 
      "tidak dapat memberikan", "tidak dapat menjawab", "tidak memiliki data",
      "tidak memiliki detail", "tidak memiliki konteks", "tidak diketahui",
      "tidak tersedia", "tidak ada informasi", "maaf, saya tidak"
    ];
    
    const containsUnknownPhrase = completeResponse.length < 10 || 
      unknownPhrases.some(phrase => completeResponse.toLowerCase().includes(phrase));
    
    // Jika AI tidak tahu jawabannya atau respons kosong, coba cari di knowledgebase
    if (!success || containsUnknownPhrase) {
      console.log(`🔍 AI tidak memiliki informasi atau gagal, mencoba mencari di knowledgebase...`);
      
      // Cari di knowledgebase
      const knowledgeResult = await searchKnowledgeBase(prompt);
      
      if (knowledgeResult) {
        console.log(`📚 Informasi ditemukan di knowledgebase, menjawab ulang...`);
        
        // Jika ada hasil dari knowledgebase, gunakan sebagai konteks tambahan
        const kbPrompt = `Pertanyaan:
${prompt}

Informasi dari knowledgebase:
${knowledgeResult}

Catatan: Jawab pertanyaan berdasarkan informasi dari knowledgebase di atas. Berikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban.`;

        // Reset variabel untuk permintaan baru
        completeResponse = '';
        hasMore = true;
        nextToken = null;
        retries = 0;
        success = false;

        while (!success && retries <= maxRetries) {
          try {
            // Pilih model: gunakan env var, atau fallback ke model alternatif jika gagal
            let currentModel = process.env.DEEPINFRA_MODEL;
            if (retries > 0) {
              currentModel = fallbackModels[retries - 1];
              console.log(`🔄 Mencoba dengan model fallback untuk KB (percobaan ${retries}): ${currentModel}`);
            }

            completeResponse = '';
            hasMore = true;
            nextToken = null;

            while (hasMore) {
              console.log(`📤 Mengirim permintaan KB ke API dengan model: ${currentModel}`);
              const res = await fetch('https://api.deepinfra.com/v1/openai/chat/completions', {
                method: 'POST',
                headers: {
                  Authorization: `Bearer ${process.env.DEEPINFRA_API_KEY}`,
                  'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                  model: currentModel,
                  messages: [{ role: 'user', content: kbPrompt }],
                  max_tokens: 1000,
                  next_token: nextToken,
                  temperature: 0.5
                })
              });

              if (!res.ok) {
                const errorText = await res.text();
                console.error(`❌ API KB error (${res.status}): ${errorText}`);
                throw new Error(`API error: ${res.status} ${errorText}`);
              }

              const json: any = await res.json();
              
              if (!json || !json.choices || json.choices.length === 0) {
                console.error('❌ Invalid API KB response:', json);
                throw new Error('Invalid API KB response');
              }
              
              const replyPart = json?.choices?.[0]?.message?.content || '';
              if (replyPart) {
                completeResponse += replyPart;
                hasMore = json?.choices?.[0]?.has_more || false;
                nextToken = json?.choices?.[0]?.next_token || null;
              } else {
                hasMore = false;
              }
            }

            // Jika kita sampai di sini tanpa error, tandai sebagai sukses
            success = true;
            
          } catch (err) {
            console.error(`❌ Error saat mengakses API KB (percobaan ${retries + 1}/${maxRetries + 1}):`, err);
            retries++;
            
            // Jika sudah mencoba semua model dan masih gagal
            if (retries > maxRetries) {
              console.error('❌ Semua model gagal untuk KB');
              break;
            }
            
            // Tunggu sebentar sebelum mencoba lagi
            await new Promise(resolve => setTimeout(resolve, 1000));
          }
        }

        // Hapus konten dalam tag <think></think>
        completeResponse = completeResponse.replace(/<think>[\s\S]*?<\/think>/g, '');

        // Hapus spasi di awal respons
        completeResponse = completeResponse.trimStart();
      }
    }
    
    // Jika masih kosong, berikan pesan default
    if (!completeResponse || completeResponse.trim().length < 10) {
      completeResponse = 'Maaf, saya tidak dapat menjawab pertanyaan tersebut saat ini. Mohon coba pertanyaan lain atau hubungi administrator untuk bantuan.';
    }
    
    // Kirim jawaban
    if (chat.isGroup) {
      const mentionText = `@${contact.id.user}`;
      // Tambahkan anotasi tipe untuk contact
      await chat.sendMessage(`${mentionText}\n${completeResponse}`, { mentions: [contact as any] });
    } else {
      await msg.reply(completeResponse);
    }
  } catch (error) {
    console.error('Error in AI response:', error);
    await msg.reply('❌ Terjadi kesalahan saat memproses pertanyaan Anda. Mohon coba lagi nanti.');
  }
}

client.on('message_create', async (msg: Message) => {
  if (msg.fromMe) return;
  const text = msg.body?.trim();
  if (!text) return;

  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📚 Panduan Penggunaan Bot:

/ai [pertanyaan] - Bertanya ke AI dengan pengetahuan umum terlebih dahulu
/tanya [pertanyaan] - Sama dengan /ai
/kb [pertanyaan] - Mencari khusus di knowledgebase
/bantuan - Menampilkan panduan ini

Bot akan mencoba menjawab pertanyaan dengan pengetahuan umum terlebih dahulu. Jika AI tidak memiliki informasi yang cukup, bot akan mencari di knowledgebase yang telah diupload.`);
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
  // if (!text.startsWith('/')) {
  //   await sendAIResponse(msg, text);
  //   return;
  // }
});

// Inisialisasi WhatsApp client terakhir
console.log('📱 Menginisialisasi WhatsApp client...');
client.initialize(); 