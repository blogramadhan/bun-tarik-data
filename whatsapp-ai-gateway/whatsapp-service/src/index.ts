import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { Client, LocalAuth, type Message } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';
import { existsSync, readFileSync, readdirSync, mkdirSync } from 'fs';
import { OpenAIEmbeddings } from '@langchain/openai';
import { MemoryVectorStore } from 'langchain/vectorstores/memory';
import { Document } from 'langchain/document';
import { RecursiveCharacterTextSplitter } from 'langchain/text_splitter';

// Knowledge Base
export const KB: Record<string, string> = {};
// Variabel untuk menyimpan gabungan semua materi
export let COMBINED_KB: string = '';
// Variabel untuk menyimpan vector store
export let vectorStore: MemoryVectorStore;

// Fungsi untuk memuat dan membuat embedding knowledge base
export async function loadAndEmbedKnowledgeBase() {
    console.log('🔄 Memuat dan membuat embedding knowledge base...');
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
        
        const documents: Document[] = [];
        
        for (const file of files) {
            try {
                const key = file.replace('.txt', '');
                const content = readFileSync(`data/text/${file}`, 'utf8');
                KB[key] = content;
                console.log(`✅ Berhasil memuat: ${file}`);
                
                // Bagi teks menjadi chunk yang lebih kecil
                const textSplitter = new RecursiveCharacterTextSplitter({
                    chunkSize: 1000,
                    chunkOverlap: 200
                });
                
                const chunks = await textSplitter.splitText(content);
                
                // Buat dokumen untuk setiap chunk
                chunks.forEach((chunk, i) => {
                    documents.push(
                        new Document({
                            pageContent: chunk,
                            metadata: {
                                source: key,
                                chunk: i
                            }
                        })
                    );
                });
                
            } catch (err) {
                console.error(`❌ Gagal memuat file ${file}:`, err);
            }
        }
        
        // Gabungkan semua materi menjadi satu (untuk backward compatibility)
        const allMaterials = Object.keys(KB)
            .filter(k => k !== '_last_loaded')
            .map(k => `=== MATERI: ${k} ===\n${KB[k]}`);
        
        if (allMaterials.length > 0) {
            COMBINED_KB = allMaterials.join('\n\n=== BATAS MATERI ===\n\n');
            console.log(`📚 Berhasil menggabungkan ${allMaterials.length} materi (${COMBINED_KB.length} karakter)`);
        }
        
        if (documents.length > 0) {
            // Buat vector store dari dokumen
            const embeddings = new OpenAIEmbeddings({
                openAIApiKey: process.env.DEEPINFRA_API_KEY,
                modelName: "text-embedding-ada-002"
            });
            
            vectorStore = await MemoryVectorStore.fromDocuments(documents, embeddings);
            console.log(`📚 Total ${documents.length} chunk dokumen berhasil diembedding`);
        }
        
        console.log(`📚 Total ${Object.keys(KB).length - 1} materi dimuat ke knowledge base`);
    } catch (err) {
        console.error('❌ Gagal membaca direktori data/text:', err);
    }
}

// Inisialisasi knowledge base
loadAndEmbedKnowledgeBase();

// WhatsApp Client
export const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] },
});

client.on('qr', (qr) => qrcode.generate(qr, { small: true }));
client.on('ready', () => console.log('✅ WhatsApp client siap!'));
client.initialize();

const selectedContext: Record<string, string> = {};

// Fungsi untuk mengirim pesan AI dengan konteks cerdas dan RAG
async function sendAIResponse(msg: Message, prompt: string, context?: string) {
  const contact = await msg.getContact();
  const chat = await msg.getChat();

  try {
    let finalPrompt = '';
    
    // Gunakan RAG jika vector store tersedia
    if (vectorStore) {
      // Cari dokumen yang relevan menggunakan vector store
      const relevantDocs = await vectorStore.similaritySearch(prompt, 3);
      
      // Ekstrak konten dari dokumen yang relevan
      const relevantContent = relevantDocs.map(doc => {
        return `[Sumber: ${doc.metadata.source}]\n${doc.pageContent}`;
      }).join('\n\n');
      
      // Batasi konteks untuk menghindari token yang terlalu banyak
      const trimmedContext = relevantContent.slice(0, 3000);

      finalPrompt = `Berikut materi yang relevan dengan pertanyaan:
${trimmedContext}

Pertanyaan:
${prompt}

Catatan: Jawab langsung tanpa menampilkan <think></think> di jawaban. Gunakan hanya informasi dari materi yang diberikan. Jika informasi tidak ada dalam materi, katakan dengan jujur bahwa informasi tersebut tidak tersedia dalam materi.`;
    } else {
      // Fallback ke metode lama jika RAG tidak tersedia
      const finalContext = context || COMBINED_KB;
      const trimmedContext = finalContext.slice(0, 3000);

      finalPrompt = finalContext ? `Berikut materi:
${trimmedContext}

Pertanyaan:
${prompt}

Catatan: Jawab langsung tanpa menampilkan <think></think> di jawaban.` : `${prompt}

Catatan: Jawab langsung tanpa menampilkan <think></think> di jawaban.`;
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
      await chat.sendMessage(`@${contact.id.user}\n${reply}`, { mentions: [contact] });
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
    await msg.reply(`📚 Materi:
${list}`);
    return;
  }

  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📚 Panduan Penggunaan Bot:

/materi - Melihat daftar materi yang tersedia
/pilih [nama_materi] - Memilih materi spesifik (opsional)
/ai [pertanyaan] - Bertanya dengan semua materi secara otomatis
/tanya [pertanyaan] - Sama dengan /ai, bertanya dengan semua materi
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
    await sendAIResponse(msg, prompt);
    return;
  }

  // Perintah /ai menggunakan semua materi secara otomatis
  if (text.startsWith('/ai ')) {
    const prompt = text.replace('/ai ', '').trim();
    
    // Dapatkan materi yang dipilih atau gunakan semua materi secara otomatis
    const contextKey = selectedContext[msg.from];
    
    if (contextKey && KB[contextKey]) {
      // Jika ada materi yang dipilih, gunakan materi tersebut
      await sendAIResponse(msg, prompt, KB[contextKey]);
    } else {
      // Jika tidak ada materi yang dipilih, gunakan semua materi
      await sendAIResponse(msg, prompt);
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
    await loadAndEmbedKnowledgeBase();
    return c.json({ status: true, message: '✅ Knowledge base berhasil dimuat ulang.' });
  } catch (err: any) {
    return c.json({ status: false, error: err.message }, 500);
  }
});

// Status API
app.get('/', (c) => c.json({ status: 'ok', lastLoaded: KB._last_loaded }));

serve(app);
console.log('✅ WhatsApp service berjalan di port 8788'); 