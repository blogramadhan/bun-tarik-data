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
            // Gunakan metode sederhana tanpa embedding jika tidak ada dokumen
            console.log(`📚 Total ${Object.keys(KB).length - 1} materi dimuat ke knowledge base`);
            
            try {
                // Buat vector store dari dokumen menggunakan API DeepInfra sebagai OpenAI compatible API
                const embeddings = new OpenAIEmbeddings({
                    apiKey: process.env.DEEPINFRA_API_KEY,
                    configuration: {
                        baseURL: "https://api.deepinfra.com/v1/openai"
                    },
                    modelName: process.env.DEEPINFRA_MODEL || "deepseek-ai/DeepSeek-R1"
                    // modelName: "text-embedding-ada-002"
                });
                
                vectorStore = await MemoryVectorStore.fromDocuments(documents, embeddings);
                console.log(`📚 Total ${documents.length} chunk dokumen berhasil diembedding`);
            } catch (err) {
                console.error('❌ Gagal membuat embedding:', err);
                console.log('⚠️ Melanjutkan tanpa fitur RAG');
            }
        }
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
async function sendAIResponse(msg: Message, prompt: string, context?: string, useKnowledgeBaseOnly: boolean = false) {
  const contact = await msg.getContact();
  const chat = await msg.getChat();

  try {
    let finalPrompt = '';
    let hasRelevantContent = false;
    
    // Gunakan RAG jika vector store tersedia
    if (vectorStore) {
      try {
        // Cari dokumen yang relevan menggunakan vector store
        const relevantDocs = await vectorStore.similaritySearch(prompt, 3);
        
        if (relevantDocs && relevantDocs.length > 0) {
          hasRelevantContent = true;
          
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

${useKnowledgeBaseOnly ? 
  "Catatan: Jawab hanya berdasarkan informasi dari materi yang diberikan. Jika informasi tidak ada dalam materi, katakan dengan jujur bahwa informasi tersebut tidak tersedia dalam materi." : 
  "Catatan: Jawab berdasarkan informasi dari materi yang diberikan jika relevan. Jika informasi tidak ada dalam materi atau tidak lengkap, Anda dapat menggunakan pengetahuan umum Anda untuk memberikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban."}`;
        }
      } catch (err) {
        console.error('❌ Error saat pencarian vektor:', err);
      }
    }
    
    // Jika tidak ada konten yang relevan dari RAG, gunakan metode lama atau pengetahuan umum
    if (!hasRelevantContent) {
      const finalContext = context || COMBINED_KB;
      
      if (finalContext && finalContext.length > 0) {
        // Ada konten di knowledge base
        const trimmedContext = finalContext.slice(0, 3000);

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
  
  // Perintah /kb untuk bertanya hanya menggunakan knowledge base
  if (text.startsWith('/kb ')) {
    const prompt = text.replace('/kb ', '').trim();
    
    // Dapatkan materi yang dipilih atau gunakan semua materi secara otomatis
    const contextKey = selectedContext[msg.from];
    
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