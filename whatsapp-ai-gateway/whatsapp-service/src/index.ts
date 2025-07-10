import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { Client, LocalAuth, type Message } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';

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

// Status API
app.get('/', (c) => {
  console.log('📊 Menerima permintaan status');
  return c.json({ 
    status: 'ok', 
    whatsapp: client.info ? 'connected' : 'initializing'
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
});

// Fungsi untuk mengirim pesan AI
async function sendAIResponse(msg: Message, prompt: string) {
  const contact = await msg.getContact();
  const chat = await msg.getChat();

  try {
    const finalPrompt = `Pertanyaan:
${prompt}

Catatan: Jawab pertanyaan ini dengan pengetahuan umum Anda. Berikan jawaban yang lengkap dan akurat. Jawab langsung tanpa menampilkan <think></think> di jawaban.`;

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
          model: process.env.DEEPINFRA_MODEL || 'mistralai/Mistral-7B-Instruct-v0.2',
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

  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📚 Panduan Penggunaan Bot:

/ai [pertanyaan] - Bertanya ke AI
/tanya [pertanyaan] - Sama dengan /ai
/bantuan - Menampilkan panduan ini`);
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