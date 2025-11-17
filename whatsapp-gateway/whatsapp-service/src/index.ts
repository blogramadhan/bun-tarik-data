import { Hono } from 'hono';
import { serve } from '@hono/node-server';
import { Client, LocalAuth, type Message } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';


// WhatsApp Client
export const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: {
    headless: true,
    args: [
      '--no-sandbox',
      '--disable-setuid-sandbox',
      '--disable-dev-shm-usage',
      '--disable-accelerated-2d-canvas',
      '--no-first-run',
      '--no-zygote',
      '--disable-gpu',
      '--disable-background-timer-throttling',
      '--disable-backgrounding-occluded-windows',
      '--disable-renderer-backgrounding',
      '--disable-software-rasterizer'
    ],
    executablePath: '/usr/bin/chromium'
  },
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
  console.log('📱 WhatsApp gateway siap menerima permintaan API');
});


// Event handler untuk pesan WhatsApp (hanya untuk logging)
client.on('message_create', async (msg: Message) => {
  if (msg.fromMe) return;
  const text = msg.body?.trim();
  if (!text) return;

  console.log(`📩 Pesan diterima dari ${msg.from}: ${text}`);
  
  // Hanya menampilkan pesan bantuan sederhana
  if (text === '/bantuan' || text === '/help') {
    await msg.reply(`📱 WhatsApp Gateway\n\nGateway ini hanya untuk mengirim dan menerima pesan WhatsApp melalui API.\n\nGunakan endpoint /send-message untuk mengirim pesan.`);
    return;
  }
});

// Inisialisasi WhatsApp client terakhir
console.log('📱 Menginisialisasi WhatsApp client...');
client.initialize(); 