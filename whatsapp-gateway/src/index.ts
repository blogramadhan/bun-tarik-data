import { serve } from 'bun';
import { Client, LocalAuth } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';

const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
  },
});

client.on('qr', qr => {
  console.log('🔐 Scan QR berikut:');
  qrcode.generate(qr, { small: true });
});

let clientReady = false;

client.on('ready', () => {
  console.log('✅ WhatsApp client siap!');
  clientReady = true;
});

client.on('auth_failure', msg => {
  console.error('❌ Autentikasi gagal:', msg);
});

client.initialize();

// API endpoint /send-message
serve({
  port: 3000,
  fetch: async (req) => {
    const url = new URL(req.url);
    if (req.method === 'POST' && url.pathname === '/send-message') {
      if (!clientReady) {
        return Response.json(
          { status: false, message: 'Client belum siap, silahkan scan QR terlebih dahulu' }, 
          { status: 503 });
      }

      const { number, message } = await req.json();

      if (!number || !message) {
        return Response.json({ status: false, message: 'Number & message wajib' }, { status: 400 });
      }

      try {
        await client.sendMessage(`${number}@c.us`, message);
        return Response.json({ status: true, message: 'Pesan terkirim' });
      } catch (err: any) {
        return Response.json({ status: false, error: err.message }, { status: 500 });
      }
    }

    return new Response('Not Found', { status: 404 });
  },
});