import { Client, LocalAuth, type Message, type Contact } from 'whatsapp-web.js';
import qrcode from 'qrcode-terminal';
import { existsSync, readFileSync, readdirSync } from 'fs';

export const KB: Record<string, string> = {};
export function loadKnowledgeBase() {
    KB._last_loaded = new Date().toISOString();
    if (!existsSync('data/text')) return;

    readdirSync('data/text').forEach(file => {
        const key = file.replace('.txt', '');
        KB[key] = readFileSync(`data/text/${file}`, 'utf8');
    });
}

loadKnowledgeBase();

export const client = new Client({
  authStrategy: new LocalAuth({ dataPath: './.wwebjs_auth' }),
  puppeteer: { headless: true, args: ['--no-sandbox', '--disable-setuid-sandbox'] },
});

client.on('qr', (qr) => qrcode.generate(qr, { small: true }));
client.on('ready', () => console.log('✅ WhatsApp client siap!'));
client.initialize();

const selectedContext: Record<string, string> = {};

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

  if (text.startsWith('/ai ')) {
    const prompt = text.replace('/ai ', '').trim();
    const contact = await msg.getContact();
    const chat = await msg.getChat();
    const contextKey = selectedContext[msg.from];
    const context = contextKey ? KB[contextKey] : '';

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
          model: 'deepseek-ai/DeepSeek-R1',
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
    
    const reply = completeResponse || '❌ Gagal menjawab.';

    if (chat.isGroup) {
      await chat.sendMessage(`@${contact.id.user}\n${reply}`, { mentions: [contact] });
    } else {
      await msg.reply(reply);
    }
  }
});