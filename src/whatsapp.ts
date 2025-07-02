import { Client } from "whatsapp-web.js";
import qrcode from "qrcode-terminal";
import * as dotenv from "dotenv";

dotenv.config();

const { WHATSAPP_RECIPIENT_NUMBERS } = process.env;

// Inisialisasi WhatsApp Client
const client = new Client({});

// Status WhatsApp connection
let whatsappReady = false;

// Setup WhatsApp client
client.on('qr', (qr) => {
  console.log('QR RECEIVED, scan dengan aplikasi WhatsApp di HP Anda:');
  qrcode.generate(qr, {small: true});
});

client.on('ready', () => {
  console.log('WhatsApp client siap!');
  whatsappReady = true;
});

client.on('disconnected', () => {
  console.log('WhatsApp client terputus!');
  whatsappReady = false;
});

// Inisialisasi WhatsApp client
client.initialize().catch(err => {
  console.error('Gagal menginisialisasi WhatsApp client:', err);
});

/**
 * Fungsi untuk mengirim pesan melalui WhatsApp
 * @param message Pesan yang akan dikirim
 * @returns Promise<void>
 */
export async function sendWhatsApp(message: string) {
  if (!whatsappReady || !WHATSAPP_RECIPIENT_NUMBERS) {
    console.log('WhatsApp tidak siap atau nomor penerima tidak dikonfigurasi');
    return;
  }
  
  try {
    const numbers = WHATSAPP_RECIPIENT_NUMBERS.split(',');
    
    for (const number of numbers) {
      // Format nomor sesuai standar WhatsApp (misalnya 628123456789)
      const formattedNumber = number.trim().startsWith('62') ? 
        `${number.trim()}@c.us` : `62${number.trim().replace(/^0+/, '')}@c.us`;
      
      await client.sendMessage(formattedNumber, message);
      console.log(`WhatsApp terkirim ke ${formattedNumber}`);
    }
  } catch (error) {
    console.error('Error mengirim WhatsApp:', error);
  }
}

/**
 * Fungsi untuk menunggu hingga WhatsApp siap (maksimal timeout detik)
 * @param timeout Waktu timeout dalam detik
 * @returns Promise<boolean> - true jika WhatsApp siap, false jika timeout
 */
export async function waitUntilWhatsAppReady(timeout = 30): Promise<boolean> {
  if (whatsappReady) return true;
  
  let attempts = 0;
  while (!whatsappReady && attempts < timeout) {
    console.log("Menunggu WhatsApp client siap...");
    await new Promise(resolve => setTimeout(resolve, 1000));
    attempts++;
  }
  
  return whatsappReady;
}

/**
 * Memeriksa apakah WhatsApp client sudah siap
 * @returns boolean
 */
export function isWhatsAppReady(): boolean {
  return whatsappReady;
} 