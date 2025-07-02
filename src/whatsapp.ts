import { Client, LocalAuth } from "whatsapp-web.js";
import qrcode from "qrcode-terminal";
import * as dotenv from "dotenv";
import { existsSync, mkdirSync } from "fs";
import { join } from "path";

dotenv.config();

const { WHATSAPP_RECIPIENT_NUMBERS } = process.env;

// Pastikan direktori untuk menyimpan data sesi tersedia
const SESSIONS_DIR = join(process.cwd(), '.wwebjs_auth');
const SESSION_DATA_DIR = join(SESSIONS_DIR, 'session-data');
if (!existsSync(SESSIONS_DIR)) {
  mkdirSync(SESSIONS_DIR, { recursive: true });
}
if (!existsSync(SESSION_DATA_DIR)) {
  mkdirSync(SESSION_DATA_DIR, { recursive: true });
}

// Inisialisasi WhatsApp Client dengan LocalAuth untuk menyimpan sesi
const client = new Client({
  authStrategy: new LocalAuth({
    dataPath: SESSIONS_DIR
  }),
  puppeteer: {
    headless: true,
    args: ['--no-sandbox', '--disable-setuid-sandbox', '--disable-gpu', '--disable-dev-shm-usage'],
    executablePath: '/usr/bin/google-chrome',
    userDataDir: SESSION_DATA_DIR
  }
});

// Status WhatsApp connection
let whatsappReady = false;

// Callback untuk QR code yang bisa diakses dari luar
export type QRCallback = (qr: string) => void;
let externalQRCallback: QRCallback | null = null;

/**
 * Mengatur callback untuk QR code
 * @param callback Fungsi yang akan dipanggil ketika QR code diterima
 */
export function setQRCallback(callback: QRCallback): void {
  externalQRCallback = callback;
}

// Setup WhatsApp client
client.on('qr', (qr) => {
  console.log('QR RECEIVED, scan dengan aplikasi WhatsApp di HP Anda:');
  qrcode.generate(qr, {small: true});
  
  // Panggil callback eksternal jika ada
  if (externalQRCallback) {
    externalQRCallback(qr);
  }
});

client.on('ready', () => {
  console.log('WhatsApp client siap!');
  whatsappReady = true;
});

client.on('authenticated', () => {
  console.log('WhatsApp berhasil diautentikasi!');
});

client.on('auth_failure', (msg) => {
  console.error('Autentikasi WhatsApp gagal:', msg);
});

client.on('disconnected', () => {
  console.log('WhatsApp client terputus!');
  whatsappReady = false;
});

/**
 * Inisialisasi WhatsApp client
 * @returns Promise<void>
 */
export async function initWhatsApp(): Promise<void> {
  try {
    await client.initialize();
  } catch (err) {
    console.error('Gagal menginisialisasi WhatsApp client:', err);
    throw err;
  }
}

// Inisialisasi WhatsApp client secara otomatis saat modul diimpor
initWhatsApp().catch(err => {
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

/**
 * Logout dan hapus data sesi WhatsApp
 * @returns Promise<void>
 */
export async function logoutWhatsApp(): Promise<void> {
  if (whatsappReady) {
    await client.logout();
    console.log('WhatsApp berhasil logout dan sesi dihapus.');
  }
} 