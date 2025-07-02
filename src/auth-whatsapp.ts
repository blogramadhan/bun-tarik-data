import { initWhatsApp, isWhatsAppReady, waitUntilWhatsAppReady, setQRCallback } from "./whatsapp";
import { writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import * as qrcode from "qrcode";

// Pastikan direktori untuk menyimpan QR code ada
const QR_DIR = join(process.cwd(), 'qrcode');
if (!existsSync(QR_DIR)) {
  mkdirSync(QR_DIR, { recursive: true });
}

// Variabel untuk menyimpan status QR code
let qrSaved = false;

// Mengatur callback untuk QR code
setQRCallback(async (qrCodeData) => {
  // Simpan QR code sebagai file PNG
  const qrFilePath = join(QR_DIR, 'whatsapp-qrcode.png');
  try {
    await qrcode.toFile(qrFilePath, qrCodeData);
    qrSaved = true;
    console.log(`📱 QR Code disimpan di: ${qrFilePath}`);
    console.log(`⚠️ Silakan unduh file QR Code ini dan scan menggunakan WhatsApp di ponsel Anda`);
    console.log(`🔍 Setelah berhasil scan, file otentikasi akan disimpan untuk penggunaan berikutnya`);
  } catch (err) {
    console.error('Gagal menyimpan QR code:', err);
  }
});

/**
 * Script untuk melakukan autentikasi WhatsApp
 * Jalankan script ini terpisah untuk scan QR Code dan autentikasi WhatsApp
 */
async function main() {
  console.log("🔑 Memulai proses autentikasi WhatsApp...");
  console.log("🔍 Silahkan siapkan aplikasi WhatsApp di HP Anda...");
  
  try {
    // Inisialisasi WhatsApp
    await initWhatsApp();
    
    // Menunggu QR Code di-scan dan proses autentikasi selesai
    console.log("⏳ Menunggu QR Code di-scan dan autentikasi selesai...");
    
    // Tunggu maksimal 5 menit (300 detik) untuk proses autentikasi
    let isReady = false;
    let attempts = 0;
    const maxAttempts = 300; // 5 menit
    
    while (!isReady && attempts < maxAttempts) {
      isReady = isWhatsAppReady();
      if (!isReady) {
        if (attempts % 30 === 0) { // Tampilkan pesan setiap 30 detik
          if (qrSaved) {
            console.log(`⏳ [${attempts}s/300s] Menunggu QR Code di-scan... QR code tersimpan di ${join(QR_DIR, 'whatsapp-qrcode.png')}`);
          } else {
            console.log(`⏳ [${attempts}s/300s] Menunggu QR Code ditampilkan...`);
          }
        }
        await new Promise(resolve => setTimeout(resolve, 1000));
        attempts++;
      }
    }
    
    if (isReady) {
      console.log("✅ Autentikasi WhatsApp berhasil!");
      console.log("🔔 Sesi telah disimpan, Anda tidak perlu scan QR Code lagi untuk penggunaan selanjutnya.");
      console.log("📱 Anda sekarang dapat menggunakan fitur notifikasi WhatsApp saat upload file.");
    } else {
      console.log("❌ Timeout! Autentikasi WhatsApp gagal. Silakan coba lagi.");
    }
  } catch (error) {
    console.error("❌ Terjadi kesalahan saat autentikasi WhatsApp:", error);
  } finally {
    // Berikan waktu untuk memastikan semua proses selesai
    console.log("⏱️ Menunggu 5 detik sebelum keluar...");
    await new Promise(resolve => setTimeout(resolve, 5000));
    process.exit(0);
  }
}

main(); 