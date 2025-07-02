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
  
  let retryCount = 0;
  const maxRetries = 3;
  
  while (retryCount < maxRetries) {
    try {
      console.log(`\n🚀 Percobaan ${retryCount + 1}/${maxRetries}`);
      
      // Reset state
      qrSaved = false;
      
      // Inisialisasi WhatsApp
      await initWhatsApp();
      
      // Menunggu QR Code di-scan dan proses autentikasi selesai
      console.log("⏳ Menunggu QR Code di-scan dan autentikasi selesai...");
      
      // Tunggu maksimal 5 menit (300 detik) untuk proses autentikasi
      let isReady = false;
      let attempts = 0;
      const maxAttempts = 300; // 5 menit
      
      while (!isReady && attempts < maxAttempts) {
        try {
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
                 } catch (innerError) {
           console.log(`⚠️ Error saat cek status: ${(innerError as Error).message}`);
           break;
         }
      }
      
      if (isReady) {
        console.log("✅ Autentikasi WhatsApp berhasil!");
        console.log("🔔 Sesi telah disimpan, Anda tidak perlu scan QR Code lagi untuk penggunaan selanjutnya.");
        console.log("📱 Anda sekarang dapat menggunakan fitur notifikasi WhatsApp saat upload file.");
        
        // Tunggu sebentar untuk memastikan sesi tersimpan
        console.log("💾 Menyimpan sesi...");
        await new Promise(resolve => setTimeout(resolve, 10000));
        return; // Berhasil, keluar dari loop
      } else {
        throw new Error("Timeout waiting for authentication");
      }
         } catch (error) {
       console.error(`❌ Percobaan ${retryCount + 1} gagal:`, (error as Error).message);
       retryCount++;
      
      if (retryCount < maxRetries) {
        console.log(`🔄 Mencoba lagi dalam 10 detik... (${retryCount}/${maxRetries})`);
        await new Promise(resolve => setTimeout(resolve, 10000));
      }
    }
  }
  
  console.log("❌ Semua percobaan autentikasi gagal. Silakan coba restart container.");
  process.exit(1);
}

main(); 