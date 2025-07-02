import { initWhatsApp, isWhatsAppReady, waitUntilWhatsAppReady } from "./whatsapp";

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
    
    // Tunggu maksimal 2 menit (120 detik) untuk proses autentikasi
    const isReady = await waitUntilWhatsAppReady(120);
    
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