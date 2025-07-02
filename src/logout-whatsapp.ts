import { initWhatsApp, logoutWhatsApp, waitUntilWhatsAppReady } from "./whatsapp";

/**
 * Script untuk logout dan menghapus sesi WhatsApp
 * Jalankan script ini jika ingin menghapus sesi dan melakukan autentikasi ulang
 */
async function main() {
  console.log("🔄 Memulai proses logout WhatsApp...");
  
  try {
    // Inisialisasi WhatsApp
    await initWhatsApp();
    
    // Tunggu hingga WhatsApp siap (maksimal 30 detik)
    const isReady = await waitUntilWhatsAppReady(30);
    
    if (isReady) {
      // Logout dan hapus sesi
      await logoutWhatsApp();
      console.log("✅ Berhasil logout dari WhatsApp.");
      console.log("🔑 Sesi WhatsApp telah dihapus. Jalankan auth-whatsapp.ts untuk membuat sesi baru.");
    } else {
      console.log("❌ Tidak dapat logout karena WhatsApp tidak dalam keadaan siap.");
    }
  } catch (error) {
    console.error("❌ Terjadi kesalahan saat logout WhatsApp:", error);
  } finally {
    // Berikan waktu untuk memastikan semua proses selesai
    console.log("⏱️ Menunggu 5 detik sebelum keluar...");
    await new Promise(resolve => setTimeout(resolve, 5000));
    process.exit(0);
  }
}

main(); 