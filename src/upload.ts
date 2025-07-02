import { S3Client, PutObjectCommand, HeadObjectCommand, ListObjectsV2Command } from "@aws-sdk/client-s3";
import { readdirSync, statSync, readFileSync } from "fs";
import { join } from "path";
import * as dotenv from "dotenv";
// import { request } from "undici";
// import nodemailer from "nodemailer";
import { sendWhatsApp, waitUntilWhatsAppReady, isWhatsAppReady } from "./whatsapp";

dotenv.config();

const {
  R2_ENDPOINT,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET_NAME,

  // TELEGRAM_BOT_TOKEN,
  // TELEGRAM_CHAT_ID,

  // EMAIL_FROM,
  // EMAIL_TO,
  // EMAIL_SMTP_HOST,
  // EMAIL_SMTP_PORT,
  // EMAIL_SMTP_USER,
  // EMAIL_SMTP_PASS,
} = process.env;

// Inisialisasi S3 Client untuk koneksi ke Cloudflare R2
const s3 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID!,
    secretAccessKey: R2_SECRET_ACCESS_KEY!,
  },
});

// Fungsi untuk mengirim notifikasi ke Telegram (dinonaktifkan)
// async function sendTelegram(message: string) {
//   if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
//   try {
//     await request(
//       `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`,
//       {
//         method: "POST",
//         headers: { "content-type": "application/json" },
//         body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text: message }),
//       }
//     );
//   } catch (e) {
//     console.error("Telegram error", e);
//   }
// }

// Fungsi untuk mengirim notifikasi melalui Email (dinonaktifkan)
// async function sendEmail(subject: string, message: string) {
//   if (!EMAIL_SMTP_HOST || !EMAIL_SMTP_USER || !EMAIL_SMTP_PASS) return;
//   try {
//     const transporter = nodemailer.createTransporter({
//       host: EMAIL_SMTP_HOST,
//       port: Number(EMAIL_SMTP_PORT),
//       secure: false,
//       auth: {
//         user: EMAIL_SMTP_USER,
//         pass: EMAIL_SMTP_PASS,
//       },
//     });

//     await transporter.sendMail({
//       from: EMAIL_FROM,
//       to: EMAIL_TO,
//       subject,
//       text: message,
//     });
//   } catch (e) {
//     console.error("Email error", e);
//   }
// }

// Fungsi untuk mengirim notifikasi ke semua platform
async function notifyAll(message: string) {
  // await Promise.all([
  //   sendTelegram(message),
  //   sendEmail("Upload R2 Notification", message),
  // ]);
  // await sendWhatsApp(message);
  console.log(`📢 Notification: ${message}`);
}

// Fungsi untuk memeriksa apakah file perlu diupload berdasarkan tahun pada path file
function shouldUploadFile(key: string): boolean {
  const currentYear = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1; // getMonth() mengembalikan 0-11

  // Memeriksa apakah path file mengandung format tahun (2022, 2023, dll)
  const yearMatch = key.match(/\b(20\d{2})\b/);
  if (!yearMatch || !yearMatch[1]) return false; 

  const fileYear = parseInt(yearMatch[1]);

  // Aturan upload:
  // 1. File untuk tahun berjalan selalu diupload, atau
  // 2. File untuk tahun sebelumnya diupload jika bulan saat ini masih Januari atau Februari
  return fileYear === currentYear || 
         (fileYear === currentYear - 1 && currentMonth <= 2);
}

// Fungsi untuk memeriksa apakah folder sudah ada di penyimpanan S3/R2
async function folderExistsInS3(bucketName: string, prefix: string): Promise<boolean> {
  try {
    // Pastikan prefix berakhir dengan '/' untuk menandakan folder
    const folderPrefix = prefix.endsWith('/') ? prefix : `${prefix}/`;
    
    const command = new ListObjectsV2Command({
      Bucket: bucketName,
      Prefix: folderPrefix,
      MaxKeys: 1,
    });
    
    const response = await s3.send(command);
    return !!(response.Contents && response.Contents.length > 0);
  } catch (error) {
    return false;
  }
}

// Fungsi untuk memeriksa apakah file sudah ada di penyimpanan S3/R2
async function fileExistsInS3(bucketName: string, key: string): Promise<boolean> {
  try {
    const command = new HeadObjectCommand({
      Bucket: bucketName,
      Key: key,
    });
    await s3.send(command);
    return true; // File ditemukan
  } catch (error) {
    return false; // File tidak ditemukan
  }
}

// Fungsi untuk mengupload semua file secara rekursif dari direktori lokal ke S3/R2
async function uploadAllFiles(localDir: string, bucketName: string, prefix = "") {
  const entries = readdirSync(localDir);
  let uploadedCount = 0;
  let failedCount = 0;
  let skippedCount = 0;
  
  for (const entry of entries) {
    const fullPath = join(localDir, entry);
    const stat = statSync(fullPath);
    const key = join(prefix, entry).replace(/\\/g, "/"); // Konversi path Windows ke format URL

    if (stat.isDirectory()) {
      // Cek apakah folder sudah ada di S3/R2
      const folderExists = await folderExistsInS3(bucketName, key);
      
      // Jika folder belum ada, lakukan upload semua file di dalamnya
      if (!folderExists) {
        console.log(`📁 Folder baru ditemukan: ${key}, mengupload semua konten...`);
        const subResult = await uploadAllFiles(fullPath, bucketName, key);
        uploadedCount += subResult.uploadedCount;
        failedCount += subResult.failedCount;
        skippedCount += subResult.skippedCount;
      } else {
        // Jika folder sudah ada, tetap periksa file berdasarkan aturan tahun
        console.log(`📁 Folder sudah ada: ${key}, memeriksa konten berdasarkan aturan tahun...`);
        const subResult = await uploadAllFiles(fullPath, bucketName, key);
        uploadedCount += subResult.uploadedCount;
        failedCount += subResult.failedCount;
        skippedCount += subResult.skippedCount;
      }
    } else {
      // Proses upload untuk file
      // Memeriksa apakah file sudah ada di S3/R2
      const exists = await fileExistsInS3(bucketName, key);
      
      if (!exists) {
        // File belum ada, upload langsung
        const fileContent = readFileSync(fullPath);
        const uploadCommand = new PutObjectCommand({
          Bucket: bucketName,
          Key: key,
          Body: fileContent,
        });

        try {
          await s3.send(uploadCommand);
          uploadedCount++;
          const msg = `✅ Berhasil upload (file baru): ${key} (${(stat.size / 1024 / 1024).toFixed(2)} MB)`;
          console.log(msg);
          await notifyAll(msg);
        } catch (error) {
          failedCount++;
          const msg = `❌ Gagal upload: ${key} - ${error}`;
          console.error(msg);
          await notifyAll(msg);
        }
      } else {
        // File sudah ada, periksa berdasarkan aturan tahun
        if (shouldUploadFile(key)) {
          const fileContent = readFileSync(fullPath);
          const uploadCommand = new PutObjectCommand({
            Bucket: bucketName,
            Key: key,
            Body: fileContent,
          });

          try {
            await s3.send(uploadCommand);
            uploadedCount++;
            const msg = `✅ Berhasil update: ${key} (${(stat.size / 1024 / 1024).toFixed(2)} MB)`;
            console.log(msg);
            await notifyAll(msg);
          } catch (error) {
            failedCount++;
            const msg = `❌ Gagal update: ${key} - ${error}`;
            console.error(msg);
            await notifyAll(msg);
          }
        } else {
          skippedCount++;
          console.log(`⏭️ Dilewati (bukan tahun berjalan/sebelumnya): ${key}`);
        }
      }
    }
  }
  
  return { uploadedCount, failedCount, skippedCount };
}

// Fungsi utama program
async function main() {
  try {
    console.log("🚀 Memulai proses upload file ke Cloudflare R2...");
    
    // Tunggu hingga WhatsApp siap (maksimal 30 detik)
    const isReady = await waitUntilWhatsAppReady(30);
    
    if (isReady) {
      await notifyAll("🚀 Proses upload ke Cloudflare R2 dimulai...");
    }
    
    const result = await uploadAllFiles("data", R2_BUCKET_NAME!);
    
    const summary = `🎉 Upload selesai! Berhasil: ${result.uploadedCount}, Gagal: ${result.failedCount}, Dilewati: ${result.skippedCount}`;
    console.log(summary);
    await notifyAll(summary);
  } catch (err: any) {
    const msg = `❌ Upload total gagal: ${err.message}`;
    console.error(msg);
    await notifyAll(msg);
  } finally {
    // Tunggu beberapa detik untuk memastikan pesan terkirim sebelum keluar
    await new Promise(resolve => setTimeout(resolve, 5000));
    process.exit();
  }
}

main();