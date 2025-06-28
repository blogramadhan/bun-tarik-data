import { S3Client, PutObjectCommand, HeadObjectCommand } from "@aws-sdk/client-s3";
import { readdirSync, statSync, readFileSync } from "fs";
import { join } from "path";
import * as dotenv from "dotenv";
// import { request } from "undici";
// import nodemailer from "nodemailer";

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

  // WHATSAPP_API_URL,
  // WHATSAPP_NUMBERS,
  // WHATSAPP_MESSAGE_HEADER,
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

// Fungsi untuk mengirim notifikasi ke WhatsApp melalui API GET Request (dinonaktifkan)
// async function sendWhatsApp(message: string) {
//   if (!WHATSAPP_NUMBERS || !WHATSAPP_API_URL) return;
//   const numbers = WHATSAPP_NUMBERS.split(",");
//   for (const number of numbers) {
//     const url = `${WHATSAPP_API_URL}?phone=${number}&text=${encodeURIComponent(`${WHATSAPP_MESSAGE_HEADER} ${message}`)}`;
//     try {
//       await request(url, { method: "GET" });
//     } catch (e) {
//       console.error("WhatsApp error", e);
//     }
//   }
// }

// Fungsi untuk mengirim notifikasi ke semua platform (saat ini hanya console.log)
async function notifyAll(message: string) {
  // await Promise.all([
  //   sendTelegram(message),
  //   sendEmail("Upload R2 Notification", message),
  //   sendWhatsApp(message),
  // ]);
  console.log(`📢 Notification: ${message}`);
}

// Fungsi untuk memeriksa apakah file perlu diupload berdasarkan tahun pada path file
function shouldUploadFile(key: string): boolean {
  const currentYear = new Date().getFullYear();
  const currentMonth = new Date().getMonth() + 1; // getMonth() mengembalikan 0-11

  // Memeriksa apakah path file mengandung format tahun (2022, 2023, dll)
  const yearMatch = key.match(/\b(20\d{2})\b/);
  if (!yearMatch || !yearMatch[1]) return false; // Jika tidak ada tahun dalam path, jangan upload

  const fileYear = parseInt(yearMatch[1]);

  // Aturan upload:
  // 1. File untuk tahun berjalan selalu diupload, atau
  // 2. File untuk tahun sebelumnya diupload jika bulan saat ini masih Januari atau Februari
  return fileYear === currentYear || 
         (fileYear === currentYear - 1 && currentMonth <= 2);
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

    if (stat.isDirectory()) {
      // Proses rekursif untuk subfolder
      const subResult = await uploadAllFiles(fullPath, bucketName, join(prefix, entry));
      uploadedCount += subResult.uploadedCount;
      failedCount += subResult.failedCount;
      skippedCount += subResult.skippedCount;
    } else {
      // Proses upload untuk file
      const key = join(prefix, entry).replace(/\\/g, "/"); // Konversi path Windows ke format URL

      // Memeriksa apakah file sudah ada di S3/R2 untuk menghindari upload ulang
      const exists = await fileExistsInS3(bucketName, key);
      if (exists) {
        skippedCount++;
        console.log(`⏭️ Dilewati (sudah ada): ${key}`);
        continue;
      }

      // Memeriksa apakah file perlu diupload berdasarkan aturan tahun
      if (!shouldUploadFile(key)) {
        skippedCount++;
        console.log(`⏭️ Dilewati (bukan tahun berjalan/sebelumnya): ${key}`);
        continue;
      }

      const fileContent = readFileSync(fullPath);
      const uploadCommand = new PutObjectCommand({
        Bucket: bucketName,
        Key: key,
        Body: fileContent,
      });

      try {
        await s3.send(uploadCommand);
        uploadedCount++;
        const msg = `✅ Berhasil upload: ${key} (${(stat.size / 1024 / 1024).toFixed(2)} MB)`;
        console.log(msg);
        await notifyAll(msg);
      } catch (error) {
        failedCount++;
        const msg = `❌ Gagal upload: ${key} - ${error}`;
        console.error(msg);
        await notifyAll(msg);
      }
    }
  }
  
  return { uploadedCount, failedCount, skippedCount };
}

// Fungsi utama program
async function main() {
  try {
    console.log("🚀 Memulai proses upload file ke Cloudflare R2...");
    const result = await uploadAllFiles("data", R2_BUCKET_NAME!);
    
    const summary = `🎉 Upload selesai! Berhasil: ${result.uploadedCount}, Gagal: ${result.failedCount}, Dilewati: ${result.skippedCount}`;
    console.log(summary);
    await notifyAll(summary);
  } catch (err: any) {
    const msg = `❌ Upload total gagal: ${err.message}`;
    console.error(msg);
    await notifyAll(msg);
  }
}

main();