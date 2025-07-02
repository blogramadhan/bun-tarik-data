import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
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


} = process.env;

// Init S3 Client
const s3 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY_ID!,
    secretAccessKey: R2_SECRET_ACCESS_KEY!,
  },
});

// Notifikasi ke Telegram
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

// Notifikasi ke Email
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



// Kirim semua notifikasi
async function notifyAll(message: string) {
  // await Promise.all([
  //   sendTelegram(message),
  //   sendEmail("Upload R2 Notification", message),
  // ]);
  console.log(`📢 Notification: ${message}`);
}

// Upload rekursif semua file
async function uploadAllFiles(localDir: string, bucketName: string, prefix = "") {
  const entries = readdirSync(localDir);
  let uploadedCount = 0;
  let failedCount = 0;
  
  for (const entry of entries) {
    const fullPath = join(localDir, entry);
    const stat = statSync(fullPath);

    if (stat.isDirectory()) {
      // Recursive untuk subfolder
      const subResult = await uploadAllFiles(fullPath, bucketName, join(prefix, entry));
      uploadedCount += subResult.uploadedCount;
      failedCount += subResult.failedCount;
    } else {
      // Upload semua file
      const fileContent = readFileSync(fullPath);
      const key = join(prefix, entry).replace(/\\/g, "/");

      const uploadCommand = new PutObjectCommand({
        Bucket: bucketName,
        Key: key,
        Body: fileContent,
      });

      try {
        await s3.send(uploadCommand);
        uploadedCount++;
        const msg = `✅ Uploaded: ${key} (${(stat.size / 1024 / 1024).toFixed(2)} MB)`;
        console.log(msg);
        await notifyAll(msg);
      } catch (error) {
        failedCount++;
        const msg = `❌ Failed: ${key} - ${error}`;
        console.error(msg);
        await notifyAll(msg);
      }
    }
  }
  
  return { uploadedCount, failedCount };
}

// Main function
async function main() {
  try {
    console.log("🚀 Starting file upload to Cloudflare R2...");
    const result = await uploadAllFiles("data", R2_BUCKET_NAME!);
    
    const summary = `🎉 Upload completed! Success: ${result.uploadedCount}, Failed: ${result.failedCount}`;
    console.log(summary);
    await notifyAll(summary);
  } catch (err: any) {
    const msg = `❌ Upload total gagal: ${err.message}`;
    console.error(msg);
    await notifyAll(msg);
  }
}

main();