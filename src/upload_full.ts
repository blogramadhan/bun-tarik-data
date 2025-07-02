import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import { readdirSync, statSync, readFileSync } from "fs";
import { join } from "path";
import * as dotenv from "dotenv";

dotenv.config();

const {
  R2_ENDPOINT,
  R2_ACCESS_KEY_ID,
  R2_SECRET_ACCESS_KEY,
  R2_BUCKET_NAME,
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

// Kirim semua notifikasi
async function notifyAll(message: string) {
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