import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createReadStream } from 'fs';
import { readdir, stat } from 'fs/promises';
import path from 'path';
import { envConfig, validateEnvConfig } from './env.config';

// Validasi environment variables sebelum membuat client
if (!validateEnvConfig()) {
  process.exit(1);
}

// Konfigurasi untuk Cloudflare R2
const s3Client = new S3Client({
  region: 'auto',
  endpoint: envConfig.R2_ENDPOINT,
  credentials: {
    accessKeyId: envConfig.R2_ACCESS_KEY_ID,
    secretAccessKey: envConfig.R2_SECRET_ACCESS_KEY,
  },
});

const bucketName = envConfig.R2_BUCKET_NAME;

/**
 * Fungsi untuk mengupload file ke Cloudflare R2
 * @param filePath Path file yang akan diupload
 * @param key Nama key di R2
 */
async function uploadFileToR2(filePath: string, key: string): Promise<void> {
  try {
    const fileStream = createReadStream(filePath);
    const fileSize = (await stat(filePath)).size;

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: fileStream,
      ContentLength: fileSize,
    });

    await s3Client.send(command);
    console.log(`Berhasil mengupload: ${key}`);
  } catch (error) {
    console.error(`Gagal mengupload ${key}:`, error);
    throw error;
  }
}

/**
 * Fungsi untuk mengupload folder ke Cloudflare R2
 * @param folderPath Path folder yang akan diupload
 * @param prefix Prefix untuk key di R2 (opsional)
 */
export async function uploadFolderToR2(folderPath: string, prefix: string = ''): Promise<void> {
  try {
    const files = await readdir(folderPath, { withFileTypes: true });

    for (const file of files) {
      const fullPath = path.join(folderPath, file.name);
      const keyPath = prefix ? `${prefix}/${file.name}` : file.name;

      if (file.isDirectory()) {
        // Jika direktori, rekursif upload folder tersebut
        await uploadFolderToR2(fullPath, keyPath);
      } else {
        // Jika file, upload file tersebut
        await uploadFileToR2(fullPath, keyPath);
      }
    }

    console.log(`Folder ${folderPath} berhasil diupload ke R2 bucket: ${bucketName}`);
  } catch (error) {
    console.error(`Gagal mengupload folder ${folderPath}:`, error);
    throw error;
  }
}

// Contoh penggunaan:
// uploadFolderToR2('./data', 'backup/data');
