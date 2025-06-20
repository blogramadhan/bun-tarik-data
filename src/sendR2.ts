import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createReadStream } from 'fs';
import { readdir, stat } from 'fs/promises';
import path from 'path';
import { envConfig, validateEnvConfig } from './env.config';

// Validasi environment variables sebelum membuat client
if (!validateEnvConfig()) {
  process.exit(1);
}

// Konfigurasi untuk Cloudflare R2 dengan timeout yang lebih besar
const s3Client = new S3Client({
  region: 'auto',
  endpoint: envConfig.R2_ENDPOINT,
  credentials: {
    accessKeyId: envConfig.R2_ACCESS_KEY_ID,
    secretAccessKey: envConfig.R2_SECRET_ACCESS_KEY,
  },
  requestHandler: {
    // Timeout yang lebih besar untuk file besar
    requestTimeout: 300000, // 5 menit
    connectionTimeout: 60000, // 1 menit
  },
  maxAttempts: 3, // Retry hingga 3 kali
});

const bucketName = envConfig.R2_BUCKET_NAME;

/**
 * Fungsi untuk mengupload file ke Cloudflare R2 dengan retry logic
 * @param filePath Path file yang akan diupload
 * @param key Nama key di R2
 * @param retries Jumlah retry yang tersisa
 */
async function uploadFileToR2(filePath: string, key: string, retries: number = 3): Promise<void> {
  try {
    const fileSize = (await stat(filePath)).size;
    console.log(`📤 Mengupload: ${key} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);

    // Untuk file besar (> 50MB), gunakan streaming dengan chunk
    if (fileSize > 50 * 1024 * 1024) {
      console.log(`📦 File besar terdeteksi, menggunakan chunked upload...`);
    }

    const fileStream = createReadStream(filePath);
    
    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: fileStream,
      ContentLength: fileSize,
    });

    await s3Client.send(command);
    console.log(`✅ Berhasil mengupload: ${key}`);
  } catch (error) {
    console.error(`❌ Gagal mengupload ${key}:`, error);
    
    if (retries > 0) {
      console.log(`🔄 Retry upload ${key} (${retries} retry tersisa)...`);
      // Tunggu sebentar sebelum retry
      await new Promise(resolve => setTimeout(resolve, 2000));
      return uploadFileToR2(filePath, key, retries - 1);
    }
    
    throw error;
  }
}

/**
 * Fungsi untuk mengupload folder ke Cloudflare R2 (semua file)
 * @param folderPath Path folder yang akan diupload
 * @param prefix Prefix untuk key di R2 (opsional)
 */
export async function uploadFolderToR2(folderPath: string, prefix: string = ''): Promise<void> {
  try {
    console.log(`📁 Memulai upload folder: ${folderPath}`);
    
    const files = await readdir(folderPath, { withFileTypes: true });
    const totalFiles = files.filter(f => f.isFile()).length;
    let uploadedFiles = 0;
    let failedFiles = 0;

    if (totalFiles === 0 && files.filter(f => f.isDirectory()).length === 0) {
      console.log(`ℹ️  Tidak ada file atau folder ditemukan di ${folderPath}`);
      return;
    }

    console.log(`📊 Ditemukan ${totalFiles} file untuk diupload`);

    for (const file of files) {
      const fullPath = path.join(folderPath, file.name);
      const keyPath = prefix ? `${prefix}/${file.name}` : file.name;

      if (file.isDirectory()) {
        // Jika direktori, rekursif upload folder tersebut
        console.log(`📂 Memproses folder: ${file.name}`);
        await uploadFolderToR2(fullPath, keyPath);
      } else {
        // Upload semua jenis file
        uploadedFiles++;
        console.log(`📄 [${uploadedFiles}/${totalFiles}] Uploading: ${file.name}`);
        try {
          await uploadFileToR2(fullPath, keyPath);
        } catch (error) {
          failedFiles++;
          console.error(`💥 Gagal mengupload ${file.name}:`, error);
        }
      }
    }

    console.log(`🎉 Upload folder selesai: ${folderPath}`);
    console.log(`📊 Statistik: ${uploadedFiles - failedFiles}/${totalFiles} file berhasil diupload`);
  } catch (error) {
    console.error(`💥 Gagal mengupload folder ${folderPath}:`, error);
    throw error;
  }
}

// Contoh penggunaan:
// uploadFolderToR2('./data', 'backup/data');
uploadFolderToR2('./data');
