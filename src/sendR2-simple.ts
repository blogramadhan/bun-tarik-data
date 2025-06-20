import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import { createReadStream } from 'fs';
import { readdir, stat } from 'fs/promises';
import path from 'path';
import { envConfig, validateEnvConfig } from './env.config';

// Validasi environment variables sebelum membuat client
if (!validateEnvConfig()) {
  process.exit(1);
}

// Konfigurasi untuk Cloudflare R2 dengan timeout yang sangat besar
const s3Client = new S3Client({
  region: 'auto',
  endpoint: envConfig.R2_ENDPOINT,
  credentials: {
    accessKeyId: envConfig.R2_ACCESS_KEY_ID,
    secretAccessKey: envConfig.R2_SECRET_ACCESS_KEY,
  },
  requestHandler: {
    requestTimeout: 600000, // 10 menit
    connectionTimeout: 120000, // 2 menit
  },
  maxAttempts: 5, // Retry hingga 5 kali
});

const bucketName = envConfig.R2_BUCKET_NAME;

/**
 * Fungsi untuk mengupload file ke Cloudflare R2 dengan retry logic yang agresif
 */
async function uploadFileToR2(filePath: string, key: string, retries: number = 5): Promise<void> {
  try {
    const fileSize = (await stat(filePath)).size;
    console.log(`📤 Mengupload: ${key} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);

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
      // Tunggu lebih lama sebelum retry
      await new Promise(resolve => setTimeout(resolve, 5000));
      return uploadFileToR2(filePath, key, retries - 1);
    }
    
    throw error;
  }
}

/**
 * Fungsi untuk mencari semua file secara recursive
 */
async function findAllFiles(dir: string, baseDir: string = dir): Promise<string[]> {
  const files: string[] = [];
  
  try {
    const items = await readdir(dir, { withFileTypes: true });
    
    for (const item of items) {
      const fullPath = path.join(dir, item.name);
      
      if (item.isDirectory()) {
        // Recursive search di subfolder
        const subFiles = await findAllFiles(fullPath, baseDir);
        files.push(...subFiles);
      } else {
        // Hitung relative path dari base directory
        const relativePath = path.relative(baseDir, fullPath);
        files.push(relativePath);
      }
    }
  } catch (error) {
    console.error(`❌ Error membaca folder ${dir}:`, error);
  }
  
  return files;
}

/**
 * Fungsi untuk mengupload semua file di folder dan subfolder
 */
export async function uploadAllFiles(folderPath: string): Promise<void> {
  try {
    console.log(`📁 Memulai pencarian file di: ${folderPath}`);
    
    // Cari semua file secara recursive
    const allFiles = await findAllFiles(folderPath);
    
    if (allFiles.length === 0) {
      console.log(`ℹ️  Tidak ada file ditemukan di ${folderPath}`);
      return;
    }

    console.log(`📊 Ditemukan ${allFiles.length} file untuk diupload`);
    
    let uploadedFiles = 0;
    let failedFiles: string[] = [];

    // Upload setiap file
    for (const relativePath of allFiles) {
      const fullPath = path.join(folderPath, relativePath);
      uploadedFiles++;
      
      console.log(`📄 [${uploadedFiles}/${allFiles.length}] Uploading: ${relativePath}`);
      
      try {
        await uploadFileToR2(fullPath, relativePath);
      } catch (error) {
        failedFiles.push(relativePath);
        console.error(`💥 Gagal upload ${relativePath}:`, error);
      }
    }

    if (failedFiles.length > 0) {
      console.log(`⚠️  ${failedFiles.length} file gagal diupload:`, failedFiles);
    }

    console.log(`🎉 Upload selesai: ${folderPath}`);
    console.log(`📊 Statistik: ${allFiles.length - failedFiles.length}/${allFiles.length} file berhasil diupload`);
  } catch (error) {
    console.error(`💥 Gagal mengupload folder ${folderPath}:`, error);
    throw error;
  }
}

// Contoh penggunaan:
// uploadAllFiles('./data');
uploadAllFiles('./data'); 