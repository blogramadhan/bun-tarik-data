import { S3Client, PutObjectCommand, CreateMultipartUploadCommand, UploadPartCommand, CompleteMultipartUploadCommand, AbortMultipartUploadCommand } from '@aws-sdk/client-s3';
import { createReadStream, createReadStream as fsCreateReadStream } from 'fs';
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
    requestTimeout: 300000, // 5 menit
    connectionTimeout: 60000, // 1 menit
  },
  maxAttempts: 3,
});

const bucketName = envConfig.R2_BUCKET_NAME;
const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB chunks

/**
 * Upload file besar menggunakan multipart upload
 */
async function uploadLargeFile(filePath: string, key: string): Promise<void> {
  const fileSize = (await stat(filePath)).size;
  console.log(`📦 Upload file besar: ${key} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);

  try {
    // 1. Inisialisasi multipart upload
    const multipartUpload = await s3Client.send(
      new CreateMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
      })
    );

    const uploadId = multipartUpload.UploadId;
    if (!uploadId) throw new Error('Upload ID tidak ditemukan');

    console.log(`🔄 Multipart upload dimulai dengan ID: ${uploadId}`);

    // 2. Upload parts
    const parts: { ETag: string; PartNumber: number }[] = [];
    const numParts = Math.ceil(fileSize / CHUNK_SIZE);

    for (let partNumber = 1; partNumber <= numParts; partNumber++) {
      const start = (partNumber - 1) * CHUNK_SIZE;
      const end = Math.min(start + CHUNK_SIZE, fileSize);
      const partSize = end - start;

      console.log(`📤 Uploading part ${partNumber}/${numParts} (${(partSize / 1024 / 1024).toFixed(2)} MB)`);

      const fileStream = createReadStream(filePath, { start, end: end - 1 });
      
      const uploadPartResponse = await s3Client.send(
        new UploadPartCommand({
          Bucket: bucketName,
          Key: key,
          UploadId: uploadId,
          PartNumber: partNumber,
          Body: fileStream,
          ContentLength: partSize,
        })
      );

      if (uploadPartResponse.ETag) {
        parts.push({
          ETag: uploadPartResponse.ETag,
          PartNumber: partNumber,
        });
      }
    }

    // 3. Complete multipart upload
    await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: bucketName,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: { Parts: parts },
      })
    );

    console.log(`✅ File besar berhasil diupload: ${key}`);
  } catch (error) {
    console.error(`❌ Gagal upload file besar ${key}:`, error);
    throw error;
  }
}

/**
 * Upload file kecil menggunakan single upload
 */
async function uploadSmallFile(filePath: string, key: string): Promise<void> {
  const fileSize = (await stat(filePath)).size;
  console.log(`📤 Upload file kecil: ${key} (${(fileSize / 1024 / 1024).toFixed(2)} MB)`);

  const fileStream = createReadStream(filePath);
  
  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: key,
    Body: fileStream,
    ContentLength: fileSize,
  });

  await s3Client.send(command);
  console.log(`✅ File kecil berhasil diupload: ${key}`);
}

/**
 * Fungsi untuk mengupload file ke Cloudflare R2 dengan retry logic
 */
async function uploadFileToR2(filePath: string, key: string, retries: number = 3): Promise<void> {
  try {
    const fileSize = (await stat(filePath)).size;
    const isLargeFile = fileSize > 100 * 1024 * 1024; // 100MB threshold

    if (isLargeFile) {
      await uploadLargeFile(filePath, key);
    } else {
      await uploadSmallFile(filePath, key);
    }
  } catch (error) {
    console.error(`❌ Gagal mengupload ${key}:`, error);
    
    if (retries > 0) {
      console.log(`🔄 Retry upload ${key} (${retries} retry tersisa)...`);
      await new Promise(resolve => setTimeout(resolve, 3000));
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
 * Fungsi untuk mengupload semua file di folder dan subfolder secara recursive
 */
export async function uploadFolderToR2(folderPath: string, prefix: string = ''): Promise<void> {
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
      const keyPath = prefix ? `${prefix}/${relativePath}` : relativePath;
      uploadedFiles++;
      
      console.log(`📄 [${uploadedFiles}/${allFiles.length}] Uploading: ${relativePath}`);
      
      try {
        await uploadFileToR2(fullPath, keyPath);
      } catch (error) {
        failedFiles.push(relativePath);
        console.error(`💥 Gagal upload ${relativePath}:`, error);
      }
    }

    if (failedFiles.length > 0) {
      console.log(`⚠️  ${failedFiles.length} file gagal diupload:`, failedFiles);
    }

    console.log(`🎉 Upload folder selesai: ${folderPath}`);
    console.log(`📊 Statistik: ${allFiles.length - failedFiles.length}/${allFiles.length} file berhasil diupload`);
  } catch (error) {
    console.error(`💥 Gagal mengupload folder ${folderPath}:`, error);
    throw error;
  }
}

// Contoh penggunaan:
// uploadFolderToR2('./data', 'backup/data');
uploadFolderToR2('./data'); 