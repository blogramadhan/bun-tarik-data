import { exec } from 'child_process';
import { promisify } from 'util';
import { readFileSync } from 'fs';

const execAsync = promisify(exec);

/**
 * Ekstrak teks dari berbagai jenis file
 * @param filePath Path ke file yang akan diekstrak
 * @returns Teks yang diekstrak dari file
 */
export async function extractTextFromFile(filePath: string): Promise<string> {
  const extension = filePath.split('.').pop()?.toLowerCase();
  
  try {
    switch (extension) {
      case 'pdf':
        // Gunakan pdftotext (dari poppler-utils)
        await execAsync(`pdftotext -layout "${filePath}" "${filePath}.txt"`);
        return readFileSync(`${filePath}.txt`, 'utf8');
        
      case 'docx':
      case 'doc':
        // Gunakan pandoc jika tersedia
        try {
          await execAsync(`pandoc "${filePath}" -t plain -o "${filePath}.txt"`);
          return readFileSync(`${filePath}.txt`, 'utf8');
        } catch {
          return `Tidak dapat mengekstrak teks dari file ${extension}. Pastikan pandoc terinstal.`;
        }
        
      case 'txt':
        // Langsung baca file teks
        return readFileSync(filePath, 'utf8');
        
      case 'jpg':
      case 'jpeg':
      case 'png':
        // OCR bisa diimplementasikan di sini jika diperlukan
        return `[Gambar: ${filePath}] - OCR tidak tersedia`;
        
      default:
        return `Tidak dapat mengekstrak teks dari file dengan format ${extension || 'tidak dikenal'}.`;
    }
  } catch (error) {
    console.error(`Error saat ekstraksi ${filePath}:`, error);
    return `Error saat mengekstrak teks: ${error}`;
  }
} 