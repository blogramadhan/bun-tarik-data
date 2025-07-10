import { exec } from 'child_process';
import { promisify } from 'util';
import { readFileSync, writeFileSync } from 'fs';
import path from 'path';

const execAsync = promisify(exec);

/**
 * Ekstrak teks dari berbagai jenis file dengan peningkatan kualitas
 * @param filePath Path ke file yang akan diekstrak
 * @returns Teks yang diekstrak dari file
 */
export async function extractTextFromFile(filePath: string): Promise<string> {
  const extension = filePath.split('.').pop()?.toLowerCase();
  const fileName = path.basename(filePath, path.extname(filePath));
  const outputPath = `${filePath}.txt`;
  
  try {
    switch (extension) {
      case 'pdf':
        // Gunakan pdftotext dengan opsi layout untuk mempertahankan struktur dokumen
        await execAsync(`pdftotext -layout -enc UTF-8 "${filePath}" "${outputPath}"`);
        const pdfText = readFileSync(outputPath, 'utf8');
        
        // Tambahkan metadata file
        return `# Dokumen: ${fileName}.pdf\n\n${pdfText}`;
        
      case 'docx':
        // Gunakan pandoc dengan opsi yang lebih baik untuk docx
        try {
          await execAsync(`pandoc "${filePath}" -f docx -t plain --wrap=none -o "${outputPath}"`);
          const docxText = readFileSync(outputPath, 'utf8');
          return `# Dokumen: ${fileName}.docx\n\n${docxText}`;
        } catch (err) {
          console.error(`Error saat mengekstrak docx dengan pandoc:`, err);
          return `Tidak dapat mengekstrak teks dari file docx. Pastikan pandoc terinstal.`;
        }
        
      case 'doc':
        // Untuk file doc lama, coba dengan antiword jika tersedia
        try {
          await execAsync(`antiword "${filePath}" > "${outputPath}"`);
          const docText = readFileSync(outputPath, 'utf8');
          return `# Dokumen: ${fileName}.doc\n\n${docText}`;
        } catch (docErr) {
          // Fallback ke pandoc jika antiword gagal
          try {
            await execAsync(`pandoc "${filePath}" -f doc -t plain --wrap=none -o "${outputPath}"`);
            const docText = readFileSync(outputPath, 'utf8');
            return `# Dokumen: ${fileName}.doc\n\n${docText}`;
          } catch {
            return `Tidak dapat mengekstrak teks dari file doc. Pastikan antiword atau pandoc terinstal.`;
          }
        }
        
      case 'txt':
        // Langsung baca file teks dengan deteksi encoding
        try {
          const txtText = readFileSync(filePath, 'utf8');
          return `# Dokumen: ${fileName}.txt\n\n${txtText}`;
        } catch (err) {
          // Coba dengan encoding lain jika UTF-8 gagal
          try {
            const txtText = readFileSync(filePath, 'latin1');
            return `# Dokumen: ${fileName}.txt\n\n${txtText}`;
          } catch {
            return `Error saat membaca file teks: ${err}`;
          }
        }

      case 'pptx':
        // Ekstrak teks dari PowerPoint
        try {
          await execAsync(`pandoc "${filePath}" -f pptx -t plain --wrap=none -o "${outputPath}"`);
          const pptxText = readFileSync(outputPath, 'utf8');
          return `# Presentasi: ${fileName}.pptx\n\n${pptxText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file pptx. Pastikan pandoc terinstal.`;
        }
        
      case 'xlsx':
      case 'xls':
        // Ekstrak teks dari Excel (memerlukan ssconvert dari gnumeric)
        try {
          const csvPath = `${filePath}.csv`;
          await execAsync(`ssconvert "${filePath}" "${csvPath}"`);
          const excelText = readFileSync(csvPath, 'utf8');
          return `# Spreadsheet: ${fileName}.${extension}\n\n${excelText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file ${extension}. Pastikan gnumeric terinstal.`;
        }
        
      case 'jpg':
      case 'jpeg':
      case 'png':
        // OCR dengan tesseract jika tersedia
        try {
          await execAsync(`tesseract "${filePath}" "${filePath.replace(/\.[^/.]+$/, '')}" -l ind+eng`);
          const ocrText = readFileSync(`${filePath.replace(/\.[^/.]+$/, '')}.txt`, 'utf8');
          return `# Gambar: ${fileName}.${extension}\n\n${ocrText}`;
        } catch {
          return `[Gambar: ${fileName}.${extension}] - OCR tidak tersedia atau gagal. Pastikan tesseract-ocr terinstal.`;
        }
        
      case 'html':
      case 'htm':
        // Ekstrak teks dari HTML
        try {
          await execAsync(`pandoc "${filePath}" -f html -t plain --wrap=none -o "${outputPath}"`);
          const htmlText = readFileSync(outputPath, 'utf8');
          return `# Dokumen HTML: ${fileName}.${extension}\n\n${htmlText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file HTML. Pastikan pandoc terinstal.`;
        }
        
      case 'rtf':
        // Ekstrak teks dari RTF
        try {
          await execAsync(`pandoc "${filePath}" -f rtf -t plain --wrap=none -o "${outputPath}"`);
          const rtfText = readFileSync(outputPath, 'utf8');
          return `# Dokumen RTF: ${fileName}.rtf\n\n${rtfText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file RTF. Pastikan pandoc terinstal.`;
        }
        
      case 'odt':
      case 'ods':
      case 'odp':
        // Ekstrak teks dari format OpenDocument
        try {
          await execAsync(`pandoc "${filePath}" -t plain --wrap=none -o "${outputPath}"`);
          const odText = readFileSync(outputPath, 'utf8');
          return `# Dokumen OpenDocument: ${fileName}.${extension}\n\n${odText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file ${extension}. Pastikan pandoc terinstal.`;
        }
        
      default:
        // Coba ekstrak dengan pandoc sebagai fallback untuk format lain
        try {
          await execAsync(`pandoc "${filePath}" -t plain --wrap=none -o "${outputPath}"`);
          const genericText = readFileSync(outputPath, 'utf8');
          return `# Dokumen: ${fileName}.${extension || 'tidak dikenal'}\n\n${genericText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file dengan format ${extension || 'tidak dikenal'}.`;
        }
    }
  } catch (error) {
    console.error(`Error saat ekstraksi ${filePath}:`, error);
    return `Error saat mengekstrak teks: ${error}`;
  }
} 