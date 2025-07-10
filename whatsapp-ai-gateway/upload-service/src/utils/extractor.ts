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
        // Gunakan pdftotext dengan opsi layout dan nopgbrk untuk mempertahankan struktur dokumen
        // dan menghindari pemisahan paragraf
        await execAsync(`pdftotext -layout -nopgbrk -enc UTF-8 "${filePath}" "${outputPath}"`);
        const pdfText = readFileSync(outputPath, 'utf8');
        
        // Hitung jumlah halaman
        const { stdout: pageCountOutput } = await execAsync(`pdfinfo "${filePath}" | grep Pages | awk '{print $2}'`);
        const pageCount = parseInt(pageCountOutput.trim()) || 'tidak diketahui';
        
        // Tambahkan metadata file yang lebih lengkap
        return `# Dokumen: ${fileName}.pdf
# Tanggal Ekstraksi: ${new Date().toISOString()}
# Jumlah Halaman: ${pageCount}

${pdfText}`;
        
      case 'docx':
        // Gunakan pandoc dengan opsi yang lebih baik untuk docx termasuk penanganan tabel
        try {
          await execAsync(`pandoc "${filePath}" -f docx -t plain --wrap=none --extract-media=./media -o "${outputPath}"`);
          const docxText = readFileSync(outputPath, 'utf8');
          return `# Dokumen: ${fileName}.docx
# Tanggal Ekstraksi: ${new Date().toISOString()}

${docxText}`;
        } catch (err) {
          console.error(`Error saat mengekstrak docx dengan pandoc:`, err);
          return `Tidak dapat mengekstrak teks dari file docx. Pastikan pandoc terinstal.`;
        }
        
      case 'doc':
        // Untuk file doc lama, coba dengan antiword jika tersedia
        try {
          await execAsync(`antiword "${filePath}" > "${outputPath}"`);
          const docText = readFileSync(outputPath, 'utf8');
          return `# Dokumen: ${fileName}.doc
# Tanggal Ekstraksi: ${new Date().toISOString()}

${docText}`;
        } catch (docErr) {
          // Fallback ke pandoc jika antiword gagal
          try {
            await execAsync(`pandoc "${filePath}" -f doc -t plain --wrap=none -o "${outputPath}"`);
            const docText = readFileSync(outputPath, 'utf8');
            return `# Dokumen: ${fileName}.doc
# Tanggal Ekstraksi: ${new Date().toISOString()}

${docText}`;
          } catch {
            return `Tidak dapat mengekstrak teks dari file doc. Pastikan antiword atau pandoc terinstal.`;
          }
        }
        
      case 'txt':
        // Langsung baca file teks dengan deteksi encoding
        try {
          const txtText = readFileSync(filePath, 'utf8');
          return `# Dokumen: ${fileName}.txt
# Tanggal Ekstraksi: ${new Date().toISOString()}

${txtText}`;
        } catch (err) {
          // Coba dengan encoding lain jika UTF-8 gagal
          try {
            const txtText = readFileSync(filePath, 'latin1');
            return `# Dokumen: ${fileName}.txt
# Tanggal Ekstraksi: ${new Date().toISOString()}

${txtText}`;
          } catch {
            return `Error saat membaca file teks: ${err}`;
          }
        }

      case 'pptx':
        // Ekstrak teks dari PowerPoint dengan opsi tambahan
        try {
          await execAsync(`pandoc "${filePath}" -f pptx -t plain --wrap=none --extract-media=./media -o "${outputPath}"`);
          const pptxText = readFileSync(outputPath, 'utf8');
          return `# Presentasi: ${fileName}.pptx
# Tanggal Ekstraksi: ${new Date().toISOString()}

${pptxText}`;
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
          return `# Spreadsheet: ${fileName}.${extension}
# Tanggal Ekstraksi: ${new Date().toISOString()}

${excelText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file ${extension}. Pastikan gnumeric terinstal.`;
        }
        
      case 'jpg':
      case 'jpeg':
      case 'png':
        // OCR dengan tesseract dengan opsi tambahan untuk meningkatkan akurasi
        try {
          // Pra-proses gambar untuk meningkatkan kualitas OCR
          const preprocessedPath = `${filePath}.preprocessed.png`;
          await execAsync(`convert "${filePath}" -colorspace gray -normalize -sharpen 0x1 "${preprocessedPath}"`);
          
          // Gunakan OEM 1 (LSTM) dan PSM 3 (fully automatic page segmentation)
          await execAsync(`tesseract "${preprocessedPath}" "${filePath.replace(/\.[^/.]+$/, '')}" -l ind+eng --oem 1 --psm 3`);
          const ocrText = readFileSync(`${filePath.replace(/\.[^/.]+$/, '')}.txt`, 'utf8');
          return `# Gambar: ${fileName}.${extension}
# Tanggal Ekstraksi: ${new Date().toISOString()}
# Metode: OCR (Optical Character Recognition)

${ocrText}`;
        } catch (err) {
          console.error(`Error saat OCR gambar:`, err);
          return `[Gambar: ${fileName}.${extension}] - OCR tidak tersedia atau gagal. Pastikan tesseract-ocr dan imagemagick terinstal.`;
        }
        
      case 'html':
      case 'htm':
        // Ekstrak teks dari HTML dengan opsi tambahan
        try {
          await execAsync(`pandoc "${filePath}" -f html -t plain --wrap=none --extract-media=./media -o "${outputPath}"`);
          const htmlText = readFileSync(outputPath, 'utf8');
          return `# Dokumen HTML: ${fileName}.${extension}
# Tanggal Ekstraksi: ${new Date().toISOString()}

${htmlText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file HTML. Pastikan pandoc terinstal.`;
        }
        
      case 'rtf':
        // Ekstrak teks dari RTF
        try {
          await execAsync(`pandoc "${filePath}" -f rtf -t plain --wrap=none -o "${outputPath}"`);
          const rtfText = readFileSync(outputPath, 'utf8');
          return `# Dokumen RTF: ${fileName}.rtf
# Tanggal Ekstraksi: ${new Date().toISOString()}

${rtfText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file RTF. Pastikan pandoc terinstal.`;
        }
        
      case 'odt':
      case 'ods':
      case 'odp':
        // Ekstrak teks dari format OpenDocument
        try {
          await execAsync(`pandoc "${filePath}" -t plain --wrap=none --extract-media=./media -o "${outputPath}"`);
          const odText = readFileSync(outputPath, 'utf8');
          return `# Dokumen OpenDocument: ${fileName}.${extension}
# Tanggal Ekstraksi: ${new Date().toISOString()}

${odText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file ${extension}. Pastikan pandoc terinstal.`;
        }
        
      default:
        // Coba ekstrak dengan pandoc sebagai fallback untuk format lain
        try {
          await execAsync(`pandoc "${filePath}" -t plain --wrap=none -o "${outputPath}"`);
          const genericText = readFileSync(outputPath, 'utf8');
          return `# Dokumen: ${fileName}.${extension || 'tidak dikenal'}
# Tanggal Ekstraksi: ${new Date().toISOString()}

${genericText}`;
        } catch {
          return `Tidak dapat mengekstrak teks dari file dengan format ${extension || 'tidak dikenal'}.`;
        }
    }
  } catch (error) {
    console.error(`Error saat ekstraksi ${filePath}:`, error);
    return `Error saat mengekstrak teks: ${error}`;
  }
} 