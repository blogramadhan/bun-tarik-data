import { readFileSync } from 'fs';
import { execSync } from 'child_process';

export async function extractTextFromFile(path: string): Promise<string> {
  const ext = path.split('.').pop()?.toLowerCase();
  if (ext === 'pdf') {
    const outTxt = path.replace('.pdf', '.txt');
    execSync(`pdftotext "${path}" "${outTxt}"`);
    return readFileSync(outTxt, 'utf8');
  }
  if (ext === 'json') {
    const raw = readFileSync(path, 'utf8');
    return JSON.stringify(JSON.parse(raw), null, 2);
  }
  return `Ekstensi .${ext} belum didukung.`;
}