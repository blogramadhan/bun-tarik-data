import { mkdirSync, writeFileSync, readdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import * as duckdb from "duckdb";
import { daerahList, jenisDataTypes, configMap, type Daerah, type JenisData } from "./config/configDARING";
import axios from "axios"; // Import axios untuk mengirim pesan WhatsApp

const tahunList = [2023, 2024, 2025];

// Membuat URL API berdasarkan parameter
function buildURL(daerah: Daerah, jenis: JenisData, tahun: number): string {
    const baseUrl = "https://isb.lkpp.go.id/isb-2/api";
    
    if (!configMap[daerah]) throw new Error(`Daerah tidak dikenal: ${daerah}`);
    const config = configMap[daerah][jenis];
    if (!config) throw new Error(`Jenis data tidak dikenal untuk daerah ${daerah}: ${jenis}`);
    
    return `${baseUrl}/${config.apiKey}/json/${config.kode}/${jenis}/tipe/12:4/parameter/${daerah}:${tahun}`;
}

// Konversi file JSON ke format Parquet
async function convertJsonToParquet() {
    console.log("🔄 Memulai konversi JSON ke Parquet...");
    
    const dataDir = "data/daring";
    if (!existsSync(dataDir)) {
        console.log("⚠️ Direktori data tidak ditemukan");
        return;
    }
    
    // Cari semua file JSON
    const jsonFiles = findJsonFiles(dataDir);
    console.log(`🔍 Ditemukan ${jsonFiles.length} file JSON untuk dikonversi`);
    
    const db = new duckdb.Database(':memory:');
    const conn = db.connect();
    
    for (const jsonFile of jsonFiles) {
        try {
            const parquetFile = jsonFile.replace('.json', '.parquet');
            mkdirSync(dirname(parquetFile), { recursive: true });
            
            conn.exec(`
                COPY (SELECT * FROM read_json('${jsonFile}', auto_detect=true))
                TO '${parquetFile}' (FORMAT 'PARQUET');
            `);
            
            console.log(`✅ Konversi berhasil: ${jsonFile} -> ${parquetFile}`);
        } catch (err: any) {
            console.error(`❌ Gagal mengkonversi ${jsonFile}: ${err.message}`);
        }
    }
    
    conn.close();
    db.close();
    console.log("✅ Konversi JSON ke Parquet selesai");
}

// Konversi file JSON ke format Excel
async function convertJsonToExcel() {
    console.log("🔄 Memulai konversi JSON ke Excel...");
    
    const dataDir = "data/daring";
    if (!existsSync(dataDir)) {
        console.log("⚠️ Direktori data tidak ditemukan");
        return;
    }
    
    // Cari semua file JSON
    const jsonFiles = findJsonFiles(dataDir);
    console.log(`🔍 Ditemukan ${jsonFiles.length} file JSON untuk dikonversi ke Excel`);
    
    const db = new duckdb.Database(':memory:');
    const conn = db.connect();
    
    for (const jsonFile of jsonFiles) {
        try {
            const excelFile = jsonFile.replace('.json', '.xlsx');
            mkdirSync(dirname(excelFile), { recursive: true });
            
            // Gunakan DuckDB untuk membaca JSON dan mengekspor ke Excel
            conn.exec(`
                INSTALL 'excel';
                LOAD 'excel';
                COPY (SELECT * FROM read_json('${jsonFile}', auto_detect=true))
                TO '${excelFile}' (FORMAT 'XLSX');
            `);
            
            console.log(`✅ Konversi berhasil: ${jsonFile} -> ${excelFile}`);
        } catch (err: any) {
            console.error(`❌ Gagal mengkonversi ${jsonFile} ke Excel: ${err.message}`);
        }
    }
    
    conn.close();
    db.close();
    console.log("✅ Konversi JSON ke Excel selesai");
}

// Mencari file JSON secara rekursif
function findJsonFiles(dir: string): string[] {
    let results: string[] = [];
    const items = readdirSync(dir, { withFileTypes: true });
    
    for (const item of items) {
        const fullPath = join(dir, item.name);
        if (item.isDirectory()) {
            results = results.concat(findJsonFiles(fullPath));
        } else if (item.name.endsWith('.json')) {
            results.push(fullPath);
        }
    }
    
    return results;
}

// Mengambil data dari API dan menyimpannya
async function fetchAndSave() {
    const currentYear = new Date().getFullYear();
    const currentMonth = new Date().getMonth() + 1; // getMonth() returns 0-11
    let totalDataFetched = 0; // Variabel untuk menghitung total data yang diambil
    let totalDataFailed = 0; // Variabel untuk menghitung total data yang gagal diambil
    let totalDataSkipped = 0; // Variabel untuk menghitung total data yang dilewati

    for (const daerah of daerahList) {
        for (const jenis of jenisDataTypes) {
            for (const tahun of tahunList) {
                // Cek apakah semua file sudah ada
                const allFilesExist = jenisDataTypes.every(jenis => {
                    const jsonPath = `data/daring/${daerah}/${jenis}/${tahun}/data.json`;
                    return existsSync(jsonPath);
                });

                // Skip jika:
                // 1. Semua file sudah ada
                // 2. Bukan tahun berjalan
                // 3. Bukan tahun sebelumnya (jika sudah melewati Februari tahun berjalan)
                if (allFilesExist && 
                    !(tahun === currentYear || 
                      (tahun === currentYear - 1 && currentMonth <= 2))) {
                    console.log(`⏭️ Melewati data ${jenis} ${tahun} untuk ${daerah} (sudah lengkap)`);
                    totalDataSkipped++; // Tambahkan jumlah data yang dilewati
                    continue;
                }

                console.log(`🔄 Mengambil ${jenis} ${tahun} untuk ${daerah} ...`);
                try {
                    // Ambil data dari API
                    const url = buildURL(daerah as Daerah, jenis as JenisData, tahun);
                    const res = await fetch(url);
                    if (!res.ok) throw new Error(`Gagal fetch: ${res.status}`);
                    const response = await res.json() as { data?: any[] } | any[];
                    
                    // Pastikan data adalah array
                    const data = Array.isArray(response) ? response : response.data || [];
                    if (!data.length) {
                        console.log(`⚠️ Tidak ada data untuk ${daerah}/${jenis}/${tahun}`);
                        continue;
                    }

                    totalDataFetched++;

                    // Simpan data ke file JSON
                    const folder = `data/daring/${daerah}/${jenis}/${tahun}`;
                    mkdirSync(folder, { recursive: true });
                    const jsonPath = join(folder, "data.json");
                    writeFileSync(jsonPath, JSON.stringify(data, null, 2));
                    console.log(`✅ JSON disimpan: ${jsonPath}`);

                } catch (err: any) {
                    totalDataFailed++; // Tambahkan jumlah data yang gagal
                    console.error(`❌ Gagal: ${daerah}/${jenis}/${tahun} =>`, err.message);
                }
            }
        }
    }
    
    // Konversi semua file JSON ke Parquet dan Excel
    await convertJsonToParquet();
    await convertJsonToExcel();

    // Kirim notifikasi via WhatsApp setelah proses selesai
    const message = `🎉 Download data DARING selesai! Berhasil: ${totalDataFetched}, Gagal: ${totalDataFailed}, Dilewati: ${totalDataSkipped}`;
    await axios.post('http://localhost:3000/send-message', {
        number: process.env.WHATSAPP_NUMBER,
        message: message
    });
    console.log(message);
}

// Jalankan program
fetchAndSave();