import { mkdirSync, writeFileSync, readdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import * as duckdb from "duckdb";
import { daerahList, jenisDataTypes, configMap, type Daerah, type JenisData } from "./config/configKATALOG";

const tahunList = [2023, 2024, 2025];

// Membuat URL API berdasarkan parameter
function buildURL(daerah: Daerah, jenis: JenisData, params: { tahun?: number, kodeKomoditas?: string }): string {
    const baseUrl = "https://isb.lkpp.go.id/isb-2/api";
    
    if (!configMap[daerah]) throw new Error(`Daerah tidak dikenal: ${daerah}`);
    const config = configMap[daerah][jenis];
    if (!config) throw new Error(`Jenis data tidak dikenal untuk daerah ${daerah}: ${jenis}`);

    let parameter = "";
    if (jenis === "Ecat-KomoditasDetail") {
        if (!params.kodeKomoditas) throw new Error("Kode komoditas diperlukan untuk detail komoditas");
        parameter = `4/parameter/${params.kodeKomoditas}`;
    } else {
        if (!params.tahun) throw new Error("Tahun diperlukan untuk data paket");
        parameter = `4:12/parameter/${params.tahun}:${daerah}`;
    }
    
    return `${baseUrl}/${config.apiKey}/json/${config.kode}/${jenis}/tipe/${parameter}`;
}

// Konversi file JSON ke format Parquet
async function convertJsonToParquet() {
    console.log("🔄 Memulai konversi JSON ke Parquet...");
    
    const dataDir = "data/katalog";
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
    
    const dataDir = "data/katalog";
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
    // Ambil data paket e-purchasing terlebih dahulu
    for (const daerah of daerahList) {
        for (const tahun of tahunList) {
            console.log(`🔄 Mengambil Ecat-PaketEPurchasing ${tahun} untuk ${daerah} ...`);
            try {
                // Ambil data dari API
                const url = buildURL(daerah as Daerah, "Ecat-PaketEPurchasing", { tahun });
                const res = await fetch(url);
                if (!res.ok) throw new Error(`Gagal fetch: ${res.status}`);
                const response = await res.json() as { data?: any[] } | any[];
                
                // Pastikan data adalah array
                const data = Array.isArray(response) ? response : response.data || [];
                if (!data.length) {
                    console.log(`⚠️ Tidak ada data untuk ${daerah}/Ecat-PaketEPurchasing/${tahun}`);
                    continue;
                }

                // Simpan data ke file JSON
                const folder = `data/katalog/${daerah}/Ecat-PaketEPurchasing/${tahun}`;
                mkdirSync(folder, { recursive: true });
                const jsonPath = join(folder, "data.json");
                writeFileSync(jsonPath, JSON.stringify(data, null, 2));
                console.log(`✅ JSON disimpan: ${jsonPath}`);

                // Kumpulkan kd_komoditas unik dari data yang baru diambil
                const uniqueKodeKomoditas = new Set<string>();
                data.forEach(item => {
                    if (item.kd_komoditas) {
                        uniqueKodeKomoditas.add(item.kd_komoditas);
                    }
                });

                // Ambil detail komoditas untuk setiap kd_komoditas unik
                console.log(`🔄 Mengambil detail untuk ${uniqueKodeKomoditas.size} komoditas untuk ${daerah} tahun ${tahun}...`);
                const komoditasDetails: Record<string, any> = {};

                for (const kodeKomoditas of uniqueKodeKomoditas) {
                    try {
                        const url = buildURL(daerah as Daerah, "Ecat-KomoditasDetail", { kodeKomoditas });
                        const res = await fetch(url);
                        if (!res.ok) throw new Error(`Gagal fetch: ${res.status}`);
                        const data = await res.json();
                        komoditasDetails[kodeKomoditas] = data;
                        // console.log(`✅ Detail komoditas ${kodeKomoditas} berhasil diambil`);
                    } catch (err: any) {
                        console.error(`❌ Gagal mengambil detail komoditas ${kodeKomoditas}:`, err.message);
                    }
                }

                // Simpan detail komoditas ke file JSON dengan struktur direktori yang sama
                const komoditasFolder = `data/katalog/${daerah}/Ecat-KomoditasDetail/${tahun}`;
                mkdirSync(komoditasFolder, { recursive: true });
                const komoditasPath = join(komoditasFolder, "data.json");
                writeFileSync(komoditasPath, JSON.stringify(komoditasDetails, null, 2));
                console.log(`✅ Detail komoditas disimpan: ${komoditasPath}`);

            } catch (err: any) {
                console.error(`❌ Gagal: ${daerah}/Ecat-PaketEPurchasing/${tahun} =>`, err.message);
            }
        }
    }
    
    // Konversi semua file JSON ke Parquet dan Excel
    await convertJsonToParquet();
    await convertJsonToExcel();
}

// Jalankan program
fetchAndSave();