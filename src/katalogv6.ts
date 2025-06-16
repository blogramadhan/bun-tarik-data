import { mkdirSync, writeFileSync, readdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import * as duckdb from "duckdb";

const tahunList = [2023, 2024, 2025];
const jenisDataTypes = [
    "RUP-PaketPenyedia-Terumumkan", 
    "RUP-PaketSwakelola-Terumumkan", 
    "RUP-StrukturAnggaranPD", 
    "RUP-MasterSatker", 
    "RUP-ProgramMaster", 
    "RUP-KegiatanMaster", 
    "RUP-SubKegiatanMaster", 
    "RUP-PaketAnggaranPenyedia"
] as const;
type JenisData = typeof jenisDataTypes[number];
const daerahList = ["D197"] as const;
type Daerah = typeof daerahList[number];

// Konfigurasi API untuk setiap daerah dan jenis data
const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "D197": {
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "999bd6d6-9e67-4c7d-83bd-650430ce2fe7", kode: "3342" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "07f8350f-d005-42ce-bcaf-a39eaf3fbb02", kode: "3345" },
        "RUP-StrukturAnggaranPD": { apiKey: "3adfa365-7962-4994-8bce-4e6ca5e10320", kode: "6987" },
        "RUP-MasterSatker": { apiKey: "ba2c6327-9451-49c9-8c61-408936baaff6", kode: "4847" },
        "RUP-ProgramMaster": { apiKey: "6d5fd703-2fbe-44fe-8b93-a88ecaaacab3", kode: "3346" },
        "RUP-KegiatanMaster": { apiKey: "024e7c91-226e-417d-be1a-1667a84595ee", kode: "3333" },
        "RUP-SubKegiatanMaster": { apiKey: "d5c9a703-07bb-4e87-8e08-ff04b23741b9", kode: "3325" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "05fe5f87-9547-4a56-991d-041433864211", kode: "3350" }
    },
};

// Membuat URL API berdasarkan parameter
function buildURL(daerah: Daerah, jenis: JenisData, tahun: number): string {
    const baseUrl = "https://isb.lkpp.go.id/isb-2/api";
    
    if (!configMap[daerah]) throw new Error(`Daerah tidak dikenal: ${daerah}`);
    const config = configMap[daerah][jenis];
    if (!config) throw new Error(`Jenis data tidak dikenal untuk daerah ${daerah}: ${jenis}`);
    
    return `${baseUrl}/${config.apiKey}/json/${config.kode}/${jenis}/tipe/4:12/parameter/${tahun}:${daerah}`;
}

// Konversi file JSON ke format Parquet
async function convertJsonToParquet() {
    console.log("🔄 Memulai konversi JSON ke Parquet...");
    
    const dataDir = "data/rup";
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
    for (const daerah of daerahList) {
        for (const jenis of jenisDataTypes) {
            for (const tahun of tahunList) {
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

                    // Simpan data ke file JSON
                    const folder = `data/rup/${daerah}/${jenis}/${tahun}`;
                    mkdirSync(folder, { recursive: true });
                    const jsonPath = join(folder, "data.json");
                    writeFileSync(jsonPath, JSON.stringify(data, null, 2));
                    console.log(`✅ JSON disimpan: ${jsonPath}`);

                } catch (err: any) {
                    console.error(`❌ Gagal: ${daerah}/${jenis}/${tahun} =>`, err.message);
                }
            }
        }
    }
    
    // Konversi semua file JSON ke Parquet
    await convertJsonToParquet();
}

// Jalankan program
fetchAndSave();