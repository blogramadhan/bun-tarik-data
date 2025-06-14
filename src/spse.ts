import { mkdirSync, writeFileSync, readdirSync, existsSync } from "fs";
import { join, dirname } from "path";
import * as duckdb from "duckdb";

const tahunList = [2023, 2024, 2025];
const jenisDataTypes = [
    "SPSE-TenderPengumuman", 
    "SPSE-TenderSelesai", 
    "SPSE-TenderSelesaiNilai", 
    "SPSE-TenderEkontrak-SPPBJ",
    "SPSE-TenderEkontrak-Kontrak", 
    "SPSE-TenderEkontrak-SPMKSPP", 
    "SPSE-TenderEkontrak-BAPBAST",
    "SPSE-NonTenderPengumuman",
    "SPSE-NonTenderSelesai",
    "SPSE-NonTenderEkontrak-SPPBJ",
    "SPSE-NonTenderEkontrak-Kontrak",
    "SPSE-NonTenderEkontrak-SPMKSPP",
    "SPSE-NonTenderEkontrak-BAPBAST",
    "SPSE-PencatatanNonTender",
    "SPSE-PencatatanNonTenderRealisasi",
    "SPSE-PencatatanSwakelola",
    "SPSE-PencatatanSwakelolaRealisasi",
    "SPSE-PesertaTender",
] as const;
type JenisData = typeof jenisDataTypes[number];
const daerahList = ["97"] as const;
type Daerah = typeof daerahList[number];

// Konfigurasi API untuk setiap daerah dan jenis data
const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "97": {
        "SPSE-TenderPengumuman": { apiKey: "2a7b43bc-e129-4432-98c0-870a8bb61096", kode: "3339" },
        "SPSE-TenderSelesai": { apiKey: "dc58375a-199b-4696-b1a1-f17e36e580e8", kode: "3347" },
        "SPSE-TenderSelesaiNilai": { apiKey: "3c675a2d-0ef8-4190-9471-4471783d5d83", kode: "3338" },
        "SPSE-TenderEkontrak-SPPBJ": { apiKey: "df4f428b-1044-44ba-8e7e-21ce86ad52b9", kode: "5843" },
        "SPSE-TenderEkontrak-Kontrak": { apiKey: "a9e5b43f-20f6-45df-84a9-8909ad4ad719", kode: "5493" },
        "SPSE-TenderEkontrak-SPMKSPP": { apiKey: "0517feff-8aec-4834-8610-1ea2f170c1f2", kode: "6043" },
        "SPSE-TenderEkontrak-BAPBAST": { apiKey: "585d9bbb-831a-48e1-b705-1c3b7437315d", kode: "5943" },
        "SPSE-NonTenderPengumuman": { apiKey: "d1b11a11-29cc-4665-962e-8aea10c00e33", kode: "3337" },
        "SPSE-NonTenderSelesai": { apiKey: "8898bed0-3d2a-4ce7-ae26-2d87f6792447", kode: "3334" },
        "SPSE-NonTenderEkontrak-SPPBJ": { apiKey: "4172a748-bddc-448b-8b82-77b211257665", kode: "6660" },
        "SPSE-NonTenderEkontrak-Kontrak": { apiKey: "74de6a0d-446c-463a-bf57-bc3713d5a1da", kode: "6552" },
        "SPSE-NonTenderEkontrak-SPMKSPP": { apiKey: "a93fa824-9c8e-480d-9deb-ff69f4bcd8bf", kode: "6876" },
        "SPSE-NonTenderEkontrak-BAPBAST": { apiKey: "3a4b4b3c-0f26-4f19-8e19-5c07cf145ef6", kode: "6768" },
        "SPSE-PencatatanNonTender": { apiKey: "f2f596dc-b82e-4eab-86f2-67f802a1a76c", kode: "3353" },
        "SPSE-PencatatanNonTenderRealisasi": { apiKey: "ea865d3e-3e94-4f29-8e54-5b45e4a5d3d7", kode: "3354" },
        "SPSE-PencatatanSwakelola": { apiKey: "db1bb5c0-d80c-4d47-a478-e17fccb0e302", kode: "3355" },
        "SPSE-PencatatanSwakelolaRealisasi": { apiKey: "d4538ef1-6212-4e2d-8e1f-f60591a593ff", kode: "3356" },
        "SPSE-PesertaTender": { apiKey: "1812800c-bfed-437d-bee9-26ed8b0fd955", kode: "3951" }
    },
};

// Membuat URL API berdasarkan parameter
function buildURL(daerah: Daerah, jenis: JenisData, tahun: number): string {
    const baseUrl = "https://isb.lkpp.go.id/isb-2/api";
    
    if (!configMap[daerah]) throw new Error(`Daerah tidak dikenal: ${daerah}`);
    const config = configMap[daerah][jenis];
    if (!config) throw new Error(`Jenis data tidak dikenal untuk daerah ${daerah}: ${jenis}`);
    
    return `${baseUrl}/${config.apiKey}/json/${config.kode}/${jenis}/tipe/4:4/parameter/${tahun}:${daerah}`;
}

// Konversi file JSON ke format Parquet
async function convertJsonToParquet() {
    console.log("🔄 Memulai konversi JSON ke Parquet...");
    
    const dataDir = "data/spse";
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
                    const folder = `data/spse/${daerah}/${jenis}/${tahun}`;
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