export const daerahList = ["D197", "D199", "D202", "D552", "D200", "D206", "D205", "D204", "D198", "D211", "D210", "D209", "D201", "D50"] as const;
export type Daerah = typeof daerahList[number];

export const jenisDataTypes = [
    "RUP-PaketPenyedia-Terumumkan", 
    "RUP-PaketSwakelola-Terumumkan", 
    "RUP-StrukturAnggaranPD", 
    "RUP-MasterSatker", 
    "RUP-ProgramMaster", 
    "RUP-KegiatanMaster", 
    "RUP-SubKegiatanMaster", 
    "RUP-PaketAnggaranPenyedia"
] as const;
export type JenisData = typeof jenisDataTypes[number];

export const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "D197": { // PROV. KALIMANTAN BARAT
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "999bd6d6-9e67-4c7d-83bd-650430ce2fe7", kode: "3342" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "07f8350f-d005-42ce-bcaf-a39eaf3fbb02", kode: "3345" },
        "RUP-StrukturAnggaranPD": { apiKey: "3adfa365-7962-4994-8bce-4e6ca5e10320", kode: "6987" },
        "RUP-MasterSatker": { apiKey: "ba2c6327-9451-49c9-8c61-408936baaff6", kode: "4847" },
        "RUP-ProgramMaster": { apiKey: "6d5fd703-2fbe-44fe-8b93-a88ecaaacab3", kode: "3346" },
        "RUP-KegiatanMaster": { apiKey: "024e7c91-226e-417d-be1a-1667a84595ee", kode: "3333" },
        "RUP-SubKegiatanMaster": { apiKey: "d5c9a703-07bb-4e87-8e08-ff04b23741b9", kode: "3325" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "05fe5f87-9547-4a56-991d-041433864211", kode: "3350" }
    },
    "D199": { // Kota Pontianak
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "c6bcddde-9f61-4aeb-814a-667800878a08", kode: "4010" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "a19f3701-4b9d-4d36-ac86-2f0fd7a08f71", kode: "4013" },
        "RUP-StrukturAnggaranPD": { apiKey: "8974bad3-4a6c-439f-a408-ea3e28cd5772", kode: "6993" },
        "RUP-MasterSatker": { apiKey: "3ceb56ea-aa95-4803-94c1-3bb9ed95756e", kode: "4853" },
        "RUP-ProgramMaster": { apiKey: "9786b715-9732-4519-b30a-bf8130a81fa5", kode: "4014" },
        "RUP-KegiatanMaster": { apiKey: "e417f5ee-d504-4de0-a03d-6084fb521da7", kode: "4001" },
        "RUP-SubKegiatanMaster": { apiKey: "9660f0a0-2cc2-4c6e-899b-35dad4da6d47", kode: "3993" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "3d6345b1-9ff4-46f9-bb14-b1e54786b9e2", kode: "4018" }
    },
    "D202": { // Kab Kubu Raya
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D552": { // Kab Mempawah
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D200": { // Kota Singkawang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D206": { // Kab Bengkayang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D205": { // Kab Landak
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D204": { // Kab Sanggau
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D198": { // Kab Sekadau
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D211": { // Kab Sintang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D210": { // Kab Melawi
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D209": { // Kab Kapuas Hulu
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D201": { // Kab Ketapang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
    "D50": { // Kab Tenggerang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "", kode: "" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "", kode: "" },
        "RUP-StrukturAnggaranPD": { apiKey: "", kode: "" },
        "RUP-MasterSatker": { apiKey: "", kode: "" },
        "RUP-ProgramMaster": { apiKey: "", kode: "" },
        "RUP-KegiatanMaster": { apiKey: "", kode: "" },
        "RUP-SubKegiatanMaster": { apiKey: "", kode: "" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "", kode: "" }
    },
}; 