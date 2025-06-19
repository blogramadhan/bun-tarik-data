export const daerahList = [
    "D197", "D199", "D202", "D552", "D200", 
    "D206", "D205", "D204", "D198", "D211", 
    "D210", "D209", "D201", "D50"] as const;

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
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "0fa6d8cd-c932-45a9-9a88-2fd56ec9fdf4", kode: "4172" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "77d2c841-d3eb-4c1b-a6ba-be1b08bbad2f", kode: "4173" },
        "RUP-StrukturAnggaranPD": { apiKey: "1def2d96-2fc3-455b-a58c-ec15f9236c59", kode: "6995" },
        "RUP-MasterSatker": { apiKey: "45a41800-d7b9-4707-ac0a-4815f443f273", kode: "4855" },
        "RUP-ProgramMaster": { apiKey: "54946e21-5bf5-4daf-aed9-fcec771d8356", kode: "4174" },
        "RUP-KegiatanMaster": { apiKey: "5e61c88f-da68-440c-81d3-428a2fe6cf44", kode: "4163" },
        "RUP-SubKegiatanMaster": { apiKey: "902d63e5-1047-4c84-96fe-e83fa4258005", kode: "4155" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "d175bb7e-894f-408f-be43-837b0f8533b7", kode: "4178" }
    },
    "D552": { // Kab Mempawah
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "684468d5-3be7-4325-8776-ebea873859d5", kode: "12191" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "497d58d3-8dee-4555-b5c1-0b1a53115299", kode: "12159" },
        "RUP-StrukturAnggaranPD": { apiKey: "dc613579-3241-4371-9c2e-a2f95b893283", kode: "12196" },
        "RUP-MasterSatker": { apiKey: "6a2fa946-fd62-44e6-9d1f-9c3c9b5f9ee4", kode: "12171" },
        "RUP-ProgramMaster": { apiKey: "e0fedd5d-288f-466f-bd44-bcd570b911a8", kode: "12169" },
        "RUP-KegiatanMaster": { apiKey: "5cc5ef08-ce37-4b63-b78d-fd56ee323565", kode: "12151" },
        "RUP-SubKegiatanMaster": { apiKey: "58dc4d1c-9dca-4587-8d0c-ee2e46e0b6b8", kode: "12160" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "ee3d9887-927d-4edf-9020-14b20d4d123c", kode: "12161" }
    },
    "D200": { // Kota Singkawang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "3d8a90e3-c62c-467b-a874-c97d4467a272", kode: "9734" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "6bd16793-733b-449d-8665-66c6f0867864", kode: "9698" },
        "RUP-StrukturAnggaranPD": { apiKey: "df277aee-4eac-4e44-957a-783dd3a66f27", kode: "9730" },
        "RUP-MasterSatker": { apiKey: "7755339c-82cb-4ed5-b033-f78bf56b04b7", kode: "9713" },
        "RUP-ProgramMaster": { apiKey: "59b401fe-d1cf-4b2c-80c2-f9745703f53f", kode: "9711" },
        "RUP-KegiatanMaster": { apiKey: "9f934500-7b74-4d82-b40c-3440b2f4c6eb", kode: "9691" },
        "RUP-SubKegiatanMaster": { apiKey: "e5898f11-22c3-4311-9314-e8101322b4d1", kode: "9700" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "87c771c6-744f-4a40-8b49-6a587c2da3d2", kode: "9701" }
    },
    "D206": { // Kab Bengkayang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "150a43e4-a51b-4999-a8c5-72a8de03602a", kode: "8127" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "8c2ef7fa-8c41-452f-a213-92bf8bca8521", kode: "8091" },
        "RUP-StrukturAnggaranPD": { apiKey: "19254004-fca9-4cf3-8717-6c6f179c8671", kode: "8123" },
        "RUP-MasterSatker": { apiKey: "9ea15884-c36b-43cf-a73d-29e6897182ac", kode: "8106" },
        "RUP-ProgramMaster": { apiKey: "cdc7ff31-a16b-4d03-af9a-0ad532b6706f", kode: "8104" },
        "RUP-KegiatanMaster": { apiKey: "27bd3cd4-b772-454a-a11a-f3aa3cfec410", kode: "8084" },
        "RUP-SubKegiatanMaster": { apiKey: "29926865-cf96-40fa-87ef-71497a53c9b8", kode: "8093" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "2cfd0532-aa67-4103-96ad-be996369339f", kode: "8094" }
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