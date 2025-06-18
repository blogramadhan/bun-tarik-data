export const daerahList = ["D197"] as const;
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