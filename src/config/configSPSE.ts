export const daerahList = ["97"] as const;
export type Daerah = typeof daerahList[number];

export const jenisDataTypes = [
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
export type JenisData = typeof jenisDataTypes[number];

export const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
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