export const daerahList = [
    "D197", "D199", "D204", "D206", "D209", "D50"
] as const;

export type Daerah = typeof daerahList[number];

export const jenisDataTypes = [
    "SIKaP-PenilaianKinerjaPenyedia-Tender",
    "SiKAP-PenilaianKinerjaPenyedia-NonTender"
] as const;

export type JenisData = typeof jenisDataTypes[number];

export const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "D197": { // PROV. KALIMANTAN BARAT
        "SIKaP-PenilaianKinerjaPenyedia-Tender": { apiKey: "0ad35ce0-2237-48c3-918c-66e84668db58", kode: "7045" },
        "SiKAP-PenilaianKinerjaPenyedia-NonTender": { apiKey: "4d59aa07-998c-4803-93a6-ba349fa68750", kode: "7047" }
    },
    "D199": { // Kota Pontianak
        "SIKaP-PenilaianKinerjaPenyedia-Tender": { apiKey: "597d6a7b-9c7a-4d88-8f1b-02f4b17f98f4", kode: "8441" },
        "SiKAP-PenilaianKinerjaPenyedia-NonTender": { apiKey: "5af24483-8395-4810-ae8e-f8c89da69e56", kode: "8442" }
    },
    "D206": { // Kab Bengkayang
        "SIKaP-PenilaianKinerjaPenyedia-Tender": { apiKey: "423538cd-cd09-4339-ba5a-f173159aebe3", kode: "12025" },
        "SiKAP-PenilaianKinerjaPenyedia-NonTender": { apiKey: "d8745e6d-de12-4596-803e-05ad0a0c1a3b", kode: "12024" }
    },
    "D204": { // Kab Sanggau
        "SIKaP-PenilaianKinerjaPenyedia-Tender": { apiKey: "a07e40d6-5349-4401-9783-24e51c868d5f", kode: "12554" },
        "SiKAP-PenilaianKinerjaPenyedia-NonTender": { apiKey: "7b2c5665-f46c-445d-a3c7-c809f5ae4748", kode: "12553" }
    },
    "D209": { // Kab Kapuas Hulu
        "SIKaP-PenilaianKinerjaPenyedia-Tender": { apiKey: "d0249b80-c18c-4f82-876f-9a5cbde1df1a", kode: "12030" },
        "SiKAP-PenilaianKinerjaPenyedia-NonTender": { apiKey: "683f68a2-9a32-46c6-bd70-23fe922a3628", kode: "12029" }
    },
    "D50": { // Kab Tanggerang
        "SIKaP-PenilaianKinerjaPenyedia-Tender": { apiKey: "4fc8a3cd-6ef1-4998-86ac-f373b22687e8", kode: "4071" },
        "SiKAP-PenilaianKinerjaPenyedia-NonTender": { apiKey: "debad062-ea1c-4418-8d0e-2158224402c0", kode: "4072" }
    }
}; 