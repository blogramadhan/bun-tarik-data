export const daerahList = [
    "D197", "D199", "D202", "D552", "D200", 
    "D206", "D205", "D204", "D198", "D211", 
    "D210", "D209", "D201", "D50", "D236"
] as const;

export type Daerah = typeof daerahList[number];

export const jenisDataTypes = [
    "Ecat-PaketEPurchasingV6"
] as const;

export type JenisData = typeof jenisDataTypes[number];

export const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "D197": { // PROV. KALIMANTAN BARAT
        "Ecat-PaketEPurchasingV6": { apiKey: "972895ec-519b-40a6-a612-0f7dc4f7191a", kode: "30960" }
    },
    "D199": { // Kota Pontianak
        "Ecat-PaketEPurchasingV6": { apiKey: "ce71f858-7a09-4c5f-a95b-9f03aa5eb519", kode: "30964" }
    },
    "D202": { // Kab Kubu Raya
        "Ecat-PaketEPurchasingV6": { apiKey: "4f171b6e-5e4b-4f07-a8eb-9895aa6b7938", kode: "30979" }
    },
    "D552": { // Kab Mempawah
        "Ecat-PaketEPurchasingV6": { apiKey: "35d43f22-8025-43cb-b5fe-f36fec6d6856", kode: "31088" }
    },
    "D200": { // Kota Singkawang
        "Ecat-PaketEPurchasingV6": { apiKey: "9d2299c2-a9c6-4fa1-bb9c-868565766b26", kode: "31054" },
    },
    "D206": { // Kab Bengkayang
        "Ecat-PaketEPurchasingV6": { apiKey: "da4b531b-3dd3-4490-ae19-5043d1bcf9a7", kode: "31019" },
    },
    "D205": { // Kab Landak
        "Ecat-PaketEPurchasingV6": { apiKey: "948b2797-4f5c-4fe0-a575-84f11941750c", kode: "31042" },
    },
    "D204": { // Kab Sanggau
        "Ecat-PaketEPurchasingV6": { apiKey: "75511006-6cfe-407b-898f-24c452c241fc", kode: "30963" },
    },
    "D198": { // Kab Sekadau
        "Ecat-PaketEPurchasingV6": { apiKey: "65c63a0a-cbb7-442d-97b6-0abee30994d1", kode: "31070" },
    },
    "D211": { // Kab Sintang
        "Ecat-PaketEPurchasingV6": { apiKey: "8113005e-fcb6-44c0-aed5-94c9776ef88f", kode: "31096" },
    },
    "D210": { // Kab Melawi
        "Ecat-PaketEPurchasingV6": { apiKey: "828986d3-b398-4f7d-82ff-ad3fc67fc276", kode: "31059" },
    },
    "D209": { // Kab Kapuas Hulu
        "Ecat-PaketEPurchasingV6": { apiKey: "a715dca3-e7c4-4e48-b907-7a7b328e2985", kode: "31034" },
    },
    "D201": { // Kab Ketapang
        "Ecat-PaketEPurchasingV6": { apiKey: "ad505a5f-58e8-4626-9572-8a6cb64ecadd", kode: "31086" },
    },
    "D50": { // Kab Tanggerang
        "Ecat-PaketEPurchasingV6": { apiKey: "2f2895f7-f360-4afc-ad2d-06f6b431114b", kode: "30933" },
    },
    "D236": { // Kab Katingan
        "Ecat-PaketEPurchasingV6": { apiKey: "a61ee84c-3b75-4bcd-be29-14e78186131a", kode: "31056" },
    }
}; 