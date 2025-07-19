export const daerahList = [
    "D197", "D199", "D202", "D552", "D200", 
    "D206", "D205", "D204", "D198", "D211", 
    "D210", "D209", "D201", "D50", "D236"
] as const;

export type Daerah = typeof daerahList[number];

export const jenisDataTypes = [
    "Bela-TokoDaringRealisasi"
] as const;

export type JenisData = typeof jenisDataTypes[number];

export const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "D197": { // PROV. KALIMANTAN BARAT
        "Bela-TokoDaringRealisasi": { apiKey: "4cf1694f-5bd9-4560-bc29-dbcab4ba98d4", kode: "3352" }
    },
    "D199": { // Kota Pontianak
        "Bela-TokoDaringRealisasi": { apiKey: "c69061fd-b7cb-4b08-9899-a36f832c473b", kode: "4020" }
    },
    "D202": { // Kab Kubu Raya
        "Bela-TokoDaringRealisasi": { apiKey: "416ee583-b4d7-4dbe-93d4-50dd8c7dbbe0", kode: "4180" }
    },
    "D552": { // Kab Mempawah
        "Bela-TokoDaringRealisasi": { apiKey: "439e2245-6cab-444a-a89a-d73688e64821", kode: "12179" }
    },
    "D200": { // Kota Singkawang
        "Bela-TokoDaringRealisasi": { apiKey: "d55b5f1b-c39b-4323-a480-4e4012f46158", kode: "9722" },
    },
    "D206": { // Kab Bengkayang
        "Bela-TokoDaringRealisasi": { apiKey: "2405dd62-8cac-4dfb-9eee-a8255523b1ce", kode: "8115" },
    },
    "D205": { // Kab Landak
        "Bela-TokoDaringRealisasi": { apiKey: "670ddb72-5d58-40f2-bc60-c63c59e8cf61", kode: "9572" },
    },
    "D204": { // Kab Sanggau
        "Bela-TokoDaringRealisasi": { apiKey: "52da3794-8b33-483a-81e6-0a0f29f99c30", kode: "4143" },
    },
    "D198": { // Kab Sekadau
        "Bela-TokoDaringRealisasi": { apiKey: "a96f0b54-1034-4ec1-a788-72cbcbe2a1f9", kode: "11083" },
    },
    "D211": { // Kab Sintang
        "Bela-TokoDaringRealisasi": { apiKey: "bd13b918-c784-4c9d-abac-c333d3297ae4", kode: "12130" },
    },
    "D210": { // Kab Melawi
        "Bela-TokoDaringRealisasi": { apiKey: "eb7960ba-2a1d-4f6c-9faa-eb1be53ca602", kode: "10574" },
    },
    "D209": { // Kab Kapuas Hulu
        "Bela-TokoDaringRealisasi": { apiKey: "9538ac44-082d-4da8-b71b-85baaf56354f", kode: "8986" },
    },
    "D201": { // Kab Ketapang
        "Bela-TokoDaringRealisasi": { apiKey: "7e5f0d13-a675-4f85-8ece-db677d9575f3", kode: "11577" },
    },
    "D50": { // Kab Tanggerang
        "Bela-TokoDaringRealisasi": { apiKey: "9db47be6-905f-4bed-a85a-f1743727a46a", kode: "3056" },
    },
    "D236": { // Kab Katingan
        "Bela-TokoDaringRealisasi": { apiKey: "2f731e7e-c8c5-4bd4-a67d-528e87e98bf2", kode: "9376" },
    },
}; 