export const daerahList = [
    "D197", "D199", "D202", "D552", "D200", 
    "D206", "D205", "D204", "D198", "D211", 
    "D210", "D209", "D201", "D50"
] as const;

export type Daerah = typeof daerahList[number];

export const jenisDataTypes = [
    "Ecat-PaketEPurchasing",
    "Ecat-KomoditasDetail",
    "Ecat-PenyediaDetail",
    "Ecat-InstansiSatker"
] as const;

export type JenisData = typeof jenisDataTypes[number];

export const configMap: Record<Daerah, Record<string, { apiKey: string, kode: string }>> = {
    "D197": { // PROV. KALIMANTAN BARAT
        "Ecat-PaketEPurchasing": { apiKey: "eeb14303-22d7-4193-8793-60bbfdb468fe", kode: "3330" },
        "Ecat-KomoditasDetail": { apiKey: "af9a6323-71f0-4ddf-843f-ba7052637b28", kode: "3336" },
        "Ecat-PenyediaDetail": { apiKey: "93c2af98-02e3-483c-b597-e0a790004e90", kode: "3328" },
        "Ecat-InstansiSatker": { apiKey: "a20ef982-c41b-49bf-8c07-cc710c465f47", kode: "3329" }
    },
    "D199": { // Kota Pontianak
        "Ecat-PaketEPurchasing": { apiKey: "7fa493f6-70d5-40c9-9d00-111803341e3c", kode: "3998" },
        "Ecat-KomoditasDetail": { apiKey: "28cb5723-64ef-499f-8d7a-61d3c9673b24", kode: "4004" },
        "Ecat-PenyediaDetail": { apiKey: "f3e3f052-b6a9-418f-b40c-469ff6123974", kode: "3996" },
        "Ecat-InstansiSatker": { apiKey: "fd0c026b-ee03-4169-8ee7-7f95ae83986a", kode: "3997" }    
    },
    "D202": { // Kab Kubu Raya
        "Ecat-PaketEPurchasing": { apiKey: "04b0200f-408e-4e10-849f-907a514473ac", kode: "4159" },
        "Ecat-KomoditasDetail": { apiKey: "1a0731bc-28ce-4c32-a653-0b8b87c2ff1c", kode: "4165" },
        "Ecat-PenyediaDetail": { apiKey: "da5053cb-9cb4-40b3-be8b-86c73384e2be", kode: "4157" },
        "Ecat-InstansiSatker": { apiKey: "c0109c8e-f0bf-4f9e-9282-0ec5407de014", kode: "4158" }
    },
    "D552": { // Kab Mempawah
        "Ecat-PaketEPurchasing": { apiKey: "61fe0bb0-0a33-4dbe-bd04-4f06968395a2", kode: "12180" },
        "Ecat-KomoditasDetail": { apiKey: "7899785b-4b94-4382-8bb3-1c0966cd4401", kode: "12163" },
        "Ecat-PenyediaDetail": { apiKey: "ff05f138-0576-4847-ad15-6b7cdaffcc2a", kode: "12153" },
        "Ecat-InstansiSatker": { apiKey: "d78399c8-c0c7-40fa-9456-477452b289bf", kode: "12154" }
    },
    "D200": { // Kota Singkawang
        "Ecat-PaketEPurchasing": { apiKey: "d541dcb9-b7c9-4452-8be8-91fc31303d88", kode: "9708" },
        "Ecat-KomoditasDetail": { apiKey: "1a71d679-4215-42c8-b41c-7100325b91d8", kode: "9703" },
        "Ecat-PenyediaDetail": { apiKey: "5a685946-dac0-46e4-a32f-561c71f783e1", kode: "9693" },
        "Ecat-InstansiSatker": { apiKey: "9aca569e-b613-4878-8cef-440510881c0c", kode: "9694" }
    },
    "D206": { // Kab Bengkayang
        "Ecat-PaketEPurchasing": { apiKey: "960b1e03-c0c6-4524-a493-c638b48d5e15", kode: "8098" },
        "Ecat-KomoditasDetail": { apiKey: "ed650b8c-220d-4ee9-84ff-e87f1c47ae06", kode: "8096" },
        "Ecat-PenyediaDetail": { apiKey: "e295924f-56cf-4a5e-afb8-10671b286fb2", kode: "8086" },
        "Ecat-InstansiSatker": { apiKey: "4c99f371-b0d5-4316-ab74-79808afe1e54", kode: "8087" }
    },
    "D205": { // Kab Landak
        "Ecat-PaketEPurchasing": { apiKey: "dd080d11-d98d-4799-8d8e-bd76b68945ba", kode: "9558" },
        "Ecat-KomoditasDetail": { apiKey: "89d78ed2-bf18-4f2d-b5a5-e441bf0771b2", kode: "9553" },
        "Ecat-PenyediaDetail": { apiKey: "8f159dee-5560-4dbd-9f2f-ccdbdeff3142", kode: "9543" },
        "Ecat-InstansiSatker": { apiKey: "6cc8d2b2-0a13-4826-837f-0c6ec66ffa89", kode: "9544" }
    },
    "D204": { // Kab Sanggau
        "Ecat-PaketEPurchasing": { apiKey: "33c53132-422b-4b66-88d0-19e548e10228", kode: "4121" },
        "Ecat-KomoditasDetail": { apiKey: "4d7e020c-6491-41dd-9e97-51e6a2d96c48", kode: "4127" },
        "Ecat-PenyediaDetail": { apiKey: "63f5a5df-3446-4940-8853-3a4e425a65fe", kode: "4119" },
        "Ecat-InstansiSatker": { apiKey: "1b17240a-ed46-4091-b04d-958b1f5c3403", kode: "4120" }
    },
    "D198": { // Kab Sekadau
        "Ecat-PaketEPurchasing": { apiKey: "6a2183ca-0e86-4a6b-aaa5-c1ebf428602b", kode: "11084" },
        "Ecat-KomoditasDetail": { apiKey: "7b0dee27-c8a8-4101-9d65-60c51647f420", kode: "11065" },
        "Ecat-PenyediaDetail": { apiKey: "70942e46-d447-4660-b1a4-ef7b6a9d2d07", kode: "11055" },
        "Ecat-InstansiSatker": { apiKey: "1936d3dd-ff27-49f3-93a7-ba7c3b343731", kode: "11056" }
    },
    "D211": { // Kab Sintang
        "Ecat-PaketEPurchasing": { apiKey: "a73705ee-4c3b-40d3-8b34-f59c77a66104", kode: "12131" },
        "Ecat-KomoditasDetail": { apiKey: "d0763611-6035-42d3-8d97-beca006fbe3d", kode: "12114" },
        "Ecat-PenyediaDetail": { apiKey: "10ebcc4a-2145-4ec3-bfbe-d2923873ee7f", kode: "12104" },
        "Ecat-InstansiSatker": { apiKey: "0c6397c2-6e24-4e21-8096-3f4f7cecb35d", kode: "12105" }
    },
    "D210": { // Kab Melawi
        "Ecat-PaketEPurchasing": { apiKey: "2f4fd744-438f-48f2-adf3-d30053190065", kode: "10575" },
        "Ecat-KomoditasDetail": { apiKey: "e2c3129b-8aa1-4967-8573-5dcf171a91ef", kode: "10556" },
        "Ecat-PenyediaDetail": { apiKey: "34e2a35f-d5c6-48a1-95cc-ac5f050400b0", kode: "10546" },
        "Ecat-InstansiSatker": { apiKey: "96a010c4-1a9c-4d92-8227-b80f6b3eef2f", kode: "10547" }    
    },
    "D209": { // Kab Kapuas Hulu
        "Ecat-PaketEPurchasing": { apiKey: "8e639d6c-2b98-485c-8451-3bf59408a24d", kode: "8968" },
        "Ecat-KomoditasDetail": { apiKey: "466b064c-d1e2-48cb-85e1-186fae4d66c2", kode: "8965" },
        "Ecat-PenyediaDetail": { apiKey: "2d9445f5-6de6-415b-9f25-1035db257f62", kode: "8955" },
        "Ecat-InstansiSatker": { apiKey: "68db0686-bfb7-4d42-8861-9822396c7a8d", kode: "8956" }
    },
    "D201": { // Kab Ketapang
        "Ecat-PaketEPurchasing": { apiKey: "2293cc18-b341-4354-b89d-377f83578eab", kode: "11578" },
        "Ecat-KomoditasDetail": { apiKey: "40412205-fb49-4b90-bfc3-ea0c4aee6f48", kode: "11559" },
        "Ecat-PenyediaDetail": { apiKey: "18f20ef7-dfd5-4472-9125-680f5df3c6e2", kode: "11549" },
        "Ecat-InstansiSatker": { apiKey: "865790ed-4ef3-4894-b047-d960ea1055b7", kode: "11550" }
    },
    "D50": { // Kab Tanggerang
        "Ecat-PaketEPurchasing": { apiKey: "eb5ad422-da2c-45e7-994d-c72af6ab2699", kode: "2422" },
        "Ecat-KomoditasDetail": { apiKey: "a9f68efa-d479-487c-af02-cb3b1272b19c", kode: "2428" },
        "Ecat-PenyediaDetail": { apiKey: "3eec651f-3d17-420c-bfb0-0516d3e662c1", kode: "2420" },
        "Ecat-InstansiSatker": { apiKey: "bd06c716-cf67-4b1a-aab2-7a4eb59eca78", kode: "2421" }
    }
}; 