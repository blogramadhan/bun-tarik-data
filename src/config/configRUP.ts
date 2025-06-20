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
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "65cb52ac-b6b2-49b9-a1c7-e06afeb65a18", kode: "9584" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "f217a243-6a38-4c87-b8b0-b5e821bd75de", kode: "9548" },
        "RUP-StrukturAnggaranPD": { apiKey: "43d27e25-ab72-40bf-80b5-34f80e5e99e2", kode: "9580" },
        "RUP-MasterSatker": { apiKey: "3d4bafd9-8c53-40cc-b6f1-5d5399af1a86", kode: "9563" },
        "RUP-ProgramMaster": { apiKey: "1edec30f-8378-4be1-a52c-328321e608be", kode: "9561" },
        "RUP-KegiatanMaster": { apiKey: "e39265fa-b3d9-422e-892f-c77105beb2dd", kode: "9541" },
        "RUP-SubKegiatanMaster": { apiKey: "78fee66a-7941-498e-b58c-097f0cc9a9aa", kode: "9550" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "36ef05a8-0ccb-490d-be31-f3d47c39b0bd", kode: "9551" }
    },
    "D204": { // Kab Sanggau
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "5b67e125-cd9a-4a80-9418-25cf368e4987", kode: "4133" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "391e5397-b0b4-4959-bda9-71edc926f194", kode: "4136" },
        "RUP-StrukturAnggaranPD": { apiKey: "0421b816-6762-42a7-b8f1-840172bd4d06", kode: "6990" },
        "RUP-MasterSatker": { apiKey: "fa9d2a23-cd9a-4dad-a3ab-d6e30aad7e29", kode: "4850" },
        "RUP-ProgramMaster": { apiKey: "4abdab3f-d400-46bf-a7d4-246f4059c336", kode: "4137" },
        "RUP-KegiatanMaster": { apiKey: "1d52ae98-5104-490e-8333-7e29f211e0d8", kode: "4124" },
        "RUP-SubKegiatanMaster": { apiKey: "682a965a-5ef0-481b-9729-517a95f5c5fc", kode: "4116" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "a7af29e3-e7a2-4ee4-9d90-5b409e910d94", kode: "4141" }
    },
    "D198": { // Kab Sekadau
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "79cbc43a-07af-4cc0-af1b-32f350e48790", kode: "11097" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "b1ff7bc7-61ba-4473-99df-5776768c0c32", kode: "11061" },
        "RUP-StrukturAnggaranPD": { apiKey: "91a289a4-be79-46f8-88ab-bd170b6ef035", kode: "11092" },
        "RUP-MasterSatker": { apiKey: "8a3f94a9-5728-4050-9a0a-adcdd1f2c96b", kode: "11074" },
        "RUP-ProgramMaster": { apiKey: "46411d73-4af5-450a-8e65-5b7c5da4f5dc", kode: "11072" },
        "RUP-KegiatanMaster": { apiKey: "b44d33a1-5389-4a1b-89cd-766de8565af2", kode: "11053" },
        "RUP-SubKegiatanMaster": { apiKey: "9ea056a5-2d18-4f9b-a725-05e14e16c5a6", kode: "11062" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "c98f286c-50e4-4fbc-8920-c07a50946317", kode: "11063" }
    },
    "D211": { // Kab Sintang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "4c1a9e1f-63fa-4e90-9a92-d4fe263aa5ce", kode: "12142" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "b7b07bc0-8481-446f-a76b-bf8f8bf45f8e", kode: "12110" },
        "RUP-StrukturAnggaranPD": { apiKey: "b51fc850-69f1-449e-94b8-6aaa295ed9b9", kode: "12147" },
        "RUP-MasterSatker": { apiKey: "4610f52b-28c6-45ac-b6f9-4cd85a81b671", kode: "12122" },
        "RUP-ProgramMaster": { apiKey: "9b9cbb5e-9e61-4644-adad-7e22a3246698", kode: "12120" },
        "RUP-KegiatanMaster": { apiKey: "8aac33bc-002a-4f92-89bd-f7c18365e3a3", kode: "12102" },
        "RUP-SubKegiatanMaster": { apiKey: "c33fae90-52c5-4106-a14e-9715c7e97b36", kode: "12111" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "9640941b-032b-4858-a91c-259c1e703ec5", kode: "12112" }
    },
    "D210": { // Kab Melawi
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "0d286cd2-a099-404b-a986-c2bdffd8cfd7", kode: "10587" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "6cbc4e2f-1e08-4206-9377-c890445b93c7", kode: "10551" },
        "RUP-StrukturAnggaranPD": { apiKey: "68474130-7fde-46e8-9b37-2ffe58449075", kode: "10583" },
        "RUP-MasterSatker": { apiKey: "df38531c-b295-4ddc-84e9-90a0180de60c", kode: "10565" },
        "RUP-ProgramMaster": { apiKey: "fe54eaea-2367-4d1c-97d2-8f2f110fb12e", kode: "10563" },
        "RUP-KegiatanMaster": { apiKey: "06ddfd2e-db4e-4a9f-ac70-c346b21bfcbd", kode: "10544" },
        "RUP-SubKegiatanMaster": { apiKey: "bb1daf75-e4c4-453a-bc80-35b6aafc5c25", kode: "10553" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "775818f0-cfae-4cc7-b794-86ed9b5cdba9", kode: "10554" }
    },
    "D209": { // Kab Kapuas Hulu
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "a3431954-0899-48bc-b6d6-754ede274c28", kode: "8998" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "285f3d47-f937-405c-bddb-3038ce7d95a1", kode: "8960" },
        "RUP-StrukturAnggaranPD": { apiKey: "ca38d348-3942-4d73-966b-20b029058acf", kode: "8994" },
        "RUP-MasterSatker": { apiKey: "90e5f253-8d87-4af4-81d6-e890054c9ea1", kode: "8977" },
        "RUP-ProgramMaster": { apiKey: "57194f10-6c1e-45a4-9a4f-ab667a124168", kode: "8975" },
        "RUP-KegiatanMaster": { apiKey: "d6738261-cac1-4d8b-abe5-e267eff08e5b", kode: "8953" },
        "RUP-SubKegiatanMaster": { apiKey: "b70e609f-4c08-4ff3-83c6-2bff13ffc524", kode: "8962" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "c4432f2b-ea2d-49e9-92b2-e03d13ea65a9", kode: "8963" }
    },
    "D201": { // Kab Ketapang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "50405646-142c-4c6b-b580-b2c4902d398f", kode: "11591" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "eae2d830-4dbc-4f9c-b41e-1b99aa0e912e", kode: "11555" },
        "RUP-StrukturAnggaranPD": { apiKey: "74929947-0837-48f9-99c4-852ebc2fdaf1", kode: "11586" },
        "RUP-MasterSatker": { apiKey: "17287b83-4ff1-462b-ad63-0193a324089a", kode: "11568" },
        "RUP-ProgramMaster": { apiKey: "218aad1d-a649-42b4-80f5-bd5a2f139a2e", kode: "11566" },
        "RUP-KegiatanMaster": { apiKey: "d8663bfe-d238-4e0d-8324-afbf608dece3", kode: "11547" },
        "RUP-SubKegiatanMaster": { apiKey: "0bd0c5fb-171a-4c12-a2c0-f6fc1759337a", kode: "11556" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "b4e6bd3c-7f9b-4fd5-bee7-344c15b1fb05", kode: "11557" }
    },
    "D50": { // Kab Tenggerang
        "RUP-PaketPenyedia-Terumumkan": { apiKey: "6f0ac58a-bc3f-41bb-84b4-a1e47e2f7724", kode: "2435" },
        "RUP-PaketSwakelola-Terumumkan": { apiKey: "e426bac7-8a8d-4ee5-8afc-9a0031368d25", kode: "2438" },
        "RUP-StrukturAnggaranPD": { apiKey: "2ee1cf5d-54b2-4227-b8e8-11f51cfecb64", kode: "6956" },
        "RUP-MasterSatker": { apiKey: "2bf65146-9150-4611-9882-45dbca90a992", kode: "4816" },
        "RUP-ProgramMaster": { apiKey: "5892abd6-568a-4229-a7a7-ca6604488767", kode: "2439" },
        "RUP-KegiatanMaster": { apiKey: "dae1d103-c087-404c-94ed-f8ce5d2f1aa6", kode: "2425" },
        "RUP-SubKegiatanMaster": { apiKey: "7ab7d33f-d980-4c04-bedb-5ba56c417e94", kode: "2417" },
        "RUP-PaketAnggaranPenyedia": { apiKey: "5490829d-7eb0-4e67-9511-2432f95e0625", kode: "2443" }
    },
}; 