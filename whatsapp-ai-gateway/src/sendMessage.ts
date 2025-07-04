import axios from 'axios';

const number = '6281234567890';
const message = '📣 Ini adalah pesan biasa, bukan AI.';

(async () => {
  try {
    const res = await axios.post('http://localhost:8787/send-message', {
      number,
      message
    });

    if (res.data.status) {
      console.log('✅ Pesan berhasil dikirim');
    } else {
      console.error('❌ Gagal:', res.data.error);
    }
  } catch (err: any) {
    console.error('❌ Error:', err.message);
  }
})();