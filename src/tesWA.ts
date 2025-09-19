import axios from 'axios';

await axios.post('http://localhost:8788/send-message', {
  number: '62811577280',
  message: 'Pesan dikirim dari file lain!'
});