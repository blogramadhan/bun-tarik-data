// Konfigurasi Environment Variables untuk Cloudflare R2
export const envConfig = {
  R2_ENDPOINT: process.env.R2_ENDPOINT || 'https://your-account-id.r2.cloudflarestorage.com',
  R2_ACCESS_KEY_ID: process.env.R2_ACCESS_KEY_ID || 'your-access-key-id',
  R2_SECRET_ACCESS_KEY: process.env.R2_SECRET_ACCESS_KEY || 'your-secret-access-key',
  R2_BUCKET_NAME: process.env.R2_BUCKET_NAME || 'your-bucket-name',
};

// Validasi environment variables
export function validateEnvConfig() {
  const requiredVars = [
    'R2_ENDPOINT',
    'R2_ACCESS_KEY_ID', 
    'R2_SECRET_ACCESS_KEY',
    'R2_BUCKET_NAME'
  ];

  const missingVars = requiredVars.filter(varName => {
    const value = process.env[varName];
    return !value || value === 'your-' + varName.toLowerCase().replace(/_/g, '-');
  });

  if (missingVars.length > 0) {
    console.error('❌ Environment variables yang diperlukan tidak ditemukan:');
    missingVars.forEach(varName => {
      console.error(`   - ${varName}`);
    });
    console.error('\n📝 Buat file .env dengan konfigurasi berikut:');
    console.error(`
      R2_ENDPOINT=https://your-account-id.r2.cloudflarestorage.com
      R2_ACCESS_KEY_ID=your-access-key-id
      R2_SECRET_ACCESS_KEY=your-secret-access-key
      R2_BUCKET_NAME=your-bucket-name
    `);
    return false;
  }

  console.log('✅ Semua environment variables sudah dikonfigurasi');
  return true;
} 