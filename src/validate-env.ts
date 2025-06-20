#!/usr/bin/env bun

import { validateEnvConfig } from './env.config';

console.log('🔍 Memvalidasi environment variables...\n');

if (validateEnvConfig()) {
  console.log('\n🎉 Validasi berhasil! Environment variables sudah siap digunakan.');
  process.exit(0);
} else {
  console.log('\n❌ Validasi gagal! Silakan periksa file .env Anda.');
  process.exit(1);
} 