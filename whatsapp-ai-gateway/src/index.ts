import { serve } from '@hono/node-server';
import app from './upload_api';
import './whatsapp';

serve({ fetch: app.fetch, port: 8787 });

console.log('📚 Web & WhatsApp Gateway aktif di http://localhost:8787');
