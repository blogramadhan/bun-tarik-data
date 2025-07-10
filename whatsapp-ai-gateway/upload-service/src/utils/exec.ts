import { exec } from 'child_process';
import { promisify } from 'util';

// Mengubah exec menjadi Promise-based
export const execAsync = promisify(exec); 