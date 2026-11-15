import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// 开发时把 /api 代理到后端 7100；构建产物由后端 express 静态托管
export default defineConfig({
  plugins: [react()],
  server: {
    port: 7200,
    proxy: {
      '/api': 'http://localhost:7100',
    },
  },
  build: {
    outDir: 'dist',
  },
});
