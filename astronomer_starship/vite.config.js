/* eslint-disable import/no-extraneous-dependencies */
/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  base: '/starship/static',
  build: {
    outDir: 'static',
    emptyOutDir: true,
    // Write all the files without a hash, which prevents cache-busting,
    // but means we don't need to modify the template index.html
    rollupOptions: {
      output: {
        entryFileNames: 'assets/[name].js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name].[ext]',
      },
    },
  },
  // proxies calls to a running `astro dev` project
  server: {
    proxy: {
      // string shorthand: http://localhost:5173/foo -> http://localhost:4567/foo
      '/api': 'http://localhost:8080',
      '/starship/proxy': 'http://localhost:8080',
    },
  },
});
