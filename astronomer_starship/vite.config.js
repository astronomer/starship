/* eslint-disable import/no-extraneous-dependencies */
/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

// Enable profiling/dev build when PROFILE env var is set
const enableProfiling = process.env.PROFILE === 'true';

export default defineConfig(({ mode }) => ({
  plugins: [
    react({
      // Enable fast refresh for better DX
      fastRefresh: true,
    }),
  ],
  base: './',
  // For React 18 profiling, we need development mode
  define: enableProfiling ? {
    'process.env.NODE_ENV': JSON.stringify('development'),
  } : {},
  build: {
    outDir: 'static',
    emptyOutDir: true,
    // Improve build performance
    target: 'es2020',
    // Generate source maps for debugging when profiling
    sourcemap: enableProfiling,
    // Minification - disable in profiling mode for better stack traces
    minify: enableProfiling ? false : 'esbuild',
    // Write all the files without a hash, which prevents cache-busting,
    // but means we don't need to modify the template index.html
    rollupOptions: {
      output: {
        entryFileNames: 'assets/[name].js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name].[ext]',
        // Manual chunks for better caching
        manualChunks: {
          vendor: ['react', 'react-dom', 'react-router-dom'],
          chakra: ['@chakra-ui/react', '@emotion/react', '@emotion/styled', 'framer-motion'],
        },
      },
    },
  },
  // Optimize dependencies
  optimizeDeps: {
    include: ['react', 'react-dom', '@chakra-ui/react'],
  },
  // Proxies calls to a running `astro dev` project
  server: {
    proxy: {
      '/api': 'http://localhost:8080',
      '/starship/proxy': 'http://localhost:8080',
    },
  },
  // Test configuration
  test: {
    globals: true,
  },
}));
