/* eslint-disable import/no-extraneous-dependencies */
/// <reference types="vitest" />
import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { visualizer } from 'rollup-plugin-visualizer';

// Enable profiling/dev build when PROFILE env var is set
const enableProfiling = process.env.PROFILE === 'true';
// Enable bundle analysis when ANALYZE env var is set
const enableAnalyze = process.env.ANALYZE === 'true';

export default defineConfig(({ mode }) => ({
  plugins: [
    react({
      // Enable fast refresh for better DX
      fastRefresh: true,
    }),
    // Bundle analyzer - run with ANALYZE=true npm run build
    enableAnalyze &&
      visualizer({
        open: true,
        gzipSize: true,
        brotliSize: true,
        filename: 'bundle-stats.html',
      }),
  ].filter(Boolean),
  base: './',
  // For React 18 profiling, we need development mode
  define: enableProfiling
    ? {
        'process.env.NODE_ENV': JSON.stringify('development'),
      }
    : {},
  build: {
    outDir: 'static',
    emptyOutDir: true,
    // Target modern browsers for smaller bundles (no legacy polyfills)
    target: 'es2020',
    // Generate source maps for debugging when profiling
    sourcemap: enableProfiling,
    // Minification - disable in profiling mode for better stack traces
    minify: enableProfiling ? false : 'esbuild',
    // CSS code splitting for smaller initial load
    cssCodeSplit: true,
    // Write all the files without a hash, which prevents cache-busting,
    // but means we don't need to modify the template index.html
    rollupOptions: {
      output: {
        entryFileNames: 'assets/[name].js',
        chunkFileNames: 'assets/[name].js',
        assetFileNames: 'assets/[name].[ext]',
        // Granular manual chunks for better caching and tree-shaking.
        // Uses a function so Chakra's transitive peer deps (@zag-js, @popperjs,
        // react-remove-scroll, @internationalized, etc.) land in the `chakra`
        // chunk alongside @chakra-ui itself. If they leak into other chunks,
        // cross-chunk named imports break at runtime after minification.
        manualChunks(id) {
          if (!id.includes('node_modules')) return undefined;
          // Chakra and its ecosystem — must stay together.
          if (
            id.includes('@chakra-ui') ||
            id.includes('@emotion') ||
            id.includes('framer-motion') ||
            id.includes('@zag-js') ||
            id.includes('@popperjs') ||
            id.includes('react-remove-scroll') ||
            id.includes('react-style-singleton') ||
            id.includes('@internationalized') ||
            id.includes('aria-hidden') ||
            id.includes('focus-lock') ||
            id.includes('react-focus-lock') ||
            id.includes('use-callback-ref') ||
            id.includes('use-sidecar')
          ) {
            return 'chakra';
          }
          if (id.includes('react-router')) return 'router';
          if (id.includes('@tanstack/react-table')) return 'table';
          if (id.includes('react-icons')) return 'icons';
          if (id.includes('axios')) return 'http';
          if (id.includes('/react-dom/') || id.includes('/react/')) return 'react-core';
          return undefined;
        },
      },
    },
    // Chunk size warnings
    chunkSizeWarningLimit: 500,
  },
  // Optimize dependencies for faster dev server startup
  optimizeDeps: {
    include: ['react', 'react-dom', '@chakra-ui/react', '@tanstack/react-table', 'axios'],
    // Exclude large dependencies from pre-bundling for faster startup
    exclude: ['react-icons'],
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
    environment: 'jsdom',
    setupFiles: ['./tests/setup.js'],
    include: ['tests/**/*.test.{js,jsx}'],
  },
}));
