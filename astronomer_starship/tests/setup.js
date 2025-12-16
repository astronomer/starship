/* eslint-disable import/no-extraneous-dependencies */
import '@testing-library/jest-dom';

// Mock window.location for tests
Object.defineProperty(window, 'location', {
  value: {
    href: 'http://localhost:8080/starship/',
    origin: 'http://localhost:8080',
  },
  writable: true,
});
