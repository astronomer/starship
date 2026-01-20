/**
 * Vitest test setup file.
 * Configures global mocks and test environment.
 */
import '@testing-library/jest-dom';
import { vi, beforeEach } from 'vitest';

// ============================================================================
// Window Location Mock
// ============================================================================

Object.defineProperty(window, 'location', {
  value: {
    href: 'http://localhost:8080/starship/',
    origin: 'http://localhost:8080',
    pathname: '/starship/',
    search: '',
    hash: '',
  },
  writable: true,
});

// ============================================================================
// LocalStorage Mock
// ============================================================================

const localStorageMock = (() => {
  let store = {};
  return {
    getItem: vi.fn((key) => store[key] ?? null),
    setItem: vi.fn((key, value) => {
      store[key] = String(value);
    }),
    removeItem: vi.fn((key) => {
      delete store[key];
    }),
    clear: vi.fn(() => {
      store = {};
    }),
    get length() {
      return Object.keys(store).length;
    },
    key: vi.fn((index) => Object.keys(store)[index] ?? null),
    // Helper to reset store between tests
    __reset: () => {
      store = {};
    },
  };
})();

Object.defineProperty(window, 'localStorage', {
  value: localStorageMock,
  writable: true,
});

// ============================================================================
// ResizeObserver Mock (required by Chakra UI)
// ============================================================================

class ResizeObserverMock {
  constructor(callback) {
    this.callback = callback;
  }

  observe() {
    // No-op for tests
  }

  unobserve() {
    // No-op for tests
  }

  disconnect() {
    // No-op for tests
  }
}

window.ResizeObserver = ResizeObserverMock;

// ============================================================================
// matchMedia Mock (required for responsive components)
// ============================================================================

Object.defineProperty(window, 'matchMedia', {
  writable: true,
  value: vi.fn().mockImplementation((query) => ({
    matches: false,
    media: query,
    onchange: null,
    addListener: vi.fn(), // Deprecated but still used by some libraries
    removeListener: vi.fn(), // Deprecated but still used by some libraries
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
    dispatchEvent: vi.fn(),
  })),
});

// ============================================================================
// IntersectionObserver Mock (used by some Chakra components)
// ============================================================================

class IntersectionObserverMock {
  constructor(callback) {
    this.callback = callback;
  }

  observe() {
    // No-op for tests
  }

  unobserve() {
    // No-op for tests
  }

  disconnect() {
    // No-op for tests
  }
}

window.IntersectionObserver = IntersectionObserverMock;

// ============================================================================
// scrollTo Mock (prevents errors in tests)
// ============================================================================

window.scrollTo = vi.fn();
Element.prototype.scrollTo = vi.fn();
Element.prototype.scrollIntoView = vi.fn();

// ============================================================================
// requestAnimationFrame Mock (recommended by Chakra UI docs)
// ============================================================================

window.requestAnimationFrame = (cb) => setTimeout(cb, 1000 / 60);

// ============================================================================
// URL Object Mock (recommended by Chakra UI docs)
// ============================================================================

window.URL.createObjectURL = vi.fn(() => 'blob:mock-url');
window.URL.revokeObjectURL = vi.fn();

// ============================================================================
// Navigator Clipboard Mock (recommended by Chakra UI docs)
// ============================================================================

Object.defineProperty(navigator, 'clipboard', {
  value: {
    writeText: vi.fn().mockResolvedValue(undefined),
    readText: vi.fn().mockResolvedValue(''),
  },
  writable: true,
  configurable: true,
});

// ============================================================================
// getComputedStyle Enhancement (for Chakra/Framer Motion)
// ============================================================================

const originalGetComputedStyle = window.getComputedStyle;
window.getComputedStyle = (element, pseudoElement) => {
  const style = originalGetComputedStyle(element, pseudoElement);
  // Ensure transform is defined (Framer Motion needs this)
  if (!style.transform) {
    style.transform = 'none';
  }
  return style;
};

// ============================================================================
// Console Error Suppression for Known Issues
// ============================================================================

const originalError = console.error;
console.error = (...args) => {
  // Suppress React 18 act() warnings in some edge cases
  if (typeof args[0] === 'string' && args[0].includes('Warning: An update to')) {
    return;
  }
  // Suppress Chakra portal warnings in tests
  if (typeof args[0] === 'string' && args[0].includes('portal')) {
    return;
  }
  originalError.call(console, ...args);
};

// ============================================================================
// Global Test Cleanup
// ============================================================================

// Reset localStorage mock between tests
beforeEach(() => {
  localStorageMock.__reset();
  localStorageMock.getItem.mockClear();
  localStorageMock.setItem.mockClear();
  localStorageMock.removeItem.mockClear();
  localStorageMock.clear.mockClear();
});
