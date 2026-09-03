import { describe, expect, test, beforeEach } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import { AppProvider, useAppDispatch, useDagHistoryConfig } from '../../src/AppContext';

function wrapper({ children }) {
  return <AppProvider>{children}</AppProvider>;
}

describe('AppContext DAG History pagination reducer', () => {
  beforeEach(() => {
    // Ensure each test starts from a clean localStorage-backed initial state.
    localStorage.clear();
  });

  test('defaults expose page=0, pageSize=50, empty search, filter off', () => {
    const { result } = renderHook(() => useDagHistoryConfig(), { wrapper });
    expect(result.current.page).toBe(0);
    expect(result.current.pageSize).toBe(50);
    expect(result.current.search).toBe('');
    expect(result.current.unmigratedOnly).toBe(false);
  });

  test('set-dag-history-page updates page only', () => {
    const { result } = renderHook(
      () => ({ config: useDagHistoryConfig(), dispatch: useAppDispatch() }),
      { wrapper },
    );
    act(() => result.current.dispatch({ type: 'set-dag-history-page', page: 3 }));
    expect(result.current.config.page).toBe(3);
    expect(result.current.config.pageSize).toBe(50);
  });

  test('set-dag-history-page-size resets page to 0 so stale offsets do not overshoot', () => {
    const { result } = renderHook(
      () => ({ config: useDagHistoryConfig(), dispatch: useAppDispatch() }),
      { wrapper },
    );
    act(() => result.current.dispatch({ type: 'set-dag-history-page', page: 5 }));
    act(() => result.current.dispatch({ type: 'set-dag-history-page-size', pageSize: 25 }));
    expect(result.current.config.pageSize).toBe(25);
    expect(result.current.config.page).toBe(0);
  });

  test('set-dag-history-search resets page and stores query', () => {
    const { result } = renderHook(
      () => ({ config: useDagHistoryConfig(), dispatch: useAppDispatch() }),
      { wrapper },
    );
    act(() => result.current.dispatch({ type: 'set-dag-history-page', page: 4 }));
    act(() => result.current.dispatch({ type: 'set-dag-history-search', search: 'example' }));
    expect(result.current.config.search).toBe('example');
    expect(result.current.config.page).toBe(0);
  });

  test('set-dag-history-unmigrated-only toggles without touching pagination', () => {
    const { result } = renderHook(
      () => ({ config: useDagHistoryConfig(), dispatch: useAppDispatch() }),
      { wrapper },
    );
    act(() => result.current.dispatch({ type: 'set-dag-history-page', page: 2 }));
    act(() => result.current.dispatch({ type: 'set-dag-history-unmigrated-only', unmigratedOnly: true }));
    expect(result.current.config.unmigratedOnly).toBe(true);
    expect(result.current.config.page).toBe(2);
  });
});
