import { describe, expect, test, vi, beforeEach } from 'vitest';
import { renderHook, waitFor, act } from '@testing-library/react';
import axios from 'axios';
import PropTypes from 'prop-types';
import { ChakraProvider } from '@chakra-ui/react';
import useMigrationData from '../../../src/hooks/useMigrationData';
import { createAxiosResponse, createAxiosError } from '../../test-utils';

// Mock axios
vi.mock('axios');

// Mock the AppContext hooks
const mockDispatch = vi.fn();
const mockTargetConfig = {
  targetUrl: 'https://test.astronomer.run/deployment',
  token: 'test-token',
};

vi.mock('../../../src/AppContext', () => ({
  useAppDispatch: () => mockDispatch,
  useTargetConfig: () => mockTargetConfig,
}));

// Wrapper component for hooks that need Chakra (for useToast)
function Wrapper({ children }) {
  return <ChakraProvider>{children}</ChakraProvider>;
}

Wrapper.propTypes = {
  children: PropTypes.node.isRequired,
};

describe('useMigrationData', () => {
  const defaultOptions = {
    route: '/api/starship/variables',
    idField: 'key',
    itemName: 'variable',
  };

  const localData = [
    { key: 'var1', val: 'value1' },
    { key: 'var2', val: 'value2' },
    { key: 'var3', val: 'value3' },
  ];

  const remoteData = [{ key: 'var1', val: 'value1' }];

  beforeEach(() => {
    vi.clearAllMocks();
  });

  describe('Initial data fetching', () => {
    test('fetches data from local and remote on mount', async () => {
      axios.get.mockResolvedValue(createAxiosResponse(localData));

      renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(axios.get).toHaveBeenCalledTimes(2);
      });
    });

    test('sets loading to true initially', () => {
      axios.get.mockImplementation(() => new Promise(() => {})); // Never resolves

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      expect(result.current.loading).toBe(true);
    });

    test('sets loading to false after fetch completes', async () => {
      axios.get.mockResolvedValue(createAxiosResponse(localData));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });
    });
  });

  describe('Data merging', () => {
    test('marks items that exist in remote', async () => {
      axios.get
        .mockResolvedValueOnce(createAxiosResponse(localData)) // Local
        .mockResolvedValueOnce(createAxiosResponse(remoteData)); // Remote

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      // var1 exists in remote, var2 and var3 do not
      const var1 = result.current.data.find((d) => d.key === 'var1');
      const var2 = result.current.data.find((d) => d.key === 'var2');
      const var3 = result.current.data.find((d) => d.key === 'var3');

      expect(var1.exists).toBe(true);
      expect(var2.exists).toBe(false);
      expect(var3.exists).toBe(false);
    });

    test('calculates totalItems correctly', async () => {
      axios.get
        .mockResolvedValueOnce(createAxiosResponse(localData))
        .mockResolvedValueOnce(createAxiosResponse(remoteData));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.totalItems).toBe(3);
      });
    });

    test('calculates migratedItems correctly', async () => {
      axios.get
        .mockResolvedValueOnce(createAxiosResponse(localData))
        .mockResolvedValueOnce(createAxiosResponse(remoteData));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.migratedItems).toBe(1);
      });
    });
  });

  describe('Error handling', () => {
    test('sets error when fetch fails', async () => {
      const error = createAxiosError('Network error', 500);
      axios.get.mockRejectedValue(error);

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.error).toBeTruthy();
      });
    });

    test('dispatches invalidate-token on 401 error', async () => {
      const error = createAxiosError('Unauthorized', 401);
      axios.get.mockRejectedValue(error);

      renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(mockDispatch).toHaveBeenCalledWith({ type: 'invalidate-token' });
      });
    });
  });

  describe('handleItemStatusChange', () => {
    test('updates item exists status', async () => {
      axios.get
        .mockResolvedValueOnce(createAxiosResponse(localData))
        .mockResolvedValueOnce(createAxiosResponse(remoteData));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      act(() => {
        result.current.handleItemStatusChange('var2', true);
      });

      const var2 = result.current.data.find((d) => d.key === 'var2');
      expect(var2.exists).toBe(true);
    });
  });

  describe('handleMigrateAll', () => {
    test('does nothing when no unmigrated items', async () => {
      const allMigrated = [{ key: 'var1', val: 'value1', exists: true }];
      axios.get.mockResolvedValue(createAxiosResponse(allMigrated));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      await act(async () => {
        await result.current.handleMigrateAll();
      });

      // No POST calls should be made
      expect(axios.post).not.toHaveBeenCalled();
    });

    test('sets isMigratingAll during migration', async () => {
      axios.get.mockResolvedValueOnce(createAxiosResponse(localData)).mockResolvedValueOnce(createAxiosResponse([]));

      axios.post.mockImplementation(
        () =>
          new Promise((resolve) => {
            setTimeout(() => resolve(createAxiosResponse({})), 50);
          }),
      );

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      let migratePromise;
      act(() => {
        migratePromise = result.current.handleMigrateAll();
      });

      expect(result.current.isMigratingAll).toBe(true);

      await act(async () => {
        await migratePromise;
      });

      expect(result.current.isMigratingAll).toBe(false);
    });

    test('posts unmigrated items to remote', async () => {
      axios.get
        .mockResolvedValueOnce(createAxiosResponse(localData))
        .mockResolvedValueOnce(createAxiosResponse(remoteData));

      axios.post.mockResolvedValue(createAxiosResponse({}));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      await act(async () => {
        await result.current.handleMigrateAll();
      });

      // Should post var2 and var3 (the unmigrated ones)
      expect(axios.post).toHaveBeenCalledTimes(2);
    });

    test('updates items to exists=true after successful migration', async () => {
      axios.get
        .mockResolvedValueOnce(createAxiosResponse(localData))
        .mockResolvedValueOnce(createAxiosResponse(remoteData));

      axios.post.mockResolvedValue(createAxiosResponse({}));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      await act(async () => {
        await result.current.handleMigrateAll();
      });

      // All items should now exist
      expect(result.current.migratedItems).toBe(3);
    });
  });

  describe('fetchData', () => {
    test('can be called manually to refresh data', async () => {
      axios.get.mockResolvedValue(createAxiosResponse(localData));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      await waitFor(() => {
        expect(result.current.loading).toBe(false);
      });

      // Clear mock to track new calls
      axios.get.mockClear();
      axios.get.mockResolvedValue(createAxiosResponse(localData));

      await act(async () => {
        await result.current.fetchData();
      });

      expect(axios.get).toHaveBeenCalledTimes(2);
    });
  });

  describe('Config pass-through', () => {
    test('exposes targetUrl from config', async () => {
      axios.get.mockResolvedValue(createAxiosResponse([]));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      expect(result.current.targetUrl).toBe(mockTargetConfig.targetUrl);
    });

    test('exposes token from config', async () => {
      axios.get.mockResolvedValue(createAxiosResponse([]));

      const { result } = renderHook(() => useMigrationData(defaultOptions), { wrapper: Wrapper });

      expect(result.current.token).toBe(mockTargetConfig.token);
    });
  });
});
