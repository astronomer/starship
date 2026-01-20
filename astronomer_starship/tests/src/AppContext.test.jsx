import { describe, expect, test, beforeEach, vi } from 'vitest';
import { renderHook, act } from '@testing-library/react';
import PropTypes from 'prop-types';
import {
  AppProvider,
  useAppState,
  useAppDispatch,
  useSetupComplete,
  useTargetConfig,
  useDagHistoryConfig,
  useTelescopeConfig,
} from '../../src/AppContext';

describe('AppContext', () => {
  beforeEach(() => {
    // Clear localStorage before each test
    localStorage.clear();
  });

  describe('AppProvider', () => {
    test('provides state to children', () => {
      const { result } = renderHook(() => useAppState(), {
        wrapper: AppProvider,
      });

      expect(result.current).toBeDefined();
      expect(result.current.targetUrl).toBe('');
      expect(result.current.isAstro).toBe(true);
    });

    test('provides dispatch to children', () => {
      const { result } = renderHook(() => useAppDispatch(), {
        wrapper: AppProvider,
      });

      expect(typeof result.current).toBe('function');
    });

    test('throws error when useAppState is used outside provider', () => {
      // Suppress console.error for this test
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      expect(() => {
        renderHook(() => useAppState());
      }).toThrow('useAppState must be used within AppProvider');

      consoleSpy.mockRestore();
    });

    test('throws error when useAppDispatch is used outside provider', () => {
      const consoleSpy = vi.spyOn(console, 'error').mockImplementation(() => {});

      expect(() => {
        renderHook(() => useAppDispatch());
      }).toThrow('useAppDispatch must be used within AppProvider');

      consoleSpy.mockRestore();
    });
  });

  describe('Reducer actions', () => {
    test('set-url updates URL-related state', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-url',
          targetUrl: 'https://org.astronomer.run/deploy',
          urlOrgPart: 'org',
          urlDeploymentPart: 'deploy',
        });
      });

      expect(result.current.state.targetUrl).toBe('https://org.astronomer.run/deploy');
      expect(result.current.state.urlOrgPart).toBe('org');
      expect(result.current.state.urlDeploymentPart).toBe('deploy');
      expect(result.current.state.isTouched).toBe(true);
      expect(result.current.state.isValidUrl).toBe(true);
    });

    test('set-url sets isValidUrl to false when parts are missing', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-url',
          targetUrl: '',
          urlOrgPart: '',
          urlDeploymentPart: '',
        });
      });

      expect(result.current.state.isValidUrl).toBe(false);
    });

    test('set-token updates token', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-token',
          token: 'my-secret-token',
        });
      });

      expect(result.current.state.token).toBe('my-secret-token');
      expect(result.current.state.isTokenTouched).toBe(true);
    });

    test('toggle-is-astro toggles isAstro and clears token', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      // Set initial token
      act(() => {
        result.current.dispatch({ type: 'set-token', token: 'initial-token' });
      });

      expect(result.current.state.isAstro).toBe(true);

      act(() => {
        result.current.dispatch({ type: 'toggle-is-astro' });
      });

      expect(result.current.state.isAstro).toBe(false);
      expect(result.current.state.token).toBeNull();
      expect(result.current.state.isSetupComplete).toBe(false);
    });

    test('set-is-starship updates isStarship', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-is-starship', isStarship: true });
      });

      expect(result.current.state.isStarship).toBe(true);
    });

    test('set-is-airflow updates isAirflow', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-is-airflow', isAirflow: true });
      });

      expect(result.current.state.isAirflow).toBe(true);
    });

    test('set-software-info updates Software deployment info', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-software-info',
          releaseName: 'my-release',
          workspaceId: 'ws-123',
          deploymentId: 'dep-456',
        });
      });

      expect(result.current.state.releaseName).toBe('my-release');
      expect(result.current.state.workspaceId).toBe('ws-123');
      expect(result.current.state.deploymentId).toBe('dep-456');
    });

    test('set-organization-id updates organization and deployment IDs', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-organization-id',
          organizationId: 'org-789',
          deploymentId: 'dep-999',
        });
      });

      expect(result.current.state.organizationId).toBe('org-789');
      expect(result.current.state.deploymentId).toBe('dep-999');
    });

    test('set-telescope-org updates telescope organization ID', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-telescope-org',
          telescopeOrganizationId: 'tele-org-123',
        });
      });

      expect(result.current.state.telescopeOrganizationId).toBe('tele-org-123');
    });

    test('set-telescope-presigned-url updates presigned URL', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-telescope-presigned-url',
          telescopePresignedUrl: 'https://presigned.url/upload',
        });
      });

      expect(result.current.state.telescopePresignedUrl).toBe('https://presigned.url/upload');
    });

    test('set-limit updates limit', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-limit', limit: 500 });
      });

      expect(result.current.state.limit).toBe(500);
    });

    test('set-batch-size updates batchSize', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-batch-size', batchSize: 50 });
      });

      expect(result.current.state.batchSize).toBe(50);
    });

    test('set-local-airflow-version updates version', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({
          type: 'set-local-airflow-version',
          version: '2.8.1',
        });
      });

      expect(result.current.state.localAirflowVersion).toBe('2.8.1');
    });

    test('reset returns to initial state', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      // Set some state
      act(() => {
        result.current.dispatch({ type: 'set-token', token: 'test-token' });
        result.current.dispatch({ type: 'set-is-starship', isStarship: true });
      });

      // Reset
      act(() => {
        result.current.dispatch({ type: 'reset' });
      });

      expect(result.current.state.token).toBeNull();
      expect(result.current.state.isStarship).toBe(false);
      expect(result.current.state.targetUrl).toBe('');
    });

    test('invalidate-token clears token and setup', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      // Setup complete state
      act(() => {
        result.current.dispatch({ type: 'set-token', token: 'valid-token' });
        result.current.dispatch({ type: 'set-is-starship', isStarship: true });
        result.current.dispatch({ type: 'set-is-airflow', isAirflow: true });
        result.current.dispatch({
          type: 'set-url',
          targetUrl: 'https://test.astronomer.run/deploy',
          urlOrgPart: 'test',
          urlDeploymentPart: 'deploy',
        });
      });

      act(() => {
        result.current.dispatch({ type: 'invalidate-token' });
      });

      expect(result.current.state.token).toBeNull();
      expect(result.current.state.isSetupComplete).toBe(false);
      expect(result.current.state.isTokenTouched).toBe(false);
    });

    test('unknown action returns current state', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      const stateBefore = { ...result.current.state };

      act(() => {
        result.current.dispatch({ type: 'unknown-action' });
      });

      expect(result.current.state).toEqual(stateBefore);
    });
  });

  describe('isSetupComplete calculation', () => {
    test('isSetupComplete is true when all conditions are met', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-is-starship', isStarship: true });
        result.current.dispatch({ type: 'set-is-airflow', isAirflow: true });
        result.current.dispatch({ type: 'set-token', token: 'valid-token' });
        result.current.dispatch({
          type: 'set-url',
          targetUrl: 'https://org.astronomer.run/deploy',
          urlOrgPart: 'org',
          urlDeploymentPart: 'deploy',
        });
      });

      expect(result.current.state.isSetupComplete).toBe(true);
    });

    test('isSetupComplete is false without token', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-is-starship', isStarship: true });
        result.current.dispatch({ type: 'set-is-airflow', isAirflow: true });
        result.current.dispatch({
          type: 'set-url',
          targetUrl: 'https://org.astronomer.run/deploy',
          urlOrgPart: 'org',
          urlDeploymentPart: 'deploy',
        });
      });

      expect(result.current.state.isSetupComplete).toBe(false);
    });

    test('isSetupComplete is false without valid URL', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-is-starship', isStarship: true });
        result.current.dispatch({ type: 'set-is-airflow', isAirflow: true });
        result.current.dispatch({ type: 'set-token', token: 'valid-token' });
      });

      expect(result.current.state.isSetupComplete).toBe(false);
    });
  });

  describe('Selector hooks', () => {
    function createWrapper() {
      function TestWrapper({ children }) {
        return <AppProvider>{children}</AppProvider>;
      }
      TestWrapper.propTypes = { children: PropTypes.node.isRequired };
      return TestWrapper;
    }

    test('useSetupComplete returns isSetupComplete', () => {
      const { result } = renderHook(() => useSetupComplete(), {
        wrapper: createWrapper(),
      });

      expect(result.current).toBe(false);
    });

    test('useTargetConfig returns target configuration', () => {
      const { result } = renderHook(
        () => ({
          config: useTargetConfig(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: createWrapper() },
      );

      expect(result.current.config).toHaveProperty('targetUrl');
      expect(result.current.config).toHaveProperty('token');
      expect(result.current.config).toHaveProperty('isAstro');
      expect(result.current.config).toHaveProperty('organizationId');
      expect(result.current.config).toHaveProperty('deploymentId');
      expect(result.current.config).toHaveProperty('localAirflowVersion');
      expect(result.current.config).toHaveProperty('releaseName');
      expect(result.current.config).toHaveProperty('urlOrgPart');
    });

    test('useDagHistoryConfig returns DAG history configuration', () => {
      const { result } = renderHook(() => useDagHistoryConfig(), {
        wrapper: createWrapper(),
      });

      expect(result.current).toHaveProperty('limit');
      expect(result.current).toHaveProperty('batchSize');
      expect(result.current.limit).toBe(100);
      expect(result.current.batchSize).toBe(10);
    });

    test('useTelescopeConfig returns telescope configuration', () => {
      const { result } = renderHook(() => useTelescopeConfig(), {
        wrapper: createWrapper(),
      });

      expect(result.current).toHaveProperty('telescopeOrganizationId');
      expect(result.current).toHaveProperty('telescopePresignedUrl');
    });

    test('useTargetConfig updates when state changes', () => {
      const { result } = renderHook(
        () => ({
          config: useTargetConfig(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: createWrapper() },
      );

      act(() => {
        result.current.dispatch({ type: 'set-token', token: 'new-token' });
      });

      expect(result.current.config.token).toBe('new-token');
    });
  });

  describe('localStorage persistence', () => {
    test('saves state to localStorage on changes', () => {
      const { result } = renderHook(
        () => ({
          state: useAppState(),
          dispatch: useAppDispatch(),
        }),
        { wrapper: AppProvider },
      );

      act(() => {
        result.current.dispatch({ type: 'set-token', token: 'persisted-token' });
      });

      // Check localStorage was called
      expect(localStorage.setItem).toHaveBeenCalled();
    });

    test('loads state from localStorage on mount', () => {
      // Pre-populate localStorage
      const savedState = {
        token: 'saved-token',
        targetUrl: 'https://saved.astronomer.run/deploy',
        isAstro: true,
      };
      localStorage.getItem.mockReturnValue(JSON.stringify(savedState));

      const { result } = renderHook(() => useAppState(), {
        wrapper: AppProvider,
      });

      expect(result.current.token).toBe('saved-token');
      expect(result.current.targetUrl).toBe('https://saved.astronomer.run/deploy');
    });

    test('handles invalid JSON in localStorage gracefully', () => {
      localStorage.getItem.mockReturnValue('invalid-json{{{');

      const { result } = renderHook(() => useAppState(), {
        wrapper: AppProvider,
      });

      // Should fall back to initial state
      expect(result.current.token).toBeNull();
      expect(result.current.targetUrl).toBe('');
    });
  });
});
