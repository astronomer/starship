/**
 * Shared test utilities for React component testing.
 * Provides wrapped render functions, mock factories, and common test helpers.
 */
import { render } from '@testing-library/react';
import { ChakraProvider } from '@chakra-ui/react';
import { MemoryRouter } from 'react-router-dom';
import PropTypes from 'prop-types';
import { createContext, useContext, useReducer, useMemo } from 'react';

// ============================================================================
// Mock App State Factory
// ============================================================================

/**
 * Default mock state matching AppContext's initialState structure.
 */
const defaultMockState = {
  targetUrl: 'https://test-org.astronomer.run/test-deployment',
  urlDeploymentPart: 'test-deployment',
  urlOrgPart: 'test-org',
  isAstro: true,
  token: 'test-token-123',
  isTouched: false,
  isTokenTouched: false,
  isValidUrl: true,
  isStarship: true,
  isAirflow: true,
  isSetupComplete: true,
  releaseName: null,
  workspaceId: null,
  deploymentId: 'deploy-123',
  organizationId: 'org-123',
  telescopeOrganizationId: '',
  telescopePresignedUrl: '',
  limit: 100,
  batchSize: 10,
  localAirflowVersion: '2.8.0',
};

/**
 * Creates a mock app state with optional overrides.
 *
 * @param {Object} overrides - Partial state to override defaults
 * @returns {Object} Complete mock state
 */
export function createMockAppState(overrides = {}) {
  return { ...defaultMockState, ...overrides };
}

// ============================================================================
// Mock App Context Provider
// ============================================================================

const MockAppStateContext = createContext(undefined);
const MockAppDispatchContext = createContext(undefined);

function mockReducer(state, action) {
  switch (action.type) {
    case 'set-url':
      return { ...state, targetUrl: action.targetUrl, isTouched: true };
    case 'set-token':
      return { ...state, token: action.token, isTokenTouched: true };
    case 'invalidate-token':
      return { ...state, token: null, isSetupComplete: false };
    default:
      return state;
  }
}

/**
 * Mock AppProvider for testing components that use AppContext.
 */
function MockAppProvider({ children, initialState = {} }) {
  const [state, dispatch] = useReducer(mockReducer, createMockAppState(initialState));

  return (
    <MockAppStateContext.Provider value={state}>
      <MockAppDispatchContext.Provider value={dispatch}>{children}</MockAppDispatchContext.Provider>
    </MockAppStateContext.Provider>
  );
}

MockAppProvider.propTypes = {
  children: PropTypes.node.isRequired,
  initialState: PropTypes.object,
};

/**
 * Hook to access mock app state in tests.
 */
export function useMockAppState() {
  const context = useContext(MockAppStateContext);
  if (context === undefined) {
    throw new Error('useMockAppState must be used within MockAppProvider');
  }
  return context;
}

/**
 * Hook to access mock app dispatch in tests.
 */
export function useMockAppDispatch() {
  const context = useContext(MockAppDispatchContext);
  if (context === undefined) {
    throw new Error('useMockAppDispatch must be used within MockAppProvider');
  }
  return context;
}

/**
 * Hook matching useTargetConfig from AppContext.
 */
export function useMockTargetConfig() {
  const state = useMockAppState();
  return useMemo(
    () => ({
      targetUrl: state.targetUrl,
      token: state.token,
      isAstro: state.isAstro,
      organizationId: state.organizationId,
      deploymentId: state.deploymentId,
      localAirflowVersion: state.localAirflowVersion,
      releaseName: state.releaseName,
      urlOrgPart: state.urlOrgPart,
    }),
    [
      state.targetUrl,
      state.token,
      state.isAstro,
      state.organizationId,
      state.deploymentId,
      state.localAirflowVersion,
      state.releaseName,
      state.urlOrgPart,
    ],
  );
}

// ============================================================================
// Wrapper Components
// ============================================================================

/**
 * Combines all providers needed for component testing.
 */
function AllProviders({ children, initialState = {}, initialRoutes = ['/'] }) {
  return (
    <ChakraProvider>
      <MemoryRouter initialEntries={initialRoutes}>
        <MockAppProvider initialState={initialState}>{children}</MockAppProvider>
      </MemoryRouter>
    </ChakraProvider>
  );
}

AllProviders.propTypes = {
  children: PropTypes.node.isRequired,
  initialState: PropTypes.object,
  initialRoutes: PropTypes.arrayOf(PropTypes.string),
};

/**
 * Minimal wrapper with just ChakraProvider (for simple components).
 */
function ChakraWrapper({ children }) {
  return <ChakraProvider>{children}</ChakraProvider>;
}

ChakraWrapper.propTypes = {
  children: PropTypes.node.isRequired,
};

// ============================================================================
// Custom Render Functions
// ============================================================================

/**
 * Renders a component with all providers (Chakra, Router, AppContext).
 *
 * @param {React.ReactElement} ui - Component to render
 * @param {Object} options - Render options
 * @param {Object} options.initialState - Initial AppContext state
 * @param {string[]} options.initialRoutes - Initial router entries
 * @param {Object} options.renderOptions - Additional RTL render options
 * @returns {Object} RTL render result plus rerender helper
 */
export function renderWithProviders(ui, { initialState = {}, initialRoutes = ['/'], ...renderOptions } = {}) {
  function Wrapper({ children }) {
    return (
      <AllProviders initialState={initialState} initialRoutes={initialRoutes}>
        {children}
      </AllProviders>
    );
  }
  Wrapper.propTypes = { children: PropTypes.node.isRequired };

  return render(ui, { wrapper: Wrapper, ...renderOptions });
}

/**
 * Renders a component with just ChakraProvider (for simple UI components).
 *
 * @param {React.ReactElement} ui - Component to render
 * @param {Object} renderOptions - Additional RTL render options
 * @returns {Object} RTL render result
 */
export function renderWithChakra(ui, renderOptions = {}) {
  return render(ui, { wrapper: ChakraWrapper, ...renderOptions });
}

// ============================================================================
// Mock Factories
// ============================================================================

/**
 * Creates a mock DAG object for testing.
 *
 * @param {Object} overrides - Properties to override
 * @returns {Object} Mock DAG data
 */
export function createMockDag(overrides = {}) {
  return {
    dag_id: 'test_dag',
    fileloc: '/dags/test_dag.py',
    owners: ['airflow'],
    is_paused: false,
    is_active: true,
    schedule_interval: '@daily',
    tags: [],
    dag_run_count: 10,
    ...overrides,
  };
}

/**
 * Creates a mock DAG run object for testing.
 *
 * @param {Object} overrides - Properties to override
 * @returns {Object} Mock DAG run data
 */
export function createMockDagRun(overrides = {}) {
  return {
    dag_id: 'test_dag',
    run_id: 'scheduled__2024-01-01T00:00:00+00:00',
    execution_date: '2024-01-01T00:00:00+00:00',
    state: 'success',
    start_date: '2024-01-01T00:00:01+00:00',
    end_date: '2024-01-01T00:01:00+00:00',
    ...overrides,
  };
}

/**
 * Creates a mock variable object for testing.
 *
 * @param {Object} overrides - Properties to override
 * @returns {Object} Mock variable data
 */
export function createMockVariable(overrides = {}) {
  return {
    key: 'test_variable',
    val: 'test_value',
    description: 'A test variable',
    ...overrides,
  };
}

/**
 * Creates a mock connection object for testing.
 *
 * @param {Object} overrides - Properties to override
 * @returns {Object} Mock connection data
 */
export function createMockConnection(overrides = {}) {
  return {
    conn_id: 'test_connection',
    conn_type: 'http',
    host: 'example.com',
    port: 443,
    login: 'user',
    password: 'secret', // pragma: allowlist secret
    schema: 'https',
    extra: '{}',
    ...overrides,
  };
}

/**
 * Creates a mock pool object for testing.
 *
 * @param {Object} overrides - Properties to override
 * @returns {Object} Mock pool data
 */
export function createMockPool(overrides = {}) {
  return {
    pool: 'test_pool',
    slots: 10,
    description: 'A test pool',
    include_deferred: false,
    ...overrides,
  };
}

// ============================================================================
// Axios Mock Helpers
// ============================================================================

/**
 * Creates a resolved axios response mock.
 *
 * @param {*} data - Response data
 * @param {number} status - HTTP status code
 * @returns {Object} Mock axios response
 */
export function createAxiosResponse(data, status = 200) {
  return {
    data,
    status,
    statusText: status === 200 ? 'OK' : 'Error',
    headers: {},
    config: {},
  };
}

/**
 * Creates a rejected axios error mock.
 *
 * @param {string} message - Error message
 * @param {number} status - HTTP status code
 * @param {*} data - Response data
 * @returns {Error} Mock axios error
 */
export function createAxiosError(message, status = 500, data = null) {
  const error = new Error(message);
  error.response = {
    data: data || { error: message },
    status,
    statusText: 'Error',
    headers: {},
    config: {},
  };
  error.isAxiosError = true;
  return error;
}

// ============================================================================
// Wait Helpers
// ============================================================================

/**
 * Waits for a specified amount of time.
 * Useful for testing async state updates.
 *
 * @param {number} ms - Milliseconds to wait
 * @returns {Promise} Resolves after delay
 */
export function wait(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/**
 * Flushes pending promises and timers.
 * Call after triggering async actions in tests.
 *
 * @returns {Promise} Resolves after microtasks complete
 */
export async function flushPromises() {
  await new Promise((resolve) => {
    setTimeout(resolve, 0);
  });
}

// Re-export everything from @testing-library/react for convenience
export * from '@testing-library/react';
