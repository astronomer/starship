import React, {
  createContext, useContext, useReducer, useEffect, useCallback,
} from 'react';
import PropTypes from 'prop-types';
import { getTargetUrlFromParts } from './util';

// ============================================================================
// INITIAL STATE - Only setup/configuration state, NOT page data
// ============================================================================
const initialState = {
  // Target deployment configuration
  targetUrl: '',
  urlDeploymentPart: '',
  urlOrgPart: '',
  isAstro: true,
  token: null,

  // Validation state
  isTouched: false,
  isTokenTouched: false,
  isValidUrl: false,
  isStarship: false,
  isAirflow: false,

  // Computed from above
  isSetupComplete: false,

  // Software-specific info
  releaseName: null,
  workspaceId: null,
  deploymentId: null,
  organizationId: null,

  // Telescope
  telescopeOrganizationId: '',
  telescopePresignedUrl: '',

  // DAG History settings
  limit: 100,
  batchSize: 10,
};

// ============================================================================
// REDUCER
// ============================================================================
function calculateIsSetupComplete(state, overrides = {}) {
  const merged = { ...state, ...overrides };
  return !!(merged.isStarship && merged.isAirflow && merged.token && merged.isValidUrl);
}

function reducer(state, action) {
  switch (action.type) {
    case 'set-url': {
      const isValidUrl = !!(action.urlOrgPart && action.urlDeploymentPart);
      return {
        ...state,
        isTouched: true,
        targetUrl: action.targetUrl,
        urlDeploymentPart: action.urlDeploymentPart,
        urlOrgPart: action.urlOrgPart,
        isValidUrl,
        isSetupComplete: calculateIsSetupComplete(state, { isValidUrl }),
      };
    }
    case 'set-token':
      return {
        ...state,
        isTokenTouched: true,
        token: action.token,
        isSetupComplete: calculateIsSetupComplete(state, { token: action.token }),
      };
    case 'toggle-is-astro':
      return {
        ...state,
        isAstro: !state.isAstro,
        targetUrl: getTargetUrlFromParts(state.urlOrgPart, state.urlDeploymentPart, !state.isAstro),
        token: null,
        isSetupComplete: false,
      };
    case 'set-is-starship':
      return {
        ...state,
        isStarship: action.isStarship,
        isSetupComplete: calculateIsSetupComplete(state, { isStarship: action.isStarship }),
      };
    case 'set-is-airflow':
      return {
        ...state,
        isAirflow: action.isAirflow,
        isSetupComplete: calculateIsSetupComplete(state, { isAirflow: action.isAirflow }),
      };
    case 'set-software-info':
      return {
        ...state,
        releaseName: action.releaseName,
        workspaceId: action.workspaceId,
        deploymentId: action.deploymentId,
      };
    case 'set-organization-id':
      return {
        ...state,
        organizationId: action.organizationId,
        deploymentId: action.deploymentId || state.deploymentId,
      };
    case 'set-telescope-org':
      return { ...state, telescopeOrganizationId: action.telescopeOrganizationId };
    case 'set-telescope-presigned-url':
      return { ...state, telescopePresignedUrl: action.telescopePresignedUrl };
    case 'set-limit':
      return { ...state, limit: action.limit };
    case 'set-batch-size':
      return { ...state, batchSize: action.batchSize };
    case 'reset':
      return initialState;
    case 'invalidate-token':
      return { ...state, isSetupComplete: false, isTokenTouched: false, token: null };
    default:
      return state;
  }
}

// ============================================================================
// CONTEXT
// ============================================================================
const AppStateContext = createContext(undefined);
const AppDispatchContext = createContext(undefined);

// ============================================================================
// HOOKS
// ============================================================================
export function useAppState() {
  const context = useContext(AppStateContext);
  if (context === undefined) {
    throw new Error('useAppState must be used within AppProvider');
  }
  return context;
}

export function useAppDispatch() {
  const context = useContext(AppDispatchContext);
  if (context === undefined) {
    throw new Error('useAppDispatch must be used within AppProvider');
  }
  return context;
}

// ============================================================================
// PROVIDER COMPONENT
// ============================================================================
const STORAGE_KEY = 'starship-config';

function getInitialState() {
  try {
    const saved = localStorage.getItem(STORAGE_KEY);
    if (saved) {
      const parsed = JSON.parse(saved);
      return { ...initialState, ...parsed };
    }
  } catch {
    // Ignore localStorage errors
  }
  return initialState;
}

export function AppProvider({ children }) {
  const [state, dispatch] = useReducer(reducer, undefined, getInitialState);

  // Persist to localStorage on state change
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
    } catch {
      // Ignore localStorage errors
    }
  }, [state]);

  // dispatch is stable from useReducer, no need to memoize
  return (
    <AppStateContext.Provider value={state}>
      <AppDispatchContext.Provider value={dispatch}>
        {children}
      </AppDispatchContext.Provider>
    </AppStateContext.Provider>
  );
}

AppProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

// ============================================================================
// SELECTOR HOOKS - For optimized re-renders
// ============================================================================
export function useSetupComplete() {
  const { isSetupComplete } = useAppState();
  return isSetupComplete;
}

export function useTargetConfig() {
  const { targetUrl, token, isAstro } = useAppState();
  return { targetUrl, token, isAstro };
}
