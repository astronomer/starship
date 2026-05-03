import { createContext, useContext, useReducer, useEffect, useMemo } from 'react';
import PropTypes from 'prop-types';
import { getTargetUrlFromParts } from './util';

// INITIAL STATE - Only setup/configuration state, NOT page data

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

  // Local Airflow info
  localAirflowVersion: null,

  // -------------------------------------------------------------------
  // Source Airflow configuration (Cutover Tool)
  // Optional. When complete, enables the Cutover tabs.
  // -------------------------------------------------------------------
  sourcePlatform: null, // 'astro' | 'mwaa' | 'gcc' | 'oss'
  // Airflow Connection id where the credentials will be stored. Users can
  // rename this if they need multiple source connections side-by-side.
  sourceConnId: 'starship_source',
  sourceUrl: '',
  sourceToken: null,
  sourceLogin: null,
  sourcePassword: null,
  // Platform-specific
  sourceImpersonationChain: '', // gcc - newline-separated textarea value
  sourceRegion: '', // mwaa
  sourceRoleArn: '', // mwaa
  sourceEnvironmentName: '', // mwaa
  // Validation
  sourceIsTouched: false,
  sourceCredsTouched: false,
  sourceIsValidUrl: false,
  sourceIsAirflow: false,
  sourceIsStarship: false,
  sourceConnectionSaved: false,
  isSourceSetupComplete: false,
};

function isValidHttpUrl(url) {
  if (!url || typeof url !== 'string') return false;
  try {
    const u = new URL(url);
    return (u.protocol === 'http:' || u.protocol === 'https:') && !!u.hostname;
  } catch {
    return false;
  }
}

function hasSourceCreds(state, overrides = {}) {
  const merged = { ...state, ...overrides };
  switch (merged.sourcePlatform) {
    case 'astro':
      return !!merged.sourceToken;
    case 'oss':
      return !!merged.sourceToken || !!(merged.sourceLogin && merged.sourcePassword);
    case 'gcc':
      return true; // ADC: no UI-side creds required
    case 'mwaa':
      return !!merged.sourceRegion;
    default:
      return false;
  }
}

function calculateIsSourceSetupComplete(state, overrides = {}) {
  const merged = { ...state, ...overrides };
  return !!(
    merged.sourcePlatform &&
    merged.sourceIsValidUrl &&
    merged.sourceIsAirflow &&
    merged.sourceIsStarship &&
    merged.sourceConnectionSaved &&
    hasSourceCreds(merged)
  );
}

// REDUCER

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
    case 'set-local-airflow-version':
      return { ...state, localAirflowVersion: action.version };
    case 'set-source-conn-id': {
      // Changing the source conn_id invalidates "is this connection saved?"
      // since the saved record might live under the old id.
      const next = {
        ...state,
        sourceConnId: action.connId,
        sourceConnectionSaved: false,
      };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'set-source-platform': {
      // Changing platform invalidates creds and saved-connection state.
      const next = {
        ...state,
        sourcePlatform: action.platform,
        sourceToken: null,
        sourceLogin: null,
        sourcePassword: null,
        sourceImpersonationChain: '',
        sourceRegion: '',
        sourceRoleArn: '',
        sourceEnvironmentName: '',
        sourceCredsTouched: false,
        sourceIsAirflow: false,
        sourceIsStarship: false,
        sourceConnectionSaved: false,
      };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'set-source-url': {
      const sourceIsValidUrl = isValidHttpUrl(action.sourceUrl);
      // URL change invalidates saved connection + connectivity checks.
      const next = {
        ...state,
        sourceIsTouched: true,
        sourceUrl: action.sourceUrl,
        sourceIsValidUrl,
        sourceIsAirflow: false,
        sourceIsStarship: false,
        sourceConnectionSaved: false,
      };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'set-source-creds': {
      // Partial update of whichever credential fields the active platform uses.
      const next = {
        ...state,
        sourceCredsTouched: true,
        ...('token' in action ? { sourceToken: action.token } : {}),
        ...('login' in action ? { sourceLogin: action.login } : {}),
        ...('password' in action ? { sourcePassword: action.password } : {}),
        ...('impersonationChain' in action ? { sourceImpersonationChain: action.impersonationChain } : {}),
        ...('region' in action ? { sourceRegion: action.region } : {}),
        ...('roleArn' in action ? { sourceRoleArn: action.roleArn } : {}),
        ...('environmentName' in action ? { sourceEnvironmentName: action.environmentName } : {}),
        // Any cred change invalidates connectivity + saved state.
        sourceIsAirflow: false,
        sourceIsStarship: false,
        sourceConnectionSaved: false,
      };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'set-source-is-airflow': {
      const next = { ...state, sourceIsAirflow: action.isAirflow };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'set-source-is-starship': {
      const next = { ...state, sourceIsStarship: action.isStarship };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'set-source-connection-saved': {
      const next = { ...state, sourceConnectionSaved: action.saved };
      return { ...next, isSourceSetupComplete: calculateIsSourceSetupComplete(next) };
    }
    case 'reset-target': {
      // Wipe target-only state. Source config is preserved so Cutover users
      // don't lose their source setup when they re-run Target Setup.
      const {
        sourcePlatform,
        sourceConnId,
        sourceUrl,
        sourceToken,
        sourceLogin,
        sourcePassword,
        sourceImpersonationChain,
        sourceRegion,
        sourceRoleArn,
        sourceEnvironmentName,
        sourceIsTouched,
        sourceCredsTouched,
        sourceIsValidUrl,
        sourceIsAirflow,
        sourceIsStarship,
        sourceConnectionSaved,
        isSourceSetupComplete,
      } = state;
      return {
        ...initialState,
        sourcePlatform,
        sourceConnId,
        sourceUrl,
        sourceToken,
        sourceLogin,
        sourcePassword,
        sourceImpersonationChain,
        sourceRegion,
        sourceRoleArn,
        sourceEnvironmentName,
        sourceIsTouched,
        sourceCredsTouched,
        sourceIsValidUrl,
        sourceIsAirflow,
        sourceIsStarship,
        sourceConnectionSaved,
        isSourceSetupComplete,
      };
    }
    case 'reset-source':
      return {
        ...state,
        sourcePlatform: null,
        sourceConnId: 'starship_source',
        sourceUrl: '',
        sourceToken: null,
        sourceLogin: null,
        sourcePassword: null,
        sourceImpersonationChain: '',
        sourceRegion: '',
        sourceRoleArn: '',
        sourceEnvironmentName: '',
        sourceIsTouched: false,
        sourceCredsTouched: false,
        sourceIsValidUrl: false,
        sourceIsAirflow: false,
        sourceIsStarship: false,
        sourceConnectionSaved: false,
        isSourceSetupComplete: false,
      };
    case 'invalidate-token':
      return {
        ...state,
        isSetupComplete: false,
        isTokenTouched: false,
        token: null,
      };
    default:
      return state;
  }
}

// CONTEXT

const AppStateContext = createContext(undefined);
const AppDispatchContext = createContext(undefined);

// HOOKS

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

// PROVIDER COMPONENT

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
      <AppDispatchContext.Provider value={dispatch}>{children}</AppDispatchContext.Provider>
    </AppStateContext.Provider>
  );
}

AppProvider.propTypes = {
  children: PropTypes.node.isRequired,
};

// SELECTOR HOOKS - For optimized re-renders
// These hooks help components subscribe to only the state they need

export function useSetupComplete() {
  const { isSetupComplete } = useAppState();
  return isSetupComplete;
}

export function useSourceSetupComplete() {
  const { isSourceSetupComplete } = useAppState();
  return isSourceSetupComplete;
}

export function useSourceHasCreds() {
  const state = useAppState();
  return hasSourceCreds(state);
}

export function useSourceConfig() {
  const state = useAppState();
  return useMemo(
    () => ({
      url: state.sourceUrl,
      platform: state.sourcePlatform,
      connId: state.sourceConnId,
    }),
    [state.sourceUrl, state.sourcePlatform, state.sourceConnId],
  );
}

export function useTargetConfig() {
  const state = useAppState();
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

export function useDagHistoryConfig() {
  const state = useAppState();
  return useMemo(
    () => ({
      limit: state.limit,
      batchSize: state.batchSize,
    }),
    [state.limit, state.batchSize],
  );
}

export function useTelescopeConfig() {
  const state = useAppState();
  return useMemo(
    () => ({
      telescopeOrganizationId: state.telescopeOrganizationId,
      telescopePresignedUrl: state.telescopePresignedUrl,
    }),
    [state.telescopeOrganizationId, state.telescopePresignedUrl],
  );
}
