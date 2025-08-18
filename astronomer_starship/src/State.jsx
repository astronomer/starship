import qs from 'qs';
// eslint-disable-next-line import/no-extraneous-dependencies
import merge from 'lodash.merge';
import { getTargetUrlFromParts } from './util';

export function getHashState() {
  const o = qs.parse(window.parent.location.search.substring(1));
  return o?.s ? JSON.parse(atob(o.s)) : {};
}

export function getInitialState(initial) {
  return { ...initial, ...JSON.parse(localStorage.getItem('state')), ...getHashState() };
}

/** Initial state of the application
 * @typedef {Object} State
 * @type {{targetUrl: null}}
 */
export const initialState = {
  DEBUG: false,
  // ### SETUP PAGE ####
  targetUrl: '',
  isSetupComplete: false,
  isTouched: false,
  isValidUrl: false,
  urlDeploymentPart: '',
  urlOrgPart: '',
  isAstro: true,
  isStarship: false,
  isAirflow: false,
  airflowVersion: '',
  airflowMajorVersion: 0,
  isProductSelected: false,
  isTokenTouched: false,
  token: null,
  deploymentId: null,
  telescopeOrganizationId: '',
  telescopePresignedUrl: '',

  // Software Specific:
  releaseName: null,
  workspaceId: null,

  // ### VARIABLES PAGE ####
  variablesLocalData: [],
  variablesRemoteData: [],
  variablesLoading: false,
  variablesError: null,
  // ### CONNECTIONS PAGE ####
  connectionsLocalData: [],
  connectionsRemoteData: [],
  connectionsLoading: false,
  connectionsError: null,
  // ### POOLS PAGE ####
  poolsLocalData: [],
  poolsRemoteData: [],
  poolsLoading: false,
  poolsError: null,
  // ### ENV PAGE ####
  envLocalData: [],
  envRemoteData: [],
  envLoading: false,
  envError: null,
  organizationId: null,
  // ### DAGS PAGE ####
  dagsData: {},
  dagsLoading: false,
  dagsError: null,
  limit: 100,
  batchSize: 10,
};

/**
 * Reducer for the application, handles setting state "type" commands
 * @param state
 * @param action
 * @returns State
 */
export const reducer = (state, action) => {
  if (state.DEBUG) {
    // eslint-disable-next-line no-console
    console.log(`Received action=${JSON.stringify(action)}`);
  }
  switch (action.type) {
    // ### SETUP PAGE ####
    case 'set-url': {
      return {
        ...state,
        isTouched: true,
        targetUrl: action.targetUrl,
        urlDeploymentPart: action.urlDeploymentPart,
        urlOrgPart: action.urlOrgPart,
        isValidUrl: action.urlOrgPart && action.urlDeploymentPart,
        isSetupComplete: state.isStarship && state.isAirflow && state.token && action.urlOrgPart && action.urlDeploymentPart,
      };
    }
    case 'set-token': {
      return {
        ...state,
        isTokenTouched: true,
        token: action.token,
        isSetupComplete: state.isStarship && state.isAirflow && action.token && state.isValidUrl,
      };
    }
    case 'toggle-is-astro': {
      return {
        ...state,
        isAstro: !state.isAstro,
        isProductSelected: true,
        targetUrl: getTargetUrlFromParts(state.urlOrgPart, state.urlDeploymentPart, !state.isAstro),
        token: null,
        isSetupComplete: false,
      };
    }
    case 'set-is-product-selected': {
      return { ...state, isProductSelected: true };
    }
    case 'set-is-starship': {
      return {
        ...state,
        isStarship: action.isStarship,
        isSetupComplete: action.isStarship && state.isAirflow && state.token && state.isValidUrl,
      };
    }
    case 'set-is-airflow': {
      return {
        ...state,
        isAirflow: action.isAirflow,
        isSetupComplete: action.isAirflow && state.isStarship && state.token && state.isValidUrl,
      };
    }
    case 'set-airflow-version': {
      return {
        ...state,
        airflowVersion: action.airflowVersion,
        airflowMajorVersion: action.airflowMajorVersion,
      };
    }
    case 'set-software-info': {
      return {
        ...state,
        releaseName: action.releaseName,
        workspaceId: action.workspaceId,
        deploymentId: action.deploymentId,
      };
    }

    // ### Telescope ###
    case 'set-telescope-org': {
      return {
        ...state,
        telescopeOrganizationId: action.telescopeOrganizationId,
      };
    }
    case 'set-telescope-presigned-url': {
      return {
        ...state,
        telescopePresignedUrl: action.telescopePresignedUrl,
      };
    }

    // ### VARIABLES PAGE ####
    case 'set-variables-loading': {
      return {
        ...state,
        variablesLocalData: [],
        variablesRemoteData: [],
        variablesLoading: true,
        variablesError: null,
      };
    }
    case 'set-variables-data': {
      return {
        ...state,
        variablesLocalData: action.variablesLocalData,
        variablesRemoteData: action.variablesRemoteData,
        variablesLoading: false,
      };
    }
    case 'set-variables-error': {
      return action.error.response.status === 401 ? {
        ...state,
        variablesError: action.error,
        variablesLoading: false,
        isSetupComplete: false,
        isTokenTouched: false,
        token: null,
      } : { ...state, variablesError: action.error };
    }

    // ### CONNECTIONS PAGE ####
    case 'set-connections-loading': {
      return {
        ...state,
        connectionsLocalData: [],
        connectionsRemoteData: [],
        connectionsError: null,
        connectionsLoading: true,
      };
    }
    case 'set-connections-data': {
      return {
        ...state,
        connectionsLocalData: action.connectionsLocalData,
        connectionsRemoteData: action.connectionsRemoteData,
        connectionsLoading: false,
      };
    }
    case 'set-connections-error': {
      return action.error.response.status === 401 ? {
        ...state,
        connectionsError: action.error,
        connectionsLoading: false,
        isSetupComplete: false,
        isTokenTouched: false,
        token: null,
      } : { ...state, connectionsError: action.error };
    }

    // ### POOLS PAGE ####
    case 'set-pools-loading': {
      return {
        ...state,
        poolsLocalData: [],
        poolsRemoteData: [],
        poolsLoading: true,
        poolsError: null,
      };
    }
    case 'set-pools-data': {
      return {
        ...state,
        poolsLocalData: action.poolsLocalData,
        poolsRemoteData: action.poolsRemoteData,
        poolsLoading: false,
      };
    }
    case 'set-pools-error': {
      return action.error.response.status === 401 ? {
        ...state,
        poolsError: action.error,
        poolsLoading: false,
        isSetupComplete: false,
        isTokenTouched: false,
        token: null,
      } : { ...state, poolsError: action.error };
    }

    // ### ENV PAGE ####
    case 'set-env-loading': {
      return {
        ...state,
        envLocalData: [],
        envRemoteData: [],
        envLoading: true,
        envError: null,
      };
    }
    case 'set-env-data': {
      return {
        ...state,
        envLocalData: action.envLocalData,
        envRemoteData: action.envRemoteData,
        organizationId: action.envRemoteData['ASTRO_ORGANIZATION_ID'] || state.organizationId,
        deploymentId: action.envRemoteData['ASTRO_DEPLOYMENT_ID'] || state.deploymentId,
        envLoading: false,
      };
    }
    case 'set-env-error': {
      return action.error.response.status === 401 ? {
        ...state,
        envError: action.error,
        envLoading: false,
        isSetupComplete: false,
        isTokenTouched: false,
        token: null,
      } : { ...state, envError: action.error };
    }

    // ### DAG PAGE ####
    case 'set-dags-loading': {
      return {
        ...state,
        dagsData: {},
        dagsLoading: true,
        dagsError: null,
      };
    }
    case 'set-dags-data': {
      return {
        ...state,
        dagsData: merge(state.dagsData, action.dagsData),
        dagsLoading: false,
      };
    }
    case 'set-dags-error': {
      return action.error.response.status === 401 ? {
        ...state,
        dagsError: action.error,
        dagsLoading: false,
        isSetupComplete: false,
        isTokenTouched: false,
        token: null,
      } : { ...state, dagsError: action.error };
    }
    case 'set-limit': {
      return {
        ...state,
        limit: action.limit,
      };
    }
    case 'set-batch-size': {
      return {
        ...state,
        batchSize: action.batchSize,
      };
    }

    // ### GENERAL ####
    case 'reset': {
      return initialState;
    }
    default: {
      // eslint-disable-next-line no-console
      console.log(`Received unknown action.type=${action.type}`);
      return state;
    }
  }
};
