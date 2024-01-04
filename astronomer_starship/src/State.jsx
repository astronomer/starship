import qs from 'qs';
import { getTargetUrlFromParts } from './util';

export function getHashState() {
  const o = qs.parse(window.parent.location.search.substring(1));
  return o?.s ? JSON.parse(atob(o.s)) : {};
}

export function setHashState(state, history) {
  const o = qs.parse(window.parent.location.search.substring(1));
  o.s = btoa(JSON.stringify(state));
  history.push(`?${qs.stringify(o)}`);
}

export function getInitialStateFromUrl(initial) {
  return { ...initial, ...getHashState() };
}

/** Initial state of the application
 * @typedef {Object} State
 * @type {{targetUrl: null}}
 */
export const initialState = {
  tab: 0,
  targetUrl: '',
  isSetupComplete: false,
  isTouched: false,
  isValidUrl: false,
  urlDeploymentPart: '',
  urlOrgPart: '',
  isAstro: true,
  isProductSelected: false,
  isTokenTouched: false,
  token: null,
};

/**
 * Reducer for the application, handles setting state "type" commands
 * @param state
 * @param action
 * @returns State
 */
export const reducer = (state, action) => {
  // console.log(`Reducing state for action ${action}:`);
  // console.log(state);
  switch (action.type) {
    case 'set-url': {
      // TODO - check https://clkvh3b46003m01kbalgwwdcy.astronomer.run/dus78e67/api/v1/health
      return {
        ...state,
        isTouched: true,
        targetUrl: action.targetUrl,
        urlDeploymentPart: action.urlDeploymentPart,
        urlOrgPart: action.urlOrgPart,
        isValidUrl: action.urlOrgPart && action.urlDeploymentPart,
        isSetupComplete: action.urlOrgPart && action.urlDeploymentPart && state.token,
      };
    }
    case 'set-token': {
      return {
        ...state,
        isTokenTouched: true,
        token: action.token,
        isSetupComplete: action.token && state.isValidUrl,
      };
    }
    case 'toggle-is-astro': {
      return {
        ...state,
        isAstro: !state.isAstro,
        isProductSelected: true,
        targetUrl: getTargetUrlFromParts(state.urlOrgPart, state.urlDeploymentPart, !state.isAstro),
        isSetupComplete: false,
      };
    }
    case 'set-is-product-selected': {
      return { ...state, isProductSelected: true };
    }
    case 'set-tab': {
      return { ...state, tab: action.tab };
    }
    case 'reset': {
      return initialState;
    }
    default: {
      return initialState;
    }
  }
};
