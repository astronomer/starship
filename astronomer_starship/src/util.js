import axios from 'axios';

/**
 * Returns the PAT URL for a given Airflow URL
 * for Astro that's https://cloud.astronomer.io/token
 * for Software (like https://deployments.basedomain.com/airflow/...) it's https://basedomain.com/token
 * @param targetUrl
 * @returns {string}
 *
 * > tokenUrlFromAirflowUrl('https://cloud.astronomer.io/...')
 * 'https://cloud.astronomer.io/token'
 */
export function tokenUrlFromAirflowUrl(targetUrl) {
  // Software
  if (!targetUrl.includes('astronomer.run')) {
    const urlBody = targetUrl.split('://')[1];
    if (urlBody) {
      const url = urlBody.split('/', 1)[0] || urlBody;
      const basedomain = url.split('deployments.', 2)[1] || url;
      return `https://${basedomain}/token`;
    }
  }
  // Astro
  return 'https://cloud.astronomer.io/token';
}

/**
 * Returns the target URL from the URL parts for Astro or Software
 * @param urlOrgPart - the org part for astro, or basedomain for software
 * @param urlDeploymentPart - the deployment hash for astro or space-name-1234 for software
 * @param isAstro - bool for whether it's astro
 * @returns {string} - url
 */
export function getTargetUrlFromParts(urlOrgPart, urlDeploymentPart, isAstro) {
  return isAstro
    ? `https://${urlOrgPart}.astronomer.run/${urlDeploymentPart}`
    : `https://deployments.${urlOrgPart}/${urlDeploymentPart}/airflow`;
}

/**
 * Returns the local URL for a given route by splitting at 'starship
 * @param route
 @returns {string}
 */
export function localRoute(route) {
  const localUrl = window.location.href.split('/starship', 1)[0];
  return localUrl + route;
}

/**
 * Returns the remote URL for a given route by combining the target URL and route
 * @param targetUrl
 * @param route
 @returns {string}
 */
export function remoteRoute(targetUrl, route) {
  return targetUrl + route;
}

/**
 * Returns the local proxy URL for a given URL (to avoid CORS issues)
 * @param url
 * @returns {string}
 */
export function proxyUrl(url) {
  return localRoute(`/starship/proxy?url=${encodeURIComponent(url)}`);
}
/**
 * Returns the headers for the proxy (to avoid CORS issues)
 * @param token
 * @returns {{STARSHIP_PROXY_TOKEN}}
 */
export function proxyHeaders(token) {
  return {
    'Starship-Proxy-Token': token,
  };
}

/**
 * Fetches data from both the local and remote endpoints
 * @param localRouteUrl
 * @param remoteRouteUrl
 * @param token
 * @param loadingDispatch - a dispatch route to call to set the loading variable
 * @param dataDispatch - dispatch route to call to set the data variables
 * @param errorDispatch - dispatch route to call to set the error variable
 */
export async function fetchData(localRouteUrl, remoteRouteUrl, token, loadingDispatch, dataDispatch, errorDispatch) {
  if (loadingDispatch) {
    loadingDispatch();
  }

  try {
    const [localRes, remoteRes] = await Promise.all([
      axios.get(localRouteUrl),
      axios.get(proxyUrl(remoteRouteUrl), { headers: proxyHeaders(token) }),
    ]);

    if (
      localRes.status === 200
      && localRes.headers['content-type'] === 'application/json'
      && remoteRes.status === 200
      && remoteRes.headers['content-type'] === 'application/json'
    ) {
      dataDispatch(localRes, remoteRes);
    } else {
      errorDispatch(new Error('Invalid response: expected JSON content-type'));
    }
  } catch (err) {
    errorDispatch(err);
  }
}

export function objectWithoutKey(object, key) {
  const { [key]: _, ...otherKeys } = object;
  return otherKeys;
}

/**
 * Constructs and returns the URL for the Astro Deployment Environment Variable API route
 *
 * @param {string} organizationId
 * @param {string} deploymentId
 * @returns {string} - The URL for the Astro Environment Variable service.
 */
export function getAstroEnvVarRoute(organizationId, deploymentId) {
  return `https://api.astronomer.io/platform/v1beta1/organizations/${organizationId}/deployments/${deploymentId}`;
}

/**
 * Constructs and returns the URL for a Houston API
 *
 * @param {string} basedomain
 * @returns {string} - The URL for the Houston service.
 */
export function getHoustonRoute(basedomain) {
  return `https://houston.${basedomain}/v1/`;
}
